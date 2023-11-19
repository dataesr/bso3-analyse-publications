import gzip
import json
import logging
import os
import pickle
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from multiprocessing import cpu_count

import lmdb
import urllib3

from application.server.main.logger import get_logger
from config.harvest_strategy_config import oa_harvesting_strategy
from config.logger_config import LOGGER_LEVEL
from config.path_config import COMPRESSION_EXT, DATA_PATH, METADATA_PREFIX, METADATA_EXT, PUBLICATION_PREFIX, PUBLICATION_EXT
from domain.ovh_path import OvhPath
from harvester.download_publication_utils import _download_publication, SUCCESS_DOWNLOAD
from infrastructure.storage import swift
from utils.file import is_valid_pdf, compress

logger = get_logger(__name__, level=LOGGER_LEVEL)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.getLogger("keystoneclient").setLevel(logging.ERROR)
logging.getLogger("swiftclient").setLevel(logging.ERROR)

NB_THREADS = cpu_count()

"""
Harvester for PDF available in open access. a LMDB index is used to keep track of the harvesting process and
possible failures.

This version uses the standard ThreadPoolExecutor for parallelizing the download/processing/upload processes.
Given the limits of ThreadPoolExecutor (input stored in memory, blocking Executor.map until the whole input
is processed and output stored in memory until all input is consumed), it works with batches of PDF of a size
indicated in the config.json file (default is 100 entries). We are moving from first batch to the second one
only when the first is entirely processed. The harvesting process is not CPU bounded so using threads is okay.
"""


class Continue(Exception):
    pass


def calculate_pct(i, count):
    try:
        return int(i // (count / 100))
    except ZeroDivisionError:
        return 0


class OAHarvester:
    def __init__(self, config, wiley_client, elsevier_client):
        self.config = config
        self.env = None  # standard lmdb env for storing biblio entries by uuid
        self.env_doi = None  # lmdb env for storing mapping between doi/pmcid and uuid
        self.env_fail = None  # lmdb env for keeping track of failures
        self._init_lmdb()  # init db
        self.wiley_client = wiley_client
        self.elsevier_client = elsevier_client
        self.swift = None
        self.batch_size = self.config["batch_size"]

        # ovh storage metadata input dump and output publications dump
        self.storage_publications = config["publications_dump"]

        # condition
        is_swift_config = ("swift" in self.config) and len(self.config["swift"]) > 0
        if is_swift_config:
            self.swift = swift.Swift(self.config)

    def _init_lmdb(self):
        # create the data path if it does not exist
        if not os.path.isdir(DATA_PATH):
            try:
                os.makedirs(DATA_PATH)
            except OSError:
                logger.exception("Creation of the directory %s failed" % DATA_PATH)
            else:
                logger.debug("Successfully created the directory %s" % DATA_PATH)

        lmdb_size = self.config["lmdb_size_Go"] * 1024 * 1024 * 1024
        # open in write mode
        envFilePath = os.path.join(DATA_PATH, "entries")
        self.env = lmdb.open(envFilePath, map_size=lmdb_size)

        envFilePath = os.path.join(DATA_PATH, "doi")
        self.env_doi = lmdb.open(envFilePath, map_size=lmdb_size)

        envFilePath = os.path.join(DATA_PATH, "fail")
        self.env_fail = lmdb.open(envFilePath, map_size=lmdb_size)

    def harvestUnpaywall(self, filepath, count, reprocess=True, destination_dir=""):
        """
        Main method, use the Unpaywall dataset for getting pdf url for Open Access resources,
        download in parallel PDF, generate thumbnails (if selected), upload resources locally
        or on OVH and update the json description of the entries
        """
        batch_size_pdf = self.config.get("batch_size", 100)
        logger.debug(f'{count} entries')
        batch_gen = self._get_batch_generator(
            filepath, count, reprocess, batch_size_pdf)
        for batch in batch_gen:
            urls = [e[0] for e in batch]
            entries = [e[1] for e in batch]
            filenames = [e[2] for e in batch]
            self.processBatch(urls, filenames, entries, destination_dir)

    def _process_entry(self, entry, reprocess):
        doi = entry["doi"]
        _check_entry(entry, doi, self.getUUIDByIdentifier, reprocess, self.env, self.env_doi)
        url, entry, filename = self._parse_entry(entry)
        return url, entry, filename

    def _parse_entry(self, entry):
        """Parse entry to get url, entry, filename"""
        latest_observation = get_latest_publication(entry)
        if latest_observation["is_oa"]:
            urls_for_pdf = {}
            oa_locations = {}
            for oa_location in latest_observation["oa_locations"]:
                if ("url_for_pdf" in oa_location) and oa_location["url_for_pdf"]:
                    for i, strategy in enumerate(oa_harvesting_strategy):
                        if strategy(oa_location):
                            update_dict(urls_for_pdf, i, oa_location["url_for_pdf"])
                            update_dict(oa_locations, i, oa_location)
                            break
            urls_for_pdf = [
                url for url_list_idx in sorted(urls_for_pdf) for url in urls_for_pdf[url_list_idx]
            ]
            oa_locations = [
                oa_location
                for oa_locations_idx in sorted(oa_locations)
                for oa_location in oa_locations[oa_locations_idx]
            ]
            if urls_for_pdf and oa_locations:
                return (
                    urls_for_pdf,
                    {
                        "id": entry["id"],
                        "doi": entry["doi"],
                        "domain": entry.get("bso_classification", "unknown"),
                        "oa_locations": oa_locations,
                    },
                    os.path.join(DATA_PATH, entry["id"] + PUBLICATION_EXT),
                )
        #else:  # closed access (via publishers APIs)
        # returns urls, entry, filename to match signature even though we really only care about the doi since we use publishers APIs.
        if entry.get("publisher_normalized") == "Wiley":
            return (
                [f"https://onlinelibrary.wiley.com/doi/pdfdirect/{entry['doi']}"],
                {"id": entry["id"], "doi": entry["doi"], "domain": entry["bso_classification"]},
                os.path.join(DATA_PATH, entry["id"] + PUBLICATION_EXT),
            )
        elif entry.get("publisher_normalized") == "Elsevier":
            return (
                [f"https://api.elsevier.com/content/article/doi/{entry['doi']}"],
                {"id": entry["id"], "doi": entry["doi"], "domain": entry["bso_classification"]},
                os.path.join(DATA_PATH, entry["id"] + PUBLICATION_EXT),
            )
        return ([], None, None)

    def _get_batch_generator(self, filepath, count, reprocess, batch_size=100):
        """Reads gzip file and returns batches of processed entries"""
        batch = []
        with gzip.open(filepath, "rt", encoding="utf-8") as gz:
            curr = 0
            for i, line in enumerate(gz):
                if calculate_pct(i, count) != curr:
                    curr = calculate_pct(i, count)
                    logger.info(f"{curr}%")
                url, entry, filename = self._process_entry(json.loads(line), reprocess)
                if url:
                    batch.append([url, entry, filename])

                if (len(batch) != 0) and (len(batch) % batch_size == 0):
                    yield batch
                    batch = []
            yield batch

    def processBatch(self, urls, filenames, entries, destination_dir=""):
        logger.debug("Processing batch")
        with ThreadPoolExecutor(max_workers=NB_THREADS) as executor:
            results = executor.map(
                _download_publication,
                urls,
                filenames,
                entries,
                [self.wiley_client] * len(entries),
                [self.elsevier_client] * len(entries),
                timeout=30,
            )
        # LMDB updates are out of the parallel process because LMDB write transaction must
        # be performed in the thread that created the transaction. It is supposed to be fast
        # but it might cause a bottleneck if the data recorded is to big
        entries = []
        for result, local_entry in results:
            logger.debug(
                f'Validating the file of the publication with doi = {local_entry["doi"]},'
                f'result = {result}, harvester used = {local_entry["harvester_used"]}'
            )
            valid_file = False
            local_filename = os.path.join(DATA_PATH, local_entry["id"])
            # TODO: if result = 'fail' no need to do the check?
            if is_valid_pdf(local_filename + PUBLICATION_EXT):
                valid_file = True
                local_entry["valid_fulltext_pdf"] = True
                logger.debug('Is Valid!')

            # Done wether we succeed or not in biblio-glutton-harvester so we keep it that way
            write_in_lmdb(env=self.env, key=local_entry["id"], value=_create_map_entry(local_entry))

            if result == SUCCESS_DOWNLOAD and valid_file:
                entries.append(local_entry)
            else:
                write_in_lmdb(
                    env=self.env_fail,
                    key=local_entry["id"],
                    value={"result": result, "url": local_entry.get("url_for_pdf", "no url")},
                )
                clean_empty_file_if_it_exists(local_filename)

        # LMDB update done so we parallelize the thumbnail/upload/file cleaning steps
        destination_dirs = len(entries) * [destination_dir]
        with ThreadPoolExecutor(max_workers=NB_THREADS) as executor:
            results = executor.map(self.manageFiles, entries, destination_dirs, timeout=30)

    def getUUIDByIdentifier(self, identifier):
        with self.env_doi.begin() as txn:
            return txn.get(identifier.encode(encoding="UTF-8"))

    def _compress_files(
        self,
        local_filename,
        local_filename_json,
        local_entry_id,
        compression_suffix=COMPRESSION_EXT,
        **kwargs,
    ):
        try:
            if os.path.isfile(local_filename):
                compress(local_filename)
                os.remove(local_filename)
                local_filename += compression_suffix

            if os.path.isfile(local_filename_json):
                compress(local_filename_json)
                os.remove(local_filename_json)
                local_filename_json += compression_suffix
        except:
            logger.error("Error compressing resource files for " + local_entry_id)

    def _write_metadata_file(self, local_filename_json, local_entry, **kwargs):
        with open(local_filename_json, "w") as outfile:
            json.dump(local_entry, outfile)

    def _upload_files(self, dest_path: OvhPath, local_filename, local_filename_json, **kwargs):
        """Uploads all the resources associated to the entry to SWIFT object storage"""
        logger.debug(f'try to upload dest_path {dest_path} local_filename {local_filename} local_filename_json {local_filename_json}')
        try:
            files_to_upload = []
            if os.path.isfile(local_filename):
                files_to_upload.append(
                    (
                        local_filename,
                        OvhPath(PUBLICATION_PREFIX, dest_path, os.path.basename(local_filename)),
                    )
                )
            if os.path.isfile(local_filename_json):
                files_to_upload.append(
                    (
                        local_filename_json,
                        OvhPath(METADATA_PREFIX, dest_path, os.path.basename(local_filename_json)),
                    )
                )
            if len(files_to_upload) > 0:
                self.swift.upload_files_to_swift(self.storage_publications, files_to_upload)
        except Exception:
            logger.exception("Error when uploading", exc_info=True)

    def _save_files_locally(
        self,
        dest_path: OvhPath,
        local_filename,
        local_entry_id,
        local_filename_json,
        compression_suffix,
        **kwargs,
    ):
        try:
            local_dest_path = os.path.join(DATA_PATH, dest_path.to_local())
            os.makedirs(local_dest_path, exist_ok=True)
            if os.path.isfile(local_filename):
                shutil.copyfile(
                    local_filename,
                    os.path.join(local_dest_path, local_entry_id + PUBLICATION_EXT + compression_suffix),
                )
            if os.path.isfile(local_filename_json):
                shutil.copyfile(
                    local_filename_json,
                    os.path.join(local_dest_path, local_entry_id + METADATA_EXT + compression_suffix),
                )
        except IOError:
            logger.exception("Invalid path")

    def _clean_up_files(self, local_filename, local_filename_json, **kwargs):
        try:
            if os.path.isfile(local_filename):
                os.remove(local_filename)
            if os.path.isfile(local_filename_json):
                os.remove(local_filename_json)
        except IOError:
            logger.exception("Temporary file cleaning failed")

    def manageFiles(self, local_entry, destination_dir=""):
        data_path = os.path.join(DATA_PATH, destination_dir)
        filepaths: dict = {
            "dest_path": OvhPath(destination_dir, generateStoragePath(local_entry["id"])),
            "local_filename": os.path.join(data_path, local_entry["id"] + PUBLICATION_EXT),
            "local_filename_json": os.path.join(data_path, local_entry["id"] + METADATA_EXT),
        }
        self._write_metadata_file(filepaths["local_filename_json"], local_entry)
        compression_suffix = ""
        if self.config["compression"]:
            compression_suffix = COMPRESSION_EXT
            self._compress_files(
                **filepaths, local_entry_id=local_entry["id"], compression_suffix=compression_suffix
            )
            filepaths = {
                k: (filepaths[k] + compression_suffix if k != "dest_path" else filepaths[k])
                for k in filepaths
            }
        if self.swift:
            self._upload_files(**filepaths)
        else:
            self._save_files_locally(
                **filepaths, local_entry_id=local_entry["id"], compression_suffix=compression_suffix
            )
        self._clean_up_files(**filepaths, local_entry_id=local_entry["id"])

    def diagnostic(self):
        """
        Log a report on failures stored during the harvesting process
        """
        with self.env_fail.begin(write=True) as txn_fail:
            nb_fails = txn_fail.stat()["entries"]
        with self.env.begin(write=True) as txn:
            nb_total = txn.stat()["entries"]
        logger.info(f"number of failed entries with OA link: {nb_fails} out of {nb_total} entries")

    def reset_lmdb(self):
        """
        Remove the local lmdb keeping track of the state of advancement of the harvesting and
        of the failed entries as well as any local publication files
        """
        # close environments
        self.env.close()
        self.env_doi.close()
        self.env_fail.close()

        shutil.rmtree(DATA_PATH)


def write_in_lmdb(env: lmdb.Environment, key: str, value: dict):
    """Write in given lmdb env key value pair, key encoded in UTF-8, value pickled"""
    with env.begin(write=True) as txn:
        txn.put(key.encode(encoding="UTF-8"), pickle.dumps(value))


def clean_empty_file_if_it_exists(local_filename_wo_ext):
    """Remove empty pdf or tar file if present"""
    local_filename = local_filename_wo_ext + PUBLICATION_EXT
    if os.path.isfile(local_filename):
        os.remove(local_filename)
    local_filename = local_filename_wo_ext + ".tar" + COMPRESSION_EXT
    if os.path.isfile(local_filename):
        os.remove(local_filename)


def update_dict(mydict, key, value):
    if key in mydict:
        mydict[key].append(value)
    else:
        mydict[key] = [value]


def _check_entry(entry, _id, getUUID_fn, reprocess, env, env_doi):
    """Check if the entry has already been processed, reprocessed if needed otherwise create an uuid for the new entry"""
    id_candidate = getUUID_fn(_id)
    if id_candidate is not None:
        id_candidate = id_candidate.decode("utf-8")
        if reprocess:
            entry["id"] = id_candidate
            # did we succeed with this entry?
            with env.begin() as txn:
                local_object = txn.get(id_candidate.encode(encoding="UTF-8"))
                if local_object is not None:
                    local_entry = pickle.loads(local_object)
                    if local_entry is not None:
                        if "resources" in local_entry and "pdf" in local_entry["resources"]:
                            # we have a PDF, so no need to reprocess and we skip
                            raise Continue()
            # otherwise we consider the entry for reprocessing
        else:
            # we don't reprocess existing entries
            raise Continue()
    else:
        # store a UUID
        entry["id"] = str(uuid.uuid4())
        with env_doi.begin(write=True) as txn_doi:
            txn_doi.put(entry["doi"].encode(encoding="UTF-8"), entry["id"].encode(encoding="UTF-8"))


def _count_entries(open_fn, filepath):
    """Check the overall number of entries based on the line number"""
    count = 0
    if os.path.exists(filepath):
        with open_fn(filepath, "rb") as fp:
            while 1:
                buffer = fp.read(8192 * 1024)
                if not buffer:
                    break
                count += buffer.count(b"\n")
        return count
    else:
        raise FileNotFoundError(f"{filepath} does not exist")


def get_latest_publication(publication_metadata: dict) -> dict:
    latest_publication_date_sorted_list = list(publication_metadata["oa_details"].keys())
    latest_publication_date_sorted_list.sort()
    latest_publication_date = latest_publication_date_sorted_list[-1]
    return publication_metadata["oa_details"][latest_publication_date]


def _create_map_entry(local_entry):
    """
    Create a simple map JSON from the full metadata entry, to be stored locally and for the dumping the JSONL map file
    """
    map_entry = {}
    map_entry["id"] = local_entry["id"]
    if "doi" in local_entry:
        map_entry["doi"] = local_entry["doi"]
    if "pmid" in local_entry:
        map_entry["pmid"] = local_entry["pmid"]
    if "pmcid" in local_entry:
        map_entry["pmcid"] = local_entry["pmcid"]
    if "istexId" in local_entry:
        map_entry["istexId"] = local_entry["istexId"]
    if "ark" in local_entry:
        map_entry["ark"] = local_entry["ark"]
    if "pii" in local_entry:
        map_entry["pii"] = local_entry["pii"]

    resources = ["json"]

    if "valid_fulltext_pdf" in local_entry and local_entry["valid_fulltext_pdf"]:
        resources.append("pdf")

    if "valid_thumbnails" in local_entry and local_entry["valid_thumbnails"]:
        resources.append("thumbnails")

    map_entry["resources"] = resources
    map_entry["harvested_date"] = date.today().strftime("%Y-%m-%d")
    map_entry["harvester_used"] = local_entry["harvester_used"]
    map_entry["url_used"] = local_entry["url_used"]
    map_entry["domain"] = local_entry["domain"]

    # add target OA link
    if "best_oa_location" in local_entry and "url_for_pdf" in local_entry["best_oa_location"]:
        pdf_url = local_entry["best_oa_location"]["url_for_pdf"]
        if pdf_url is not None:
            map_entry["oa_link"] = pdf_url

    return map_entry


def generateStoragePath(identifier):
    """
    Convert a file name into a path with file prefix as directory paths:
    123456789 -> 12/34/56/78/123456789
    """
    return OvhPath(identifier[:2], identifier[2:4], identifier[4:6], identifier[6:8], identifier)
