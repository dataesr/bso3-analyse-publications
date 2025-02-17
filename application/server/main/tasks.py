import sys
import os
import json
import gzip
from typing import List, Tuple
from time import time
from glob import glob
import subprocess
from concurrent.futures import ThreadPoolExecutor
import pickle
import pandas as pd
import requests

from infrastructure.database.db_handler import DBHandler
from infrastructure.storage.swift import Swift
from config.harvester_config import config_harvester
from application.server.main.utils import download_object, upload_object, download_container, delete_object, upload_object_with_destination
from harvester.OAHarvester import OAHarvester
from config.db_config import engine
from ovh_handler import download_files, upload_and_clean_up
from config.processing_service_namespaces import ServiceNamespace, grobid_ns, softcite_ns, datastet_ns
from config.logger_config import LOGGER_LEVEL
from application.server.main.parse_grobid import json_grobid
from application.server.main.parse_datastet import json_datastet
from application.server.main.parse_softcite import json_softcite


from grobid_client.grobid_client import GrobidClient

# la ligne suivante empêche le worker de démarrer ?
#from softdata_mentions_client.client import softdata_mentions_client

from run_grobid import run_grobid
from run_softcite import run_softcite
from run_datastet import run_datastet

from application.server.main.logger import get_logger
logger = get_logger(__name__)

def create_task_harvest_test(source_metadata_file, wiley_client, elsevier_client):
    swift_handler = Swift(config_harvester)
    db_handler = DBHandler(engine=engine, table_name='harvested_status_table', swift_handler=swift_handler)
    filtered_metadata_filename='test_filter.gz'
    download_object(container='misc', filename=filtered_metadata_filename, out=filtered_metadata_filename)
    harvester = OAHarvester(config_harvester, wiley_client, elsevier_client)
    harvester.harvestUnpaywall(filtered_metadata_filename, 1)
    harvester.diagnostic()
    logger.debug(f'{db_handler.count()} rows in database before harvesting')
    db_handler.update_database()
    logger.debug(f'{db_handler.count()} rows in database after harvesting')
    harvester.reset_lmdb()

def create_task_harvest(source_metadata_file, wiley_client, elsevier_client):
    swift_handler = Swift(config_harvester)
    db_handler = DBHandler(engine=engine, table_name='harvested_status_table', swift_handler=swift_handler)
    os.system('mkdir -p /tmp/bso-publications-split')
    download_object(container='bso_dump', filename=source_metadata_file, out=f'/tmp/{source_metadata_file}')
    input_metadata_filename = f'/tmp/{source_metadata_file}'
    filtered_metadata_filename = f'/tmp/{source_metadata_file}_filtered.gz'
    doi_list=[]
    nb_elements = write_partitioned_filtered_metadata_file(db_handler, input_metadata_filename, filtered_metadata_filename, doi_list)
    harvester = OAHarvester(config_harvester, wiley_client, elsevier_client)
    harvester.harvestUnpaywall(filtered_metadata_filename, nb_elements)
    harvester.diagnostic()
    logger.debug(f'{db_handler.count()} rows in database before harvesting')
    db_handler.update_database()
    logger.debug(f'{db_handler.count()} rows in database after harvesting')
    harvester.reset_lmdb()

def write_partitioned_filtered_metadata_file(db_handler: DBHandler,
                                             source_metadata_file: str, filtered_metadata_filename: str,
                                             doi_list: List[str]) -> None:
    
    #doi_already_harvested_list = [entry[0] for entry in db_handler.fetch_all()]
    download_object('misc', 'harvested_doi.csv', 'harvested_doi.csv') 
    doi_already_harvested_list = set(json.load(open('harvested_doi.csv', 'r')))

    with open(source_metadata_file, 'rt') as f_in:
        metadata_input_file_content_list = [json.loads(line) for line in f_in.readlines()]

    # TODO if publication without doi
    # filtered_publications_metadata_json_list = [entry for entry in metadata_input_file_content_list if entry.get('doi') else {'doi': entry['id'], **entry}]
    # instead of:
    filtered_publications_metadata_json_list = metadata_input_file_content_list
    
    #if len(doi_list) > 0:
    #    filtered_publications_metadata_json_list = [
    #        entry for entry in filtered_publications_metadata_json_list if entry.get('doi') in doi_list
    #    ]
    filtered_publications_metadata_json_list = [
      entry for entry in filtered_publications_metadata_json_list if (entry.get('doi')) and (entry.get('doi') not in doi_already_harvested_list) and ('fr' in entry.get('bso_country', []))
    ]
    logger.debug(
        f'Number of publications in the file {filtered_metadata_filename} after filtering: {len(filtered_publications_metadata_json_list)}')
    with gzip.open(filtered_metadata_filename, 'wt') as f_out:
        f_out.write(os.linesep.join([json.dumps(entry) for entry in filtered_publications_metadata_json_list]))
    return len(filtered_publications_metadata_json_list)

def create_task_process(grobid_partition_files, softcite_partition_files, datastet_partition_files):
    logger.debug(f"Call with args: {grobid_partition_files, softcite_partition_files, datastet_partition_files}")
    _swift = Swift(config_harvester)
    db_handler: DBHandler = DBHandler(engine=engine, table_name='harvested_status_table', swift_handler=_swift)

    services_ns = [grobid_ns, softcite_ns, datastet_ns]
    list_partition_files = [grobid_partition_files, softcite_partition_files, datastet_partition_files]
    for service_ns, partition_files in zip(services_ns, list_partition_files):
        download_files(_swift, service_ns.dir, partition_files)

    start_time = time()
    run_processing_services()
    total_time = time()
    logger.info(f"Total runtime: {round(total_time - start_time, 3)}s for {max(map(len, list_partition_files))} files")


    for service_ns in services_ns:
        entries_to_update = compile_records_for_db(service_ns, db_handler)
        upload_and_clean_up(_swift, service_ns)
        db_handler.update_database_processing(entries_to_update)

def run_processing_services():
    from softdata_mentions_client.client import softdata_mentions_client
    """Run parallel calls to the different services when there are files to process"""
    processing_futures = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        if next(iter(glob(grobid_ns.dir + '*')), None):
            processing_futures.append(
                executor.submit(run_grobid, None, grobid_ns.dir, GrobidClient))
        if next(iter(glob(softcite_ns.dir + '*')), None):
            processing_futures.append(
                executor.submit(run_softcite, None, softcite_ns.dir, softdata_mentions_client))
        if next(iter(glob(datastet_ns.dir + '*')), None):
            processing_futures.append(
                executor.submit(run_datastet, None, datastet_ns.dir, softdata_mentions_client))
    for future in processing_futures:
        future.result()

def compile_records_for_db(service_ns: ServiceNamespace, db_handler: DBHandler) -> List[Tuple[str,str,str]]:
    """List the output files of a service to determine what to update in db.
    Returns [(uuid, service, version), ...]"""
    local_files = glob(service_ns.dir + '*')
    service_version = get_service_version(service_ns.suffix, local_files)
    processed_publications = [
        (db_handler._get_uuid_from_path(file), service_ns.service_name, service_version)
        for file in local_files if file.endswith(service_ns.suffix)
    ]
    return processed_publications

def get_service_version(file_suffix: str, local_files: List[str]) -> str:
    """Return the version of the service that produced the files"""
    processed_files = [file for file in local_files if file.endswith(file_suffix)]
    if processed_files:
        if file_suffix == datastet_ns.suffix:
            return get_softdata_version(processed_files[0])
        if file_suffix == softcite_ns.suffix:
            return get_softdata_version(processed_files[0])
        elif file_suffix == grobid_ns.suffix:
            return get_grobid_version()
    else:
        return "0"

def get_softdata_version(softdata_file_path: str) -> str:
    """Get the version of softcite or datastet used by reading from an output file"""
    with open(softdata_file_path, 'r') as f:
        softdata_version = json.load(f)['version']
    return softdata_version


def get_grobid_version() -> str:
    """Get the version of grobid used by a request to the grobid route /api/version"""
    with open(grobid_ns.config_path, 'r') as f:
        config = json.load(f)
    url = f"http://{config['grobid_server']}:{config['grobid_port']}/api/version"
    grobid_version = requests.get(url).text
    return grobid_version

def create_task_collect_results(args):
    volume = '/data'
    container = 'bso3_publications_dump'
    prefix_uid = args.get('prefix_uid', '')
    logger.debug(f'analyze for prefix {prefix_uid}')
    GROBID_VERSIONS = args.get('GROBID_VERSIONS', [])
    SOFTCITE_VERSIONS = args.get('SOFTCITE_VERSIONS', [])
    DATASTET_VERSIONS = args.get('DATASTET_VERSIONS', [])
    if args.get('download', False):
        for fileType in ['metadata', 
                'grobid-0.8.0/publication', 'softcite-0.8.0/publication', 'datastet-0.8.0/publication',
                'grobid-0.8.0-newround/publication', 'softcite-0.8.0-newround/publication', 'datastet-0.8.0-newround/publication',
                ]:
            logger.debug(f'getting {fileType} data')
            download_container(container, f'{fileType}/{prefix_uid}', volume)
    read_all_results(prefix_uid, GROBID_VERSIONS, SOFTCITE_VERSIONS, DATASTET_VERSIONS)

def clean_os(prefix_uid):
    volume = '/data'
    container = 'bso3_publications_dump'
    fr_dois = pickle.load(open('/data/french_dois.pkl', 'rb'))
    nb_files = 0
    nb_files_del = 0
    for root, dirs, files in os.walk(f'{volume}/{container}/metadata/{prefix_uid}'):
        if files:
            for f in files:
                metadata_filename = f'{root}/{f}'
                try:
                    df_metadata = pd.read_json(metadata_filename, lines=True, orient='records')[['doi', 'id', 'domain']]
                    df_metadata.columns = ['doi', 'uid', 'domain']
                    res = df_metadata.to_dict(orient='records')[0]
                    nb_files += 1
                    if res['doi'] not in fr_dois:
                        os_filename = metadata_filename.replace('/data/bso3_publications_dump/', '')
                        pdf_filename = os_filename.replace('metadata/', 'publication/').replace('.json.gz', '.pdf.gz')
                        logger.debug(f"delete {res['doi']} {res['uid']} {metadata_filename}")
                        delete_object(container, os_filename)
                        delete_object(container, pdf_filename)
                        nb_files_del += 1
                        logger.debug(f'file {nb_files_del} deleted, {nb_files} read')
                except:
                    logger.debug(f'error with metadata {metadata_filename}')
                    continue


def read_all_results(prefix_uid, GROBID_VERSIONS, SOFTCITE_VERSIONS, DATASTET_VERSIONS):
    volume = '/data'
    container = 'bso3_publications_dump'
    ix = 0
    all_data = []
    for root, dirs, files in os.walk(f'{volume}/{container}/metadata/{prefix_uid}'):
        if files:
            for f in files:
                metadata_filename = f'{root}/{f}'
                grobid_filenames, softcite_filenames, datastet_filenames   = [], [], []
                for label in ['0.8.0', '0.8.0-newround']:
                    if label == '0.8.0':
                        grobid_filenames.append(root.replace('metadata', f'grobid-{label}/publication') + '/' + f.replace('.json.gz', '.grobid.tei.xml'))
                    elif label == '0.8.0-newround':
                        grobid_filenames.append(root.replace('metadata', f'grobid-{label}/publication') + '/' + f.replace('.json.gz', '.pdf.tei.xml'))
                    softcite_filenames.append(root.replace('metadata', f'softcite-{label}/publication') + '/' + f.replace('.json.gz', '.software.json'))
                    datastet_filenames.append(root.replace('metadata', f'datastet-{label}/publication') + '/' + f.replace('.json.gz', '.dataset.json'))
                try:
                    df_metadata = pd.read_json(metadata_filename, lines=True, orient='records')[['doi', 'id']]
                    df_metadata.columns = ['doi', 'uid']
                    res = df_metadata.to_dict(orient='records')[0]
                    res['sources'] = ['bso3']
                    res['bso3_downloaded'] = True
                except:
                    logger.debug(f'error with metadata {metadata_filename}')
                    continue
                for grobid_filename in grobid_filenames:
                    if os.path.exists(grobid_filename):
                        res.update(json_grobid(grobid_filename, GROBID_VERSIONS))
                        res['bso3_analyzed_grobid'] = True
                for softcite_filename in softcite_filenames:
                    if os.path.exists(softcite_filename):
                        res.update(json_softcite(softcite_filename, SOFTCITE_VERSIONS))
                        res['bso3_analyzed_softcite'] = True
                for datastet_filename in datastet_filenames:
                    if os.path.exists(datastet_filename):
                        res.update(json_datastet(datastet_filename, DATASTET_VERSIONS))
                        res['bso3_analyzed_datastet'] = True
                ix += 1
                all_data.append(res)
                if ix % 1000 == 0:
                    logger.debug(f'{ix} files read')
    result_filename = f'bso3_data_{prefix_uid}.jsonl'
    pd.DataFrame(all_data).to_json(result_filename, lines=True, orient='records')
    upload_object_with_destination(container, result_filename, f'final_for_bso_2025_v2/{result_filename}')
