import pickle
from datetime import datetime
from typing import List, Tuple

import lmdb
from application.server.main.logger import get_logger
from config import DB_MODEL_VERSION_INITIAL_VALUE, DB_HARVESTING_DATE_COLUMN_NAME
from config.logger_config import LOGGER_LEVEL
from config.path_config import COMPRESSION_EXT, PUBLICATION_PREFIX, PUBLICATION_EXT
from domain.ovh_path import OvhPath
from domain.processed_entry import ProcessedEntry
from harvester.OAHarvester import generateStoragePath
from sqlalchemy import text
from sqlalchemy.engine import Engine
from config.processing_service_namespaces import grobid_ns, softcite_ns, datastet_ns

logger = get_logger(__name__, level=LOGGER_LEVEL)


class DBHandler:
    def __init__(self, engine: Engine, table_name: str, swift_handler):
        self.table_name: str = table_name
        self.engine: Engine = engine
        self.swift_handler = swift_handler
        self.config = swift_handler.config

    def fetch_all(self):
        """Return the table content"""
        result = self.engine.execute(f'SELECT * FROM {self.table_name}')
        return [ProcessedEntry(*entry) for entry in result.fetchall()]

    def count(self):
        """Return the number of rows in the table"""
        # (X,) rows
        return self.engine.execute(f'SELECT count(*) FROM {self.table_name}').fetchone()[0]

    def select_all_where_uuids(self, uuids: List[str]):
        """Return the table content where uuids match"""
        if uuids:
            result = self.engine.execute(
                f"SELECT * FROM {self.table_name} WHERE uuid IN (" + ", ".join([f"'{uuid}'" for uuid in uuids]) + ")")
            return [ProcessedEntry(*entry) for entry in result.fetchall()]
        return []

    def write_entity_batch(self, records: List):
        cur = self.engine.raw_connection().cursor()
        args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode("utf-8") for x in records)
        with self.engine.connect() as connection:
            try:
                statement = f"""
                    INSERT INTO {self.table_name} (
                    doi, uuid, is_harvested, softcite_version, grobid_version, harvester_used, domain, url_used,
                    {DB_HARVESTING_DATE_COLUMN_NAME}, datastet_version
                    )
                    VALUES {args_str}
                    ON CONFLICT (uuid) DO UPDATE
                        SET datastet_version = excluded.datastet_version,
                            softcite_version = excluded.softcite_version,
                            grobid_version = excluded.grobid_version;
                """
                connection.execute(text(statement))
            except Exception:
                logger.error('Writing to postgres error : ', exc_info=True)

    def _get_uuid_from_path(self, path: str) -> str:
        end_path = path.split('/')[-1]
        uuid = end_path.split('.')[0]
        return uuid

    def _get_lmdb_content_str(self, lmdb_path, map_size):
        env = lmdb.open(lmdb_path, map_size=map_size)
        content_str = []
        with env.begin().cursor() as cur:
            for doi, uuid in cur:
                content_str.append((doi.decode('utf-8'), uuid.decode('utf-8')))
        return content_str

    def _get_lmdb_content_pickle(self, lmdb_path, map_size):
        env = lmdb.open(lmdb_path, map_size=map_size)
        dict_content = {}
        with env.begin().cursor() as cur:
            for k, v in cur:
                dict_content[k.decode('utf-8')] = pickle.loads(v)
        return dict_content

    def update_database_processing(self, entries: List[Tuple[str, str, str]]):
        """Update database with the version of the service (grobid, softcite, dataseer) used to process the publication
        entries = [(publication_uuid, service_used, version_used), ...]
        """
        if entries:
            publications_processed = {}
            get_uuid = lambda x: x[0]
            db_entries = self.select_all_where_uuids([get_uuid(x) for x in entries])
            for entry in entries:
                publication_uuid, service_used, version_used = entry
                if publication_uuid not in publications_processed:
                    publications_processed[publication_uuid] = next(db_entry for db_entry in db_entries
                                                                    if db_entry.uuid == publication_uuid)
                if service_used == grobid_ns.service_name:
                    publications_processed[publication_uuid].grobid_version = version_used
                elif service_used == softcite_ns.service_name:
                    publications_processed[publication_uuid].softcite_version = version_used
                elif service_used == datastet_ns.service_name:
                    publications_processed[publication_uuid].datastet_version = version_used
            records = list(publications_processed.values())
            if records:
                self.write_entity_batch(records)

    def update_database(self):
        container = self.config['publications_dump']
        lmdb_size = self.config['lmdb_size_Go'] * 1024 * 1024 * 1024
        extension_publication: str = PUBLICATION_EXT + COMPRESSION_EXT

        # get uuid entry content (harvester_used and domain are in it)
        dict_local_uuid_entries = self._get_lmdb_content_pickle('data/entries', lmdb_size)

        # Important : lmdb_reset must be reset
        local_doi_uuid = self._get_lmdb_content_str('data/doi', lmdb_size)
        doi_uuid_uploaded: list = []

        for content_tuple in local_doi_uuid:
            full_path_expected_in_remote: str = str(OvhPath(PUBLICATION_PREFIX, generateStoragePath(content_tuple[1]),
                                                            content_tuple[1] + extension_publication))
            publications_harvested_by_path: list = self.swift_handler.get_swift_list(container,
                                                                                     dir_name=full_path_expected_in_remote)
            if len(publications_harvested_by_path) > 0:
                doi_uuid_uploaded.append(content_tuple)

        records = [ProcessedEntry(*entry,
                                  "1",
                                  DB_MODEL_VERSION_INITIAL_VALUE,
                                  DB_MODEL_VERSION_INITIAL_VALUE,
                                  dict_local_uuid_entries[entry[1]]['harvester_used'],
                                  dict_local_uuid_entries[entry[1]]['domain'],
                                  dict_local_uuid_entries[entry[1]]['url_used'],
                                  datetime.now(),
                                  DB_MODEL_VERSION_INITIAL_VALUE
                                  ) for entry in doi_uuid_uploaded]

        if records:
            self.write_entity_batch(records)
