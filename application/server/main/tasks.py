import sys
import os
import json
import gzip
from typing import List, Tuple
from infrastructure.database.db_handler import DBHandler
from infrastructure.storage.swift import Swift
from config.harvester_config import config_harvester
from application.server.main.utils import download_object
from harvester.OAHarvester import OAHarvester
from config.db_config import engine

from application.server.main.logger import get_logger
logger = get_logger(__name__)


def create_task_harvest(source_metadata_file, wiley_client, elsevier_client):
    swift_handler = Swift(config_harvester)
    db_handler = DBHandler(engine=engine, table_name='harvested_status_table', swift_handler=swift_handler)
    os.system('mkdir -p /tmp/bso-publications-split')
    download_object(container='bso_dump', filename=source_metadata_file, out=f'/tmp/{source_metadata_file}')
    input_metadata_filename = f'/tmp/{source_metadata_file}'
    filtered_metadata_filename = f'/tmp/{source_metadata_file}_filtered.gz'
    doi_list=[]
    write_partitioned_filtered_metadata_file(db_handler, input_metadata_filename, filtered_metadata_filename, doi_list)
    harvester = OAHarvester(config_harvester, wiley_client, elsevier_client)
    harvester.harvestUnpaywall(filtered_metadata_filename)
    harvester.diagnostic()
    logger.debug(f'{db_handler.count()} rows in database before harvesting')
    db_handler.update_database()
    logger.debug(f'{db_handler.count()} rows in database after harvesting')
    harvester.reset_lmdb()

def write_partitioned_filtered_metadata_file(db_handler: DBHandler,
                                             source_metadata_file: str, filtered_metadata_filename: str,
                                             doi_list: List[str]) -> None:
    
    #doi_already_harvested_list = [entry[0] for entry in db_handler.fetch_all()]
    doi_already_harvested_list = [] #[entry[0] for entry in db_handler.fetch_all()]

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
        entry for entry in filtered_publications_metadata_json_list if (entry.get('doi') not in doi_already_harvested_list) and entry.get('doi') and 'fr' in entry.get('bso_country', [])
    ]
    logger.debug(
        f'Number of publications in the file {filtered_metadata_filename} after filtering: {len(filtered_publications_metadata_json_list)}')
    with gzip.open(filtered_metadata_filename, 'wt') as f_out:
        f_out.write(os.linesep.join([json.dumps(entry) for entry in filtered_publications_metadata_json_list]))
