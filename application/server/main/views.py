import os
import requests
import redis
import json
import string
from typing import Callable, List

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from config import ELSEVIER, WILEY
from infrastructure.database.db_handler import DBHandler
from infrastructure.storage.swift import Swift
from config.harvester_config import config_harvester
from harvester.base_api_client import BaseAPIClient
from harvester.elsevier_client import ElsevierClient
from harvester.exception import FailedRequest
from harvester.wiley_client import WileyClient
from application.server.main.utils import init_cmd
from application.server.main.tasks import create_task_harvest, create_task_process, create_task_collect_results
from application.server.main.utils import upload_object, get_ip
from ovh_handler import generateStoragePath, get_partitions
from config.db_config import engine
from domain.processed_entry import ProcessedEntry
from domain.ovh_path import OvhPath
from config.path_config import COMPRESSION_EXT, PUBLICATION_PREFIX, PUBLICATION_EXT
from config.processing_service_namespaces import (datastet_ns, grobid_ns,
                                                  softcite_ns)


from application.server.main.logger import get_logger
logger = get_logger(__name__)

default_timeout = 4320000

main_blueprint = Blueprint('main', __name__, )


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')

def safe_instanciation_client(Client: BaseAPIClient, config: dict) -> BaseAPIClient:
    try:
        client = Client(config)
    except FailedRequest:
        current_ip = get_ip()
        client = None
        logger.error(f"Current IP = {current_ip} - Did not manage to initialize the {config['name']} client. The {config['name']} client instance will be set to None"
                     f" and standard download will be used in the case of a {config['name']} client URL.", exc_info=True)
    return client

@main_blueprint.route("/harvest_partitions", methods=["POST"])
def run_task_harvest_partitions():
    args = request.get_json(force=True)
    response_objects = []
    current_ip = get_ip()
    logger.debug(current_ip)
    wiley_client = safe_instanciation_client(WileyClient, config_harvester[WILEY])
    elsevier_client = safe_instanciation_client(ElsevierClient, config_harvester[ELSEVIER])


    swift_handler = Swift(config_harvester)
    db_handler = DBHandler(engine=engine, table_name='harvested_status_table', swift_handler=swift_handler)
    existing_dois = True
    if existing_dois:
        logger.debug('fetching existing DOI in database')
        doi_already_harvested_list = [entry[0] for entry in db_handler.fetch_all()]
        logger.debug(f'{len(doi_already_harvested_list)} DOI in database')
        json.dump(doi_already_harvested_list, open('harvested_doi.csv', 'w'))
        upload_object('misc', 'harvested_doi.csv')

    os.system('cd /src && mkdir -p tmp')
    
    os.system(f'cd /src/tmp && {init_cmd} download bso_dump bso-publications-latest.jsonl.gz')
    os.system(f'cd /src/tmp && zcat bso-publications-latest.jsonl.gz | split -l 5000 - bso-publications-split')
    list_files = []
    for f in os.listdir('/src/tmp'):
        if 'bso-publications-split' in f:
            os.system(f'cd /src/tmp && {init_cmd} upload bso_dump {f}')
            list_files.append(f)

    #os.system(f'{init_cmd} list bso_dump --prefix bso-publications-split > /src/tmp/list_files')
    #list_files = [k.strip() for k in open('/src/tmp/list_files', 'r').readlines()]

    logger.debug(f'len files = {len(list_files)}')
    for partition_index, current_file in enumerate(list_files):
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue(name="pdf-harvester", default_timeout=default_timeout)
            task_kwargs = {
                "source_metadata_file": current_file,
                "wiley_client": wiley_client,
                "elsevier_client": elsevier_client
            }
            task = q.enqueue(create_task_harvest, **task_kwargs)
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
    return jsonify(response_objects)

@main_blueprint.route("/process", methods=["POST"])
def run_task_process():
    """
    Process publications using Grobid, Softcite and Datastet
    """
    args = request.get_json(force=True)
    partition_size = args.get("partition_size", 100)
    grobid_ns.spec_version = args.get("spec_grobid_version", "0")
    softcite_ns.spec_version = args.get("spec_softcite_version", "0")
    datastet_ns.spec_version = args.get("spec_datastet_version", "0")
    break_after_one = args.get("break_after_one", False)
    prepare_process_task_arguments(partition_size, grobid_ns, softcite_ns, datastet_ns)
    response_objects = []
    logger.debug('grobid : ')
    logger.debug(len(grobid_ns.partitions))
    logger.debug(grobid_ns.partitions[0])
    logger.debug('softcite : ')
    logger.debug(softcite_ns.partitions[0])
    logger.debug(softcite_ns.partitions[0])
    logger.debug('datastet : ')
    logger.debug(datastet_ns.partitions[0])
    logger.debug(datastet_ns.partitions[0])
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="pdf-processor", default_timeout=default_timeout)
        for grobid_partition, softcite_partition, datastet_partition in zip(grobid_ns.partitions, softcite_ns.partitions, datastet_ns.partitions):
            task = q.enqueue(
                create_task_process,
                kwargs={
                    "grobid_partition_files": grobid_partition,
                    "softcite_partition_files": softcite_partition,
                    "datastet_partition_files": datastet_partition,
                },
            )
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
            if break_after_one:
                break
    return jsonify(response_objects)

@main_blueprint.route("/collect", methods=["POST"])
def run_task_collect():
    """
    Collect results from Grobid, Softcite and Datastet
    """
    args = request.get_json(force=True)

    hex_digits = string.hexdigits[0:16]
    for x in hex_digits:
        for y in hex_digits:
            prefix = f'{x}{y}'
            new_args = args.copy()
            new_args['prefix_uid'] = prefix
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue(name="collect", default_timeout=default_timeout)
                task = q.enqueue(create_task_collect_results, new_args)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202

def get_grobid_filter(record):
    uuid = record[1]
    if os.path.isfile(f'/data/bso3_publications_dump/grobid-0.8.0/publication/{uuid[0:2]}/{uuid[2:4]}/{uuid[4:6]}/{uuid[6:8]}/{uuid}/{uuid}.grobid.tei.xml'):
        return False
    return True

def get_softcite_filter(record):
    uuid = record[1]
    if os.path.isfile(f'/data/bso3_publications_dump/softcite-0.8.0/publication/{uuid[0:2]}/{uuid[2:4]}/{uuid[4:6]}/{uuid[6:8]}/{uuid}/{uuid}.software.json'):
        return False
    return True

def get_datastet_filter(record):
    uuid = record[1]
    if os.path.isfile(f'/data/bso3_publications_dump/datastet-0.8.0/publication/{uuid[0:2]}/{uuid[2:4]}/{uuid[4:6]}/{uuid[6:8]}/{uuid}/{uuid}.dataset.json'):
        return False
    return True

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def load_already_processed():
    already_processed = {}
    for current_service in ['grobid', 'softcite', 'datastet']:
        nb_loaded = 0
        logger.debug(f'loadind existing data for {current_service}')
        for e in os.walk(f'/data/bso3_publications_dump/{current_service}-0.8.0/publication'):
            if len(e)>=3:
                    for f in e[2]:
                        if '.json' in f or '.xml' in f:
                            current_uuid = f.split('.')[0]
                            nb_loaded += 1
                            if current_uuid not in already_processed:
                                already_processed[current_uuid] = []
                            if current_service not in already_processed[current_uuid]:
                                already_processed[current_uuid].append(current_service)
        logger.debug(f'{len(already_processed)} UUID processed')
    return already_processed


def prepare_process_task_arguments(partition_size, grobid_ns, softcite_ns, datastet_ns):
    """Populate services namespaces with list of partitions"""
    storage_handler = Swift(config_harvester)
    db_handler: DBHandler = DBHandler(engine=engine, table_name="harvested_status_table", swift_handler=storage_handler)
    db_records = db_handler.fetch_all() # list of tuples ex ('10.3390/molecules22020239', '00002286-7505-4a3d-ab3d-718925e1510d', '1', '0.7.3-SNAPSHOT', '0.7.2', 'standard', 'Biology (fond.)', 'https://europepmc.org/articles/pmc6155680?pdf=render', datetime.datetime(2022, 6, 24, 19, 0), '0.7.3-SNAPSHOT')

    records_to_process = {'grobid': [], 'softcite': [], 'datastet': [] }

    already_processed = load_already_processed()

    ITER_IX = 1000
    max_ix = int(len(db_records) / ITER_IX)+1
    for ix, r in enumerate(db_records):
        current_uuid = r[1]
        already_done = []
        if current_uuid in already_processed:
            already_done = already_processed[current_uuid]
        if 'datastet' not in already_done:
            records_to_process['datastet'].append(r)
        if 'softcite' not in already_done:
            records_to_process['softcite'].append(r)
        if 'grobid' not in already_done:
            records_to_process['grobid'].append(r)
        if ix % ITER_IX == 0:
            logger.debug(f'checking which files to process ix = {ix/ITER_IX} / {max_ix}')
    files_to_process = {}
    for s in ['grobid', 'softcite', 'datastet']:
        logger.debug(f'{s} : {len(records_to_process[s])} files to process')
        files_to_process[s] = sorted([
            str(OvhPath(PUBLICATION_PREFIX, generateStoragePath(record.uuid), record.uuid + PUBLICATION_EXT + COMPRESSION_EXT))
            for record in records_to_process[s]
        ])
    grobid_ns.partitions = get_partitions(files_to_process['grobid'], partition_size)
    softcite_ns.partitions = get_partitions(files_to_process['softcite'], partition_size)
    datastet_ns.partitions = get_partitions(files_to_process['datastet'], partition_size)

    #grobid_filter = lambda record: record.grobid_version < grobid_ns.spec_version
    #softcite_filter = lambda record: record.softcite_version < softcite_ns.spec_version
    #datastet_filter = lambda record: record.datastet_version < datastet_ns.spec_version
    #filter_services = [grobid_filter, softcite_filter, datastet_filter]
    services = [grobid_ns, softcite_ns, datastet_ns]
    #for service, _filter in zip(services, filter_services):
    #    service.partitions = get_partitions(get_files_to_process(db_records, _filter), partition_size)
    # Padding partitions with empty lists so we can use zip later
    # otherwise zip would stop iterating as the end of the shortest list
    longuest_list_len = max(len(grobid_ns.partitions), len(softcite_ns.partitions), len(datastet_ns.partitions))
    for service in services:
        service.partitions += [[]] * (longuest_list_len - len(service.partitions))

def get_files_to_process(db_records: List[ProcessedEntry],
                         publications_filter: Callable) -> List[str]:
    """Return a list of pdf gzipped files that pass the filter"""
    records_to_process = filter_publications(db_records, publications_filter)
    files_to_process = sorted([
            str(OvhPath(PUBLICATION_PREFIX, generateStoragePath(record.uuid), record.uuid + PUBLICATION_EXT + COMPRESSION_EXT))
            for record in records_to_process
        ])
    return files_to_process

def filter_publications(db_records: List[ProcessedEntry], condition: Callable) -> List[ProcessedEntry]:
    """Return db_records matching the condition"""
    return [record for record in db_records if condition(record)]

@main_blueprint.route("/load_matcher", methods=["POST"])
def run_task_load_matcher():
    requests.get('http://affiliation-matcher:5001/load')

@main_blueprint.route('/tasks/<task_id>', methods=['GET'])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('pdf-harvester')
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id(),
                'task_status': task.get_status(),
                'task_result': task.result,
            }
        }
    else:
        response_object = {'status': 'error'}
    return jsonify(response_object)

@main_blueprint.route("/processor_tasks/<task_id>", methods=["GET"])
def get_status_processing(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("pdf-processor")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)
