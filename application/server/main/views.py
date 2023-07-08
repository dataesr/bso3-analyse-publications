import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from config import ELSEVIER, WILEY
from config.harvester_config import config_harvester
from harvester.base_api_client import BaseAPIClient
from harvester.elsevier_client import ElsevierClient
from harvester.exception import FailedRequest
from harvester.wiley_client import WileyClient
from application.server.main.utils import init_cmd
from application.server.main.tasks import create_task_harvest


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
        client = None
        logger.error(f"Did not manage to initialize the {config['name']} client. The {config['name']} client instance will be set to None"
                     f" and standard download will be used in the case of a {config['name']} client URL.", exc_info=True)
    return client

@main_blueprint.route("/harvest_partitions", methods=["POST"])
def run_task_harvest_partitions():
    args = request.get_json(force=True)
    response_objects = []
    wiley_client = safe_instanciation_client(WileyClient, config_harvester[WILEY])
    elsevier_client = safe_instanciation_client(ElsevierClient, config_harvester[ELSEVIER])

    os.system('cd /src && mkdir -p tmp')
    os.system(f'{init_cmd} list bso_dump --prefix bso-publications-split > /src/tmp/list_files')
    list_files = [k.strip() for k in open('/src/tmp/list_files', 'r').readlines()]

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
        break
    return jsonify(response_objects)

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