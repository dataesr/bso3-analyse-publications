from elasticsearch import Elasticsearch
import os
from application.server.main.logger import get_logger

ES_LOGIN_BSO3_BACK = os.getenv("ES_LOGIN_BSO3_BACK", "")
ES_PASSWORD_BSO3_BACK = os.getenv("ES_PASSWORD_BSO3_BACK", "")
ES_URL = os.getenv("ES_URL2", "http://localhost:9200")
client = None
logger = get_logger(__name__)


def get_client():
    global client
    if client is None:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_BSO3_BACK, ES_PASSWORD_BSO3_BACK))
    return client


def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


def get_analyzers() -> dict:
    return {
        'light': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding'
            ]
        }
    }


def get_filters() -> dict:
    return {
        'french_elision': {
            'type': 'elision',
            'articles_case': True,
            'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c', 'jusqu', 'quoiqu', 'lorsqu', 'puisqu']
        }
    }


def reset_index(index: str) -> None:
    es = get_client()
    delete_index(index)
    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        }
    }
    mappings = {'properties': {}}
    for f in ['authors.first_name', 'authors.last_name', 'authors.full_name', 'rawForm', 'normalizedForm']: 
        mappings['properties'][f] = {
            'type': 'text',
            'analyzer': 'light',
            'fields': {
                'keyword': {
                    'type':  'keyword'
                }
            }
        }
    for f in ['authors.affiliations.name', 'context']: 
        mappings['properties'][f] = {
            'type': 'text',
            'analyzer': 'light',
        }
    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')
