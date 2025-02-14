import jsonlines
import os
import string
from urllib import parse
from application.server.main.utils import download_container
from application.server.main.elastic import reset_index
from application.server.main.logger import get_logger
logger = get_logger(__name__)


# Elastic Searh configurations
ES_LOGIN_BSO3_BACK = os.getenv("ES_LOGIN_BSO3_BACK", "")
ES_PASSWORD_BSO3_BACK = os.getenv("ES_PASSWORD_BSO3_BACK", "")
ES_URL = os.getenv("ES_URL2", "http://localhost:9200")

def get_mentions_data():
    download_container('bso3_publications_dump', download_prefix='final_for_bso_2025_v2', volume_destination="/data")

def format_mentions(obj):
    mentions = []
    raw_mentions = []
    if obj.get('datastet_details') and obj.get('datastet_details').get('raw_mentions'):
        raw_mentions += obj.get('datastet_details').get('raw_mentions')
    if obj.get('softcite_details') and obj.get('softcite_details').get('raw_mentions'):
        raw_mentions += obj.get('softcite_details').get('raw_mentions')
    for m in raw_mentions:
        elt = {}
        for f in ['doi', 'authors', 'grants', 'has_grant', 'affiliations']:
            if obj.get(f):
                elt[f] = obj[f]
            if f == 'authors' and obj.get('authors') and isinstance(obj['authors'], list):
                for a in elt['authors']:
                    if isinstance(a, dict):
                        if a.get('affiliations'):
                            del a['affiliations']
                        full_name = ''
                        if a.get('first_name') and a['first_name']:
                            full_name += a['first_name']
                            del a['first_name']
                        if a.get('last_name'):
                            full_name += ' ' + a['last_name']
                            del a['last_name']
                        if full_name:
                            a['full_name'] = full_name.strip()
        elt.update(m)
        mentions.append(elt)
    return mentions

def transform_and_load(suffix, new_index_name):
    all_mentions = []
    with jsonlines.open(f'/data/bso3_publications_dump/final_for_bso_2025_v2/bso3_data_{suffix}.jsonl') as reader:
        for obj in reader:
            all_mentions += format_mentions(obj)
    with jsonlines.open(f'/data/current_mentions_{suffix}.jsonl', mode='w') as writer:
        writer.write_all(all_mentions)
    
    es_url_without_http = ES_URL.replace("https://", "").replace("http://", "")
    es_host = f"https://{ES_LOGIN_BSO3_BACK}:{parse.quote(ES_PASSWORD_BSO3_BACK)}@{es_url_without_http}"
    logger.debug("loading mentions index")
    elasticimport = (
            f"elasticdump --input=/data/current_mentions_{suffix}.jsonl --output={es_host}{new_index_name} --type=data --limit 100 "
            + "--transform='doc._source=Object.assign({},doc)'"
    )
    logger.debug(f"{elasticimport}")
    logger.debug("starting import in elastic.py")
    os.system(elasticimport)
    os.system(f'rm -rf /data/current_mentions_{suffix}.jsonl')

def load_all_mentions(args):
    new_index_name = args.get('index_name')
    reset_index(index=new_index_name)
    hex_digits = string.hexdigits[0:16]
    for x in hex_digits:
        for y in hex_digits:
            suffix = f'{x}{y}'
            transform_and_load(suffix, new_index_name)
