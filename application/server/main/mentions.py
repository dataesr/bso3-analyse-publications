import jsonlines
import os
import string
from urllib import parse
from application.server.main.utils import download_container
from application.server.main.elastic import reset_index
from application.server.main.forges import detect_forge
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
                for author in elt['authors']:
                    if isinstance(author, dict):
                        if author.get('affiliations'):
                            del author['affiliations']
                        full_name = ''
                        if author.get('first_name') and author['first_name']:
                            full_name += author['first_name']
                            del author['first_name']
                        if author.get('last_name'):
                            full_name += ' ' + author['last_name']
                            del author['last_name']
                        if full_name:
                            author['full_name'] = full_name.strip()
        if elt.get('authors'):
            assert(isinstance(elt['authors'], list))
            elt['authors'] = ' ;;; '.join([f"{author.get('full_name')} {author.get('email', '')}" for author in elt['authors']])
        if elt.get('affiliations'):
            assert(isinstance(elt['affiliations'], list))
            elt['affiliations'] = ' ;;; '.join([affiliation.get('name') for affiliation in elt['affiliations']])
        elt.update(m)
        if isinstance(elt.get('context'), str):
            forges_urls = detect_forge(elt['context'])
            if forges_urls:
                elt['urls'] = [u for u in forges_urls if obj.get('doi').lower() not in u.lower()]
        mentions.append(elt)
    return mentions

def detect_urls(suffix):
    all_mentions_with_url, all_mentions_with_url_simple = [], []
    with jsonlines.open(f'/data/bso3_publications_dump/final_for_bso_2025_v2/bso3_data_{suffix}.jsonl') as reader:
        for obj in reader:
            tmp = format_mentions(obj)
            for t in tmp:
                if t.get('urls'):
                    all_mentions_with_url.append(t)
                    logger.debug(f"url detected in {t['doi']} : {t['urls']}")
                    all_mentions_with_url_simple.append({'doi': t['doi'], 'urls': t['urls']})

    os.system('mkdir -p /data/mentions_with_url')
    with jsonlines.open(f'/data/mentions_with_url/current_mentions_with_url_{suffix}.jsonl', mode='w') as writer:
        writer.write_all(all_mentions_with_url_simple)

def detect_all_urls(args):
    hex_digits = string.hexdigits[0:16]
    for x in hex_digits:
        for y in hex_digits:
            suffix = f'{x}{y}'
            detect_urls(suffix)

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
