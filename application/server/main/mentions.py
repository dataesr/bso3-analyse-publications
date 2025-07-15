import jsonlines
import os
import string
import pickle
from urllib import parse
import requests
import pandas as pd
from application.server.main.utils import download_container
from application.server.main.elastic import reset_index
from application.server.main.forges import detect_forge, get_canonical, get_swhid, clean_url
from application.server.main.logger import get_logger

logger = get_logger(__name__)

# Elastic Searh configurations
ES_LOGIN_BSO3_BACK = os.getenv("ES_LOGIN_BSO3_BACK", "")
ES_PASSWORD_BSO3_BACK = os.getenv("ES_PASSWORD_BSO3_BACK", "")
ES_URL = os.getenv("ES_URL2", "http://localhost:9200")

def get_mentions_data(all_mentions_path='final_for_bso_2025_v2'):
    download_container('bso3_publications_dump', download_prefix=all_mentions_path, volume_destination="/data")

def format_mentions(obj, url_to_swhid_dict):
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
                elt['urls'] = [{'url': u} for u in forges_urls if obj.get('doi').lower() not in u.lower()]
                for u in elt['urls']:
                    cleans = list(set([clean_url(u).lower(), get_canonical(u)]))
                    for c in cleans:
                        if c in url_to_swhid_dict:
                            u['swhid'] = url_to_swhid_dict[c]
        mentions.append(elt)
    return mentions

def detect_urls(suffix, all_mentions_path):
    all_mentions_with_url_simple = []
    with jsonlines.open(f'/data/bso3_publications_dump/{all_mentions_path}/bso3_data_{suffix}.jsonl') as reader:
        for obj in reader:
            tmp = format_mentions(obj)
            for t in tmp:
                if t.get('urls'):
                    urls = [u['url'] for u in t['urls']]
                    logger.debug(f"url detected in {t['doi']} : {urls}")
                    all_mentions_with_url_simple.append({'doi': t['doi'], 'urls': urls})

    os.system('mkdir -p /data/mentions_with_url')
    with jsonlines.open(f'/data/mentions_with_url/current_mentions_with_url_{suffix}.jsonl', mode='w') as writer:
        writer.write_all(all_mentions_with_url_simple)

def detect_all_urls(args):
    all_mentions_path = args.get('all_mentions_path')
    if args.get('detect_urls', False):
        hex_digits = string.hexdigits[0:16]
        for x in hex_digits:
            for y in hex_digits:
                suffix = f'{x}{y}'
                detect_urls(suffix, all_mentions_path)
    swh(args)

def get_status(url):
    logger.debug(url)
    try:
        r = requests.head(url, timeout=60)
        return r.status_code
    except:
        return 'erreur'

def swh(args):
    check_missing_again = args.get('check_missing_again', False)
    url_to_swhid_dict = {}
    try:
        url_to_swhid_dict = pickle.load(open('/data/url_to_swhid_dict.pkl', 'rb'))
        logger.debug(f'url_to_swhid_dict loaded in {len(url_to_swhid_dict)} entries')
    except:
        pass
    cmd = 'cd /data/mentions_with_url && cat current_mentions_with_url* | sort -u > all_mentions_with_url.jsonl'
    os.system(cmd)
    df = pd.read_json('/data/mentions_with_url/all_mentions_with_url.jsonl', lines=True)
    all_urls = []
    for r in df.itertuples():
        all_urls += [clean_url(u).lower() for u in r.urls]
        all_urls += [get_canonical(u) for u in r.urls]
    all_urls = [u for u in list(set(all_urls)) if u]
    for ix, u in enumerate(all_urls):
        if ix%25 == 0:
            logger.debug(f'{ix} / {len(all_urls)} saving {len(url_to_swhid_dict)} swhids/urls')
            pickle.dump(url_to_swhid_dict, open('/data/url_to_swhid_dict.pkl', 'wb'))
        skip = False
        if u in url_to_swhid_dict:
            if url_to_swhid_dict[u] == 'dead_link':
                skip = True
            if (check_missing_again is False) and url_to_swhid_dict[u] == 'missing':
                skip = True
            if url_to_swhid_dict[u] not in ['missing', 'dead_link']:
                skip = True
        for f in ['doi', 'zenodo', 'europa.eu']:
            if f in u:
                skip = True
        if skip:
            continue
        status = get_status(u)
        logger.debug('ok')
        if status == 200:
            swhid = get_swhid(u)
            if swhid:
                url_to_swhid_dict[u] = swhid
            else:
                url_to_swhid_dict[u] = 'missing'
        else:
            url_to_swhid_dict[u] = 'dead_link'
    pickle.dump(url_to_swhid_dict, open('/data/url_to_swhid_dict.pkl', 'wb'))
    #swh_api_url = f'https://archive.softwareheritage.org/api/1/origin/search/{xx}/?use_ql=false&fields=url,visit_types,id,metadata_authorities_url&limit=100&with_visit=true' 

def transform_and_load(suffix, all_mentions_path, new_index_name, url_to_swhid_dict):
    all_mentions = []
    with jsonlines.open(f'/data/bso3_publications_dump/{all_mentions_path}/bso3_data_{suffix}.jsonl') as reader:
        for obj in reader:
            all_mentions += format_mentions(obj, url_to_swhid_dict)
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
    url_to_swhid_dict = pickle.load(open('/data/url_to_swhid_dict.pkl', 'rb'))
    new_index_name = args.get('index_name')
    all_mentions_path = args.get('all_mentions_path')
    if all_mentions_path is None:
        logger.debug('missing all_mentions_path')
        return
    get_mentions_data(all_mentions_path)
    reset_index(index=new_index_name)
    hex_digits = string.hexdigits[0:16]
    for x in hex_digits:
        for y in hex_digits:
            suffix = f'{x}{y}'
            transform_and_load(suffix, all_mentions_path, new_index_name, url_to_swhid_dict)
