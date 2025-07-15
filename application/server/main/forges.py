import re
import os
import pandas as pd
import requests
from retry import retry
import time
from application.server.main.logger import get_logger

logger = get_logger(__name__)

SWH_BEARER = os.getenv('SWH_BEARER')

def build_rgx_forges():
    forges = pd.read_csv('forges.csv', header=None)[0].to_list()
    forges = [f.replace('/', '\/').replace('.', '\.') for f in forges]
    forges_rgx = [r"\b" + f + r"\/[\w\-\.]+(?:\/[\w\-\.]+)*\b" for f in forges] 
    rgx = re.compile('|'.join(forges_rgx))
    return rgx

FORGE_RGX = build_rgx_forges()

def detect_forge(s):
    return list(set(re.findall(FORGE_RGX, s)))

def clean_url(url):
    if 'github' in url:
        url = url.replace('www.', '')
    if url[0:4] != 'http':
        return 'https://'+url
    return url

def get_canonical(url):
    url_split = url.lower().split('/')
    try:
        return clean_url(url_split[0] + '/' + url_split[1] + '/' + url_split[2])
    except:
        return None

def get_query(url):
    return """query getOriginDict {
       origin(url: \""""+url+"""\") {
         url
         latestSnapshot {
           headBranch {
             target {
               node {
                 ... on Release {
                   target {
                     node {
                       ... on Revision {
                         directory {
                           swhid
                         }
                       }
                       ... on Directory {
                     swhid
                   }
                        }
                   }
                 }
                 ... on Revision {
                  directory {
                    swhid
                  }
                }
                ... on Directory {
                  swhid
                }
              }
            }
          }
        }
      }
    }
    """


@retry(delay=1000, tries=5, backoff=10)
def search_url(url):
    url_swh = 'https://archive.softwareheritage.org/api/1/origin/search/'+url+'/?use_ql=false&fields=url%2Cvisit_types&limit=100&with_visit=true'
    headers = {'Authorization': f'Bearer {SWH_BEARER}'}
    r = requests.get(url_swh, headers=headers)
    assert(r.status_code == 200)
    for c in r.json():
        candidate_url = c['url']
        candidate_url_strip = candidate_url
        if candidate_url[-1] == '/':
            candidate_url_strip = candidate_url[0:-1]
        if candidate_url_strip.lower() in url.lower():
            return candidate_url
    return None

def find_swhid_attrs(d):
    if isinstance(d, dict):
        for f in d:
            if f == 'swhid' and d[f]:
                return d[f]
            if find_swhid_attrs(d[f]):
                return find_swhid_attrs(d[f])
    return None

@retry(delay=300, tries=5, backoff=2)
def get_swhid(url):
    time.sleep(1)
    swhid = None
    url_candidate = search_url(url)
    if url_candidate is None:
        return None
    query = get_query(url_candidate)
    headers = {'Authorization': f'Bearer {SWH_BEARER}'}
    r = requests.post("https://archive.softwareheritage.org/graphql/", json={"query" : query}, headers=headers)
    res = r.json()
    try:
        origin = res['data']['origin']
        if origin:
            node = origin['latestSnapshot']['headBranch']['target']['node']
            swhid = find_swhid_attrs(node)
            if swhid:
                logger.debug(f'swhid {swhid} detected for {url}')
            else:
                logger.debug(f'swhid not detected from {node}')
    except:
        logger.debug(f'error in getting swhid for {url} {res}')
    return swhid
