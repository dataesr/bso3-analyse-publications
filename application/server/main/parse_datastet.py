import json
import copy
from application.server.main.logger import get_logger

logger = get_logger(__name__)

def json_datastet(filename, DATASTET_VERSIONS):
    try:
        p = json.load(open(filename, 'r'))
    except:
        logger.debug(f'error reading datastet {filename}')
        return {}
    version = p['version']
    if version not in DATASTET_VERSIONS:
        return {}
    logger.debug(filename)
    return parse_mentions(p=p, tool_type='datastet', version=version)

def parse_mentions(p, tool_type, version):
    details = {'mentions': [], 'raw_mentions': [], 
                        'used': [], 'created': [], 'shared': [],
                        'url': [], 'wikidata': [],
                        'has_used': False, 'has_created': False, 'has_shared': False,
                        'nb_used': 0, 'nb_created': 0, 'nb_shared': 0, 'nb_mentions': 0
                       }
    res = {f'{tool_type}_details': details}
    res[f'{tool_type}_version'] = version
    is_implicit_only = True
    for m in p['mentions']:
        raw_mention = copy.deepcopy(m)
        current_mention = {}
        if m.get('wikidataId'):
            current_mention['wikidata'] = m['wikidataId']
        if m.get('url'):
            current_mention['url'] = m['url']['normalizedForm']
        file_type = m['type']
        if file_type not in m:
            file_type = file_type + '-name'
            assert(file_type in m)
        is_implicit = False
        if ('implicit' in file_type) or ('implicit' in m.get('software-type', '')):
            is_implicit = True
        else:
            is_implicit = False
            is_implicit_only = False
        raw_mention['is_implicit'] = is_implicit
        name = m[f'{file_type}']['normalizedForm']
        for f in m[f'{file_type}']:
            for k in ['offset', 'bounding']:
                if k in f:
                    del raw_mention[f'{file_type}'][f]
        for f in ['boundingBoxes', 'offsetEnd', 'offsetStart']:
            if m.get('data-device', {}).get(f):
                del raw_mention['data-device'][f]
        for f in ['bestTypeScore', 'hasDataset']:
            if m.get('dataset-implicit', {}).get(f):
                del raw_mention['dataset-implicit'][f]
        current_mention['name'] = name
        for mention_type in ['used', 'created', 'shared']:
            if 'documentContextAttributes' in m:
                if 'document_context' not in raw_mention:
                    raw_mention['document_context'] = {}
                raw_mention['document_context'][mention_type] = m['documentContextAttributes'][mention_type]['value']
            if 'mentionContextAttributes' in m:
                if 'mention_context' not in raw_mention:
                    raw_mention['mention_context'] = {}
                raw_mention['mention_context'][mention_type] = m['mentionContextAttributes'][mention_type]['value']
            if 'documentContextAttributes' in m:
                current_mention[mention_type] = m['documentContextAttributes'][mention_type]['value']
            else:
                current_mention[mention_type] = m['mentionContextAttributes'][mention_type]['value']
            if current_mention[mention_type] and name not in details[mention_type]:
                if name not in details[mention_type]:
                    details[mention_type].append(name)
                for k in ['wikidata', 'url']:
                    if current_mention.get(k):
                        if current_mention[k] not in details[k]:
                            details[k].append(current_mention[k])
                details[f'has_{mention_type}'] = True
                details[f'nb_{mention_type}'] += 1

        for f in ['mentionContextAttributes', 'documentContextAttributes']:
            if f in raw_mention:
                del raw_mention[f]
        details['raw_mentions'].append(raw_mention)
        if current_mention not in details['mentions']:
            details['mentions'].append(current_mention)
    details['nb_mentions'] = len(details['raw_mentions'])
    details['is_implicit_only'] = is_implicit_only
    return res
