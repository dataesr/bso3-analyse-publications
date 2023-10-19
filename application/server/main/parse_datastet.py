import json
from application.server.main.logger import get_logger

logger = get_logger(__name__)

def json_datastet(filename, DATASTET_VERSIONS):
    try:
        p = json.load(open(filename, 'r'))
        version = p['version']
        if version not in DATASTET_VERSIONS:
            return {}
        return parse_mentions(p, 'datastet')
    except:
        logger.debug(f'error with datastet {filename}')
        return {}

def parse_mentions(p, file_type):
    details = {'mentions': [], 
                        'used': [], 'created': [], 'shared': [],
                        'url': [], 'wikidata': [],
                        'has_used': False, 'has_created': False, 'has_shared': False,
                        'nb_used': 0, 'nb_created': 0, 'nb_shared': 0
                       }
    res = {f'{file_type}_details': details}
    for m in p['mentions']:
        current_mention = {}
        if m.get('wikidataId'):
            current_mention['wikidata'] = m['wikidataId']
        if m.get('url'):
            current_mention['url'] = m['url']['normalizedForm']
        name = ''
        if file_type == 'datastet':
            name = m['normalizedForm']
        elif file_type == 'softcite':
            name = m['software-name']['normalizedForm']
        current_mention['name'] = name
        for mention_type in ['used', 'created', 'shared']:
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
        if current_mention not in details['mentions']:
            details['mentions'].append(current_mention)
    return res
