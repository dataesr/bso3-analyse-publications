import json
from application.server.main.logger import get_logger
from application.server.main.parse_datastet import parse_mentions

logger = get_logger(__name__)

def json_softcite(filename, SOFTCITE_VERSIONS):
    try:
        p = json.load(open(filename, 'r'))
        version = p['version']
        if version not in SOFTCITE_VERSIONS:
            return {}
        return parse_mentions(p, 'softcite')
    except:
        logger.debug(f'error with softcite {filename}')
        return {}
