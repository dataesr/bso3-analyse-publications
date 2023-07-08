import logging

from config.harvester_config import config_harvester

is_debug_level = config_harvester['is_level_debug']
if is_debug_level == 1:
    LOGGER_LEVEL = logging.DEBUG
else:
    LOGGER_LEVEL = logging.WARNING
