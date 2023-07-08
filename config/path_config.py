import os

# Project path
PROJECT_DIRNAME = os.path.dirname(os.path.dirname(__file__))

# Config paths
CONFIG_PATH = os.path.join(PROJECT_DIRNAME, "config.json")
CONFIG_PATH_TEST = os.path.join(PROJECT_DIRNAME, "tests", "unit_tests", "config_test.json")

# Data path
DATA_PATH = os.path.join(PROJECT_DIRNAME, "data")
PUBLICATIONS_DOWNLOAD_DIR = os.path.join(PROJECT_DIRNAME, "downloaded_publications/")
GROBID_DIR = os.path.join(PROJECT_DIRNAME, "grobid/")
SOFTCITE_DIR = os.path.join(PROJECT_DIRNAME, "softcite/")
DATASTET_DIR = os.path.join(PROJECT_DIRNAME, "datastet/")
CONFIG_PATH_GROBID = os.path.join(PROJECT_DIRNAME, "config/config_grobid.json")
CONFIG_PATH_SOFTCITE = os.path.join(PROJECT_DIRNAME, "config/config_softcite.json")
CONFIG_PATH_DATASTET = os.path.join(PROJECT_DIRNAME, "config/config_datastet.json")

# Log path
LOG_PATH = os.path.join(PROJECT_DIRNAME, os.path.join("logs", "logger.log"))

# Metadata paths
DESTINATION_DIR_METADATA = os.path.join(PROJECT_DIRNAME, "tmp")
METADATA_LOCAL_FILE = "bso-publications-5k.jsonl.gz"  # 'bso-publications-20220118.jsonl.gz'

# Prefixes in ovh dump
METADATA_PREFIX = "metadata"
PUBLICATION_PREFIX = "publication"
DATASTET_PREFIX = "datastet"
SOFTCITE_PREFIX = "softcite"
GROBID_PREFIX = "grobid"

# Output file suffixes
COMPRESSION_EXT = ".gz"
METADATA_EXT = ".json"
PUBLICATION_EXT = ".pdf"
DATASTET_EXT = ".dataset.json"
SOFTCITE_EXT = ".software.json"
GROBID_EXT = ".tei.xml"
