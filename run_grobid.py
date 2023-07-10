import os
import json
from config.path_config import CONFIG_PATH_GROBID, PUBLICATIONS_DOWNLOAD_DIR


def run_grobid(config_path, data_path, GrobidClient):
    if not config_path:
        config = {
            'grobid_server': os.getenv("GROBID_SERVER"),
            'grobid_port': os.getenv("GROBID_PORT"),
            'batch_size': int(os.getenv("GROBID_BATCH_SIZE")),
            'coordinates': json.loads(os.getenv('GROBID_COORDINATES')),
            'sleep_time': int(os.getenv("GROBID_SLEEP_TIME")),
            'timeout': int(os.getenv("GROBID_TIMEOUT"))
        }
    else:
        config = json.load(open(config_path))
    client = GrobidClient(**config)
    client.process("processFulltextDocument", data_path, output=data_path, consolidate_header=True,
                   consolidate_citations=False, tei_coordinates=True, force=True, verbose=True, n=4)


if __name__ == "__main__":
    from dotenv import load_dotenv
    from grobid_client.grobid_client import GrobidClient
    load_dotenv(override=True)

    # from tests.unit_tests.fixtures.grobid_client import GrobidClient
    run_grobid(CONFIG_PATH_GROBID, PUBLICATIONS_DOWNLOAD_DIR, GrobidClient)
