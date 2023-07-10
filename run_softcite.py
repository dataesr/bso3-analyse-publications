import os
import json
from config.path_config import PUBLICATIONS_DOWNLOAD_DIR, CONFIG_PATH_SOFTCITE


def run_softcite(config_path, data_path, smc):
    if not config_path:
        config = {
            "software_mention_host": os.getenv("SOFTWARE_MENTION_HOST"),
            "software_mention_port": os.getenv("SOFTWARE_MENTION_PORT"),
            "batch_size": int(os.getenv("SOFTWARE_MENTION_BATCH_SIZE")),
            "concurrency": int(os.getenv("SOFTWARE_MENTION_CONCURRENCY")),
            "data_path": os.getenv("SOFTWARE_MENTION_DATA_PATH"),
            "log_file": os.getenv("SOFTWARE_MENTION_LOG_FILE"),
            "log_level": os.getenv("SOFTWARE_MENTION_LOG_LEVEL"),
            "sleep_time": int(os.getenv("SOFTWARE_MENTION_SLEEP_TIME")),
            "timeout": int(os.getenv("SOFTWARE_MENTION_TIMEOUT"))
        }
    else:
        config = json.load(open(config_path))

    client = smc(**config)
    client.config["software_mention_url"] = f"http://{client.config['software_mention_host']}:{client.config['software_mention_port']}"
    client.annotate_directory(target="software", directory=data_path)


if __name__ == "__main__":
    from dotenv import load_dotenv
    from softdata_mentions_client.client import softdata_mentions_client
    load_dotenv(override=True)

    # from tests.unit_tests.fixtures.software_mention_client import software_mentions_client as smc

    run_softcite(CONFIG_PATH_SOFTCITE, PUBLICATIONS_DOWNLOAD_DIR, softdata_mentions_client)
