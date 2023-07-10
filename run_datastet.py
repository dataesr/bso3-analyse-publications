import os
import json
from config.path_config import CONFIG_PATH_DATASTET, PUBLICATIONS_DOWNLOAD_DIR


def run_datastet(config_path, data_path, softdata_mentions_client):
    if not config_path:
        config = {
            "dataset_mention_host": os.getenv("DATASET_MENTION_HOST"),
            "dataset_mention_port": os.getenv("DATASET_MENTION_PORT"),
            "batch_size": int(os.getenv("DATASET_MENTION_BATCH_SIZE")),
            "concurrency": int(os.getenv("DATASET_MENTION_CONCURRENCY")),
            "data_path": os.getenv("DATASET_MENTION_DATA_PATH"),
            "log_file": os.getenv("DATASET_MENTION_LOG_FILE"),
            "log_level": os.getenv("DATASET_MENTION_LOG_LEVEL"),
            "sleep_time": int(os.getenv("DATASET_MENTION_SLEEP_TIME")),
            "timeout": int(os.getenv("DATASET_MENTION_TIMEOUT"))
        }
    else:
        config = json.load(open(config_path))
    client = softdata_mentions_client(**config)
    client.config["dataset_mention_url"] = f"http://{client.config['dataset_mention_host']}:{client.config['dataset_mention_port']}"
    client.annotate_directory(target="dataset", directory=data_path)



if __name__ == "__main__":
    from dotenv import load_dotenv
    from softdata_mentions_client.client import softdata_mentions_client
    load_dotenv(override=True)
    run_datastet(CONFIG_PATH_DATASTET, PUBLICATIONS_DOWNLOAD_DIR, softdata_mentions_client)

