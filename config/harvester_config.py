import json
import os

from dotenv import load_dotenv

from config import (
    WILEY,
    WILEY_TOKEN_KEY,
    WILEY_PUBLICATION_URL_KEY,
    ELSEVIER,
    ELSEVIER_API_TOKEN_KEY,
    ELSEVIER_INST_TOKEN_KEY,
    ELSEVIER_PUBLICATION_URL_KEY,
)
from config.path_config import CONFIG_PATH


def get_harvester_config(config_file_path: str) -> dict:
    # TODO: catch exception if file not found
    config_harvester = json.load(open(config_file_path, "r"))
    load_environment_variables()

    # Add env var secrets & pwd for swift - ovh
    config_harvester["swift"] = {}
    config_harvester["swift"]["os_username"] = os.getenv("OS_USERNAME")
    config_harvester["swift"]["os_password"] = os.getenv("OS_PASSWORD")
    config_harvester["swift"]["os_user_domain_name"] = os.getenv("OS_USER_DOMAIN_NAME")
    config_harvester["swift"]["os_project_domain_name"] = os.getenv("OS_PROJECT_DOMAIN_NAME")
    config_harvester["swift"]["os_project_name"] = os.getenv("OS_PROJECT_NAME")
    config_harvester["swift"]["os_project_id"] = os.getenv("OS_PROJECT_ID")
    config_harvester["swift"]["os_region_name"] = os.getenv("OS_REGION_NAME")
    config_harvester["swift"]["os_auth_url"] = os.getenv("OS_AUTH_URL")

    config_harvester["publications_dump"] = 'bso3_publications_dump'
    config_harvester["swift_container"] = config_harvester["publications_dump"]

    # Add env var secrets & pwd for database - postgres
    config_harvester["db"] = {}
    config_harvester["db"]["db_user"] = os.getenv("DB_USER")
    config_harvester["db"]["db_password"] = os.getenv("DB_PASSWORD")
    config_harvester["db"]["db_host"] = os.getenv("DB_HOST")
    config_harvester["db"]["db_port"] = os.getenv("DB_PORT")
    config_harvester["db"]["db_name"] = os.getenv("DB_NAME")

    # Wiley config
    config_harvester[WILEY] = {
        "name": WILEY,
        "HEADERS": {
            'Accept': 'application/pdf',
            "Wiley-TDM-Client-Token": os.getenv(WILEY_TOKEN_KEY)
        },
        "PUBLICATION_URL": os.getenv(WILEY_PUBLICATION_URL_KEY),
        "health_check_doi": "10.1111/jofi.12230",
        "throttle_parameters": {
            "max_num_requests": 1,
            "window_size": 15
        }
    }
    # Elsevier config
    config_harvester[ELSEVIER] = {
        "name": ELSEVIER,
        "HEADERS": {
            'Accept': 'application/pdf',
            'X-ELS-APIKey': os.getenv(ELSEVIER_API_TOKEN_KEY),
            'X-ELS-Insttoken': os.getenv(ELSEVIER_INST_TOKEN_KEY)
        },
        "PUBLICATION_URL": os.getenv(ELSEVIER_PUBLICATION_URL_KEY),
        "health_check_doi": "10.1016/j.biocon.2013.06.003",
        "throttle_parameters": {
            "max_num_requests": 1,
            "window_size": 1
        }
    }
    return config_harvester


def load_environment_variables() -> None:
    try:
        load_dotenv()
    except Exception as e:
        print(f"File .env not found: {str(e)}")


config_harvester = get_harvester_config(CONFIG_PATH)
