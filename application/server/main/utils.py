import os
import requests
from application.server.main.logger import get_logger
logger = get_logger(__name__)

key = os.getenv('OS_PASSWORD2')
project_name = os.getenv('OS_PROJECT_NAME')
project_id = os.getenv('OS_TENANT_ID')
tenant_name = os.getenv('OS_TENANT_NAME')
username = os.getenv('OS_USERNAME2')
user = f'{tenant_name}:{username}'
init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"

def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'Downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    os.system(cmd)

def upload_object(container: str, filename: str) -> None:
    logger.debug(f'Uploading {filename} to {container}')
    cmd = init_cmd + f' upload {container} {filename}'
    os.system(cmd)

def download_container(container, download_prefix, volume_destination):
    cmd =  init_cmd + f' download {container} -D {volume_destination}/{container} --skip-identical'
    if download_prefix:
        cmd += f" --prefix {download_prefix}"
    os.system(cmd)
    if download_prefix:
        return f'{volume_destination}/{container}/{download_prefix}'
    return f'{volume_destination}/{container}'

def get_ip():
    ip = requests.get('https://api.ipify.org').text
    return ip
