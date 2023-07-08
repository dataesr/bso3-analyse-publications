import os
from dotenv import load_dotenv
load_dotenv()

user = f'{os.getenv("OS_TENANT_NAME")}:{os.getenv("OS_USERNAME")}'
key = os.getenv("OS_PASSWORD")
project_id = os.getenv("OS_TENANT_ID")
project_name = os.getenv("OS_PROJECT_NAME")

init_cmd = f'swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA'
