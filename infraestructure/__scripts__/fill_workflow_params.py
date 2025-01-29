import os

print(f'cwd: {os.getcwd()}')

import sys

sys.path.append('..')

import os
from pathlib import Path

from src.settings import (
    DATAPROC_STATUS_CLOUD_FUNCTION_URL,
    GET_PARAMETERS_CLOUD_FUNCTION_URL,
    MANUAL_FILES_INGESTION_CLOUD_FUNCTION_URL,
    PROJECT_ID,
    SCRIPTS_CONFIGS_BUCKET,
    SECURE_DIRS_INGESTION_CLOUD_FUNCTION_URL,
    STAGE,
)

replace_dict = {
    '{{STAGE}}': STAGE,
    '{{PROJECT_ID}}': PROJECT_ID,
    '{{MANUAL_FILES_INGESTION_CLOUD_FUNCTION_URL}}': MANUAL_FILES_INGESTION_CLOUD_FUNCTION_URL,
    '{{DATAPROC_STATUS_CLOUD_FUNCTION_URL}}': DATAPROC_STATUS_CLOUD_FUNCTION_URL,
    '{{GET_PARAMETERS_CLOUD_FUNCTION_URL}}': GET_PARAMETERS_CLOUD_FUNCTION_URL,
    '{{SCRIPTS_CONFIGS_BUCKET}}': SCRIPTS_CONFIGS_BUCKET,
    '{{SECURE_DIRS_INGESTION_CLOUD_FUNCTION_URL}}': SECURE_DIRS_INGESTION_CLOUD_FUNCTION_URL,
}

path = Path('..', 'src', 'workflow_pipelines')
output_path = Path(path, '_stage_version')
output_path.mkdir(parents=True, exist_ok=True)
dir_list = os.listdir(path)


if __name__ == '__main__':
    print(f'stage: {STAGE}')

    for file_name in dir_list:
        if file_name.endswith('.yml'):
            with open(Path(path, file_name), 'r', encoding='utf-8') as file:
                file_content = file.read()

            for param, value in replace_dict.items():
                file_content = file_content.replace(param, value)

            with open(Path(output_path, file_name), 'w', encoding='utf-8') as file:
                file.write(file_content)
