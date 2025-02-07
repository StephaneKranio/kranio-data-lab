import os
import sys
import zipfile
from pathlib import Path

#env = sys.argv[1]

origin_path_source_prd = 'data-services'
commons_path_source = "04-cloud_functions"
origin_path_output = f'terraform/cloud-functions-files'
path = Path(origin_path_output)
path.mkdir(parents=True, exist_ok=True)


def zip_files(source_dir, output_zip):
    main_py_path = os.path.join(source_dir, 'main.py')
    requirements_txt_path = os.path.join(source_dir, 'requirements.txt')

    with zipfile.ZipFile(output_zip, 'w') as zipf:

        if os.path.exists(main_py_path):
            zipf.write(main_py_path, arcname='main.py')
        else:
            print(f'el fichero main.py no se encuentro en la carpeta {source_dir}')
            
        if os.path.exists(requirements_txt_path):
            zipf.write(requirements_txt_path, arcname='requirements.txt')
        else:
            print(
                f'el fichero requirements.txt no se encuentro en la carpeta {source_dir}'
            )

# cloud functions zip creation

source_directory_prd_chat_bison = f'{origin_path_source_prd}/{commons_path_source}/check_dataproc-status'
output_zipfile_prd_chat_bison = f'{origin_path_output}/check_dataproc-status.zip'
zip_files(source_directory_prd_chat_bison, output_zipfile_prd_chat_bison)

source_directory_prd_prompt_cb = f'{origin_path_source_prd}/{commons_path_source}/get_parameters'
output_zipfile_prd_prompt_cb = f'{origin_path_output}/get_parameters.zip'
zip_files(source_directory_prd_prompt_cb, output_zipfile_prd_prompt_cb)