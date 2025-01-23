import logging
import os
import time

import google.auth
import google.auth.transport.requests
import requests

logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)-23s   %(levelname)-9s:   %(message)s'
)


def get_access_token():
    credentials, project = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    access_token = credentials.token

    return access_token


def get_status(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    resp_json = response.json()
    status = resp_json.get('response', {})
    error_status = resp_json.get('error', {})

    return resp_json, status, error_status


def wait(secs):
    for i in range(secs):
        time.sleep(1)


def check_status(status, error_status, operation_name, resp_json):
    status_msg = ''

    if status:
        status_msg = f"Batch {operation_name} has been completed with state: {resp_json['response']['state']}"
        logging.info(status_msg)

    elif error_status:
        status_msg = f"""
            Batch {operation_name} FAILED with error code: {resp_json['error']['code']}
            Message: {resp_json['error']['message']}
            """
        # logging.info(status_msg)
        raise ValueError(status_msg)

    return status_msg


def main(request):
    headers = {'Authorization': f'Bearer {get_access_token()}'}

    project_id = os.environ['PROJECT_ID']
    # stage = os.environ['STAGE']
    region = os.environ['REGION']

    url = f'https://dataproc.googleapis.com/v1/projects/{project_id}/regions/{region}/operations/*'

    operation_name = request.args.get('operation_name')
    params = {'name': operation_name}

    resp_json, status, error_status = get_status(url, headers, params)

    while status == {} and error_status == {}:
        resp_json, status, error_status = get_status(url, headers, params)
        # time.sleep(1.5 * 2.1 * 3.2)
        wait(secs=60)

    status_msg = check_status(status, error_status, operation_name, resp_json)

    return (status_msg, 200)
