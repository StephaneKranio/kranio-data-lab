import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)-23s   %(levelname)-9s:   %(message)s'
)

current_date = datetime.now()


def get_pipeline_date(request):
    request_json = request.get_json()

    pipeline_date = current_date.strftime('%Y-%m-%d')
    if 'pipeline_date' in request_json:
        pipeline_date = request_json['pipeline_date']

    return pipeline_date


def main(request):
    pipeline_date = get_pipeline_date(request)
    execution_date = current_date.strftime('%Y-%m-%d %H:%M:%S')
    dataproc_formatted_date = current_date.strftime('%Y%m%d-%H%M%S')

    response = {
        'pipeline_date': pipeline_date,
        'execution_date': execution_date,
        'dataproc_formatted_date': dataproc_formatted_date,
    }

    logging.info(f'response: {response}')

    return json.dumps(response), 200, {'Content-Type': 'application/json'}
