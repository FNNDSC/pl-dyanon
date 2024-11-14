import requests
from loguru import logger
import sys

LOG = logger.debug

logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> │ "
    "<level>{level: <5}</level> │ "
    "<yellow>{name: >28}</yellow>::"
    "<cyan>{function: <30}</cyan> @"
    "<cyan>{line: <4}</cyan> ║ "
    "<level>{message}</level>"
)
logger.remove()
logger.add(sys.stderr, format=logger_format)

def register_pacsfiles(directive: dict, url: str, pacs_name: str):
    """
    This method uses the async API endpoint of `pfdcm` to send a single 'retrieve' request that in
    turn uses `oxidicom` to push and register PACS files to a CUBE instance
    """
    pfdcm_dicom_api = f'{url}PACS/thread/pypx/'
    headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
    body = {
        "PACSservice": {
            "value": pacs_name
        },
        "listenerService": {
            "value": "default"
        },
        "PACSdirective": {
            "withFeedBack": True,
            "then": "retrieve",
            "thenArgs": '',
            "dblogbasepath": '/home/dicom/log',
            "json_response": False
        }
    }
    body["PACSdirective"].update(directive)
    LOG(body)

    try:
        response = requests.post(pfdcm_dicom_api, json=body, headers=headers)
        return response
    except Exception as er:
        print(er)


def get_pfdcm_status(directive: dict, url: str, pacs_name: str):
    """
    Get the status of PACS from `pfdcm`
    by running the synchronous API of `pfdcm`
    """

    pfdcm_status_url = f'{url}/PACS/sync/pypx/'
    headers = {'Content-Type': 'application/json', 'accept': 'application/json'}

    body = {
        "PACSservice": {
            "value": pacs_name
        },
        "listenerService": {
            "value": "default"
        },
        "PACSdirective": {
            "withFeedBack": True,
            "then": "status",
            "thenArgs": '',
            "dblogbasepath": '/home/dicom/log',
            "json_response": False
        }
    }
    body["PACSdirective"].update(directive)

    try:
        response = requests.post(pfdcm_status_url, json=body, headers=headers)
        return response
    except Exception as ex:
        print(er)
