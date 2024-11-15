import requests
from loguru import logger
import sys
import copy
from collections import ChainMap
import json

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


def sanitize(directive: dict) -> (dict, dict):
    """
    Remove any field that contains name or description
    as pfdcm doesn't allow partial text search and these fields
    may contain partial text.
    """
    partial_directive = []
    clone_directive = copy.deepcopy(directive)
    for key in directive.keys():
        if "Name" in key or "Description" in key:
            partial_directive.append({key:clone_directive.pop(key)})
    return clone_directive, dict(ChainMap(*partial_directive))

def autocomplete_directive(directive: dict, response: dict) -> (dict,int):
    """
    Autocomplete certain fields in the search directive using response
    object from pfdcm
    """
    new_dir = copy.deepcopy(directive)
    _,partial_directive = sanitize(directive)
    file_count = 0
    d_response = json.loads(response.text)
    for l_series in d_response['pypx']['data']:
        for series in l_series["series"]:
            # iteratively check for all search fields and update the search record simultaneously
            for key in directive.keys():
                if directive[key].lower() in series[key]["value"].lower():
                    partial_directive[key] = series[key]["value"]
                    _["SeriesInstanceUID"] = series["SeriesInstanceUID"]["value"]
                else:
                    return _, file_count
            file_count += int(series["NumberOfSeriesRelatedInstances"]["value"])
    # _.update(partial_directive)
    return _, file_count

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

    pfdcm_status_url = f'{url}PACS/sync/pypx/'
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
    LOG(body)

    try:
        response = requests.post(pfdcm_status_url, json=body, headers=headers)
        return response
    except Exception as ex:
        print(ex)



