import requests

def register_pacsfiles(directive: dict, url: str, pacs_name: str):
    """
    This method uses the async API endpoint of `pfdcm` to send a single 'retrieve' request that in
    turn uses `oxidicom` to push and register PACS files to a CUBE instance
    """
    pfdcm_dicom_api = f'{url}/PACS/thread/pypx/'
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

    try:
        response = requests.post(pfdcm_dicom_api, json=body, headers=headers)
        return response
    except Exception as er:
        print(er)