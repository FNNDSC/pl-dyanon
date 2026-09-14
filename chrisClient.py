### Python Chris Client Implementation ###

from base_client import BaseClient
import json
import requests
from loguru import logger
import sys
from pipeline import Pipeline
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

class ChrisClient(BaseClient):
    def __init__(self, url: str, token: str):
        self.api_base = url.rstrip('/')
        self.auth = token
        self.headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}
        self.pacs_series_url = f"{url}/pacs/series/"

    def health_check(self):
        endpoint = f"{self.api_base}/"
        response = requests.request("GET", endpoint, headers=self.headers, timeout=30)

        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            return response.text

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass
    async def anonymize(self, params: dict, pv_id: int):
        pipe = Pipeline(self.api_base, self.auth)
        plugin_params = {
            'PACS-query': {
                "src_ip": params["pull"]["host"],
                "src_port": params["pull"]["port"],
                "src_aet": params["pull"]["aet"],
                "dst_aet": params["pull"]["aec"],
                "PACSdirective": json.dumps(params["search"])
            },
            'PACS-retrieve': {
                "src_ip": params["pull"]["host"],
                "src_port": params["pull"]["port"],
                "src_aet": params["pull"]["aet"],
                "dst_aet": params["pull"]["aec"],
                "inputJSONfile": "search_results.json",
                "copyInputFile": True
            },
            'verify-registration': {
                "CUBEurl": self.api_base,
                "inputJSONfile": "search_results.json",
                "tagStruct": json.dumps(params["anon"]),
                "orthancUrl": params["push"]["url"],
                "orthancUsername": params["push"]["username"],
                "orthancPassword": params["push"]["password"],
                "PFDCMurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
                "pushToRemote": params["push"]["aec"],
                "SMTPServer": params["notify"]["smtp_server"],
                "recipients": params["notify"]["recipients"],
                "preserveTags": params["preserve"]["preserveTags"],
                "imgCount": params["filter"]["imgCount"],
                "dicomFilter": params["filter"]["dicomFilter"]
            }
        }

        # If a PACS's host/port/aet were resolved (via the CUBE-FS config
        # file, see cube_pacs_config.py), pass them along to the plugins that
        # talk to PFDCM/the PACS directly, overriding whatever is statically
        # configured for this PACS name. NOTE: verify these exact parameter
        # names ("PACShost"/"PACSport"/"PACSaet") against the actual
        # PACS-query / PACS-retrieve / verify-registration plugin versions
        # pinned in your pipeline before relying on this in production.
        if "host" in params["pull"] and "port" in params["pull"] and "aet" in params["pull"]:
            pacs_overrides = {
                "src_ip": params["pull"]["host"],
                "src_port": params["pull"]["port"],
                "src_aet": params["pull"]["aet"],
                "dst_aet": params["pull"]["aec"],
            }
            plugin_params["PACS-query"].update(pacs_overrides)
            plugin_params["PACS-retrieve"].update(pacs_overrides)
            plugin_params["verify-registration"].update(pacs_overrides)

        logger.info(plugin_params)

        d_ret = await pipe.run_pipeline(
            previous_inst=pv_id,
            pipeline_name=params["pipeline"]["name"],
            pipeline_params=plugin_params)
        return d_ret