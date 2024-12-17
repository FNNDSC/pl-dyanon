### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json
import time
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
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
        self.cl.pacs_series_url = f"{url}pacs/series/"
        self.req = PACSClient(self.cl.pacs_series_url,username,password)

    def create_con(self,params:dict):
        return self.cl

    def health_check(self):
        return self.cl.get_chris_instance()

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass
    def anonymize(self, params: dict, pv_id: int):
        pipe = Pipeline(self.cl)
        plugin_params = {
            'PACS-query': {
                "PACSurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
                "PACSdirective": json.dumps(params["search"])
            },
            'PACS-retrieve': {
                "PACSurl": params["pull"]["url"],
                "PACSname": params["pull"]["pacs"],
                "inputJSONfile": "search_results.json",
                "copyInputFile": True
            },
            'verify-registration': {
                "CUBEurl": self.cl.url,
                "CUBEuser": self.cl.username,
                "CUBEpassword": self.cl.password,
                "inputJSONfile": "search_results.json",
                "tagStruct": json.dumps(params["anon"]),
                "orthancUrl": params["push"]["url"],
                "orthancUsername": params["push"]["username"],
                "orthancPassword": params["push"]["password"],
                "pushToRemote": params["push"]["aec"]
            }
        }
        pipe.workflow_schedule(pv_id,"PACS query, retrieve, and registration verification in CUBE 20241217",plugin_params)
        # workflow = pipe.pipelineWithName_getNodes("PACS query, retrieve, and registration verification in CUBE 20241217",{})
    def anonymize_(self, params: dict, pv_id: int):
        prefix = "dynanon"
        pl_px_qy_id = self.__get_plugin_id({"name": "pl-pacs_query", "version": "1.0.3"})
        pl_px_rt_id = self.__get_plugin_id({"name": "pl-pacs_retrieve", "version": "1.0.2"})
        pl_rg_ch_id = self.__get_plugin_id({"name": "pl-reg_chxr", "version": "1.0.7"})

        # 1) Run PACS query
        px_qy_params = {"PACSurl": params["pull"]["url"],
                        "PACSname": params["pull"]["pacs"],
                        "PACSdirective": json.dumps(params["search"]),
                        "previous_id" : pv_id
                        }
        px_qy_inst_id = self.__create_feed(pl_px_qy_id, px_qy_params)

        # 2) Run PACS retrieve
        px_rt_params = {"PACSurl": params["pull"]["url"],
                        "PACSname": params["pull"]["pacs"],
                        "inputJSONfile": "search_results.json",
                        "copyInputFile": True,
                        "previous_id": px_qy_inst_id
                        }
        px_rt_inst_id = self.__create_feed(pl_px_rt_id, px_rt_params)

        # 3) Verify registration and run anonymize and push
        anon_params = json.dumps(params["anon"])
        rg_ch_params = {"CUBEurl": self.cl.url,
                        "CUBEuser": self.cl.username,
                        "CUBEpassword": self.cl.password,
                        "inputJSONfile": "search_results.json",
                        "tagStruct": anon_params,
                        "orthancUrl": params["push"]["url"],
                        "username": params["push"]["username"],
                        "password": params["push"]["password"],
                        "pushToRemote": params["push"]["aec"],
                        "previous_id": px_rt_inst_id}
        rg_ch_inst_id = self.__create_feed(pl_rg_ch_id, rg_ch_params)

    def __wait_for_node(self,pl_inst_id):
        """
        Wait for a node to transition to a finishedState
        """
        response = self.cl.get_plugin_instance_by_id(pl_inst_id)
        poll_count = 0
        total_polls = 50
        wait_poll = 5
        while 'finished' not in response['status'] and poll_count <= total_polls:
            response = self.cl.get_plugin_instance_by_id(pl_inst_id)
            time.sleep(wait_poll)
            poll_count += 1

    def __create_feed(self, plugin_id: str,params: dict):
        response = self.cl.create_plugin_instance(plugin_id, params)
        return response['id']

    def __get_plugin_id(self, params: dict):
        response = self.cl.get_plugins(params)
        if response['total'] > 0:
            return response['data'][0]['id']
        raise Exception(f"No plugin found with matching search criteria {params}")





