### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json
import time
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
        prefix = "dynanon"
        pl_px_qy_id = self.__get_plugin_id({"name": "pl-pacs_query", "version": "1.0.3"})
        pl_px_rt_id = self.__get_plugin_id({"name": "pl-pacs_retrieve", "version": "1.0.2"})
        pl_rg_ch_id = self.__get_plugin_id({"name": "pl-reg_chxr", "version": "1.0.2"})
        pl_id = self.__get_plugin_id({"name": "pl-dsdircopy", "version": "1.0.2"})
        pl_sub_id = self.__get_plugin_id({"name": "pl-pfdicom_tagsub", "version": "3.3.4"})
        pl_dcm_id = self.__get_plugin_id({"name": "pl-orthanc_push", "version": "1.2.7"})

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

        # 3) Verify registration
        rg_ch_params = {"CUBEurl": self.cl.url,
                        "CUBEuser": self.cl.username,
                        "CUBEpassword": self.cl.password,
                        "inputJSONfile": "search_results.json",
                        "previous_id": px_rt_inst_id}
        rg_ch_inst_id = self.__create_feed(pl_rg_ch_id, rg_ch_params)

        # 4) Run dircopy
        dicom_dir = self.req.get_pacs_files(params["search"])

        # empty directory check
        if len(dicom_dir) == 0:
            LOG(f"No directory found in CUBE containing files for search : {params['search']}")
            return
        pv_in_id = self.__create_feed(pl_id, {"previous_id": rg_ch_inst_id, 'dir': dicom_dir})

        # 5) Run anonymization
        anon_params = json.dumps(params["anon"])
        data = {"previous_id": pv_in_id, "tagStruct": anon_params, 'fileFilter': '.dcm'}
        tag_sub_id = self.__create_feed(pl_sub_id, data)

        # 6) Push results
        dir_send_data = {
            "previous_id": tag_sub_id,
            'inputFileFilter': "**/*dcm",
            "orthancUrl": params["push"]["url"],
            "username":params["push"]["username"],
            "password": params["push"]["password"],
            "pushToRemote": params["push"]["aec"]
        }
        pl_inst_id = self.__create_feed(pl_dcm_id, dir_send_data)

        # feed_name = self.__create_feed_name(prefix,params["search"])
        # # search for dicom dir
        # dicom_dir = self.req.get_pacs_files(params["search"])
        # anon_params = json.dumps(params["anon"])
        #
        # # run dircopy
        #
        # pv_in_id = self.__create_feed(pl_id,{"previous_id":pv_id,'dir':dicom_dir})
        # # run dicom_headeredit
        #
        # data = {"previous_id": pv_in_id, "tagStruct": anon_params, 'fileFilter': '.dcm'}
        # tag_sub_id = self.__create_feed(pl_sub_id, data)
        #
        # dir_send_data = {
        #     "previous_id": tag_sub_id,
        #     'inputFileFilter': "**/*dcm",
        #     "orthancUrl": params["send"]["url"],
        #     "username":params["send"]["username"],
        #     "password": params["send"]["password"],
        #     "pushToRemote": params["send"]["aec"]
        # }
        # pl_inst_id = self.__create_feed(pl_dcm_id, dir_send_data)
        # if params["send"]["wait"]:
        #     self.__wait_for_node(pl_inst_id)

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

    def __create_feed_name(self, prefix: str, params: dict) -> str:
        name = ""
        for val in params.values():
            name += f"-{val}"
        return  f"{prefix}{name}"

    def __get_plugin_id(self, params: dict):
        response = self.cl.get_plugins(params)
        if response['total'] > 0:
            return response['data'][0]['id']
        raise Exception(f"No plugin found with matching search criteria {params}")



