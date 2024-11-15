### Python Chris Client Implementation ###

from base_client import BaseClient
from chrisclient import client
from chris_pacs_service import PACSClient
import json

class ChrisClient(BaseClient):
    def __init__(self, url: str, username: str, password: str):
        self.cl = client.Client(url, username, password)
        self.cl.pacs_series_url = "http://localhost:8000/api/v1/pacs/series/"
        self.req = PACSClient(username,password)

    def create_con(self,params:dict):
        return self.cl

    def pacs_pull(self):
        pass
    def pacs_push(self):
        pass

    def anonymize(self, params: dict, pv_id: int):
        prefix = "dynanon"
        feed_name = self.__create_feed_name(prefix,params["search"])
        # search for dicom dir
        dicom_dir = self.req.get_pacs_files(params["search"])
        anon_params = json.dumps(params["anon"])

        # run dircopy
        pl_id = self.__get_plugin_id({"name":"pl-dsdircopy","version":"1.0.2"})
        pv_in_id = self.__create_feed(pl_id,{"previous_id":pv_id,'dir':dicom_dir,'title':feed_name})
        # run dicom_headeredit
        pl_sub_id = self.__get_plugin_id({"name":"pl-pfdicom_tagsub", "version":"3.3.4"})
        data = {"previous_id": pv_in_id, "tagStruct": anon_params, 'fileFilter': '.dcm'}
        tag_sub_id = self.__create_feed(pl_sub_id, data)
        pl_dcm_id = self.__get_plugin_id({"name":"pl-dicom_dirsend", "version":"1.2.0"})
        dir_send_data = {
            "previous_id": tag_sub_id,
            'fileFilter': "dcm",
            "host": params["send"]["host"],
            "port":params["send"]["port"],
            "calledAETitle": params["send"]["aec"]
        }
        self.__create_feed(pl_dcm_id, dir_send_data)


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



