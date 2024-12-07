import requests
from chrisclient import request


class PACSClient(object):
    def __init__(self, url: str, username: str, password: str):
        self.cl = request.Request(username, password)
        self.pacs_series_search_url = f"{url}search/"


    def get_pacs_files(self, params: dict):
        l_dir_path = set()
        resp = self.cl.get(self.pacs_series_search_url,params)
        for item in resp.items:
            for link in item.links:
                folder = self.cl.get(link.href)
                for item_folder in folder.items:
                    path = item_folder.data.path.value
                    l_dir_path.add(path)
        return ','.join(l_dir_path)
