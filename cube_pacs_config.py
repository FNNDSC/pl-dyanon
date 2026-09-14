"""
cube_pacs_config.py
====================
Resolves PACS connection details (host/port/aet) for a given PACS key by
reading a JSON configuration file that lives in CUBE's own file storage
(a user-uploaded file, browsable/searchable via CUBE's `userfiles` REST
resource -- what this plugin refers to as "the CUBE FS").

This lets a single CSV drive searches against *multiple* PACS servers:
each row names a PACS key (see the "PACS" CSV column in dyanon.py), and
that key is looked up in the CUBE-hosted config file to get the host,
port, and AE title to substitute into the PACS-query / PACS-retrieve /
verify-registration pipeline parameters.

Expected shape of the config file (a plain JSON object keyed by PACS name):

    {
        "MINICHRISORTHANC": {"host": "0.0.0.0", "port": 4242, "aet": "ORTHANC"},
        "SOMEHOSPITALPACS":  {"host": "10.0.1.5", "port": 104,  "aet": "HOSPITALPACS"}
    }

The file itself must already exist somewhere in the requesting user's CUBE
file space (e.g. uploaded via `chris` CLI, `pfurl`, or the ChRIS_ui file
browser) before this plugin runs. Path matching is exact and is whatever
CUBE reports as `fname` for the file (e.g. "home/rudolph/uploads/pacs_config.json").
"""

import json
import sys

import requests
from loguru import logger
from requests.exceptions import RequestException, Timeout, HTTPError
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

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

REQUIRED_FIELDS = {"host", "port", "aet"}


class PACSConfigError(Exception):
    """Raised whenever a PACS key can't be resolved to connection details."""
    pass


class CubePacsConfig:
    """
    Fetches and caches a JSON PACS-config file from CUBE's file storage, and
    resolves individual PACS keys to their {host, port, aet} connection info.
    """

    def __init__(self, cube_url: str, token: str, config_path: str):
        self.api_base = cube_url.rstrip('/')
        self.token = token
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {token}",
        }
        self.config_path = config_path
        self._cache = None  # lazily populated on first lookup

    @retry(
        retry=retry_if_exception_type((RequestException, Timeout, HTTPError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _get(self, url: str, **kwargs) -> requests.Response:
        response = requests.get(url, headers=self.headers, timeout=30, **kwargs)
        response.raise_for_status()
        return response

    @staticmethod
    def _item_field(item: dict, field_name: str):
        """Pull a scalar field's value out of a collection+json item's 'data' list."""
        for field in item.get("data", []):
            if field.get("name") == field_name:
                return field.get("value")
        return None

    @staticmethod
    def _item_link(item: dict, rel: str):
        """Pull a link's href out of a collection+json item's 'links' list."""
        for link in item.get("links", []):
            if link.get("rel") == rel:
                return link.get("href")
        return None

    def _find_folder(self, folder_path: str) -> dict:
        """
        Search CUBE's filebrowser API for the folder at an exact path, and
        return that folder's collection+json item.
        """
        search_url = f"{self.api_base}/filebrowser/search/"
        LOG(f"Searching CUBE FS for folder: {folder_path}")
        response = self._get(search_url, params={"path": folder_path})

        try:
            items = response.json().get("collection", {}).get("items", [])
        except ValueError as ex:
            raise PACSConfigError(
                f"Unexpected response from CUBE while searching for folder "
                f"'{folder_path}': {ex}"
            )

        if not items:
            raise PACSConfigError(
                f"No folder found in CUBE FS matching path '{folder_path}'. "
                f"Make sure --PACSconfigFile points at a path that exists in "
                f"your CUBE file space."
            )

        return items[0]

    def _list_folder_files(self, folder_item: dict) -> list:
        """
        Follow a folder item's 'files' link and return every file item found
        there (collection+json items), following pagination if present.
        """
        files_url = self._item_link(folder_item, "files")
        if files_url is None:
            raise PACSConfigError(
                f"Folder record for '{self.config_path}' has no 'files' link "
                f"to list its contents."
            )

        l_items = []
        url = files_url
        # Follow pagination defensively; a config folder shouldn't realistically
        # have more than a handful of pages of files in it.
        for _ in range(50):
            if not url:
                break
            response = self._get(url)
            try:
                collection = response.json().get("collection", {})
            except ValueError as ex:
                raise PACSConfigError(
                    f"Unexpected response from CUBE while listing files under "
                    f"'{self.config_path}': {ex}"
                )
            l_items.extend(collection.get("items", []))
            url = None
            for link in collection.get("links", []):
                if link.get("rel") == "next":
                    url = link.get("href")
                    break

        return l_items

    def _fetch_config_file_json(self) -> dict:
        """
        Resolve --PACSconfigFile to an actual file in CUBE's file storage and
        download/parse its contents as JSON.

        --PACSconfigFile may point directly at a file (e.g.
        "home/chris/uploads/config/pacs_config.json"), or at a folder (e.g.
        "home/chris/uploads/config" or ".../config/") -- in which case that
        folder must contain exactly one file, which is used.
        """
        normalized = self.config_path.strip("/")
        is_folder_only = self.config_path.endswith("/") or "." not in normalized.rsplit("/", 1)[-1]

        if is_folder_only:
            folder_path, file_name = normalized, None
        else:
            folder_path, _, file_name = normalized.rpartition("/")

        folder_item = self._find_folder(folder_path)
        file_items = self._list_folder_files(folder_item)

        if not file_items:
            raise PACSConfigError(
                f"No files found in CUBE FS folder '{folder_path}'. Make sure "
                f"the PACS config JSON file has been uploaded there."
            )

        if file_name:
            matches = [
                item for item in file_items
                if str(self._item_field(item, "fname") or "").rsplit("/", 1)[-1] == file_name
            ]
            if not matches:
                l_found = [self._item_field(item, "fname") for item in file_items]
                raise PACSConfigError(
                    f"No file named '{file_name}' found under CUBE FS folder "
                    f"'{folder_path}'. Files found there: {l_found}"
                )
            target_item = matches[0]
        elif len(file_items) == 1:
            target_item = file_items[0]
        else:
            l_found = [self._item_field(item, "fname") for item in file_items]
            raise PACSConfigError(
                f"--PACSconfigFile ('{self.config_path}') names a folder "
                f"containing multiple files; point it at the specific JSON "
                f"file instead. Files found there: {l_found}"
            )

        file_resource_url = self._item_link(target_item, "file_resource")
        if file_resource_url is None:
            raise PACSConfigError(
                f"CUBE file record for "
                f"'{self._item_field(target_item, 'fname')}' has no "
                f"downloadable file_resource link."
            )

        content_response = self._get(file_resource_url)

        try:
            return content_response.json()
        except ValueError:
            try:
                return json.loads(content_response.text)
            except json.JSONDecodeError as ex:
                raise PACSConfigError(
                    f"PACS config file '{self.config_path}' is not valid JSON: {ex}"
                )

    def load(self, force: bool = False) -> dict:
        """
        Fetch (or return the cached copy of) the full PACS config mapping.
        """
        if self._cache is None or force:
            self._cache = self._fetch_config_file_json()
            LOG(f"Loaded PACS config for keys: {list(self._cache.keys())}")
        return self._cache

    def get_pacs_details(self, pacs_key: str) -> dict:
        """
        Resolve a single PACS key to its {host, port, aet} connection details.
        Raises PACSConfigError if the key is missing or incomplete.
        """
        if not pacs_key:
            raise PACSConfigError("No PACS key was provided to resolve.")

        config = self.load()
        details = config.get(pacs_key)

        if details is None:
            raise PACSConfigError(
                f"No PACS configuration found for key '{pacs_key}' in "
                f"'{self.config_path}'. Known keys: {list(config.keys())}"
            )

        missing = REQUIRED_FIELDS - details.keys()
        if missing:
            raise PACSConfigError(
                f"PACS configuration for '{pacs_key}' is missing required "
                f"field(s): {sorted(missing)}"
            )

        return {
            "host": details["host"],
            "port": details["port"],
            "aet": details["aet"],
            "aec": details["aec"],
        }