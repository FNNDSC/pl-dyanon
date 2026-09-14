import json
from unittest.mock import MagicMock, patch

import pytest

from cube_pacs_config import CubePacsConfig, PACSConfigError

CUBE_URL = "http://localhost:8000/api/v1/"
TOKEN = "sometoken"
FOLDER_PATH = "home/chris/uploads/config"
FILE_NAME = "pacs_config.json"
FILES_URL = f"{CUBE_URL}filebrowser/1778116/files/"
FILE_RESOURCE_URL = f"{CUBE_URL}files/1/abc/"

VALID_CONFIG = {
    "HOSPITALPACS": {"host": "10.0.1.5", "port": 104, "aet": "HOSPITALPACS", "aec": "CHRIS"},
    "MINICHRISORTHANC": {"host": "0.0.0.0", "port": 4242, "aet": "ORTHANC", "aec": "CHRISLOCAL"},
}


def _folder_search_response(found: bool = True):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    if found:
        resp.json.return_value = {
            "collection": {
                "items": [
                    {
                        "data": [
                            {"name": "id", "value": 1778116},
                            {"name": "path", "value": FOLDER_PATH},
                        ],
                        "links": [
                            {"rel": "files", "href": FILES_URL},
                        ],
                    }
                ]
            }
        }
    else:
        resp.json.return_value = {"collection": {"items": []}}
    return resp


def _files_listing_response(fnames, next_url=None):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    items = []
    for i, fname in enumerate(fnames):
        items.append({
            "data": [
                {"name": "id", "value": i},
                {"name": "fname", "value": fname},
            ],
            "links": [
                {"rel": "file_resource", "href": f"{FILE_RESOURCE_URL}{i}/"},
            ],
        })
    collection = {"items": items, "links": []}
    if next_url:
        collection["links"].append({"rel": "next", "href": next_url})
    resp.json.return_value = {"collection": collection}
    return resp


def _content_response(payload: dict):
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = payload
    return resp


@patch("cube_pacs_config.requests.get")
def test_get_pacs_details_success_with_explicit_filename(mock_get):
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([f"{FOLDER_PATH}/{FILE_NAME}"]),
        _content_response(VALID_CONFIG),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    details = cfg.get_pacs_details("HOSPITALPACS")

    assert details == {"host": "10.0.1.5", "port": 104, "aet": "HOSPITALPACS", "aec": "CHRIS"}
    assert mock_get.call_count == 3


@patch("cube_pacs_config.requests.get")
def test_get_pacs_details_success_with_folder_only_single_file(mock_get):
    # Mirrors real-world usage: --PACSconfigFile pointed at a folder path
    # (trailing slash) that contains exactly one file.
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([f"{FOLDER_PATH}/{FILE_NAME}"]),
        _content_response(VALID_CONFIG),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/")
    details = cfg.get_pacs_details("MINICHRISORTHANC")

    assert details == {"host": "0.0.0.0", "port": 4242, "aet": "ORTHANC", "aec": "CHRISLOCAL"}


@patch("cube_pacs_config.requests.get")
def test_folder_with_multiple_files_and_no_filename_raises(mock_get):
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([
            f"{FOLDER_PATH}/pacs_config.json",
            f"{FOLDER_PATH}/notes.txt",
        ]),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("HOSPITALPACS")


@patch("cube_pacs_config.requests.get")
def test_config_file_is_cached_across_lookups(mock_get):
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([f"{FOLDER_PATH}/{FILE_NAME}"]),
        _content_response(VALID_CONFIG),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    cfg.get_pacs_details("HOSPITALPACS")
    cfg.get_pacs_details("MINICHRISORTHANC")

    # Only one folder search + one files listing + one download, despite two lookups
    assert mock_get.call_count == 3


@patch("cube_pacs_config.requests.get")
def test_missing_folder_raises(mock_get):
    mock_get.side_effect = [_folder_search_response(found=False)]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("HOSPITALPACS")


@patch("cube_pacs_config.requests.get")
def test_missing_filename_in_folder_raises(mock_get):
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([f"{FOLDER_PATH}/other.json"]),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("HOSPITALPACS")


@patch("cube_pacs_config.requests.get")
def test_empty_folder_raises(mock_get):
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([]),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("HOSPITALPACS")


@patch("cube_pacs_config.requests.get")
def test_unknown_pacs_key_raises(mock_get):
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([f"{FOLDER_PATH}/{FILE_NAME}"]),
        _content_response(VALID_CONFIG),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("SOMEUNKNOWNPACS")


@patch("cube_pacs_config.requests.get")
def test_incomplete_pacs_entry_raises(mock_get):
    incomplete_config = {"HOSPITALPACS": {"host": "10.0.1.5", "aet": "HOSPITALPACS"}}
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response([f"{FOLDER_PATH}/{FILE_NAME}"]),
        _content_response(incomplete_config),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("HOSPITALPACS")


@patch("cube_pacs_config.requests.get")
def test_files_listing_follows_pagination(mock_get):
    next_url = f"{FILES_URL}?page=2"
    mock_get.side_effect = [
        _folder_search_response(),
        _files_listing_response(["home/chris/uploads/config/other.json"], next_url=next_url),
        _files_listing_response([f"{FOLDER_PATH}/{FILE_NAME}"]),
        _content_response(VALID_CONFIG),
    ]

    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    details = cfg.get_pacs_details("HOSPITALPACS")

    assert details == {"host": "10.0.1.5", "port": 104, "aet": "HOSPITALPACS", "aec": "CHRIS"}
    assert mock_get.call_count == 4


def test_empty_pacs_key_raises():
    cfg = CubePacsConfig(CUBE_URL, TOKEN, f"{FOLDER_PATH}/{FILE_NAME}")
    with pytest.raises(PACSConfigError):
        cfg.get_pacs_details("")