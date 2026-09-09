
import asyncio
from types import SimpleNamespace

import pandas as pd
import pytest

from dyanon import (
    _get_or_env,
    create_query,
    register_and_anonymize,
)


# ---------------------------------------------------------------------------
# _get_or_env
# ---------------------------------------------------------------------------

def test_get_or_env_returns_explicit_value(monkeypatch):
    monkeypatch.setenv("TEST_VALUE", "from-env")

    result = _get_or_env("explicit", "TEST_VALUE")

    assert result == "explicit"


def test_get_or_env_returns_environment_value(monkeypatch):
    monkeypatch.setenv("TEST_VALUE", "from-env")

    result = _get_or_env("", "TEST_VALUE")

    assert result == "from-env"


def test_get_or_env_raises_when_value_and_environment_missing(monkeypatch):
    monkeypatch.delenv("TEST_VALUE", raising=False)

    with pytest.raises(KeyError):
        _get_or_env("", "TEST_VALUE")


# ---------------------------------------------------------------------------
# create_query
# ---------------------------------------------------------------------------

def test_create_query_single_row():
    df = pd.DataFrame(
        {
            "search_PatientID": ["12345"],
            "search_StudyDate": ["20260101"],
            "anon_PatientName": ["ANON"],
            "anon_PatientID": ["ABC001"],
        }
    )

    jobs = create_query(df)

    assert jobs == [
        {
            "search": {
                "PatientID": "12345",
                "StudyDate": "20260101",
            },
            "anon": {
                "PatientName": "ANON",
                "PatientID": "ABC001",
            },
        }
    ]


def test_create_query_multiple_rows():
    df = pd.DataFrame(
        {
            "PACS": ["BCH_PROD", "BCH_RESEARCH"],
            "search_PatientID": ["111", "222"],
            "anon_PatientName": ["PATIENT_A", "PATIENT_B"],
        }
    )

    jobs = create_query(df)

    assert len(jobs) == 2

    assert jobs[0] == {
        "pacs": "BCH_PROD",
        "search": {
            "PatientID": "111",
        },
        "anon": {
            "PatientName": "PATIENT_A",
        },
    }

    assert jobs[1] == {
        "pacs": "BCH_RESEARCH",
        "search": {
            "PatientID": "222",
        },
        "anon": {
            "PatientName": "PATIENT_B",
        },
    }


def test_create_query_ignores_unrelated_columns():
    df = pd.DataFrame(
        {
            "PACS": ["BCH_PROD"],
            "search_PatientID": ["123"],
            "anon_PatientName": ["ANON"],
            "notes": ["ignore me"],
            "project": ["test"],
        }
    )

    jobs = create_query(df)

    assert jobs == [
        {
            "pacs": "BCH_PROD",
            "search": {
                "PatientID": "123",
            },
            "anon": {
                "PatientName": "ANON",
            },
        }
    ]


def test_create_query_column_detection_is_case_insensitive():
    df = pd.DataFrame(
        {
            "PACS": ["BCH_PROD"],
            "SEARCH_PatientID": ["123"],
            "ANON_PatientName": ["ANON"],
        }
    )

    jobs = create_query(df)

    assert jobs[0]["pacs"] == "BCH_PROD"

    assert jobs[0]["search"] == {
        "PatientID": "123",
    }

    assert jobs[0]["anon"] == {
        "PatientName": "ANON",
    }


def test_create_query_removes_column_suffix_after_dot():
    df = pd.DataFrame(
        {
            "PACS": ["BCH_PROD"],
            "search_PatientID.string": ["123"],
            "anon_PatientName.string": ["ANON"],
        }
    )

    jobs = create_query(df)

    assert jobs[0] == {
        "pacs": "BCH_PROD",
        "search": {
            "PatientID": "123",
        },
        "anon": {
            "PatientName": "ANON",
        },
    }


def test_create_query_with_only_search_columns():
    df = pd.DataFrame(
        {
            "PACS": ["BCH_PROD"],
            "search_PatientID": ["123"],
        }
    )

    jobs = create_query(df)

    assert jobs == [
        {
            "pacs": "BCH_PROD",
            "search": {
                "PatientID": "123",
            },
            "anon": {},
        }
    ]


def test_create_query_with_only_anon_columns():
    df = pd.DataFrame(
        {
            "PACS": ["BCH_PROD"],
            "anon_PatientName": ["ANON"],
        }
    )

    jobs = create_query(df)

    assert jobs == [
        {
            "pacs": "BCH_PROD",
            "search": {},
            "anon": {
                "PatientName": "ANON",
            },
        }
    ]


def test_create_query_empty_dataframe():
    df = pd.DataFrame(
        columns=[
            "PACS",
            "search_PatientID",
            "anon_PatientName",
        ]
    )

    jobs = create_query(df)

    assert jobs == []


# ---------------------------------------------------------------------------
# register_and_anonymize
# ---------------------------------------------------------------------------

def make_options():
    return SimpleNamespace(
        PFDCMurl="http://pfdcm:4005/api/v1/",
        PACSname="MINICHRISORTHANC",
        recipients="user@example.com",
        SMTPServer="smtp.example.org",
        orthancUrl="http://orthanc:8042",
        orthancUsername="orthanc",
        orthancPassword="password",
        pushToRemote="REMOTE_PACS",
        preserveTags="PatientSex,StudyDescription",
        imgCount=">10",
        dicomFilter="Modality=MR",
        pipelineName="test-pipeline",
        pluginInstanceID=123,
    )


class FakeChrisClient:

    def __init__(self):
        self.received_params = None
        self.received_pv_id = None

    async def anonymize(self, params, pv_id):
        self.received_params = params
        self.received_pv_id = pv_id

        return {
            "leaf_node_id": 999,
        }


def test_register_and_anonymize_builds_expected_job():
    options = make_options()
    cube = FakeChrisClient()

    job = {
        "search": {
            "PatientID": "12345",
        },
        "anon": {
            "PatientName": "ANON",
        },
    }

    result = asyncio.run(
        register_and_anonymize(
            options,
            job,
            cube,
        )
    )

    assert result == {
        "leaf_node_id": 999,
    }

    assert cube.received_pv_id == 123

    assert job["pull"] == {
        "url": "http://pfdcm:4005/api/v1/",
        "pacs": "MINICHRISORTHANC",
    }

    assert job["notify"] == {
        "recipients": "user@example.com",
        "smtp_server": "smtp.example.org",
    }

    assert job["push"] == {
        "url": "http://orthanc:8042",
        "username": "orthanc",
        "password": "password",
        "aec": "REMOTE_PACS",
        "wait": False,
    }

    assert job["preserve"] == {
        "preserveTags": "PatientSex,StudyDescription",
    }

    assert job["filter"] == {
        "imgCount": ">10",
        "dicomFilter": "Modality=MR",
    }

    assert job["pipeline"] == {
        "name": "test-pipeline",
    }


def test_register_and_anonymize_passes_wait_true():
    options = make_options()
    cube = FakeChrisClient()

    job = {
        "search": {},
        "anon": {},
    }

    asyncio.run(
        register_and_anonymize(
            options,
            job,
            cube,
            wait=True,
        )
    )

    assert job["push"]["wait"] is True


def test_register_and_anonymize_uses_current_global_pacs_name():
    """
    Baseline behavior before adding per-row PACS selection.

    This test is particularly useful for the new feature branch because
    it documents what the plugin does today.
    """
    options = make_options()
    options.PACSname = "BCH_TEST_PACS"

    cube = FakeChrisClient()

    job = {
        "search": {},
        "anon": {},
    }

    asyncio.run(
        register_and_anonymize(
            options,
            job,
            cube,
        )
    )

    assert job["pull"]["pacs"] == "BCH_TEST_PACS"


def test_register_and_anonymize_propagates_client_error():
    options = make_options()

    class FailingClient:
        async def anonymize(self, params, pv_id):
            raise RuntimeError("pipeline failed")

    with pytest.raises(RuntimeError, match="pipeline failed"):
        asyncio.run(
            register_and_anonymize(
                options,
                {
                    "search": {},
                    "anon": {},
                },
                FailingClient(),
            )
        )