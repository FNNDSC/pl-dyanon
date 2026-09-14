import pandas as pd

from dyanon import create_query


def _base_df(extra_columns: dict) -> pd.DataFrame:
    data = {
        "search_PatientID": ["1234"],
        "anon_PatientID": ["anon-1234"],
    }
    data.update(extra_columns)
    return pd.DataFrame(data)


def test_create_query_extracts_pacs_column_per_row():
    df = _base_df({"PACS": ["HOSPITALPACS"]})
    l_job = create_query(df)

    assert len(l_job) == 1
    assert l_job[0]["pacs_key"] == "HOSPITALPACS"
    assert l_job[0]["search"] == {"PatientID": "1234"}
    assert l_job[0]["anon"] == {"PatientID": "anon-1234"}


def test_create_query_pacs_column_is_case_and_whitespace_insensitive():
    df = _base_df({" pacs ": ["  MINICHRISORTHANC  "]})
    l_job = create_query(df)

    assert l_job[0]["pacs_key"] == "MINICHRISORTHANC"


def test_create_query_defaults_pacs_key_to_empty_when_column_missing():
    df = _base_df({})
    l_job = create_query(df)

    assert l_job[0]["pacs_key"] == ""


def test_create_query_defaults_pacs_key_to_empty_when_cell_blank():
    df = _base_df({"PACS": [""]})
    l_job = create_query(df)

    assert l_job[0]["pacs_key"] == ""


def test_create_query_pacs_column_excluded_from_search_directive():
    df = _base_df({"PACS": ["HOSPITALPACS"]})
    l_job = create_query(df)

    assert "PACS" not in l_job[0]["search"]
    assert "PACS" not in l_job[0]["anon"]