from pathlib import Path
import pytest

from dyanon import parser, main


def test_help_output_documents_every_option(capsys):
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for flag in (
        "--pluginInstanceID",
        "--CUBEurl",
        "--CUBEtoken",
        "--maxThreads",
        "--thread",
        "--wait",
        "--PFDCMurl",
        "--PACSname",
        "--recipients",
        "--pipelineName",
        "--imgCount",
        "--dicomFilter"

    ):
        assert flag in out
