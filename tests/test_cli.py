from duckdb_utils import example_function, cli
from click.testing import CliRunner
from pathlib import Path
import subprocess
import sys
from unittest import mock
import json
import os
import pytest
import textwrap


@pytest.mark.parametrize(
    "options",
    (
        ["-h"],
        ["--help"],
        ["insert", "-h"],
        ["insert", "--help"],
    ),
)
def test_help(options):
    result = CliRunner().invoke(cli.cli, options)
    assert result.exit_code == 0
    assert result.output.startswith("Usage: ")
    assert "-h, --help" in result.output
