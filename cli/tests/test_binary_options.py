"""The options passed on to the binary: parsed wherever they are, checked
against the binary itself."""

from __future__ import annotations

import os
import re
import subprocess

import pytest

from conftest import FakeApi, dry_run_log, events_of
from hf_s3ream_cli import binary_options as bo
from hf_s3ream_cli.events import Exit, Fail
from hf_s3ream_cli.main import main


def test_a_value_is_never_read_as_src_or_dest():
    own, passed = bo.split(["--exclude", "*.tmp", "s3://a/", "org/b", "--exclude=*.log", "--no-verify", "--yes"])
    assert own == ["s3://a/", "org/b", "--yes"]
    assert passed == ["--exclude", "*.tmp", "--exclude", "*.log", "--no-verify"]


@pytest.mark.parametrize(
    ("argv", "hint"),
    [
        (["--hf-token", "hf_x"], "hf auth login"),
        (["--dry-run"], "--analyze"),
        (["--jobs-namespace=org"], "--namespace"),
        (["--run-label", "x"], "--label"),
        (["--no-verify=1"], "--no-verify alone"),
        (["--exclude"], "--exclude GLOB"),
    ],
)
def test_a_bad_option_says_what_to_write(argv, hint):
    with pytest.raises(Fail) as e:
        bo.split(["s3://a/", "org/b", *argv])
    assert e.value.exit_code == Exit.USAGE and hint in e.value.hint


def test_options_jobs_would_ignore_need_local(capsys):
    assert main(["s3://a/", "org/b", "--parallel-files", "64"], api=FakeApi([])) == Exit.USAGE
    ev = events_of(capsys.readouterr().out)[-1]
    assert ev["code"] == "usage" and "--parallel-files" in ev["message"] and "--local" in ev["hint"]


def test_an_unknown_option_is_a_usage_error_event(capsys):
    assert main(["s3://a/", "org/b", "--paralel-files", "64"], api=FakeApi([])) == Exit.USAGE
    ev = events_of(capsys.readouterr().out)[-1]
    assert ev["type"] == "error" and ev["code"] == "usage" and "--paralel-files" in ev["message"]


def test_passed_options_reach_the_dry_run_and_the_planner(capsys):
    api = FakeApi([dry_run_log()])
    code = main(["--exclude", "*.tmp", "s3://src/", "alice/dst", "--analyze", "--aws-region", "eu-west-3"], api=api)
    assert code == Exit.OK
    cmd = api.jobs[0]["command"]
    assert cmd[:3] == ["hf-s3ream", "s3://src/", "alice/dst"]
    assert cmd[-4:] == ["--exclude", "*.tmp", "--aws-region", "eu-west-3"]


def binary_options_in_help(text: str) -> dict[str, bool]:
    """{--option: takes a value} from the binary's --help."""
    out = {}
    for m in re.finditer(r"^\s+(?:-\w, )?(--[a-z0-9-]+)( <[A-Z0-9_]+>)?", text, re.M):
        out[m.group(1)] = bool(m.group(2))
    return out


@pytest.mark.skipif(not os.environ.get("HF_S3REAM_BIN"), reason="needs HF_S3REAM_BIN: a built hf-s3ream binary")
def test_every_option_of_the_binary_is_in_one_table():
    help_text = subprocess.run([os.environ["HF_S3REAM_BIN"], "--help"], capture_output=True, text=True).stdout
    options = binary_options_in_help(help_text)
    assert "--exclude" in options and options["--exclude"] is True
    tables = set(bo.PASSED) | set(bo.SET_BY_CLI) | set(bo.REFUSED)
    assert sorted(set(options) - tables) == [], "new binary options: add them to binary_options.py"
    assert sorted(tables - set(options)) == [], "options the binary no longer has"
    for name, opt in bo.PASSED.items():
        assert options[name] == (opt.value is not None), f"{name}: value or flag?"
