"""What a review of the branch found (simplicity, reliability), as tests."""

from __future__ import annotations

import shlex

from conftest import RESULT, FakeApi, dry_run_log, events_of, json_line
from hf_s3ream_cli import IMAGE
from hf_s3ream_cli.events import Exit
from hf_s3ream_cli.flow import Follow, Options, resume_command
from hf_s3ream_cli.hub import Run
from test_contract import copy, session


def test_skipped_files_count_once():
    """A completed range counts its skipped files already (`*_done`):
    adding the skipped ones again counted every existing file twice."""
    f = Follow(Run("r", "ns", None), started_epoch=0)
    # Every file already at the destination: nothing copied, all skipped.
    f.snap = {"ranges": 1, "done": 1, "files_done": 8, "bytes_done": 2000, "files_planned": 8,
              "bytes_planned": 2000, "listing_done": True, "files_copied": 0, "bytes_copied": 0,
              "committed": 0, "skipped": 8, "skipped_bytes": 2000}  # fmt: skip
    p = f.progress()
    assert (p["files_done_total"], p["bytes_done_total"], p["pct"]) == (8, 2000, 100.0)
    assert (p["files_copied"], p["bytes_copied"]) == (0, 0)
    # Half copied, half skipped, nothing completed yet: the live counters add up.
    f.snap.update(done=0, files_done=0, bytes_done=0, files_copied=4, bytes_copied=1000, committed=4,
                  skipped=4, skipped_bytes=1000)  # fmt: skip
    p = f.progress()
    assert (p["files_done_total"], p["bytes_done_total"]) == (8, 2000)


def test_an_ended_label_is_not_reused():
    """A label names one copy: the cost and `status` then mean that copy."""
    api = FakeApi([dry_run_log(), [RESULT]])
    copy(api, yes=True, detach=True, label="nightly")
    api.infos[api.jobs[1]["id"]].status.stage = "COMPLETED"
    code, evs, _ = copy(api, yes=True, label="nightly")
    assert code == Exit.ALREADY_RUNNING
    assert evs[-1]["code"] == "label_used" and "hf s3ream status nightly" in evs[-1]["hint"]
    assert len(api.jobs) == 2  # nothing started


def test_a_failed_start_can_be_retried_with_its_label():
    """Only a planner takes the label: a dry-run that failed does not."""
    api = FakeApi([["Error: boom"], dry_run_log()])
    code, _, _ = copy(api, yes=True, analyze_only=True, label="nightly")
    assert code == Exit.SOURCE
    code, _, _ = copy(api, yes=True, analyze_only=True, label="nightly")
    assert code == Exit.OK


def test_the_resume_command_is_the_same_copy():
    o = Options(source="s3://my bucket/x/", dest="org/dst", anonymous=True, namespace="org", range_gib=8,
                extra=("--exclude", "*.tmp"))  # fmt: skip
    argv = shlex.split(resume_command(o))
    assert argv[:5] == ["hf", "s3ream", "s3://my bucket/x/", "org/dst", "--skip-existing"]
    assert "--no-sign-request" in argv and argv[-2:] == ["--exclude", "*.tmp"]
    assert argv[argv.index("--namespace") + 1] == "org" and argv[argv.index("--range-gib") + 1] == "8"
    assert "--image" not in argv and IMAGE not in argv


def test_attach_to_a_planner_stopped_by_a_signal_is_stopped():
    """A planner that catches SIGTERM prints PLAN_STOPPED, then exits non-zero:
    its stage is ERROR. `status` and `attach` must both say stopped (130)."""
    stopped = json_line("PLAN_STOPPED", signal="SIGTERM", canceled=2)
    api = FakeApi([dry_run_log(), [stopped]])
    copy(api, yes=True, detach=True, label="nightly")
    api.infos[api.jobs[1]["id"]].status.stage = "ERROR"
    s, buf, _ = session(api)
    assert s.attach("nightly", None) == Exit.STOPPED
    st = events_of(buf.getvalue())[-1]
    assert st["type"] == "status" and st["result"]["state"] == "stopped"


def test_a_public_read_of_a_private_bucket_says_to_drop_the_flag(monkeypatch):
    """Seen live: AWS_PROFILE exported, --no-sign-request kept, AccessDenied."""
    monkeypatch.setenv("AWS_PROFILE", "science-cluster")
    log = [
        "Error: aws-sdk-s3 list_objects_v2 (not retried)",
        "    1: unhandled error (AccessDenied)",
        '    2: Error { code: "AccessDenied", message: "Access Denied", aws_request_id: "x" }',
    ]
    code, evs, _ = copy(FakeApi([log]), yes=True, anonymous=True)
    warning = next(e for e in evs if e["type"] == "warning")
    assert "AWS_PROFILE" in warning["message"] and "not used" in warning["message"]
    assert code == Exit.SOURCE and evs[-1]["cause"] == "AccessDenied: Access Denied"
    assert evs[-1]["hint"].startswith("this bucket does not allow anonymous reads: drop --no-sign-request")


def test_signed_reads_get_no_such_warning(monkeypatch):
    monkeypatch.setenv("AWS_PROFILE", "science-cluster")
    _, evs, _ = copy(FakeApi([dry_run_log()]), yes=True, analyze_only=True)
    assert not [e for e in evs if e["type"] == "warning"]
