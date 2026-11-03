"""The remote flow against a fake HfApi: no Job is started."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from hf_s3ream_cli import IMAGE, markers
from hf_s3ream_cli.remote import (
    CopyError,
    Options,
    RemoteCopy,
    minutes_touched,
    run_cost,
    split_dest,
)

DRY_RUN_LOG = [
    "2026-11-03T16:00:00Z  INFO hf_s3ream: dry run",
    'LISTING {"listed":8,"kept":8,"bytes":28004946,"done":true}',
    'DRYRUN_STATS {"count":8,"total_bytes":28004946,"region":"eu-west-3"}',
    (
        'DRYRUN_PLAN {"commit_bound":false,"copier_flavor":"cpu-upgrade","copiers":1,"launch_stagger_ms":1500,'
        '"max_inflight":1,"planner_flavor":"cpu-basic","range_gib":1,"timeout_s":7200,"usd":0.0017,"wall_s":77}'
    ),
    "DRYRUN_BUCKET ok",
]

PLANNER_LOG = [
    'PLANNING {"listed":8,"ranges":1,"bytes":28004946}',
    'RANGE {"idx":0,"files":8,"bytes":28004946}',
    'COPIER {"idx":0,"job_id":"c0","attempt":1}',
    (
        'PLAN_PROGRESS {"ranges":1,"done":0,"active":1,"error":0,"canceled":0,'
        '"files_done":0,"bytes_done":0,"files_planned":8,"bytes_planned":28004946}'
    ),
    'PLAN_DONE {"ranges":1,"files":8,"bytes":28004946}',
    (
        'PLAN_PROGRESS {"ranges":1,"done":1,"active":0,"error":0,"canceled":0,'
        '"files_done":8,"bytes_done":28004946,"files_planned":8,"bytes_planned":28004946}'
    ),
    'VERIFYING {"dest_files":8,"planned_files":8}',
]
RESULT = 'PLAN_RESULT {"ranges":1,"completed":1,"failed":0,"canceled":0,"retried":0,"verified":true}'


def t(h, m, s):
    return datetime(2026, 11, 3, h, m, s, tzinfo=timezone.utc)


class FakeApi:
    """The part of HfApi the CLI uses. Logs are scripted per Job."""

    token = "hf_fake"

    def __init__(self, logs, final_logs=None):
        self.logs = logs  # one list of lines per run_job, in order
        self.final_logs = final_logs or {}
        self.jobs = []
        self.canceled = []
        self.buckets = []

    def whoami(self):
        return {"name": "alice"}

    def create_bucket(self, bucket_id, private=None, exist_ok=False):
        self.buckets.append((bucket_id, private, exist_ok))

    def run_job(self, **spec):
        job_id = f"job{len(self.jobs)}"
        self.jobs.append({"id": job_id, **spec})
        return SimpleNamespace(id=job_id)

    def fetch_job_logs(self, job_id, namespace, follow=False):
        lines = self.logs[int(job_id[3:])]
        if not follow:
            yield from self.final_logs.get(job_id, lines)
            return
        yield from lines

    def wait_for_job(self, job_id, namespace):
        return SimpleNamespace(status=SimpleNamespace(stage="COMPLETED"))

    def inspect_job(self, job_id, namespace):
        return SimpleNamespace(status=SimpleNamespace(stage="ERROR"))

    def cancel_job(self, job_id, namespace):
        self.canceled.append(job_id)

    def list_jobs_hardware(self):
        return [
            SimpleNamespace(name="cpu-basic", unit_cost_usd=0.000167),
            SimpleNamespace(name="cpu-upgrade", unit_cost_usd=0.0005),
        ]

    def list_jobs(self, namespace, labels):
        return [
            SimpleNamespace(flavor="cpu-basic", started_at=t(12, 0, 10), finished_at=t(12, 2, 5)),
            SimpleNamespace(flavor="cpu-upgrade", started_at=t(12, 0, 50), finished_at=t(12, 1, 10)),
            SimpleNamespace(flavor="cpu-upgrade", started_at=None, finished_at=None),
        ]


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)


def copy(api, interrupt_on=None, **kw):
    """A RemoteCopy on `api`. `interrupt_on`: Ctrl-C (in the main thread, as
    a terminal does) when a printed line contains this text, once."""
    out = []
    armed = [interrupt_on]

    def show(line):
        out.append(line)
        if armed[0] and armed[0] in line:
            armed[0] = None
            raise KeyboardInterrupt

    opts = Options(source="s3://src/prefix/", dest="alice/dst", yes=True, **kw)
    cancels = []
    rc = RemoteCopy(api, opts, ask=lambda q: True, spawn_cancel=lambda ns, j: cancels.append((ns, j)), out=show)
    return rc, out, cancels


def test_destinations_read_like_the_space():
    assert split_dest("dst", "alice") == ("alice", "dst")
    assert split_dest("org/dst", "alice") == ("org", "dst")
    assert split_dest("hf://buckets/org/dst/a/b", "alice") == ("org", "dst")
    for bad in ["hf://datasets/org/x", "org/dst/path", "hf://buckets/org", "hf://buckets/org/dst//x", "a@b/c"]:
        with pytest.raises(CopyError):
            split_dest(bad, "alice")


def test_markers():
    assert markers.parse('PLAN_DONE {"ranges":2}') == ("PLAN_DONE", {"ranges": 2})
    assert markers.parse("DRYRUN_BUCKET ok") == ("DRYRUN_BUCKET", "ok")
    assert markers.parse("PLAN_DONE {broken") is None
    assert markers.parse("INFO something") is None


def test_a_copy_runs_the_plan_of_the_dry_run():
    api = FakeApi([DRY_RUN_LOG, PLANNER_LOG], final_logs={"job1": PLANNER_LOG + [RESULT]})
    rc, out, cancels = copy(api)
    assert rc.run() == 0
    assert api.buckets == [("alice/dst", True, True)]
    dry, planner = api.jobs
    assert dry["command"] == ["hf-s3ream", "s3://src/prefix/", "alice/dst", "--dry-run"]
    assert dry["flavor"] == "cpu-basic" and dry["timeout"] == 120
    cmd = planner["command"]
    assert cmd[:3] == ["hf-s3ream", "s3://src/prefix/", "alice/dst"]
    assert cmd[3] == "--plan"
    flags = dict(zip(cmd[4::2], cmd[5::2]))  # after --plan, every flag has a value
    assert flags["--range-gib"] == "1" and flags["--max-inflight"] == "1"
    assert flags["--copier-flavor"] == "cpu-upgrade" and flags["--copier-image"] == IMAGE
    assert flags["--jobs-namespace"] == "alice" and flags["--run-label"] == planner["labels"]["hf-s3ream-run"]
    assert planner["flavor"] == "cpu-basic" and planner["timeout"] == 7200
    assert planner["secrets"] == {
        "HF_TOKEN": "hf_fake",
        "AWS_ACCESS_KEY_ID": "AKIAFAKE",
        "AWS_SECRET_ACCESS_KEY": "secret",
    }
    # The result line came only in the settled log, after the stream closed.
    assert any(line.startswith("Done: 1/1 ranges completed, verified") for line in out)
    assert any("1/1 ranges done" in line for line in out)
    assert cancels == []


def test_a_public_source_sends_no_aws_secret():
    api = FakeApi([DRY_RUN_LOG, PLANNER_LOG + [RESULT]])
    rc, _, _ = copy(api, anonymous=True)
    rc.run()
    for job in api.jobs:
        assert set(job["secrets"]) == {"HF_TOKEN"}
        assert "--no-sign-request" in job["command"]


def test_ctrl_c_cancels_the_planner_from_a_detached_process():
    api = FakeApi([DRY_RUN_LOG, PLANNER_LOG + [RESULT]])
    rc, out, cancels = copy(api, interrupt_on="0/1+ ranges done")
    assert rc.run() == 130
    assert cancels == [("alice", "job1")]
    assert any(line.startswith("Stopped.") for line in out)


def test_a_failed_range_fails_the_command():
    failed = 'PLAN_RESULT {"ranges":1,"completed":0,"failed":1,"canceled":0,"retried":2,"verified":null}'
    rc, _, _ = copy(FakeApi([DRY_RUN_LOG, PLANNER_LOG + [failed]]))
    assert rc.run() == 1


def test_a_planner_without_result_fails_the_command():
    rc, out, _ = copy(FakeApi([DRY_RUN_LOG, PLANNER_LOG]))
    assert rc.run() == 1
    assert any("ended ERROR without a result" in line for line in out)


def test_detach_returns_once_the_planner_is_started():
    api = FakeApi([DRY_RUN_LOG, []])
    rc, _, _ = copy(api, detach=True)
    assert rc.run() == 0 and len(api.jobs) == 2


def test_analyze_only_starts_no_planner():
    api = FakeApi([DRY_RUN_LOG])
    rc, out, _ = copy(api, analyze_only=True)
    assert rc.run() == 0 and len(api.jobs) == 1
    assert any(line.startswith("Plan: 1 copiers on cpu-upgrade") for line in out)


def test_an_old_image_without_a_plan_is_refused():
    log = [line for line in DRY_RUN_LOG if not line.startswith("DRYRUN_PLAN")]
    rc, _, _ = copy(FakeApi([log]))
    with pytest.raises(CopyError, match="does not print DRYRUN_PLAN"):
        rc.run()


def test_overrides_drop_the_estimate():
    rc, out, _ = copy(FakeApi([DRY_RUN_LOG, PLANNER_LOG + [RESULT]]), range_gib=8, analyze_only=True)
    rc.run()
    assert any("8 GiB each" in line for line in out)
    assert any("unknown (plan changed)" in line for line in out)


def test_cost_counts_each_clock_minute_touched():
    assert minutes_touched(t(12, 28, 28), t(12, 30, 57)) == 3  # 2 min 29 s
    assert minutes_touched(t(9, 22, 54), t(9, 23, 34)) == 2  # 40 s
    usd, n = run_cost(FakeApi([]), "alice", "s3ream-x", now=t(13, 0, 0))
    assert n == 3
    assert usd == pytest.approx(3 * 0.000167 + 2 * 0.0005)


def test_aws_credentials_from_the_aws_cli(monkeypatch):
    from hf_s3ream_cli.remote import aws_secrets

    monkeypatch.delenv("AWS_ACCESS_KEY_ID")
    out = json.dumps({"AccessKeyId": "A", "SecretAccessKey": "S", "SessionToken": "T"})
    run = lambda *a, **k: SimpleNamespace(returncode=0, stdout=out, stderr="")
    assert aws_secrets(False, run=run) == {
        "AWS_ACCESS_KEY_ID": "A",
        "AWS_SECRET_ACCESS_KEY": "S",
        "AWS_SESSION_TOKEN": "T",
    }
    fail = lambda *a, **k: SimpleNamespace(returncode=255, stdout="", stderr="SSO session expired")
    with pytest.raises(CopyError, match="SSO session expired"):
        aws_secrets(False, run=fail)
