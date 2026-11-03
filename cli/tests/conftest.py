"""A fake Hub for the CLI tests: no network, no Job started."""

from __future__ import annotations

import io
import json
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from hf_s3ream_cli.events import EVENTS

T0 = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)


def dry_run_log(plan=True, bucket="ok", count=8, total=28004946):
    lines = [
        "2026-11-03T12:00:00Z  INFO hf_s3ream: dry run",
        json_line("LISTING", listed=count, kept=count, bytes=total, done=True),
        json_line("DRYRUN_STATS", count=count, total_bytes=total, region="eu-west-3"),
    ]
    if plan:
        lines.append(
            json_line(
                "DRYRUN_PLAN",
                commit_bound=False,
                copier_flavor="cpu-upgrade",
                copiers=1,
                launch_stagger_ms=1500,
                max_inflight=1,
                planner_flavor="cpu-basic",
                range_gib=1,
                timeout_s=7200,
                usd=0.0017,
                wall_s=77,
            )
        )
    lines.append(f"DRYRUN_BUCKET {bucket}")
    return lines


def json_line(name, **v):
    return f"{name} {json.dumps(v)}"


def progress_line(done=0, active=1, bytes_copied=0, mibps=0.0, listing_done=False, ranges=2, committed=0):
    return json_line(
        "PLAN_PROGRESS", ranges=ranges, done=done, active=active, error=0, canceled=0, files_done=done * 4,
        bytes_done=done * 1000, files_planned=8, bytes_planned=2000, listing_done=listing_done,
        files_copied=committed, bytes_copied=bytes_copied, committed=committed, skipped=0, skipped_bytes=0,
        mibps_5s=mibps, hf_mibps_5s=mibps, committed_fps_5s=3.0,
    )  # fmt: skip


PLANNER_LOG = [
    json_line("PLANNING", listed=8, ranges=1, bytes=1000),
    json_line("RANGE", idx=0, files=4, bytes=1000),
    json_line("COPIER", idx=0, job_id="c0", attempt=1),
    progress_line(active=1, bytes_copied=400, mibps=80.0, committed=1),
    json_line("PLAN_DONE", ranges=2, files=8, bytes=2000),
    progress_line(done=1, active=1, bytes_copied=1500, mibps=120.0, listing_done=True, committed=6),
    progress_line(done=2, active=0, bytes_copied=2000, listing_done=True, committed=8),
    json_line("VERIFYING", dest_files=8, planned_files=8),
]
RESULT = json_line("PLAN_RESULT", ranges=2, completed=2, failed=0, canceled=0, retried=0, verified=True)


class HTTPError(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.response = SimpleNamespace(status_code=status)


class FakeApi:
    """The part of HfApi the CLI uses. Each Job's log is scripted; a log can
    hold a `gate` (threading.Event) that blocks the stream until it is set,
    as a running Job does."""

    token = "hf_fake"

    def __init__(self, logs=None, can_pay=True, bucket_exists=False, active_label=None):
        self.logs = list(logs or [])  # one log per run_job, in order
        self.can_pay = can_pay
        self.bucket_exists = bucket_exists
        self.active_label = active_label
        self.jobs = []  # specs of run_job
        self.infos = {}  # id -> job info
        self.canceled = []
        self.lock = threading.Lock()

    # account, bucket
    def whoami(self):
        return {"name": "alice", "canPay": self.can_pay}

    def create_bucket(self, bucket_id, private=None):
        if self.bucket_exists:
            raise HTTPError(409)
        self.bucket_exists = True

    def bucket_info(self, bucket_id):
        return SimpleNamespace(private=True)

    # jobs
    def run_job(self, **spec):
        with self.lock:
            job_id = f"{len(self.jobs):024x}"
            self.jobs.append({"id": job_id, **spec})
            info = SimpleNamespace(
                id=job_id, created_at=T0, started_at=T0, finished_at=None, flavor=spec["flavor"],
                command=spec["command"], labels=spec["labels"], status=SimpleNamespace(stage="RUNNING"),
            )  # fmt: skip
            self.infos[job_id] = info
            return info

    def _log(self, job_id):
        return self.logs[int(job_id, 16)]

    def fetch_job_logs(self, job_id, namespace=None, follow=False, tail=None):
        log = self._log(job_id)
        if not follow:
            lines = [x for x in log if isinstance(x, str)]
            yield from lines[-tail:] if tail else lines
            return
        for x in log:
            if isinstance(x, threading.Event):
                x.wait(timeout=20)
            else:
                yield x
        self._end(job_id)

    def _end(self, job_id, stage="COMPLETED"):
        info = self.infos.get(job_id)
        if info and info.status.stage == "RUNNING":
            info.status.stage = stage
            info.finished_at = T0 + timedelta(minutes=2)

    def wait_for_job(self, job_id, namespace=None):
        return self.infos[job_id]

    def inspect_job(self, job_id, namespace=None):
        if job_id not in self.infos:
            raise HTTPError(404)
        return self.infos[job_id]

    def cancel_job(self, job_id, namespace=None):
        self.canceled.append(job_id)
        self._end(job_id, "CANCELED")

    def list_jobs(self, namespace=None, labels=None, status=None):
        label = (labels or {}).get("hf-s3ream-run")
        out = [j for j in self.infos.values() if j.labels.get("hf-s3ream-run") == label]
        if label and label == self.active_label:
            out.append(
                SimpleNamespace(id="f" * 24, labels=labels, command=["hf-s3ream", "--plan"], created_at=T0,
                                status=SimpleNamespace(stage="RUNNING"), started_at=T0, finished_at=None,
                                flavor="cpu-basic")
            )  # fmt: skip
        if status:
            out = [j for j in out if j.status.stage in status]
        return out

    def list_jobs_hardware(self):
        return [SimpleNamespace(name="cpu-basic", unit_cost_usd=0.000167),
                SimpleNamespace(name="cpu-upgrade", unit_cost_usd=0.0005)]  # fmt: skip


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAFAKE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.delenv("AWS_CREDENTIAL_EXPIRATION", raising=False)


def events_of(text: str) -> list[dict]:
    """Parse JSON-lines output, and check every event against the contract."""
    evs = [json.loads(line) for line in text.splitlines() if line.strip()]
    for ev in evs:
        assert ev["schema"] == 1, ev
        assert ev["type"] in EVENTS, ev
        missing = [k for k in EVENTS[ev["type"]] if k not in ev]
        assert not missing, f"{ev['type']} misses {missing}: {ev}"
        assert "time" in ev
    return evs


def json_ui():
    from hf_s3ream_cli.render import JsonUI

    buf = io.StringIO()
    return JsonUI(out=buf), buf
