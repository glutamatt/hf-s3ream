"""What the CLI asks of the Hub and of AWS: runs, Jobs, logs, cost, credentials."""

from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from . import markers
from .events import Exit, Fail

LABEL_KEY = "hf-s3ream-run"
ACTIVE = ["RUNNING", "SCHEDULING"]
TERMINAL = {"COMPLETED", "ERROR", "CANCELED", "DELETED"}
LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
JOB_ID_RE = re.compile(r"^[0-9a-f]{24}$")


def new_label() -> str:
    return f"s3ream-{int(time.time() * 1000):x}"


def check_label(label: str) -> str:
    if not LABEL_RE.match(label):
        raise Fail(
            "usage",
            f"bad label {label!r}",
            Exit.USAGE,
            "use 1 to 63 characters: letters, digits, '.', '_' or '-'",
        )
    return label


# ---------- arguments ----------


def split_dest(dest: str, user_ns: str) -> tuple[str, str]:
    """(namespace, bucket) of a destination, as the Space and the binary read it."""

    def bad(msg: str) -> Fail:
        return Fail("usage", msg, Exit.USAGE, "write the destination as org/name or hf://buckets/org/name[/path]")

    value = dest.strip()
    if value.startswith("hf://") and not value.startswith("hf://buckets/"):
        raise bad("the destination must be a bucket (hf://buckets/...)")
    is_uri = value.startswith("hf://buckets/")
    parts = (value[len("hf://buckets/") :] if is_uri else value).strip("/").split("/")
    if not is_uri and len(parts) == 1 and parts[0]:
        return user_ns, parts[0]
    if len(parts) < 2 or not parts[0] or not parts[1] or "@" in parts[0] or "@" in parts[1]:
        raise bad(f"bad destination {dest!r}")
    if not is_uri and len(parts) > 2:
        raise bad("a destination path needs the hf://buckets/org/name/path form")
    if any(not p for p in parts[2:]):
        raise bad("the destination path must not contain empty segments")
    return parts[0], parts[1]


def full_dest(dest: str, ns: str) -> str:
    """The destination as the binary reads it: a bare bucket name means a
    bucket of `ns`, which the binary cannot guess."""
    value = dest.strip()
    if "/" in value.strip("/") or value.startswith("hf://"):
        return value
    bucket_ns, name = split_dest(value, ns)
    return f"{bucket_ns}/{name}"


def ensure_bucket(api, bucket: str) -> tuple[bool, bool | None]:
    """Create the bucket if needed: (created, private). A new bucket is
    private: the source may be private data."""
    try:
        api.create_bucket(bucket, private=True)
        return True, True
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        if status != 409:
            raise Fail(
                "auth",
                f"cannot create or use the bucket {bucket}: {e}",
                Exit.AUTH,
                "check that your token can write to this namespace",
            ) from e
    try:
        return False, getattr(api.bucket_info(bucket), "private", None)
    except Exception:  # noqa: BLE001 — visibility is only shown
        return False, None


def aws_secrets(anonymous: bool, run=subprocess.run, env=None) -> tuple[dict[str, str], str | None]:
    """(the AWS credentials the copy reads the source with, their expiry or None).

    From the environment first; else from the AWS CLI (`aws configure
    export-credentials`, which follows AWS_PROFILE, SSO included). Nothing
    for a public source read anonymously.
    """
    if anonymous:
        return {}, None
    env = os.environ if env is None else env
    keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    if env.get(keys[0]) and env.get(keys[1]):
        return {k: env[k] for k in keys if env.get(k)}, env.get("AWS_CREDENTIAL_EXPIRATION")
    hint = (
        "set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or AWS_PROFILE; for a public bucket, pass --no-sign-request"
    )
    try:
        res = run(
            ["aws", "configure", "export-credentials", "--format", "process"],
            capture_output=True,
            text=True,
            env=dict(env),
        )
    except FileNotFoundError:
        raise Fail("auth", "no AWS credentials found", Exit.AUTH, hint) from None
    if res.returncode != 0:
        err = res.stderr.strip().splitlines()[-1] if res.stderr.strip() else "no credentials"
        err = re.sub(r"^aws: \[ERROR\]: ", "", err)
        sso = " — run `aws sso login`" if "sso" in err.lower() or "token" in err.lower() else ""
        raise Fail("auth", f"AWS CLI: {err}{sso}", Exit.AUTH, hint)
    c = json.loads(res.stdout)
    out = {"AWS_ACCESS_KEY_ID": c["AccessKeyId"], "AWS_SECRET_ACCESS_KEY": c["SecretAccessKey"]}
    if c.get("SessionToken"):
        out["AWS_SESSION_TOKEN"] = c["SessionToken"]
    return out, c.get("Expiration")


# ---------- runs ----------


@dataclass
class Run:
    label: str
    namespace: str
    planner: object  # JobInfo of the planner

    @property
    def planner_id(self) -> str:
        return self.planner.id

    @property
    def ref(self) -> str:
        return f"{self.namespace}/{self.planner.id}"

    @property
    def url(self) -> str:
        return f"https://huggingface.co/jobs/{self.ref}"

    @property
    def started(self) -> datetime | None:
        return getattr(self.planner, "created_at", None)


def is_planner(job) -> bool:
    return "--plan" in (getattr(job, "command", None) or [])


def run_jobs(api, namespace: str, label: str, active: bool = False) -> list:
    return list(api.list_jobs(namespace=namespace, labels={LABEL_KEY: label}, status=ACTIVE if active else None))


def find_run(api, namespace: str, ref: str) -> Run:
    """A run from its label, or from the id of its planner Job."""
    if JOB_ID_RE.match(ref):
        try:
            job = api.inspect_job(job_id=ref, namespace=namespace)
        except Exception as e:
            raise Fail("not_found", f"no Job {ref} in {namespace}: {e}", Exit.NOT_FOUND) from e
        label = (getattr(job, "labels", None) or {}).get(LABEL_KEY)
        if not label or not is_planner(job):
            raise Fail("not_found", f"Job {ref} is not the planner of an hf-s3ream copy", Exit.NOT_FOUND)
        return Run(label, namespace, job)
    planners = [j for j in run_jobs(api, namespace, ref) if is_planner(j)]
    if not planners:
        raise Fail(
            "not_found",
            f"no copy labeled {ref!r} in {namespace}",
            Exit.NOT_FOUND,
            "pass the label printed at start (s3ream-...) or the planner Job id; --namespace for an org",
        )
    planners.sort(key=lambda j: j.created_at or datetime.min.replace(tzinfo=timezone.utc))
    return Run(ref, namespace, planners[-1])


# ---------- cost ----------


def minutes_touched(started: datetime, finished: datetime) -> int:
    """HF Jobs bill each clock minute a Job runs in (measured 2026-10-20)."""
    return int(finished.timestamp() // 60) - int(started.timestamp() // 60) + 1


_prices: dict[str, float] = {}


def billed_by_job(api) -> dict[str, float]:
    """USD billed so far for each Job of the user's own namespace, this month
    (live: a running Job shows up within seconds). Empty when not readable.
    Jobs of an organization are not in it."""
    try:
        from huggingface_hub import get_session
        from huggingface_hub.utils import build_hf_headers

        r = get_session().get(
            f"{api.endpoint}/api/settings/billing/usage/jobs",
            headers=build_hf_headers(token=api.token),
            timeout=20,
        )
        r.raise_for_status()
        return {d["jobId"]: d["totalCostMicroUsd"] / 1e6 for d in r.json()["usage"]["jobDetails"]}
    except Exception:  # noqa: BLE001 — then the cost comes from the run times
        return {}


def run_cost(api, namespace: str, label: str, now: datetime | None = None) -> float:
    """USD for every Job of the run: what the billing says for the user's own
    Jobs; for the others (an organization's), the clock minutes of each Job
    times its hourly price — close to the bill, not the bill."""
    now = now or datetime.now(timezone.utc)
    billed = billed_by_job(api)
    usd = 0.0
    for job in run_jobs(api, namespace, label):
        if job.id in billed:
            usd += billed[job.id]
            continue
        if not _prices:
            _prices.update({h.name: h.unit_cost_usd for h in api.list_jobs_hardware()})
        if job.started_at is not None:
            usd += minutes_touched(job.started_at, job_end(job, now)) * _prices.get(job.flavor, 0.0)
    return usd


def job_end(job, now: datetime) -> datetime:
    """When a Job stopped running. A canceled Job can come back with no
    `finished_at` and no `durations` (seen live): it then counts its first
    minute only, a low bound."""
    if job.finished_at is not None:
        return job.finished_at
    running = getattr(getattr(job, "durations", None), "running_secs", None)
    if running is not None:
        return job.started_at + timedelta(seconds=running)
    stage = getattr(getattr(job, "status", None), "stage", None)
    return job.started_at if stage in TERMINAL else now


# ---------- stopping ----------


def spawn_cancel(namespace: str, job_id: str) -> None:
    """Cancel a Job from a detached process (see cancel.py): it survives this one."""
    subprocess.Popen(
        [sys.executable, "-m", "hf_s3ream_cli.cancel", namespace, job_id],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def sweep(api, run: Run, on_round=None, rounds: int = 40, every_s: float = 3.0) -> tuple[int, int]:
    """Cancel the planner, then every Job of the run still running, until none
    is left (the planner cancels its copiers itself; this makes sure).
    Returns (Jobs canceled here, Jobs left running)."""
    canceled = 0
    left: list = []
    for i in range(rounds):
        try:
            left = run_jobs(api, run.namespace, run.label, active=True)
        except Exception:  # noqa: BLE001 — try again next round
            left = left or [run.planner]
        if on_round:
            on_round(len(left))
        if not left:
            return canceled, 0
        for job in left:
            try:
                api.cancel_job(job_id=job.id, namespace=run.namespace)
                canceled += 1
            except Exception:  # noqa: BLE001, S110 — an ended Job refuses; the next round checks
                pass
        if i + 1 < rounds:
            time.sleep(every_s)
    return canceled, len(left)


# ---------- logs ----------


class LogPump(threading.Thread):
    """Puts the log lines of a Job on `q` as ("line", text, job_id), then
    ("eof", None, job_id). The job id lets a reader skip the leftovers of an
    earlier stream (the dry-run's end can land after the planner starts).

    The stream can close before the planner's last lines: after it ends, the
    tail of the log is read again and the final markers not seen yet are put.
    """

    FINAL = ("PLAN_RESULT", "PLAN_STOPPED", "VERIFY")

    def __init__(self, api, namespace: str, job_id: str, q: queue.Queue, tail: int | None = None):
        super().__init__(daemon=True)
        self.api, self.ns, self.job, self.q, self.tail = api, namespace, job_id, q, tail

    def run(self) -> None:
        seen: set[str] = set()
        try:
            for line in self.api.fetch_job_logs(job_id=self.job, namespace=self.ns, follow=True, tail=self.tail):
                if line.startswith(self.FINAL):
                    seen.add(line)
                self.q.put(("line", line, self.job))
            self.api.wait_for_job(job_id=self.job, namespace=self.ns)
            for line in self.api.fetch_job_logs(job_id=self.job, namespace=self.ns, tail=50):
                if line.startswith(self.FINAL) and line not in seen:
                    self.q.put(("line", line, self.job))
        except Exception as e:  # noqa: BLE001 — shown as a warning; the caller settles the state
            self.q.put(("warning", f"log stream: {e}", self.job))
        finally:
            self.q.put(("eof", None, self.job))


def last_markers(api, namespace: str, job_id: str, tail: int = 200) -> dict:
    """The last line of each marker in the tail of a Job's log."""
    out: dict = {}
    for line in api.fetch_job_logs(job_id=job_id, namespace=namespace, tail=tail):
        m = markers.parse(line)
        if m:
            out[m[0]] = m[1]
    return out
