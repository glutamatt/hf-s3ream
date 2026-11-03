"""Run a copy on HF Jobs: the same Jobs as the web Space, from a terminal.

1. Analyze: a short dry-run Job lists the source. Its DRYRUN_PLAN line is the
   plan (copier flavor, range size, copiers at once, timeout, cost estimate),
   computed by the binary (src/reco.rs) — not here.
2. Run: one planner Job. The planner starts and watches the copiers.
3. Follow: the planner's log only. PLAN_PROGRESS gives the progress.
4. Ctrl-C stops the copy: cancelling the planner stops every copier.

Everything that decides how to copy lives in the binary. This module launches,
shows, and stops.
"""

from __future__ import annotations

import json
import math
import os
import queue
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from huggingface_hub import get_token

from . import IMAGE, markers

RUST_LOG = "hf_s3ream=info,xet_data=warn,xet_client=warn"
RUN_LABEL_KEY = "hf-s3ream-run"
# The dry-run only lists metadata. A normal source ends in seconds; past this,
# the Job is canceled (HF Jobs does not always enforce its own timeout).
DRY_RUN_TIMEOUT_S = 120
TERMINAL = {"COMPLETED", "ERROR", "CANCELED", "DELETED"}


class CopyError(Exception):
    """A problem to show the user, without a traceback."""


# ---------- arguments ----------


def split_dest(dest: str, user_ns: str) -> tuple[str, str]:
    """(namespace, bucket) of a destination, as the Space and the binary read it."""
    value = dest.strip()
    if value.startswith("hf://") and not value.startswith("hf://buckets/"):
        raise CopyError("the destination HF URI must use the buckets/ type: hf://buckets/org/name[/path]")
    is_uri = value.startswith("hf://buckets/")
    parts = (value[len("hf://buckets/") :] if is_uri else value).strip("/").split("/")
    if not is_uri and len(parts) == 1 and parts[0]:
        return user_ns, parts[0]
    if len(parts) < 2 or not parts[0] or not parts[1] or "@" in parts[0] or "@" in parts[1]:
        raise CopyError("the destination must be org/name or hf://buckets/org/name[/path]")
    if not is_uri and len(parts) > 2:
        raise CopyError("a destination path needs the hf://buckets/org/name/path form")
    if any(not p for p in parts[2:]):
        raise CopyError("the destination path must not contain empty segments")
    return parts[0], parts[1]


def aws_secrets(anonymous: bool, run: Callable = subprocess.run) -> dict[str, str]:
    """The AWS credentials the Jobs read the source with.

    From the environment first; else from the AWS CLI (`aws configure
    export-credentials`, which follows AWS_PROFILE, SSO included). Nothing for
    a public source read anonymously.
    """
    if anonymous:
        return {}
    keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
    if os.environ.get(keys[0]) and os.environ.get(keys[1]):
        return {k: os.environ[k] for k in keys if os.environ.get(k)}
    try:
        res = run(
            ["aws", "configure", "export-credentials", "--format", "process"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        raise CopyError(
            "no AWS credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, install the AWS CLI, "
            "or pass --no-sign-request for a public bucket"
        )
    if res.returncode != 0:
        raise CopyError(f"no AWS credentials: `aws configure export-credentials` failed: {res.stderr.strip()}")
    c = json.loads(res.stdout)
    out = {"AWS_ACCESS_KEY_ID": c["AccessKeyId"], "AWS_SECRET_ACCESS_KEY": c["SecretAccessKey"]}
    if c.get("SessionToken"):
        out["AWS_SESSION_TOKEN"] = c["SessionToken"]
    if c.get("Expiration"):
        print(f"AWS credentials expire at {c['Expiration']}: the copy must end before.", file=sys.stderr)
    return out


# ---------- formatting ----------


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if n < 1024 or unit == "PiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PiB"


def fmt_dur(s: float) -> str:
    s = round(s)
    if s < 90:
        return f"{s} s"
    if s < 5400:
        return f"{math.ceil(s / 60)} min"
    return f"{s / 3600:.1f} h"


def fmt_usd(usd: float) -> str:
    return f"${usd:.2f}" if usd < 20 else f"${usd:.0f}"


# ---------- cost ----------


def minutes_touched(started: datetime, finished: datetime) -> int:
    """HF Jobs bill each clock minute a Job runs in (measured 2026-10-20)."""
    first = int(started.timestamp() // 60)
    last = int(finished.timestamp() // 60)
    return last - first + 1


def run_cost(api, namespace: str, label: str, now: datetime | None = None) -> tuple[float, int]:
    """(USD, Jobs) for every Job of the run, from their run times and the price
    list. Close to the bill, not the bill: the billing page is the reference."""
    now = now or datetime.now(timezone.utc)
    prices = {h.name: h.unit_cost_usd for h in api.list_jobs_hardware()}
    usd, n = 0.0, 0
    for job in api.list_jobs(namespace=namespace, labels={RUN_LABEL_KEY: label}):
        n += 1
        if job.started_at is None:
            continue
        usd += minutes_touched(job.started_at, job.finished_at or now) * prices.get(job.flavor, 0.0)
    return usd, n


# ---------- logs ----------


def follow_lines(api, namespace: str, job_id: str, deadline_s: float | None = None):
    """Yield the log lines of a Job until it ends (or `deadline_s` passes).

    The stream can end before the last lines: once the Job has ended, the
    whole log is read again and the lines not seen yet are yielded.
    """
    q: queue.Queue[str | None] = queue.Queue()

    def pump():
        try:
            for line in api.fetch_job_logs(job_id=job_id, namespace=namespace, follow=True):
                q.put(line)
        except Exception as e:  # noqa: BLE001 — any stream error: the settle step reads what was missed
            print(f"log stream: {e}", file=sys.stderr)
        finally:
            q.put(None)

    threading.Thread(target=pump, daemon=True).start()
    seen = 0
    end = time.monotonic() + deadline_s if deadline_s else None
    while True:
        try:
            line = q.get(timeout=1.0)
        except queue.Empty:
            if end and time.monotonic() > end:
                return
            continue
        if line is None:
            break
        seen += 1
        yield line
    # Settle: the final lines (PLAN_RESULT) can land after the stream closed.
    api.wait_for_job(job_id=job_id, namespace=namespace)
    for i, line in enumerate(api.fetch_job_logs(job_id=job_id, namespace=namespace)):
        if i >= seen:
            yield line


# ---------- the copy ----------


@dataclass
class Options:
    source: str
    dest: str
    anonymous: bool = False
    skip_existing: bool = False
    yes: bool = False
    detach: bool = False
    analyze_only: bool = False
    namespace: str | None = None
    image: str = IMAGE
    copier_flavor: str | None = None
    range_gib: int | None = None
    max_inflight: int | None = None
    timeout_s: int | None = None
    extra: tuple[str, ...] = ()


def spawn_cancel(namespace: str, job_id: str) -> None:
    """Cancel the planner from a detached process (see cancel.py)."""
    subprocess.Popen(
        [sys.executable, "-m", "hf_s3ream_cli.cancel", namespace, job_id],
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


class RemoteCopy:
    def __init__(self, api, opts: Options, *, ask: Callable[[str], bool], spawn_cancel=spawn_cancel, out=print):
        self.api, self.o, self.ask, self.spawn_cancel, self.out = api, opts, ask, spawn_cancel, out

    # -- 0. who, where
    def prepare(self) -> None:
        o = self.o
        if not o.source.startswith("s3://"):
            raise CopyError("the source must be s3://bucket/prefix/")
        split_dest(o.dest, "")  # the form, before any network call
        try:
            self.ns = o.namespace or self.api.whoami()["name"]
        except Exception as e:
            raise CopyError(f"could not read your Hugging Face account ({e}): run `hf auth login`") from e
        bucket_ns, bucket = split_dest(o.dest, self.ns)
        token = self.api.token or get_token()
        if not token:
            raise CopyError("no Hugging Face token: run `hf auth login`, or set HF_TOKEN")
        self.secrets = {"HF_TOKEN": token, **aws_secrets(o.anonymous)}
        # A new bucket is private: the source may be private data.
        self.api.create_bucket(f"{bucket_ns}/{bucket}", private=True, exist_ok=True)

    def _job(self, args: list[str], *, flavor: str, timeout_s: int, labels=None):
        command = ["hf-s3ream", self.o.source, self.o.dest, *args, *self.o.extra]
        if self.o.anonymous:
            command.append("--no-sign-request")
        return self.api.run_job(
            image=self.o.image,
            command=command,
            env={"RUST_LOG": RUST_LOG},
            secrets=self.secrets,
            flavor=flavor,
            timeout=timeout_s,
            labels=labels,
            namespace=self.ns,
        )

    # -- 1. analyze
    def analyze(self) -> dict:
        job = self._job(["--dry-run"], flavor="cpu-basic", timeout_s=DRY_RUN_TIMEOUT_S)
        self.out(f"Analyzing the source (dry-run Job {job.id}, at most {DRY_RUN_TIMEOUT_S // 60} min)…")
        stats = plan = bucket = None
        for line in follow_lines(self.api, self.ns, job.id, deadline_s=DRY_RUN_TIMEOUT_S):
            m = markers.parse(line)
            if m is None:
                continue
            name, v = m
            if name == "DRYRUN_STATS":
                stats = v
            elif name == "DRYRUN_PLAN":
                plan = v
            elif name == "DRYRUN_BUCKET":
                bucket = v
            elif name == "LISTING" and not v.get("done"):
                self.out(f"  listing… {v.get('listed', 0):,} objects, {fmt_bytes(v.get('bytes', 0))} so far")
        if plan is None:
            self.api.cancel_job(job_id=job.id, namespace=self.ns)
            if stats is None:
                raise CopyError(
                    "the dry-run gave no plan: the source could not be read, or its listing takes more than "
                    f"{DRY_RUN_TIMEOUT_S // 60} min. Check the AWS credentials and the region, or give the plan "
                    "yourself: --copier-flavor, --range-gib, --max-inflight, --timeout."
                )
            raise CopyError(f"the image {self.o.image} does not print DRYRUN_PLAN: use an image that does")
        if bucket == "error":
            raise CopyError("the dry-run could not get write access to the destination bucket")
        self.out(f"Source: {stats['count']:,} files, {fmt_bytes(stats['total_bytes'])} (region {stats.get('region')})")
        return plan

    def apply_overrides(self, plan: dict) -> dict:
        o = self.o
        for key, value in (
            ("copier_flavor", o.copier_flavor),
            ("range_gib", o.range_gib),
            ("max_inflight", o.max_inflight),
            ("timeout_s", o.timeout_s),
        ):
            if value is not None:
                plan = {**plan, key: value, "usd": None, "wall_s": None}
        return plan

    def show_plan(self, plan: dict) -> None:
        cost = f"≈ {fmt_usd(plan['usd'])}" if plan.get("usd") is not None else "unknown (plan changed)"
        wall = f"≈ {fmt_dur(plan['wall_s'])}" if plan.get("wall_s") is not None else "unknown"
        why = "limited by commits (~1k files/s per bucket)" if plan.get("commit_bound") else "limited by bandwidth"
        self.out(
            f"Plan: {plan.get('copiers') or '?'} copiers on {plan['copier_flavor']}, {plan['range_gib']} GiB each, "
            f"{plan['max_inflight']} at once; time {wall}, {why}.\n"
            f"Cost {cost} on the account of {self.ns} (Jobs are billed per started minute)."
        )

    # -- 2. run
    def launch(self, plan: dict):
        self.label = f"s3ream-{int(time.time() * 1000):x}"
        args = [
            "--plan",
            "--range-gib", str(plan["range_gib"]),
            "--max-inflight", str(plan["max_inflight"]),
            "--launch-stagger-ms", str(plan.get("launch_stagger_ms", 1500)),
            "--copier-image", self.o.image,
            "--copier-flavor", plan["copier_flavor"],
            "--jobs-namespace", self.ns,
            "--run-label", self.label,
        ]  # fmt: skip
        if self.o.skip_existing:
            args.append("--skip-existing")
        job = self._job(
            args,
            flavor=plan.get("planner_flavor", "cpu-basic"),
            timeout_s=int(plan["timeout_s"]),
            labels={RUN_LABEL_KEY: self.label},
        )
        self.planner = job.id
        ref = f"{self.ns}/{job.id}"
        self.out(f"Planner Job: https://huggingface.co/jobs/{ref}")
        self.out(f"  follow: hf jobs logs -f {ref}   stop: hf jobs cancel {ref}")
        return job

    # -- 3. follow
    def follow(self) -> int:
        self.out("Copying. Ctrl-C stops the copy (the Jobs run on, if you close the terminal).")
        result = None
        stopping = False
        try:
            for line in follow_lines(self.api, self.ns, self.planner):
                m = markers.parse(line)
                if m is None:
                    continue
                result = self._show(*m) or result
        except KeyboardInterrupt:
            # First thing: the cancel must not depend on this process living on.
            self.spawn_cancel(self.ns, self.planner)
            stopping = True
            self.out("\nStopping the copy: the planner is canceled, it cancels its copiers…")
            try:
                for line in follow_lines(self.api, self.ns, self.planner, deadline_s=60):
                    m = markers.parse(line)
                    if m and m[0] in ("PLAN_STOPPED", "PLAN_RESULT"):
                        self._show(*m)
            except KeyboardInterrupt:
                pass
        self._show_cost()
        if stopping:
            self.out("Stopped. Files already committed stay in the bucket; run again with --skip-existing to finish.")
            return 130
        if result is None:
            stage = self.api.inspect_job(job_id=self.planner, namespace=self.ns).status.stage
            self.out(f"The planner ended {stage} without a result: see its log.")
            return 1
        ok = not result.get("failed") and not result.get("canceled") and result.get("verified") is not False
        return 0 if ok else 1

    def _show(self, name: str, v) -> dict | None:
        if name == "PLAN_PROGRESS":
            more = "" if getattr(self, "plan_done", False) else "+"
            pct = 100 * v["bytes_done"] / v["bytes_planned"] if v["bytes_planned"] else 0
            bad = f", {v['error']} failed" if v["error"] else ""
            self.out(
                f"  {v['done']}/{v['ranges']}{more} ranges done, {v['active']} running{bad} — "
                f"{fmt_bytes(v['bytes_done'])} of {fmt_bytes(v['bytes_planned'])}{more} ({pct:.0f} %)"
            )
        elif name == "PLAN_DONE":
            self.plan_done = True
            self.out(f"  listing done: {v['ranges']} ranges, {v['files']:,} files, {fmt_bytes(v['bytes'])}")
        elif name == "VERIFYING":
            self.out("  copy done, checking the destination…")
        elif name == "PLAN_STOPPED":
            self.out(f"  planner stopped: {v.get('canceled', 0)} copiers canceled")
        elif name == "PLAN_RESULT":
            verified = {True: ", verified", False: ", destination check FAILED", None: ""}[v.get("verified")]
            self.out(
                f"Done: {v['completed']}/{v['ranges']} ranges completed"
                f"{', ' + str(v['failed']) + ' failed' if v.get('failed') else ''}"
                f"{', ' + str(v['canceled']) + ' canceled' if v.get('canceled') else ''}{verified}."
            )
            return v
        return None

    def _show_cost(self) -> None:
        try:
            usd, n = run_cost(self.api, self.ns, self.label)
            self.out(
                f"Cost ≈ {fmt_usd(usd)} for {n} Jobs (from their run times; your billing page has the exact figure)."
            )
        except Exception as e:  # noqa: BLE001 — the cost is a bonus: never fail the command on it
            self.out(f"Cost: could not read it ({e}).")

    # -- all of it
    def run(self) -> int:
        self.prepare()
        plan = self.analyze()
        plan = self.apply_overrides(plan)
        self.show_plan(plan)
        if self.o.analyze_only:
            return 0
        if not self.o.yes and not self.ask("Start the copy?"):
            self.out("Nothing started.")
            return 0
        self.launch(plan)
        if self.o.detach:
            return 0
        return self.follow()
