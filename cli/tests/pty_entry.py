"""The CLI with a fake Hub, for test_pty.py: run by the real `hf` as an
extension, in a pseudo-terminal. The planner "runs" until it is canceled.

The cancel goes through the real path (a detached process, as on Ctrl-C);
here that process only drops a flag file, which ends the fake planner.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from types import SimpleNamespace

from hf_s3ream_cli import hub
from hf_s3ream_cli.main import main

FLAG = os.environ["PTY_TEST_FLAG"]


def detached_cancel(namespace: str, job_id: str) -> None:
    subprocess.Popen(
        [sys.executable, "-c", f"import time; time.sleep(0.5); open({FLAG!r}, 'w').write({job_id!r})"],
        start_new_session=True,
    )


hub.spawn_cancel = detached_cancel


def line(name: str, **v) -> str:
    return f"{name} {json.dumps(v)}"


class Fake:
    token = "hf_fake"

    def __init__(self):
        self.jobs = {}

    def whoami(self):
        return {"name": "alice", "canPay": True}

    def create_bucket(self, bucket_id, private=None):
        pass

    def run_job(self, **spec):
        job = SimpleNamespace(
            id=f"{len(self.jobs):024x}", created_at=datetime.now(timezone.utc), started_at=None, finished_at=None,
            flavor=spec["flavor"], command=spec["command"], labels=spec["labels"],
            status=SimpleNamespace(stage="RUNNING"),
        )  # fmt: skip
        self.jobs[job.id] = job
        return job

    def fetch_job_logs(self, job_id, namespace=None, follow=False, tail=None):
        job = self.jobs[job_id]
        if "--dry-run" in job.command:
            yield line("DRYRUN_STATS", count=8, total_bytes=2000, region="eu-west-3")
            yield line(
                "DRYRUN_PLAN", copier_flavor="cpu-upgrade", copiers=2, launch_stagger_ms=1500, max_inflight=2,
                planner_flavor="cpu-basic", range_gib=1, timeout_s=7200, usd=0.01, wall_s=90, commit_bound=False,
            )  # fmt: skip
            yield "DRYRUN_BUCKET ok"
            job.status.stage = "COMPLETED"
            return
        if not follow:
            return
        i = 0
        while not os.path.exists(FLAG):
            i += 1
            yield line(
                "PLAN_PROGRESS", ranges=2, done=0, active=2, error=0, canceled=0, files_planned=8,
                bytes_planned=2000, listing_done=True, files_copied=i, bytes_copied=min(1999, 10 * i),
                committed=i, skipped=0, skipped_bytes=0, mibps_5s=150.0, committed_fps_5s=2.0,
            )  # fmt: skip
            time.sleep(0.3)
            if os.environ.get("PTY_TEST_FINISH") and i >= 20:
                yield line("PLAN_RESULT", ranges=2, completed=2, failed=0, canceled=0, retried=0, verified=True)
                job.status.stage = "COMPLETED"
                return
        yield line("PLAN_STOPPED", signal="SIGTERM", ranges=2, canceled=2)
        job.status.stage = "CANCELED"

    def wait_for_job(self, job_id, namespace=None):
        return self.jobs[job_id]

    def inspect_job(self, job_id, namespace=None):
        return self.jobs[job_id]

    def cancel_job(self, job_id, namespace=None):
        with open(FLAG, "w") as f:
            f.write(job_id)

    def list_jobs(self, namespace=None, labels=None, status=None):
        if os.path.exists(FLAG):
            for j in self.jobs.values():
                if j.status.stage == "RUNNING":
                    j.status.stage = "CANCELED"
        out = list(self.jobs.values())
        return [j for j in out if not status or j.status.stage in status]

    def list_jobs_hardware(self):
        return [SimpleNamespace(name="cpu-basic", unit_cost_usd=0.000167)]


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], api=Fake()))
