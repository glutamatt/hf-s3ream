"""The CLI contract: exit codes, errors, and the JSON events of `--json`.

Every JSON line on stdout is one event: `{"schema": 1, "type": ..., "time": ...}`
plus the fields listed in EVENTS. Sizes are bytes, durations seconds, rates
MiB/s or files/s, money USD. Fields may be added within a schema version;
none is removed or renamed without a new version.
"""

from __future__ import annotations

from enum import IntEnum

SCHEMA = 1


class Exit(IntEnum):
    OK = 0  # copy complete (and verified), or started with --detach, or the query worked
    FAILED = 1  # the copy ended with failed ranges, or the destination check failed
    USAGE = 2  # bad arguments
    AUTH = 3  # token, account (credits), or bucket access
    NOT_STARTED = 4  # confirmation missing or declined; --max-cost over, or not checkable
    SOURCE = 5  # the dry-run could not read the source
    ALREADY_RUNNING = 6  # this --label already names a copy, running or ended
    NOT_FOUND = 7  # status / stop / attach: no such run
    LOCAL = 8  # --local: Docker is missing or not running
    STOPPED = 130  # stopped by the user (Ctrl-C, SIGINT)


# type -> the fields it always carries (besides schema, type, time).
EVENTS: dict[str, tuple[str, ...]] = {
    "start": ("command", "version", "source", "dest"),
    "account": ("user", "namespace", "can_pay"),
    "bucket": ("bucket", "private", "created"),
    "listing": ("listed", "bytes"),
    "source": ("files", "bytes", "region", "partial"),
    "plan": (
        "copier_flavor", "range_gib", "max_inflight", "timeout_s", "planner_flavor",
        "copiers", "wall_s", "usd", "commit_bound", "estimated", "partial",
    ),
    "started": ("run", "planner_job", "namespace", "url", "attach", "stop"),
    "progress": (
        "run", "state", "ranges", "done", "active", "running", "scheduling", "failed", "canceled", "listing_done",
        "files_planned", "bytes_planned", "files_copied", "bytes_copied", "committed",
        "skipped", "skipped_bytes", "bytes_done_total", "files_done_total", "mibps", "files_per_s", "pct",
        "elapsed_s", "eta_s",
        "cost_usd",
    ),
    "warning": ("message",),
    "stopping": ("run", "reason"),
    "detached": ("run", "planner_job", "attach", "stop"),
    "result": (
        "run", "state", "ranges", "completed", "failed", "canceled", "verified",
        "files", "bytes", "elapsed_s", "cost_usd", "exit_code",
    ),
    "status": ("run", "planner_job", "state", "stage", "progress", "result", "cost_usd"),
    "stopped": ("run", "canceled_jobs", "left", "already_ended"),
    "error": ("code", "message", "hint", "exit_code"),
}  # fmt: skip


class Fail(Exception):
    """A problem to report: an `error` event (or a red line), then exit."""

    def __init__(self, code: str, message: str, exit_code: Exit, hint: str | None = None, **extra):
        super().__init__(message)
        self.code, self.message, self.exit_code, self.hint, self.extra = code, message, exit_code, hint, extra

    def event(self) -> dict:
        return {
            "type": "error",
            "code": self.code,
            "message": self.message,
            "hint": self.hint,
            "exit_code": int(self.exit_code),
            **self.extra,
        }
