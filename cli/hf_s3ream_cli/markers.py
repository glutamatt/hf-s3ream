"""The marker lines the binary prints for its clients: `NAME {json}`.

The web Space reads the same lines (space/app.js). Any other log line is for
people and passes through untouched.
"""

from __future__ import annotations

import json
from typing import Any

KNOWN = {
    # dry-run
    "LISTING",
    "DRYRUN_STATS",
    "DRYRUN_PLAN",
    "DRYRUN_BUCKET",
    # planner
    "PLANNING",
    "RANGE",
    "COPIER",
    "PLAN_PROGRESS",
    "PLAN_DONE",
    "VERIFYING",
    "VERIFY",
    "PLAN_STOPPED",
    "PLAN_RESULT",
}


def parse(line: str) -> tuple[str, Any] | None:
    """(name, payload) for a marker line, None for any other line.

    DRYRUN_BUCKET carries a word (`ok` / `error`), the others a JSON object.
    A marker with a broken payload is not a marker.
    """
    name, _, rest = line.partition(" ")
    if name not in KNOWN or not rest:
        return None
    if name == "DRYRUN_BUCKET":
        return name, rest.strip()
    try:
        return name, json.loads(rest)
    except ValueError:
        return None
