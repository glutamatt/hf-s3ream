"""A source too big to list in the dry-run: the binary's partial plan, shown
as low bounds, and a budget that cannot be checked."""

from __future__ import annotations

import io

from conftest import PLANNER_LOG, RESULT, FakeApi, dry_run_log
from hf_s3ream_cli.events import Exit
from test_contract import copy, types


def test_a_partial_plan_is_launched_as_printed():
    api = FakeApi([dry_run_log(partial=True, count=79_000, total=4_009_448_772_827), PLANNER_LOG + [RESULT]])
    code, evs, _ = copy(api, yes=True)
    assert code == Exit.OK
    source = next(e for e in evs if e["type"] == "source")
    assert source["partial"] is True and source["listed_in_s"] == 120
    plan = next(e for e in evs if e["type"] == "plan")
    assert plan["partial"] is True and plan["usd"] is None and plan["copiers"] is None
    assert plan["min_usd"] == 5.7568 and plan["min_copiers"] == 59
    cmd = api.jobs[1]["command"]
    flags = dict(zip(cmd[4::2], cmd[5::2], strict=False))
    assert flags["--range-gib"] == "64" and flags["--max-inflight"] == "128"
    assert api.jobs[1]["timeout"] == 86400


def test_max_cost_cannot_be_checked_on_a_partial_plan():
    code, evs, _ = copy(FakeApi([dry_run_log(partial=True)]), yes=True, max_cost=100)
    assert code == Exit.NOT_STARTED
    err = evs[-1]
    assert err["code"] == "cost_unknown" and err["min_usd"] == 5.7568
    assert "drop --max-cost" in err["hint"]
    assert "plan" in types(evs)  # the plan is shown before the refusal


def test_max_cost_under_the_low_bound_is_over_budget():
    code, evs, _ = copy(FakeApi([dry_run_log(partial=True)]), yes=True, max_cost=1)
    assert code == Exit.NOT_STARTED
    assert evs[-1]["code"] == "over_budget" and evs[-1]["min_usd"] == 5.7568


def test_a_timeout_set_by_hand_keeps_the_estimate():
    api = FakeApi([dry_run_log(), PLANNER_LOG + [RESULT]])
    code, evs, _ = copy(api, yes=True, timeout_s=36_000, max_cost=1)
    plan = next(e for e in evs if e["type"] == "plan")
    assert code == Exit.OK and plan["usd"] == 0.0017 and plan["timeout_s"] == 36_000
    assert api.jobs[1]["timeout"] == 36_000


def test_the_terminal_shows_low_bounds_and_the_timeout():
    from rich.console import Console

    from hf_s3ream_cli.render import TuiUI

    buf = io.StringIO()
    ui = TuiUI(console=Console(file=buf, width=100, color_system=None), keys_active=False)
    ui.emit({"type": "source", "files": 79_000, "bytes": 4_009_448_772_827, "region": "us-east-1",
             "partial": True, "listed_in_s": 120})  # fmt: skip
    ui.emit({"type": "plan", "copier_flavor": "cpu-performance", "range_gib": 64, "max_inflight": 128,
             "timeout_s": 86400, "planner_flavor": "cpu-basic", "copiers": None, "wall_s": None, "usd": None,
             "commit_bound": False, "estimated": True, "partial": True, "min_copiers": 59, "min_wall_s": 125,
             "min_usd": 5.7568})  # fmt: skip
    out = buf.getvalue()
    assert "79,000+ files" in out and "3.6 TiB+" in out and "listing stopped after 2 min" in out
    assert "59+ copiers" in out and "more than 2 min" in out and "more than $5.76" in out
    assert "after 24 h" in out and "--timeout" in out
