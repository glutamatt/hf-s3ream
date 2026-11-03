"""The agent contract: JSON events, exit codes, never a question without a terminal."""

from __future__ import annotations

import queue

import pytest
from conftest import PLANNER_LOG, RESULT, FakeApi, dry_run_log, events_of, json_ui, progress_line
from hf_s3ream_cli import IMAGE, hub
from hf_s3ream_cli.events import Exit
from hf_s3ream_cli.flow import Options, Session
from hf_s3ream_cli.main import main


def session(api):
    ui, buf = json_ui()
    cancels = []
    s = Session(api, ui, queue.Queue(), keys=False, spawn_cancel=lambda ns, j: cancels.append(j))
    s.sweep_every_s = 0
    return s, buf, cancels


def copy(api, **kw):
    s, buf, cancels = session(api)
    opts = Options(source="s3://src/prefix/", dest="alice/dst", **kw)
    try:
        code = s.copy(opts)
    except hub.Fail as e:  # main() turns this into an error event
        s.ui.emit(e.event())
        code = int(e.exit_code)
    return code, events_of(buf.getvalue()), cancels


def types(evs):
    return [e["type"] for e in evs]


def test_a_full_copy_reads_as_events():
    api = FakeApi([dry_run_log(), PLANNER_LOG + [RESULT]])
    code, evs, _ = copy(api, yes=True)
    assert code == Exit.OK
    t = types(evs)
    assert t[:5] == ["start", "account", "bucket", "source", "plan"]
    assert "started" in t and t[-1] == "result"
    plan = next(e for e in evs if e["type"] == "plan")
    assert plan["usd"] == 0.0017 and plan["estimated"] is True
    result = evs[-1]
    assert result["state"] == "completed" and result["verified"] is True and result["exit_code"] == 0
    assert result["files"] == 8 and result["bytes"] == 2000
    progress = [e for e in evs if e["type"] == "progress"]
    assert progress[-1]["pct"] == 100.0 and max(e["mibps"] for e in progress) == 120.0
    # The planner runs the plan of the dry-run, with the run label.
    dry, planner = api.jobs
    assert dry["command"] == ["hf-s3ream", "s3://src/prefix/", "alice/dst", "--dry-run"]
    cmd = planner["command"]
    flags = dict(zip(cmd[4::2], cmd[5::2]))
    assert cmd[3] == "--plan"
    assert flags["--copier-flavor"] == "cpu-upgrade" and flags["--copier-image"] == IMAGE
    assert flags["--run-label"] == planner["labels"]["hf-s3ream-run"] == result["run"]
    assert planner["flavor"] == "cpu-basic" and planner["timeout"] == 7200


def test_without_yes_nothing_starts_and_nothing_waits():
    api = FakeApi([dry_run_log()])
    code, evs, _ = copy(api)
    assert code == Exit.NOT_STARTED == 4
    assert evs[-1]["code"] == "needs_confirmation" and "--yes" in evs[-1]["hint"]
    assert len(api.jobs) == 1  # the dry-run only


def test_max_cost_guards_the_budget():
    code, evs, _ = copy(FakeApi([dry_run_log()]), yes=True, max_cost=0.001)
    assert code == Exit.NOT_STARTED
    assert evs[-1]["code"] == "over_budget" and evs[-1]["estimate_usd"] == 0.0017
    code, _, _ = copy(FakeApi([dry_run_log(), PLANNER_LOG + [RESULT]]), yes=True, max_cost=1)
    assert code == Exit.OK


def test_a_label_already_running_is_refused():
    api = FakeApi([dry_run_log()], active_label="my-copy")
    code, evs, _ = copy(api, yes=True, label="my-copy")
    assert code == Exit.ALREADY_RUNNING == 6
    assert "hf s3ream attach my-copy" in evs[-1]["hint"] and evs[-1]["run"] == "my-copy"
    assert api.jobs == []


@pytest.mark.parametrize(
    ("api", "kw", "code", "err"),
    [
        (FakeApi([dry_run_log()], can_pay=False), {}, Exit.AUTH, "billing"),
        (FakeApi([dry_run_log(bucket="error")]), {}, Exit.AUTH, "bucket"),
        (FakeApi([["ERROR could not list"]]), {}, Exit.SOURCE, "source"),
        (FakeApi([dry_run_log(plan=False)]), {}, Exit.SOURCE, "source"),
        (FakeApi([]), {"label": "bad label!"}, Exit.USAGE, "usage"),
        (FakeApi([]), {"max_cost": 5, "range_gib": 8}, Exit.USAGE, "usage"),
    ],
)
def test_each_failure_has_its_code(api, kw, code, err):
    got, evs, _ = copy(api, yes=True, **kw)
    assert got == code
    assert evs[-1]["type"] == "error" and evs[-1]["code"] == err and evs[-1]["exit_code"] == code
    assert evs[-1]["hint"]


def test_a_plan_set_by_hand_runs_without_estimate():
    code, evs, _ = copy(
        FakeApi([dry_run_log(), PLANNER_LOG + [RESULT]]), yes=True, analyze_only=True, range_gib=8,
    )  # fmt: skip
    plan = next(e for e in evs if e["type"] == "plan")
    assert code == Exit.OK and plan["range_gib"] == 8 and plan["usd"] is None


def test_detach_returns_the_commands_to_follow_and_stop():
    api = FakeApi([dry_run_log(), PLANNER_LOG + [RESULT]])
    code, evs, _ = copy(api, yes=True, detach=True, label="nightly")
    assert code == Exit.OK
    d = evs[-1]
    assert d["type"] == "detached" and d["run"] == "nightly"
    assert d["attach"] == "hf s3ream attach nightly" and d["stop"] == "hf s3ream stop nightly"


def test_a_failed_range_fails_the_command():
    failed = RESULT.replace('"completed": 2, "failed": 0', '"completed": 1, "failed": 1')
    code, evs, _ = copy(FakeApi([dry_run_log(), PLANNER_LOG + [failed]]), yes=True)
    assert code == Exit.FAILED and evs[-1]["state"] == "failed" and "--skip-existing" in evs[-1]["resume"]


def test_a_retried_range_is_a_warning():
    log = [*PLANNER_LOG[:3], '{"x":1}', 'COPIER {"idx": 0, "job_id": "c1", "attempt": 2, "prev": "ERROR"}', RESULT]
    _, evs, _ = copy(FakeApi([dry_run_log(), log]), yes=True)
    assert any(e["type"] == "warning" and "attempt 2 of 3" in e["message"] for e in evs)


def test_status_stop_attach_on_a_run():
    api = FakeApi([dry_run_log(), [progress_line(done=1, active=1, bytes_copied=1500, listing_done=True)]])
    copy(api, yes=True, detach=True, label="nightly")
    planner = api.jobs[1]["id"]

    s, buf, _ = session(api)
    assert s.status("nightly", None) == Exit.OK
    st = events_of(buf.getvalue())[-1]
    assert st["type"] == "status" and st["state"] == "copying" and st["planner_job"] == planner
    assert st["progress"]["pct"] == 75.0 and st["result"] is None

    s, buf, _ = session(api)
    assert s.status(planner, None) == Exit.OK  # by Job id too

    s, buf, _ = session(api)
    assert s.stop("nightly", None) == Exit.OK
    stopped = events_of(buf.getvalue())[-1]
    assert stopped["type"] == "stopped" and stopped["left"] == 0 and planner in api.canceled

    s, buf, _ = session(api)
    assert s.stop("nightly", None) == Exit.OK  # idempotent
    assert events_of(buf.getvalue())[-1]["already_ended"] is True

    s, buf, _ = session(api)
    assert s.attach("nightly", None) == Exit.STOPPED  # ended canceled: its result


def test_an_unknown_run_is_exit_7():
    s, _buf, _ = session(FakeApi([]))
    with pytest.raises(hub.Fail) as e:
        s.status("nope", None)
    assert e.value.exit_code == Exit.NOT_FOUND == 7


def test_main_without_a_terminal_speaks_json(capsys):
    code = main(["s3://src/", "alice/dst", "--analyze"], api=FakeApi([dry_run_log()]))
    assert code == 0
    evs = events_of(capsys.readouterr().out)
    assert types(evs)[-1] == "plan"


def test_main_errors_are_events_with_codes(capsys):
    assert main(["foo", "bar"], api=FakeApi([])) == Exit.USAGE
    ev = events_of(capsys.readouterr().out)[-1]
    assert ev["code"] == "usage" and "s3://" in ev["hint"]


def test_an_image_without_plan_progress_still_shows_the_plan():
    """v0.3.12 prints RANGE and PLAN_DONE, no PLAN_PROGRESS (seen on a real run)."""
    log = [l for l in PLANNER_LOG if not l.startswith("PLAN_PROGRESS")] + [RESULT]
    log = [l.replace('"ranges": 2, "files": 8, "bytes": 2000', '"ranges": 1, "files": 4, "bytes": 1000') for l in log]
    code, evs, _ = copy(FakeApi([dry_run_log(), log]), yes=True)
    progress = [e for e in evs if e["type"] == "progress"]
    assert progress and progress[-1]["ranges"] == 1 and progress[-1]["bytes_planned"] == 1000
    assert code == Exit.OK and evs[-1]["files"] == 4 and evs[-1]["bytes"] == 1000


def test_a_canceled_job_without_end_date_costs_what_is_known():
    """Seen live: a canceled Job can come back with finished_at = None."""
    from datetime import timedelta
    from types import SimpleNamespace

    from conftest import T0

    later = T0 + timedelta(hours=5)
    job = SimpleNamespace(
        started_at=T0, finished_at=None, status=SimpleNamespace(stage="CANCELED"),
        durations=SimpleNamespace(running_secs=30),
    )  # fmt: skip
    assert hub.job_end(job, now=later) == T0 + timedelta(seconds=30)
    job.durations = None  # also seen live
    assert hub.job_end(job, now=later) == T0
    job.status.stage = "RUNNING"
    assert hub.job_end(job, now=later) == later


def test_a_closed_pipe_ends_quietly(monkeypatch, capsys):
    """Seen live: `hf s3ream status RUN --json | head` printed a traceback."""

    class Closed:
        def write(self, _):
            raise BrokenPipeError

        def flush(self):
            raise BrokenPipeError

    import hf_s3ream_cli.main as m
    from hf_s3ream_cli.render import JsonUI

    monkeypatch.setattr(m, "make_ui", lambda json_out, keys_active: JsonUI(out=Closed()))
    assert m.main(["status", "x"], api=FakeApi([])) == 141


def test_the_eta_of_big_files_follows_the_bytes_only():
    """Seen live: big files commit in bursts, and a files-based ETA jumped
    from 98 s to 179 s and back. Small files keep the files-based one."""
    import time as _time

    from hf_s3ream_cli.flow import Follow
    from hf_s3ream_cli.hub import Run

    f = Follow(Run("r", "ns", None), started_epoch=_time.time() - 100)
    snap = {"listing_done": True, "files_planned": 2000, "bytes_planned": 100 * 2**30,
            "bytes_copied": 50 * 2**30, "committed": 10}  # fmt: skip
    f.snap = dict(snap)
    assert 95 <= f.progress()["eta_s"] <= 105  # half the bytes in 100 s: ~100 s left
    f.snap = {**snap, "bytes_planned": 2000 * 2**20, "bytes_copied": 1999 * 2**20}  # 1 MiB files
    assert f.progress()["eta_s"] > 1000  # 10 of 2000 committed in 100 s: commits set the pace
