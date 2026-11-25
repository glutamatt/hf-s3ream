"""Ctrl-C twice to stop, `d` to detach: the state machine, then the loop."""

from __future__ import annotations

import queue
import threading
import time

from conftest import PLANNER_LOG, RESULT, FakeApi, dry_run_log, events_of, json_ui
from hf_s3ream_cli.control import CTRL_C, StopControl
from hf_s3ream_cli.events import Exit
from hf_s3ream_cli.flow import Options, Session


def test_one_ctrl_c_asks_and_times_out():
    c = StopControl(confirm_s=5)
    assert c.key(CTRL_C, now=0) == "confirm"
    assert c.left(2) == 3
    assert c.tick(4.9) is None
    assert c.tick(5.0) == "resume" and c.state == "copying"


def test_two_ctrl_c_stop_and_a_third_leaves():
    c = StopControl()
    c.key(CTRL_C, 0)
    assert c.key(CTRL_C, 1) == "stop" and c.state == "stopping"
    assert c.key("x", 2) is None
    assert c.key(CTRL_C, 3) == "quit"


def test_any_other_key_keeps_copying():
    c = StopControl()
    c.key(CTRL_C, 0)
    assert c.key("\x1b", 1) == "resume" and c.state == "copying"


def test_d_detaches_at_any_time_before_the_stop():
    assert StopControl().key("d", 0) == "detach"
    c = StopControl()
    c.key(CTRL_C, 0)
    assert c.key("d", 1) == "detach"


def test_a_signal_stops_without_question():
    c = StopControl()
    assert c.signal() == "stop" and c.state == "stopping"


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def started(api, keys=True):
    """A copy whose planner log waits on a gate: it runs until the test acts."""
    ui, buf = json_ui()
    ui.interactive = True  # the loop does not care; JSON keeps the test readable
    clock = Clock()
    cancels = []
    q: queue.Queue = queue.Queue()
    s = Session(api, ui, q, keys=keys, spawn_cancel=lambda ns, j: cancels.append(j), clock=clock)
    s.sweep_every_s = 0
    out = {}

    def run():
        out["code"] = s.copy(Options(source="s3://src/", dest="alice/dst", yes=True))

    t = threading.Thread(target=run, daemon=True)
    t.start()
    # Wait until the planner runs.
    deadline = time.time() + 5
    while '"started"' not in buf.getvalue():
        assert time.time() < deadline
        time.sleep(0.01)
    return s, q, clock, buf, cancels, t, out


def wait(t):
    t.join(timeout=10)
    assert not t.is_alive()


def test_ctrl_c_then_wait_keeps_copying_then_the_copy_ends():
    gate = threading.Event()
    api = FakeApi([dry_run_log(), [*PLANNER_LOG[:4], gate, *PLANNER_LOG[4:], RESULT]])
    _s, q, clock, buf, cancels, t, out = started(api)
    q.put(("key", CTRL_C))
    time.sleep(0.3)
    clock.t += 6  # the 5 s confirmation runs out
    time.sleep(0.3)
    gate.set()
    wait(t)
    assert out["code"] == Exit.OK and cancels == []
    assert events_of(buf.getvalue())[-1]["state"] == "completed"


def test_ctrl_c_twice_stops_the_whole_copy():
    gate = threading.Event()
    api = FakeApi([dry_run_log(), [*PLANNER_LOG[:4], gate, '{"never": 1}']])
    _s, q, _clock, buf, cancels, t, out = started(api)
    planner = api.jobs[1]["id"]
    q.put(("key", CTRL_C))
    q.put(("key", CTRL_C))
    # The planner, once canceled, prints PLAN_STOPPED and its stream ends.
    time.sleep(0.3)
    api.logs[1][-1] = 'PLAN_STOPPED {"signal": "SIGTERM", "ranges": 2, "canceled": 1}'
    gate.set()
    wait(t)
    assert out["code"] == Exit.STOPPED == 130
    assert cancels == [planner]  # from the detached helper
    evs = events_of(buf.getvalue())
    assert [e["type"] for e in evs][-2:] == ["stopping", "result"] or "stopping" in [e["type"] for e in evs]
    assert evs[-1]["state"] == "stopped" and "--skip-existing" in evs[-1]["resume"]


def test_d_detaches_and_the_copy_runs_on():
    gate = threading.Event()
    api = FakeApi([dry_run_log(), [*PLANNER_LOG[:4], gate, RESULT]])
    _s, q, _clock, buf, cancels, t, out = started(api)
    q.put(("key", "d"))
    wait(t)
    gate.set()
    assert out["code"] == Exit.OK and cancels == [] and api.canceled == []
    assert events_of(buf.getvalue())[-1]["type"] == "detached"


def test_sigint_without_terminal_stops_at_once():
    gate = threading.Event()
    api = FakeApi([dry_run_log(), [*PLANNER_LOG[:4], gate, "PLAN_STOPPED {}"]])
    s, q, _clock, _buf, cancels, t, out = started(api, keys=False)
    s.spawn_cancel(*s.sigint_target)  # what the SIGINT handler does first
    q.put(("signal", "INT"))
    time.sleep(0.2)
    gate.set()
    wait(t)
    assert out["code"] == Exit.STOPPED
    assert cancels == [api.jobs[1]["id"]]
