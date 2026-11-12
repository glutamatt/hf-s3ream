"""What the commands do: copy, status, stop, attach.

Remote first: the copy runs on HF Jobs, like the web Space. A dry-run Job
reads the source and prints the plan (DRYRUN_PLAN, computed by the binary);
one planner Job runs the copy and prints its progress (PLAN_PROGRESS, with
the live sums of its copiers). This module launches, follows and stops: all
the copy logic lives in the binary.

Everything shown goes through `ui.emit(event)` (the event lines) and
`ui.live(view)` (the live block): see render.py.
"""

from __future__ import annotations

import queue
import shlex
import signal
import threading
import time
from dataclasses import dataclass, field, replace

from . import IMAGE, hub, markers
from .control import CONFIRM_S, CTRL_C, StopControl
from .events import Exit, Fail

VERSION = IMAGE.rsplit(":", 1)[-1]
RUST_LOG = "hf_s3ream=info,xet_data=warn,xet_client=warn"
# The dry-run Job stops listing after DRY_RUN_LIST_S and sizes the copy from
# what it listed (`partial` plan: a very large source, the planner lists it
# in full). Its timeout leaves room for scheduling, the image pull and the
# bucket check; it is only a net, as is the same deadline here.
DRY_RUN_LIST_S = 120
DRY_RUN_TIMEOUT_S = 300
COST_EVERY_S = 60.0
COMMIT_HEAVY_AVG_BYTES = 4 * 2**20


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
    label: str | None = None
    max_cost: float | None = None
    image: str = IMAGE
    copier_flavor: str | None = None
    range_gib: int | None = None
    max_inflight: int | None = None
    timeout_s: int | None = None
    extra: tuple[str, ...] = field(default_factory=tuple)
    # The Jobs' log filter: the dry-run's and the planner's, which passes it on to its copiers.
    rust_log: str = RUST_LOG

    def overrides(self) -> dict:
        o = {
            "copier_flavor": self.copier_flavor,
            "range_gib": self.range_gib,
            "max_inflight": self.max_inflight,
            "timeout_s": self.timeout_s,
        }
        return {k: v for k, v in o.items() if v is not None}

    def resizes(self) -> bool:
        """The plan is changed by hand in a way that changes its time and
        cost: then there is no estimate. (The timeout changes neither.)"""
        return any(v is not None for v in (self.copier_flavor, self.range_gib, self.max_inflight))


class Session:
    """One command's link to the Hub, the screen and the keyboard.

    `q` receives everything the command waits for: log lines, keys, signals,
    cost reads, stop progress. `keys` says whether key presses arrive on it
    (a terminal), `ui` shows the events.
    """

    def __init__(self, api, ui, q: queue.Queue, keys: bool, spawn_cancel=None, clock=time.monotonic):
        self.api, self.ui, self.q, self.keys = api, ui, q, keys
        self.spawn_cancel = spawn_cancel or (lambda ns, job: hub.spawn_cancel(ns, job))
        self.clock = clock
        self.user: str | None = None
        # The Job a SIGINT must cancel at once (`hf` kills this process 0.25 s
        # after a SIGINT): the dry-run, then the planner.
        self.sigint_target: tuple[str, str] | None = None
        self.sweep_every_s = 3.0

    # ---------- shared steps ----------

    def whoami(self, namespace: str | None, emit: bool = True) -> str:
        try:
            info = self.api.whoami()
        except Exception as e:
            raise Fail(
                "auth", f"could not read your Hugging Face account: {e}", Exit.AUTH, "run `hf auth login`"
            ) from e
        self.user = info["name"]
        ns = namespace or self.user
        if emit:
            self.ui.emit({"type": "account", "user": self.user, "namespace": ns, "can_pay": info.get("canPay")})
        if info.get("canPay") is False and ns == self.user:
            raise Fail(
                "billing",
                "your account cannot run Jobs: Jobs need a positive credit balance",
                Exit.AUTH,
                "add credits at https://huggingface.co/settings/billing",
            )
        return ns

    def cmd(self, verb: str, run: str, ns: str) -> str:
        org = f" --namespace {ns}" if ns != self.user else ""
        return f"hf s3ream {verb} {run}{org}"

    # ---------- copy ----------

    def copy(self, o: Options) -> int:
        self.ui.emit({"type": "start", "command": "copy", "version": VERSION, "source": o.source, "dest": o.dest})
        if not o.source.startswith("s3://"):
            raise Fail("usage", "the source must be an S3 URL", Exit.USAGE, "write it as s3://bucket/prefix/")
        hub.split_dest(o.dest, "-")  # the form, before any network call
        label = hub.check_label(o.label) if o.label else hub.new_label()
        if o.max_cost is not None and o.resizes():
            raise Fail(
                "usage",
                "--max-cost needs the estimate, and a plan changed by hand has none",
                Exit.USAGE,
                "drop --copier-flavor/--range-gib/--max-inflight, or drop --max-cost",
            )
        token = self.api.token or _get_token()
        if not token:
            raise Fail("auth", "no Hugging Face token", Exit.AUTH, "run `hf auth login`, or set HF_TOKEN")
        ns = self.whoami(o.namespace)
        if o.label:
            self.check_label_free(label, ns)
        if o.anonymous and (unused := hub.ignored_credentials()):
            self.ui.emit({"type": "warning", "message": hub.ignored_credentials_message(unused)})
        aws, expires = hub.aws_secrets(o.anonymous)
        if expires:
            self.ui.emit(
                {"type": "warning", "message": f"the AWS credentials expire at {expires}: the copy must end before"}
            )
        secrets = {"HF_TOKEN": token, **aws}
        # A bare bucket name is a bucket of `ns`: the Jobs get the full form.
        o = replace(o, dest=hub.full_dest(o.dest, ns))
        self.ensure_bucket(o.dest, ns)

        plan = self.analyze(o, ns, secrets, label)
        if o.max_cost is not None:
            check_budget(plan, o.max_cost)
        if o.analyze_only:
            return Exit.OK
        if not o.yes and not self.confirm("Start the copy?"):
            raise Fail("not_started", "nothing started", Exit.NOT_STARTED, "add --yes to start without a question")

        run = self.launch(o, ns, secrets, label, plan)
        attach, stop = self.cmd("attach", label, ns), self.cmd("stop", label, ns)
        self.ui.emit(
            {"type": "started", "run": label, "planner_job": run.planner_id, "namespace": ns, "url": run.url,
             "attach": attach, "stop": stop}
        )  # fmt: skip
        if o.detach:
            self.ui.emit(
                {"type": "detached", "run": label, "planner_job": run.planner_id, "attach": attach, "stop": stop}
            )
            return Exit.OK
        return self.follow(run, resume=resume_command(o), bucket_url=bucket_url(o.dest, ns))

    def check_label_free(self, label: str, ns: str) -> None:
        """A label names one copy: refused once a planner carries it, running
        or ended. (A dry-run alone does not count: a start that failed before
        the copy can be retried with the same label.) So `status`, `attach`
        and the cost always mean that one copy."""
        planners = [j for j in hub.run_jobs(self.api, ns, label) if hub.is_planner(j)]
        if not planners:
            return
        if any(j.status.stage not in hub.TERMINAL for j in planners):
            raise Fail(
                "already_running",
                f"a copy labeled {label!r} is still running",
                Exit.ALREADY_RUNNING,
                f"follow it with `{self.cmd('attach', label, ns)}`, or stop it with `{self.cmd('stop', label, ns)}`",
                run=label,
            )
        raise Fail(
            "label_used",
            f"the label {label!r} belongs to a copy that has ended",
            Exit.ALREADY_RUNNING,
            f"see it with `{self.cmd('status', label, ns)}`, or pick another label",
            run=label,
        )

    def ensure_bucket(self, dest: str, ns: str) -> None:
        bucket_ns, name = hub.split_dest(dest, ns)
        bucket = f"{bucket_ns}/{name}"
        created, private = hub.ensure_bucket(self.api, bucket)
        self.ui.emit({"type": "bucket", "bucket": bucket, "private": private, "created": created})

    def _job(self, o: Options, ns: str, secrets: dict, args: list, flavor: str, timeout_s: int, label: str):
        command = ["hf-s3ream", o.source, o.dest, *args, *o.extra]
        if o.anonymous:
            command.append("--no-sign-request")
        return self.api.run_job(
            image=o.image,
            command=command,
            env={"RUST_LOG": o.rust_log},
            secrets=secrets,
            flavor=flavor,
            timeout=timeout_s,
            labels={hub.LABEL_KEY: label},
            namespace=ns,
        )

    def analyze(self, o: Options, ns: str, secrets: dict, label: str) -> dict:
        args = ["--dry-run", "--dry-run-max-secs", str(DRY_RUN_LIST_S)]
        job = self._job(o, ns, secrets, args, "cpu-basic", DRY_RUN_TIMEOUT_S, label)
        deadline = self.clock() + DRY_RUN_TIMEOUT_S
        hub.LogPump(self.api, ns, job.id, self.q).start()
        self.sigint_target = (ns, job.id)
        stats = plan = bucket_ok = listing = None
        failure = markers.Failure()
        # The countdown of the listing starts with the Job's first line.
        view: dict = {"kind": "analyze", "deadline": None}
        self.ui.live(view)
        with self.signals():
            while True:
                if self.clock() > deadline:
                    break
                try:
                    kind, val = take(self.q, job.id, timeout=0.2)
                except queue.Empty:
                    self.ui.live(view)
                    continue
                if kind == "eof":
                    break
                if kind in ("key", "signal") and (val == CTRL_C or kind == "signal"):
                    self.ui.live(None)
                    if kind == "key":  # a signal was handled at once (see signals())
                        self.cancel_now(ns, job.id)
                    raise Fail("interrupted", "interrupted: nothing started", Exit.STOPPED)
                if kind != "line":
                    continue
                if view["deadline"] is None:
                    view["deadline"] = self.clock() + DRY_RUN_LIST_S
                m = markers.parse(val)
                if not m:
                    failure.feed(val)
                    continue
                name, v = m
                if name == "DRYRUN_STATS":
                    stats = v
                elif name == "DRYRUN_PLAN":
                    plan = v
                elif name == "DRYRUN_BUCKET":
                    bucket_ok = v == "ok"
                elif name == "LISTING" and not v.get("done"):
                    listing = v
                    view.update(listed=v.get("listed", 0), bytes=v.get("bytes", 0))
                    self.ui.emit({"type": "listing", "listed": v.get("listed", 0), "bytes": v.get("bytes", 0)})
                if plan is not None and bucket_ok is not None:
                    break
        self.ui.live(None)
        self.sigint_target = None
        if plan is None:
            try:
                self.api.cancel_job(job_id=job.id, namespace=ns)
            except Exception:  # noqa: BLE001, S110 — it may have ended already
                pass
        if bucket_ok is False:
            raise Fail(
                "bucket",
                "the Job could not get write access to the destination bucket",
                Exit.AUTH,
                "check that your token can write to this bucket",
            )
        overrides = o.overrides()
        job_url = f"https://huggingface.co/jobs/{ns}/{job.id}"
        if plan is None and failure.usage:
            # The planner would refuse the same options: say so, even with the whole plan set by hand.
            raise Fail(
                "usage",
                f"hf-s3ream refused an option: {failure.usage}",
                Exit.USAGE,
                f"see `hf s3ream --help`. The Job's log: {job_url}",
            )
        if plan is None:
            if len(overrides) == 4:
                plan = {"planner_flavor": "cpu-basic", "launch_stagger_ms": 1500}
                self.ui.emit({"type": "warning", "message": "no plan from the dry-run: running the plan set by hand"})
            elif stats is not None:
                raise Fail(
                    "source",
                    f"the image {o.image} prints no plan (DRYRUN_PLAN)",
                    Exit.SOURCE,
                    "use the image this CLI pins (drop --image), or set the whole plan by hand",
                )
            elif listing:
                raise Fail(
                    "source",
                    f"the dry-run Job ended while it was listing ({listing.get('listed', 0):,} objects so far)",
                    Exit.SOURCE,
                    f"see its log: {job_url}",
                )
            else:
                cause = failure.cause()
                hint = "check the AWS credentials and the region; a bucket open only to a VPC needs --local"
                if cause and cause.startswith("NoSuchBucket"):
                    # The region probe fails too on a missing bucket: the name is the problem.
                    hint = "check the bucket name in SRC"
                elif cause and cause.startswith("AccessDenied") and o.anonymous:
                    hint = (
                        "this bucket does not allow anonymous reads: drop --no-sign-request "
                        "and give AWS credentials (environment or AWS_PROFILE)"
                    )
                elif failure.region_fallback:
                    hint = (
                        "the bucket's region could not be detected, so us-east-1 was used: "
                        "pass --aws-region if the bucket lives elsewhere"
                    )
                raise Fail(
                    "source",
                    f"the dry-run could not read the source{f': {cause}' if cause else ''}",
                    Exit.SOURCE,
                    f"{hint}. The Job's log: {job_url}",
                    cause=cause,
                )
        partial = bool(plan.get("partial"))
        if stats is not None:
            self.ui.emit(
                {
                    "type": "source",
                    "files": stats["count"],
                    "bytes": stats["total_bytes"],
                    "region": stats.get("region"),
                    "partial": partial,
                    "listed_in_s": DRY_RUN_LIST_S if partial else None,
                }
            )
        plan = {**plan, **overrides, "estimated": True, "partial": partial}
        if o.resizes():
            plan.update(usd=None, wall_s=None, copiers=None, commit_bound=None)
            plan.update({k: None for k in ("min_usd", "min_wall_s", "min_copiers") if k in plan})
        for k in ("copiers", "wall_s", "usd", "commit_bound"):
            plan.setdefault(k, None)
        self.ui.emit({"type": "plan", **plan})
        return plan

    def confirm(self, question: str) -> bool:
        if not (self.ui.interactive and self.keys):
            raise Fail(
                "needs_confirmation",
                "the copy needs a confirmation, and there is no terminal to ask",
                Exit.NOT_STARTED,
                "add --yes to start without a question (and --max-cost USD as a budget guard)",
            )
        self.ui.ask_line(question)
        while True:
            kind, val = take(self.q, None)
            if kind == "key":
                yes = val in ("y", "Y")
                self.ui.console.print("yes" if yes else "no")
                return yes

    def launch(self, o: Options, ns: str, secrets: dict, label: str, plan: dict) -> hub.Run:
        args = [
            "--plan",
            "--range-gib", str(plan["range_gib"]),
            "--max-inflight", str(plan["max_inflight"]),
            "--launch-stagger-ms", str(plan.get("launch_stagger_ms", 1500)),
            "--copier-image", o.image,
            "--copier-flavor", plan["copier_flavor"],
            "--jobs-namespace", ns,
            "--run-label", label,
        ]  # fmt: skip
        if o.skip_existing:
            args.append("--skip-existing")
        job = self._job(o, ns, secrets, args, plan.get("planner_flavor", "cpu-basic"), int(plan["timeout_s"]), label)
        return hub.Run(label, ns, job)

    # ---------- follow ----------

    def follow(self, run: hub.Run, tail: int | None = None, resume: str | None = None, bucket_url: str | None = None):
        """Show the copy until it ends, is stopped, or is left (detach)."""
        hub.LogPump(self.api, run.namespace, run.planner_id, self.q, tail=tail).start()
        self.sigint_target = (run.namespace, run.planner_id)
        threading.Thread(target=self._cost_loop, args=(run,), daemon=True).start()
        control = StopControl()
        f = Follow(run, self.started_epoch(run))
        view = {"kind": "copy", "control": "copying", "confirm_s": CONFIRM_S}
        sweep_done = stream_done = False
        stop_left = None

        def show():
            view.update(progress=f.progress(), state=f.state, control=control.state, left=control.left(self.clock()))
            self.ui.live(view)

        show()
        with self.signals():
            while True:
                try:
                    kind, val = take(self.q, run.planner_id, timeout=0.1)
                except queue.Empty:
                    control.tick(self.clock())
                    show()
                    continue
                action = None
                if kind == "line":
                    ev = f.line(val)
                    if ev:
                        self.ui.live(None)
                        self.ui.emit(ev)
                    if f.last_progress_changed:
                        self.ui.emit({"type": "progress", **f.progress()})
                        f.last_progress_changed = False
                elif kind == "warning":
                    self.ui.live(None)
                    self.ui.emit({"type": "warning", "message": val})
                elif kind == "cost":
                    f.cost = val
                elif kind == "key":
                    action = control.key(val, self.clock())
                elif kind == "signal":
                    action = control.signal()
                elif kind == "sweep":
                    stop_left = val
                    view["stopping"] = {"planner_done": f.planner_ended or val == 0, "left": val}
                elif kind == "swept":
                    sweep_done = True
                    stop_left = val
                elif kind == "eof":
                    stream_done = True

                if action == "stop":
                    # From a detached process: it survives if the terminal
                    # closes. (A SIGINT already canceled, see signals().)
                    if kind == "key":
                        self.spawn_cancel(run.namespace, run.planner_id)
                    view["stopping"] = {"planner_done": False, "left": None}
                    self.ui.live(None)
                    self.ui.emit({"type": "stopping", "run": run.label, "reason": "user"})
                    threading.Thread(target=self._sweep, args=(run,), daemon=True).start()
                elif action == "detach":
                    self.ui.live(None)
                    self.ui.emit(
                        {"type": "detached", "run": run.label, "planner_job": run.planner_id,
                         "attach": self.cmd("attach", run.label, run.namespace),
                         "stop": self.cmd("stop", run.label, run.namespace)}
                    )  # fmt: skip
                    return Exit.OK
                elif action == "quit":
                    self.ui.live(None)
                    self.ui.emit(
                        {"type": "warning", "message": "left while stopping: the stop goes on. Check with "
                         f"`{self.cmd('status', run.label, run.namespace)}`"}
                    )  # fmt: skip
                    return Exit.STOPPED

                if control.state == "stopping":
                    if sweep_done and (stream_done or stop_left == 0):
                        return self._finish(f, run, stopped=True, left=stop_left, resume=resume)
                elif f.result is not None or stream_done:
                    return self._finish(f, run, stopped=False, resume=resume, bucket_url=bucket_url)
                show()

    def _finish(self, f, run, stopped: bool, left=None, resume=None, bucket_url=None) -> int:
        self.ui.live(None)
        try:
            f.cost = hub.run_cost(self.api, run.namespace, run.label)
        except Exception:  # noqa: BLE001, S110 — the cost is a bonus
            pass
        if not stopped and f.result is None:
            stage = self._stage(run)
            stopped = stage == "CANCELED"
            if not stopped:
                self.ui.emit(
                    {"type": "warning", "message": f"the planner ended {stage} without a result: see {run.url}"}
                )
        ev = f.result_event(stopped)
        if stopped and resume:
            ev["resume"] = resume
        if not stopped and ev["state"] == "failed" and resume:
            ev["resume"] = resume
        if bucket_url:
            ev["bucket_url"] = bucket_url
        if left:
            self.ui.emit({"type": "warning", "message": f"{left} Job(s) of this copy did not stop: see {run.url}"})
        self.ui.emit(ev)
        return ev["exit_code"]

    def _stage(self, run: hub.Run) -> str:
        try:
            return self.api.inspect_job(job_id=run.planner_id, namespace=run.namespace).status.stage
        except Exception:  # noqa: BLE001
            return "UNKNOWN"

    def _sweep(self, run: hub.Run) -> None:
        _canceled, left = hub.sweep(
            self.api, run, on_round=lambda n: self.q.put(("sweep", n)), every_s=self.sweep_every_s
        )
        self.q.put(("swept", left))

    def _cost_loop(self, run: hub.Run) -> None:
        time.sleep(15)
        while True:
            try:
                self.q.put(("cost", hub.run_cost(self.api, run.namespace, run.label)))
            except Exception:  # noqa: BLE001, S110 — try again next minute
                pass
            time.sleep(COST_EVERY_S)

    def started_epoch(self, run: hub.Run) -> float:
        return run.started.timestamp() if run.started else time.time()

    def cancel_now(self, ns: str, job_id: str) -> None:
        """Cancel the dry-run (Ctrl-C as a key: this process lives on)."""
        try:
            self.api.cancel_job(job_id=job_id, namespace=ns)
        except Exception:  # noqa: BLE001 — it ends by itself within 2 minutes anyway
            self.spawn_cancel(ns, job_id)

    def signals(self):
        """SIGINT on the queue, for the time of a `with` block. In a terminal
        with keys, Ctrl-C is a key instead and SIGINT only comes from outside."""
        return _SigintToQueue(self)

    # ---------- status / stop / attach ----------

    def status(self, ref: str, namespace: str | None) -> int:
        ns = self.whoami(namespace, emit=False)
        run = hub.find_run(self.api, ns, ref)
        self.ui.emit(self.status_event(run, self._stage(run)))
        return Exit.OK

    def status_event(self, run: hub.Run, stage: str) -> dict:
        """Where a copy stands, from one read of its planner's stage and log."""
        found = hub.last_markers(self.api, run.namespace, run.planner_id)
        f = Follow(run, self.started_epoch(run))
        for name in ("PLAN_PROGRESS", "PLAN_DONE", "VERIFYING", "PLAN_RESULT", "PLAN_STOPPED"):
            if name in found:
                f.marker(name, found[name])
        try:
            f.cost = hub.run_cost(self.api, run.namespace, run.label)
        except Exception:  # noqa: BLE001, S110
            pass
        if stage in hub.TERMINAL:
            # A planner stopped by a signal can end in ERROR after PLAN_STOPPED.
            stopped = stage == "CANCELED" or "PLAN_STOPPED" in found
            result = f.result_event(stopped)
            state = result["state"]
        else:
            result = None
            state = "starting" if stage == "SCHEDULING" else f.state
        return {"type": "status", "run": run.label, "planner_job": run.planner_id, "state": state, "stage": stage,
                "progress": f.progress() if f.snap else None, "result": result,
                "cost_usd": round(f.cost, 4) if f.cost is not None else None}  # fmt: skip

    def stop(self, ref: str, namespace: str | None) -> int:
        ns = self.whoami(namespace, emit=False)
        run = hub.find_run(self.api, ns, ref)
        if not hub.run_jobs(self.api, ns, run.label, active=True):
            self.ui.emit({"type": "stopped", "run": run.label, "canceled_jobs": 0, "left": 0, "already_ended": True})
            return Exit.OK
        self.ui.live({"kind": "sweep", "text": f"Stopping {run.label}: canceling its Jobs…"})

        def on_round(n):
            self.ui.live(
                {"kind": "sweep", "text": f"Stopping {run.label}: {n} Job{'s' if n != 1 else ''} still running…"}
            )

        canceled, left = hub.sweep(self.api, run, on_round=on_round, every_s=self.sweep_every_s)
        self.ui.live(None)
        self.ui.emit(
            {"type": "stopped", "run": run.label, "canceled_jobs": canceled, "left": left, "already_ended": False}
        )
        return Exit.OK if left == 0 else Exit.FAILED

    def attach(self, ref: str, namespace: str | None) -> int:
        ns = self.whoami(namespace, emit=False)
        run = hub.find_run(self.api, ns, ref)
        stage = self._stage(run)
        if stage in hub.TERMINAL:
            ev = self.status_event(run, stage)
            self.ui.emit(ev)
            return ev["result"]["exit_code"]
        self.ui.emit({"type": "start", "command": "attach", "version": VERSION, "source": None, "dest": None})
        return self.follow(run, tail=60)


class Follow:
    """The state of a copy, from the planner's marker lines."""

    def __init__(self, run: hub.Run, started_epoch: float):
        self.run, self.started_epoch = run, started_epoch
        self.snap: dict | None = None
        self.state = "copying"
        self.result: dict | None = None
        self.stopped: dict | None = None
        self.cost: float | None = None
        self.planner_ended = False
        self.has_progress = False
        self.last_progress_changed = False

    def line(self, text: str) -> dict | None:
        m = markers.parse(text)
        return self.marker(*m) if m else None

    def marker(self, name: str, v) -> dict | None:
        """Fold one marker in; return an event to show, if any."""
        if name == "PLAN_PROGRESS":
            self.snap = v
            self.has_progress = True
            self.last_progress_changed = True
        elif name == "RANGE" and not self.has_progress:
            # An image without PLAN_PROGRESS: count the plan from the ranges.
            s = self.snap = self.snap or {}
            s["ranges"] = s.get("ranges", 0) + 1
            s["files_planned"] = s.get("files_planned", 0) + v.get("files", 0)
            s["bytes_planned"] = s.get("bytes_planned", 0) + v.get("bytes", 0)
            self.last_progress_changed = True
        elif name == "PLAN_DONE":
            s = self.snap = self.snap or {}
            s["listing_done"] = True
            if not self.has_progress:
                s.update(ranges=v.get("ranges", 0), files_planned=v.get("files", 0), bytes_planned=v.get("bytes", 0))
            self.last_progress_changed = True
        elif name == "VERIFYING":
            self.state = "verifying"
        elif name == "COPIER" and (v.get("attempt") or 1) > 1:
            return {
                "type": "warning",
                "message": f"range {v.get('idx')}: copier failed ({v.get('prev', 'ERROR')}), "
                f"new attempt {v['attempt']} of 3",
            }
        elif name == "PLAN_STOPPED":
            self.stopped = v
            self.planner_ended = True
        elif name == "PLAN_RESULT":
            self.result = v
            self.planner_ended = True
        return None

    def progress(self) -> dict:
        s = self.snap or {}
        elapsed = max(0.0, time.time() - self.started_epoch)
        skipped, skipped_bytes = s.get("skipped", 0), s.get("skipped_bytes", 0)
        planned_b, planned_f = s.get("bytes_planned", 0), s.get("files_planned", 0)
        # Two sources: the copiers' live counters (copied, skipped), and the
        # completed ranges (`*_done`, the whole range: skipped files included).
        done_b = max(s.get("bytes_copied", 0) + skipped_bytes, s.get("bytes_done", 0))
        # A file counts once committed; before the first commit, once copied.
        live_f = s.get("committed", 0) or s.get("files_copied", 0)
        done_f = max(live_f + skipped, s.get("files_done", 0))
        bytes_copied, files_copied = done_b - skipped_bytes, done_f - skipped
        pct = min(100.0, 100.0 * done_b / planned_b) if planned_b else 0.0
        committed = s.get("committed", 0)
        eta = None
        if s.get("listing_done") and elapsed > 0:
            etas = []
            if bytes_copied > 0 and planned_b > done_b:
                etas.append((planned_b - done_b) / (bytes_copied / elapsed))
            # Files committed set the pace only for small files (average
            # <= 4 MiB, the planner's own test): big files commit in bursts
            # (every 16 GiB), and a files-based ETA would jump around.
            to_commit = planned_f - skipped
            commit_bound = planned_f > 0 and planned_b / planned_f <= COMMIT_HEAVY_AVG_BYTES
            if commit_bound and committed > 0 and to_commit > committed:
                etas.append((to_commit - committed) / (committed / elapsed))
            eta = max(etas) if etas else None
        return {
            "run": self.run.label,
            "state": self.state,
            "ranges": s.get("ranges", 0),
            "done": s.get("done", 0),
            "active": s.get("active", 0),
            "failed": s.get("error", 0),
            "canceled": s.get("canceled", 0),
            "listing_done": bool(s.get("listing_done")),
            "files_planned": planned_f,
            "bytes_planned": planned_b,
            "files_copied": files_copied,
            "bytes_copied": bytes_copied,
            "committed": committed,
            "skipped": skipped,
            "skipped_bytes": skipped_bytes,
            "bytes_done_total": done_b,
            "files_done_total": done_f,
            "mibps": s.get("mibps_5s", 0.0),
            "files_per_s": s.get("committed_fps_5s", 0.0),
            "pct": round(pct, 1),
            "elapsed_s": round(elapsed),
            "eta_s": round(eta) if eta is not None else None,
            "cost_usd": round(self.cost, 4) if self.cost is not None else None,
        }

    def result_event(self, stopped: bool) -> dict:
        r = self.result or {}
        p = self.progress()
        ranges = r.get("ranges", p["ranges"])
        failed, canceled = r.get("failed", p["failed"]), r.get("canceled", p["canceled"])
        verified = r.get("verified")
        if stopped:
            state, code = "stopped", Exit.STOPPED
        elif self.result and not failed and not canceled and verified is not False:
            state, code = "completed", Exit.OK
        else:
            state, code = "failed", Exit.FAILED
        files, bytes_ = p["files_done_total"], p["bytes_done_total"]
        if state == "completed" and not self.has_progress:
            # An image without PLAN_PROGRESS: a complete copy is the whole plan.
            files, bytes_ = p["files_planned"], p["bytes_planned"]
        return {
            "type": "result",
            "run": self.run.label,
            "state": state,
            "ranges": ranges,
            "completed": r.get("completed", p["done"]),
            "failed": failed,
            "canceled": canceled,
            "verified": verified,
            "files": files,
            "bytes": bytes_,
            "elapsed_s": p["elapsed_s"],
            "cost_usd": p["cost_usd"],
            "exit_code": int(code),
        }


def take(q: queue.Queue, job: str | None, timeout: float | None = None) -> tuple[str, object]:
    """The next (kind, value) on the queue for this step: items from the log
    of another Job (an earlier step) are dropped. Raises queue.Empty."""
    while True:
        item = q.get(timeout=timeout)
        if len(item) == 3 and item[2] != job:
            continue
        return item[0], item[1]


def resume_command(o: Options) -> str:
    """The same copy again, skipping what is already in the bucket: every
    option that changes what is read or written, quoted for a shell."""
    argv = ["hf", "s3ream", o.source, o.dest, "--skip-existing"]
    if o.anonymous:
        argv.append("--no-sign-request")
    if o.namespace:
        argv += ["--namespace", o.namespace]
    for flag, value in (
        ("--copier-flavor", o.copier_flavor),
        ("--range-gib", o.range_gib),
        ("--max-inflight", o.max_inflight),
        ("--timeout", o.timeout_s),
    ):
        if value is not None:
            argv += [flag, str(value)]
    if o.image != IMAGE:
        argv += ["--image", o.image]
    return shlex.join([*argv, *o.extra])


def check_budget(plan: dict, max_cost: float) -> None:
    """--max-cost: refuse a plan whose estimate is over the budget, or whose
    cost cannot be known (a source too big to list in a dry-run)."""
    usd = plan.get("usd")
    if usd is not None:
        if usd > max_cost:
            raise Fail(
                "over_budget",
                f"the estimate ({usd:.2f} USD) is over --max-cost {max_cost:.2f}",
                Exit.NOT_STARTED,
                "raise --max-cost, or copy a smaller prefix",
                estimate_usd=usd,
            )
        return
    low = plan.get("min_usd")
    if low is not None and low > max_cost:
        raise Fail(
            "over_budget",
            f"the part of the source listed alone costs about {low:.2f} USD, over --max-cost {max_cost:.2f}",
            Exit.NOT_STARTED,
            "raise --max-cost, or copy a smaller prefix",
            estimate_usd=None,
            min_usd=low,
        )
    known = f" (the part listed alone: about {low:.2f} USD)" if low is not None else ""
    raise Fail(
        "cost_unknown",
        f"the cost of this copy is unknown, so --max-cost cannot be checked: the source is too big "
        f"to list in {DRY_RUN_LIST_S // 60} minutes{known}",
        Exit.NOT_STARTED,
        "drop --max-cost to start anyway, or copy a smaller prefix",
        estimate_usd=None,
        min_usd=low,
    )


def bucket_url(dest: str, ns: str) -> str:
    bucket_ns, name = hub.split_dest(dest, ns)
    return f"https://huggingface.co/buckets/{bucket_ns}/{name}"


def _get_token():
    from huggingface_hub import get_token

    return get_token()


class _SigintToQueue:
    """On SIGINT: cancel the current Job from a detached process right away
    (under `hf`, this process has 0.25 s left), then tell the loop."""

    def __init__(self, session: Session):
        self.session = session
        self.old = None

    def _on_sigint(self, signum, frame) -> None:
        target = self.session.sigint_target
        if target:
            self.session.spawn_cancel(*target)
        self.session.q.put(("signal", "INT"))

    def __enter__(self):
        if threading.current_thread() is threading.main_thread():
            self.old = signal.signal(signal.SIGINT, self._on_sigint)
        return self

    def __exit__(self, *exc):
        if self.old is not None:
            signal.signal(signal.SIGINT, self.old)
