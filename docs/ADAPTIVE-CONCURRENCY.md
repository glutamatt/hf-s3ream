# Adaptive concurrency

A copy runs many copier Jobs at the same time. This page explains how hf-s3ream decides how many copiers run, and how fast each one sends. The other choices (the plan, the check at the end) are in [DESIGN.md](DESIGN.md).

- [In short](#in-short)
- [The fleet size](#the-fleet-size)
- [On a real copy](#on-a-real-copy)
- [Why this rule](#why-this-rule)
- [The copier brake](#the-copier-brake)
- [Settings and logs](#settings-and-logs)
- [Testing](#testing)

## In short

Two loops share the work:

- **The fleet size**, in the planner, every 2 minutes: how many copiers run at the same time. The fleet grows while more copiers bring more throughput. It gets smaller when more copiers bring only cost or latency.
- **The copier brake**, in each copier, within seconds: how many bytes per second the copier sends. The copier sends at full speed. It slows down only when the storage answers "slow down" (429, 5xx).

Both serve two of the [goals](DESIGN.md#goals): throughput at a known cost, and no harm to CAS. CAS is the storage behind HF Buckets, and all the users of the Hub share it. The copiers need no coordination: they all see the same CAS, so they react together.

## The fleet size

At first, each new copier adds about the same throughput. After some point, a new copier adds nothing: it costs a Job, and the copy does not end sooner. What the storage can take also changes during a copy, because of other users or new capacity. So the planner measures its fleet again and again, and makes it larger or smaller.

![Throughput against the number of copiers running, during two real copies: it grows by ~1.1 GiB/s per copier, then stays near 43 GiB/s past ~45 copiers.](images/throughput-per-copier.png)

*Two copies of public NOAA data (files of ~40–50 MiB), while their fleet grew. Up to ~40 copiers, each copier adds ~1.1 GiB/s. Above ~45, the throughput stays near 43 GiB/s: the destination registers ~1,000 files per second, and more copiers do not change that. Data: the `PLAN_PROGRESS` lines of the planner ([`images/data/`](images/data/)).*

### The rule

The rule is AIMD, as in TCP: add slowly, cut by a factor. The rule watches the **power** `P = throughput / latency` (Kleinrock's power). `P` grows while the copiers add throughput faster than they add latency. It is highest where CAS starts to queue.

```text
target = 16                        # copiers allowed at once, within the plan's limit

every minute, but not during the measure after a cut:
    if the fleet filled its target at some point of that minute:
        target = target + 4

while ranges are left to launch, measure 2 minutes, then again:
    N    = the copiers launched during the measure, on average
    P    = throughput / latency
    best = the measure with the most power in the last 10 minutes
    if the previous measure cut the fleet:          decide nothing   # it is still shrinking
    elif P is lower than best, at the same size:    target = 0.7 × N, forget the old measures
    elif P is lower than best, with more copiers:   target = 0.7 × N
    elif P is not higher, with 25 % more copiers:   target = 0.7 × N
```

The words of this rule:

- **The fleet `N`** counts the copiers launched: running, or waiting for a machine. When a fleet cannot fill its target (a slow listing, few ranges left), its target does not grow more.
- **The throughput** is the bytes that the copiers send per second.
- **The latency** is the geometric mean of two calls that CAS answers: the upload grant and the xorb commit. Each copier gives the 90th percentile of each call, and the planner takes the median over the copiers. The shard is not in the latency: copiers send one at the end of a session, often together, so its time depends more on this than on CAS. The PUT is not in it either: it goes to the storage, not to CAS.
- **Lower, higher** mean by more than 2σ. The 2 minutes are cut in 4 slices. The noise `σ` is the spread of the slices around their trend, pooled over the last 10 minutes.
- **The best** measure: of the measures with the most power, the one with the fewest copiers. The same power with fewer copiers costs less.
- **The first measure** starts 30 s after the first 16 copiers run. There is no measure when no range is left to launch: the fleet then only ends its last ranges, and there is nothing to decide.

What the fleet does in each case:

| What happens | What the fleet does |
|---|---|
| The latency stays flat and the throughput grows | It keeps growing. |
| CAS adds capacity | The power grows: it keeps growing. |
| The latency grows faster than the throughput | It shrinks. |
| Copiers add nothing (a slow source, the destination's file registrations) | It goes up and down between its best size and 25 % above it. |
| CAS gets slower at the same size (other users) | It shrinks, and starts again from new measures. |

## On a real copy

![Throughput and fleet size during a 32.9 TiB copy: the fleet grows by 4 copiers a minute from 16 to 64, the throughput reaches ~43 GiB/s and stays there, the fleet is cut to 41, then drains as the last ranges end.](images/fleet-timeline.png)

*A copy of `s3://noaa-goes18/ABI-L1b-RadF/2024/` (838,802 files, 32.9 TiB, verified). The fleet grows by 4 copiers a minute. The throughput stops growing near 43 GiB/s, while the fleet continues to grow, up to 64. The next measure sees no clear gain with 25 % more copiers than the best size. So it cuts the fleet to 41. Then no range is left to launch, and the last copiers end. At the end, one range ran again. The throughput is a 5-second sample every ~30 s, so it is noisy. Data: the `PLAN_PROGRESS` lines of the planner ([`images/data/goes18-2024.csv`](images/data/goes18-2024.csv)).*

## Why this rule

- **Steps, not doublings.** When copiers are added at a regular pace, CAS has time to scale. Doubling from 32 to 64 copiers added ~35 GiB/s in one minute, and CAS answered with many 500 errors. Doubling from 8 was also slow at the start. So the fleet now starts at 16.
- **A gentle cut.** × 0.7 is the β of CUBIC, gentler than the ½ of Reno. When the throughput stays flat, the cut comes at 25 % above the best size, so it lands near the best size (0.7 × 1.25 ≈ 0.9 of it). No copier is stopped: the fleet only gets smaller as its ranges end.
- **One cut per event.** A fleet gets smaller as its ranges end. So the measure after a cut sees a fleet that is still getting smaller, not its new size. This measure decides nothing, and the fleet does not grow during it. TCP also cuts only once per congestion event. (Without this rule, a real copy was cut twice in a row, 64 → 41 → 36.)
- **Few rules.** An earlier version added copiers with the steps of Rprop, tested a larger fleet every 10 minutes, and got smaller at each tie. Its rules added up, and in the tests the fleet went from 4 to 41 copiers and back.
- **A best that ages.** Each measure leaves the 10-minute window on its own, so the best measure ages slowly. Before, a single best measure expired at once. Then the next measure took its place, even a measure taken after the knee, and the fleet lost its reference.
- **Pooled noise.** 4 slices give a bad estimate of their noise. On a real copy, one agitated measure (copiers that ended and started together) gave σ = 14 %. This hid a real loss of 25 % of the power.
- **Errors stay with the copiers.** The planner does not watch the errors. When copiers brake, the throughput of the fleet drops, so the power drops, and the next measure gives copiers back. The reaction is in proportion: one 500 error on one copier out of 60 is noise; an overload that brakes many copiers is a clear loss.

## The copier brake

The fleet size sets how much the whole fleet sends. The brake in each copier reacts within seconds, between two measures of the fleet.

**An adaptive rate** (`--brake`, default `on`). A token bucket sets how many bytes per second the copier gives to xet. The copier listens to the events of xet itself (through a tracing layer, whatever `RUST_LOG` shows). It watches for **errors that say "slow down"** from CAS: 429, 5xx, timeouts, closed connections. One such error alone is common; three in 10 s count. Other errors (403, 404, …) do not brake.

The rate follows the slow start of TCP, then the gentle cut of CUBIC:

| What the copier sees | What it does |
|---|---|
| no error yet | starts at 64 MiB/s and doubles the rate every 2 s while it uses it: full speed in ~10 s, no ceiling |
| a congestion error | cuts the rate it really used to 70 %, then holds 10 s |
| after the hold | doubles back up to 90 % of that rate, then adds 1/32 of it every 2 s |

- At most one cut every 15 s. The holds vary by ±20 %, so the copiers do not all go up again at the same moment. (With a cut by half and a hold of 30 s, a fleet of 64 went from ~70 to ~5 GiB/s, with all the copiers at once, and it came back slowly.)
- The rate never goes below 16 MiB/s.
- When a copier does not use its rate (a slow source, or its own full speed), it does not raise the rate.
- No ceiling. An earlier version had a fleet budget: a safe level, split between the copiers. It limited each copier to a share of it, and the fleet stayed near 20 GiB/s, with any number of copiers.

**It measures the time of each upload call**: the upload grant, the xorb commit and the shard (CAS answers these), and the PUT of each xorb to the storage (measured to show where a slow upload comes from). Each `PROGRESS` line gives the 90th percentile of each call over the last seconds (`brake.latency`, in ms, or in ms per MiB for the xorbs and shards). When CAS starts to queue, the slow calls get slower first, while the median stays flat. One copier lives only a few minutes: this is too short to separate a trend from noise. So the planner reads these times across the whole fleet ([The fleet size](#the-fleet-size)).

The xet client also keeps at most 64 xorb uploads in flight per copier (its high-performance mode allows 124). `HF_XET_CLIENT_AC_MAX_UPLOAD_CONCURRENCY` can still set another value.

## Settings and logs

- `--max-inflight` (or *Advanced* in the Space) sets the limit of the plan: the fleet size stays within it. The plan proposes up to 64.
- The planner reads the logs of the copiers to measure the fleet. With `--no-live-progress`, it does not read them, and it starts up to `--max-inflight` copiers at once.
- `--brake shadow` computes the rate of the copier and logs it, but does not apply it. `--brake off` turns the brake off.
- In the log of the planner, `PLAN_PROGRESS` gives the latency of the fleet (`latency`) and the size allowed (`target`). Each measure is logged as a `growth: measure` line: the size before and after, the change of power, `σ`, and the reason.
- In the log of a copier, each `PROGRESS` line gives the state of the brake (`brake`: rate, last action, cuts, errors seen, latency). Each cut is logged.

## Testing

`bench/brake_bench.py` runs real copiers against a local fake of S3, the Hub API and CAS. It has a simple model of CAS under load (cores, a queue, 429 and 503, an autoscaler). Nothing leaves the machine. The rule of the fleet size has its own tests in `src/growth.rs`, on simulated fleets: a CAS that scales, a CAS that queues, a flat throughput, a CAS that adds capacity, copiers that brake.
