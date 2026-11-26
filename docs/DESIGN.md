# Design

This page explains the choices behind hf-s3ream, and why they were made: many copiers with one listing, the plan, and the check at the end. How many copiers run at once, and how fast each one sends, has its own page: [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md). How the pieces fit together is in [ARCHITECTURE.md](ARCHITECTURE.md).

- [Goals](#goals)
- [Many copiers, one listing](#many-copiers-one-listing)
- [The plan](#the-plan)
- [Verification](#verification)

## Goals

1. **Throughput at a known cost.** A copy should be as fast as the destination allows, and a copier that adds no throughput should not run: it only costs.
2. **No harm to the shared storage.** CAS, the storage behind HF Buckets, is shared by every Hub user. A copy must not push it past what it can take (see [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md)).
3. **A copy you can trust.** Every copy ends with a check of the destination against the plan.

## Many copiers, one listing

One machine is limited by its network card, however well it is tuned. hf-s3ream does not use one machine: each copier is a Job with its own path from S3 to the bucket, so more copiers copy more at the same time, until another limit takes over.

- **One listing.** The planner lists the prefix once and cuts it into contiguous key ranges, balanced by bytes. The cut does not depend on the layout of the keys. (The first version split the files by a hash of their name, in a Slurm array.)
- **Ranges of different sizes.** Each range takes ½ to 1½ of the range size, so copiers started together do not all end together.
- **Idempotent copiers.** A copier that fails is started again on the same range. Xet only uploads the chunks the storage does not have yet, so a second attempt costs little.
- **In memory.** A copier streams S3 into the Xet upload, with no local disk. Reads and uploads run at the same time (see [Inside a copier](ARCHITECTURE.md#inside-a-copier)).

## The plan

The dry-run proposes the plan from the size and the file count it listed. `hf s3ream` and the Space get the same plan: it comes from the binary (`src/reco.rs`).

- **Most copies are bound by bandwidth.** Each copier is an independent S3 → CAS path, so the throughput grows with the number of copiers. The plan uses `cpu-performance` copiers: on the smaller flavors, the speed changed a lot from one Job to the next. The ranges are ~64 GiB up to 1 TiB, so up to 16 copiers; 16 copiers between 1 and 3 TiB; ~192 GiB past 3 TiB (a few minutes per copier: with shorter ranges, copiers ended every minute, and each new one waited for a machine and paid a started minute). The plan aims at 256 ranges at most, so past ~48 TiB the ranges grow. At most 64 copiers run at once, and the planner sets how many within that limit (see [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md)).
- **Many small files are bound by the commits.** The destination registers about 1,000 files per second per repo. When the files average 4 MiB or less and the registrations take longer than the bytes, more copiers would not finish sooner: the plan uses the smallest fleet that keeps up (2 or more copiers), on a cheaper flavor when that is enough.
- **A copy under 5 GiB** takes one small copier: a copier's startup takes longer than the transfer.

A range ends at its size or at 2 million keys, whichever comes first. You can change the plan by hand (with `--copier-flavor`, `--range-gib` and `--max-inflight`, or under *Advanced* in the Space): there is no cost estimate then.

## Verification

After the copy, the planner (or a single-process run, after `DONE`) reads the destination back:

1. It waits until the bucket's `updatedAt` stops moving (2 minutes at most).
2. It lists the destination prefix once and compares file count and bytes with the plan, range by range.
3. For a range that does not add up, it re-lists the source range and looks up each path (`paths-info`): absent is `missing`, another size is `short`. A missing key that is also gone from the source (`HEAD` 404) is `raced`.
4. It prints one `VERIFY {…}` line, and the command that re-copies each failed range. `PLAN_RESULT` gets `verified: true|false|null`.

`missing` or `short` makes the run exit non-zero. `extra`, `raced` and `outside` (files outside the plan) are reported only. A range where a missing file and an unplanned file of the same size cancel out is not detected. The check compares names and sizes, not the content of the files. `--no-verify` skips the check; `--dry-run` never runs it.

Why a check at all: the bucket commit API can answer `200` while some files of the batch failed; the failures are listed in the response body. A test copy of about half a million files once missed three files that way, with no error in any log. The copiers now read that list and send those files again, and the final check catches what any other gap would leave.
