# Design

This page explains the choices of hf-s3ream, and the reasons for them: many copiers with one listing, the plan, and the check at the end. How many copiers run at the same time, and how fast each one sends, is on its own page: [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md). How the parts work together is in [ARCHITECTURE.md](ARCHITECTURE.md).

- [Goals](#goals)
- [Many copiers, one listing](#many-copiers-one-listing)
- [The plan](#the-plan)
- [Verification](#verification)

## Goals

1. **Throughput at a known cost.** A copy should be as fast as the destination allows. A copier that adds no throughput should not run: it only costs money.
2. **No harm to the shared storage.** CAS, the storage behind HF Buckets, is shared by all the users of the Hub. A copy must not give it more work than it can take (see [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md)).
3. **A copy you can trust.** Each copy ends with a check of the destination against the plan.

## Many copiers, one listing

One machine is limited by its network card, even with good settings. hf-s3ream does not use one machine. Each copier is a Job, with its own path from S3 to the bucket. So more copiers copy more data at the same time, until another limit stops the growth.

- **One listing.** The planner lists the prefix once. It cuts the list into contiguous key ranges, with about the same number of bytes in each. The cut does not depend on the form of the keys. (The first version split the files by a hash of their name, in a Slurm array.)
- **Ranges of different sizes.** Each range gets ½ to 1½ of the range size. So copiers that start together do not all end together.
- **Idempotent copiers.** When a copier fails, it starts again on the same range. Xet only uploads the chunks that the storage does not have yet, so a second attempt costs little.
- **In memory.** A copier streams S3 into the Xet upload, with no local disk. Reads and uploads run at the same time (see [Inside a copier](ARCHITECTURE.md#inside-a-copier)).

## The plan

The dry-run proposes the plan from the size and the number of files that it listed (`src/reco.rs`). There are three cases.

- **Most copies are limited by the bandwidth.** Each copier is an independent path from S3 to CAS, so the throughput grows with the number of copiers.
  - The copiers are `cpu-performance`: on the smaller flavors, the speed changed a lot from one Job to the next.
  - The ranges are ~64 GiB up to 1 TiB (so up to 16 copiers), 16 copiers between 1 and 3 TiB, and ~192 GiB above 3 TiB. This gives each copier a few minutes of work. With shorter ranges, copiers ended every minute. Each new copier waited for a machine, and paid a whole minute that it had only started.
  - The plan aims at 256 ranges at most, so above ~48 TiB the ranges get bigger.
  - At most 64 copiers run at the same time. Within that limit, the planner sets the number (see [Adaptive concurrency](ADAPTIVE-CONCURRENCY.md)).
- **Many small files are limited by the commits.** The destination registers about 1,000 files per second for each repo. When the files are 4 MiB or less on average, and the registrations take longer than the bytes, more copiers would not finish sooner. So the plan uses the smallest fleet that is fast enough (2 copiers or more), on a cheaper flavor when that is enough.
- **A copy under 5 GiB** gets one small copier: the start of a copier takes longer than the transfer.

A range ends at its size or at 2 million keys, at the first of the two. What the plan shows, and how to change it by hand: [The plan](GUIDE.md#the-plan) in the guide.

## Verification

After the copy, the planner (or a single-process run, after `DONE`) reads the destination:

1. It waits until the `updatedAt` of the bucket stops changing (2 minutes at most).
2. It lists the destination prefix once. For each range, it compares the number of files and the bytes with the plan.
3. For a range where the numbers are different, it lists the source range again, and looks up each path (`paths-info`). An absent file is `missing`, a file with another size is `short`. A missing key that is also gone from the source (`HEAD` 404) is `raced`. A missing key that arrived in the source after the listing is `added` (see [A source that grows during the copy](#a-source-that-grows-during-the-copy)).
4. It prints one `VERIFY {…}` line, and the command that copies each failed range again. `PLAN_RESULT` gets `verified: true|false|null`.

`missing` or `short` makes the run exit with a non-zero code. `extra`, `raced`, `added` and `outside` (files outside the plan) are only reported. In one range, a missing file and an unplanned file of the same size cancel each other, so the check does not see them. The check compares names and sizes, not the content of the files. `--no-verify` skips the check. `--dry-run` never runs it.

Why a check at all? The commit API of the bucket can answer `200` while some files of the batch failed. The failures are listed in the body of the answer. A test copy of about half a million files once missed three files in this way, with no error in any log. Now the copiers read this list and send these files again. The final check finds any other gap.

### A source that grows during the copy

The check compares the destination with the listing. Files that arrive in the source after the listing are `added`. They are not copied, and they do not make the run fail.

- The check logs each range that has some, with its first key and the command that copies the range again.
- To copy them, run the same copy again with `--skip-existing`. The CLI prints this command as `Resume:`, and the Space calls it **Skip existing**. It copies only what is not in the destination yet.
- The check counts `added` files only in the ranges that it lists again. When a range has what the listing promised, the check does not read the source again.

How the copy decides that a key is `added`: S3 lists up to 1,000 keys per page, and reads each page at its own moment. For each page, the copy keeps its first key, its last key and its newest `LastModified`.

- **Inside a page**, between its first and last key, the page decides for a missing key. A key that the page listed is never newer than the page. So a key newer than the page was not there when the page was read.
- **Outside all pages**, before the first, after the last or between two pages, the copy listed no key. A missing key there was not in the source when its page was read, whatever its date.
- **In the newest second of a page.** S3 gives the date of a key to the second. For the newest second of each page, the copy also keeps the keys that the page listed (up to 16). A missing key of that second that the page did not list was not there when the page was read.

Both dates come from S3, so a difference between the clock of S3 and the clock of the machine that runs the copy has no effect. Sometimes these rules cannot prove that a key came after its page. Then the key stays `missing` and the run fails. This happens when more than 16 keys of the page are in its newest second, or with a multipart upload (S3 gives it the date of its start) that started before the newest key of the page.
