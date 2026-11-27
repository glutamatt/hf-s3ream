---
title: hf-s3ream
emoji: 🪣
colorFrom: yellow
colorTo: gray
sdk: static
pinned: false
hf_oauth: true
hf_oauth_expiration_minutes: 3600
hf_oauth_scopes:
  - jobs
  - contribute-repos
  - write-repos
  - read-billing
short_description: Stream an S3 prefix into a HuggingFace Bucket, on HF Jobs.
---

# hf-s3ream — S3 → HuggingFace Bucket, on HF Jobs

Web front-end for [hf-s3ream](https://github.com/glutamatt/hf-s3ream). Sign in
with Hugging Face, give an S3 prefix and a destination bucket, and the page
starts the copy on **HF Jobs** for you. You need no CLI, and nothing runs on
your machine.

From a terminal, the `hf s3ream` command starts the same copy, with the same
plan: `hf extensions install glutamatt/hf-s3ream`, then see the
[getting started](https://github.com/glutamatt/hf-s3ream#from-a-terminal) section.

**It is a static page: there is no backend.** The browser signs you in with
HF (OAuth, in the browser). It calls the Hugging Face API directly (CORS is
enabled for Spaces) to create the bucket and run the Job(s). The copy itself
runs in the `hf-s3ream` container, on HF Jobs.

## Flow

1. **Sign in with HF.** The OAuth scopes are `jobs` (run the Jobs),
   `contribute-repos` (create the bucket), `write-repos` (write an existing
   bucket) and `read-billing` (the cost of each Job, shown during and after
   the copy). Each started minute of the Jobs is billed to *your* account,
   which needs a positive credit balance.
   The sign-in lasts **60 hours** (`hf_oauth_expiration_minutes: 3600`),
   from the sign-in, not from Run. Each Job uses it until the end of the copy,
   so it must last longer than the planner timeout. The longest timeout that
   the page proposes is 48 hours. With 1 hour of margin, this leaves 11 hours
   between the sign-in and Run. The token stays in the secrets of the Jobs
   after the copy, so the 60 hours also limit how long it is valid there.
   If your sign-in
   would end before the copy can end, the page does not start the copy: sign
   in again, or set a shorter planner timeout. When you reload the page, you
   must sign in again.
2. Enter the **S3 source**, the **destination bucket**, and **AWS
   credentials**. For a source that allows anonymous reads, switch on
   **Public bucket** instead: then no credentials are sent, and the Jobs read
   S3 without a signature (`--no-sign-request`).
3. **Analyze.** The page creates or checks the bucket through the HF API. A
   missing bucket is created as a **private** bucket. An existing bucket keeps
   its visibility, and the page shows it. Then the page starts a cheap
   `hf-s3ream --dry-run` Job. This Job lists the source (it checks the S3
   read, the region and the size), and proposes a plan with its time and its
   cost, before any real transfer. (The browser cannot check S3, because of
   the CORS rules of the bucket, so the dry-run Job does it from the HF side.)
4. **Run copy.** The page starts one **planner** Job. The planner lists the
   prefix once, cuts it into key ranges, and starts one **copier** Job for
   each range. It watches the copiers, and starts a failed copier again. It
   starts a few copiers at first, then more each minute while the destination
   is fast enough, up to the limit of the plan. The page shows the progress
   of the whole fleet on one live graph. The planner works alone, so the copy
   ends even if you close the tab. Then the planner checks the destination
   against the plan. The final banner shows the result and the first missing
   paths, and one line gives the total of the fleet and its cost.

## Security

The page is static, so **your AWS credentials never reach a server that we
run.** They stay in your browser. The page sends them only into the
**encrypted `secrets`** of the Job, through the Hugging Face API (like
`hf jobs run -s`). Use a key with few rights if you can: read-only, valid for
a short time, and only for the source prefix.

> HF Jobs runs outside your AWS VPC. A source bucket that only accepts a VPC
> endpoint cannot be reached from here. The dry-run shows it before the copy.
> For these buckets, run `hf s3ream --local` (or `docker run`) from a machine
> inside the VPC.

## Deploy

Static Space. GitHub Actions deploys it from the
[`space/`](https://github.com/glutamatt/hf-s3ream/tree/main/space) folder
(`hf upload` on each push to `main`).
