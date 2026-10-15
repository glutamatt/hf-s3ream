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

Web front-end for [hf-s3ream](https://github.com/glutamatt/hf-s3ream): sign in
with Hugging Face, point it at an S3 prefix and a destination bucket, and it
launches the copy on **HF Jobs** for you — no CLI, no local compute.

**It's a fully static page — there is no backend.** The browser signs you in
with HF (OAuth, client-side) and calls the Hugging Face API directly (CORS is
enabled for Spaces) to create the bucket and run the Job(s). The actual
streaming copy runs in the `hf-s3ream` container on HF Jobs.

## Flow

1. **Sign in with HF** — OAuth scopes `jobs` (run the Job), `contribute-repos`
   (create the bucket), `write-repos` (write an existing one), `read-billing`
   (credit check). Jobs are billed to *your* account.
   The sign-in lasts **60 hours** (`hf_oauth_expiration_minutes: 3600`),
   counted from sign-in, not from Run. Every Job uses it until the copy ends,
   so it must outlive the planner timeout. The longest timeout the page
   proposes is 48 hours; with 1 hour of margin, that leaves 11 hours between
   sign-in and Run. The token stays in the Jobs' secrets after the copy, so it
   is kept no longer than that. If your sign-in would end before the copy can,
   the page does not launch it: sign in again, or set a shorter planner
   timeout. A page reload asks you to sign in again.
2. Enter the **S3 source**, **destination bucket**, and **AWS credentials** —
   or switch on **Public bucket** for a source that allows anonymous reads: no
   credentials are sent and the Jobs read S3 unsigned (`--no-sign-request`).
3. **Preflight** — create/check the bucket via the HF API (a missing bucket is
   created **private**; an existing one keeps its visibility, which the page
   shows), and launch a cheap
   `hf-s3ream --dry-run` Job that lists the source (validates S3 read + region +
   size) before any real transfer. (S3 can't be checked from the browser —
   bucket CORS — so the dry-run Job does it on HF's side.)
4. **Run** — launch one **planner** Job that lists the prefix once, cuts it
   into key ranges, and spawns one **copier** Job per range (monitored and
   respawned on failure). The page streams the whole fleet's progress into a
   live aggregate graph; the planner is autonomous, so the copy completes even
   if you close the tab. The planner then checks the destination against the
   plan; the final banner shows the verdict and the first missing paths.

## Security

Because the page is static, **your AWS credentials never touch any server we
run.** They stay in your browser and are sent only into the Job's **encrypted
`secrets`** via the Hugging Face API (same as `hf jobs run -s`). Prefer a
**scoped, read-only, short-lived** key for just the source prefix.

> HF Jobs runs off your AWS VPC. A source bucket locked to a VPC endpoint is
> unreachable here — the dry-run preflight will surface it. Run the container
> yourself from inside the VPC (`docker run`) for those.

## Deploy

Static Space, deployed from the [`space/`](https://github.com/glutamatt/hf-s3ream/tree/main/space)
subdir via GitHub Actions (`hf upload` on push to `main`).
