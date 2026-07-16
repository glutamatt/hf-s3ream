---
title: hf-s3ream
emoji: 🪣
colorFrom: yellow
colorTo: gray
sdk: static
pinned: false
hf_oauth: true
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
2. Enter the **S3 source**, **destination bucket**, and **AWS credentials**.
3. **Preflight** — create/check the bucket via the HF API, and launch a cheap
   `hf-s3ream --dry-run` Job that lists the source (validates S3 read + region +
   size) before any real transfer. (S3 can't be checked from the browser —
   bucket CORS — so the dry-run Job does it on HF's side.)
4. **Run** — launch one **planner** Job that lists the prefix once, cuts it
   into key ranges, and spawns one **copier** Job per range (monitored and
   respawned on failure). The page streams the whole fleet's progress into a
   live aggregate graph; the planner is autonomous, so the copy completes even
   if you close the tab.

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
