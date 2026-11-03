"""`hf s3ream`: copy an S3 prefix into a Hugging Face Bucket.

By default the copy runs on HF Jobs, like the web Space: one planner Job lists
the source and starts copier Jobs. This package only launches the Jobs and
shows their progress. `--local` runs the same image in local Docker instead.
"""

# Released image the Jobs run. Bump with each vX.Y.Z tag, together with
# space/app.js (IMAGE) and src/main.rs (--copier-image default):
# cli/tests/test_pins.py checks that the three agree.
IMAGE = "ghcr.io/glutamatt/hf-s3ream:v0.3.12"
