"""Cancel a planner Job from a process of its own: `python -m hf_s3ream_cli.cancel NS ID`.

On Ctrl-C, `hf` (huggingface_hub 2.4) gives an extension 0.25 s, then kills
it. That is too short to be sure an HTTP call ends. So the CLI starts this
helper in a new session first: it survives the kill and cancels the planner.
The planner then cancels its copiers, and a copier whose planner has ended
cancels itself.
"""

import sys

from huggingface_hub import HfApi


def main() -> None:
    namespace, job_id = sys.argv[1], sys.argv[2]
    HfApi().cancel_job(job_id=job_id, namespace=namespace)


if __name__ == "__main__":
    main()
