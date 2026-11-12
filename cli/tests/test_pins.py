"""The CLI, the Space and the planner must run the same image."""

import re
from pathlib import Path

from hf_s3ream_cli import IMAGE

ROOT = Path(__file__).resolve().parents[2]


def test_the_cli_the_space_and_the_planner_pin_the_same_image():
    space = re.search(r'const IMAGE = "([^"]+)"', (ROOT / "space/app.js").read_text())
    # The planner's --copier-image default, the only image named in main.rs.
    planner = re.search(r'default_value = "(ghcr\.io/glutamatt/hf-s3ream:[^"]+)"', (ROOT / "src/main.rs").read_text())
    assert space and planner
    assert IMAGE == space.group(1) == planner.group(1)
