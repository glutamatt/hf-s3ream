# Contributing

This repo uses [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, and `feat!:` for a breaking change.

Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): it maps the code. The reasons for the plan are in [docs/DESIGN.md](docs/DESIGN.md). The fleet size and the brakes are in [docs/ADAPTIVE-CONCURRENCY.md](docs/ADAPTIVE-CONCURRENCY.md).

## Where things deploy from

The front-end of the Space is in [`space/`](space/). GitHub Actions deploys it on each push to `main`. The `hf s3ream` extension is in [`cli/`](cli/). `hf extensions install` also takes it from `main`. How to run the CLI tests: [`cli/README.md`](cli/README.md#development).

## Releases

Releases are manual. The release commit changes `version` in `Cargo.toml` and `pyproject.toml`. It also changes the image tag in `src/main.rs` (`--copier-image`), `space/app.js` and `cli/hf_s3ream_cli/__init__.py`. `cli/tests/test_pins.py` checks that the three tags are the same. Push the tag first: `git tag vX.Y.Z && git push origin vX.Y.Z`. The tag starts `release.yml`. It builds and pushes the container image (`ghcr.io/glutamatt/hf-s3ream:vX.Y.Z` + `:latest`), and publishes a GitHub release. Update `main` only when the image is there: the Space and the extension run the image that they pin.

> Why are releases not automated? The crate uses xet-core APIs that are not published, through git dependencies. Release tools (release-plz) run `cargo package` *with verification* to compute the next versions. This verify build takes the dependencies from crates.io (packaging removes the git specs), so it fails on the unpublished APIs. Look at this again if xet-core publishes these APIs, or if release-plz gets a `--no-verify` option to find the version.

## Docs and charts

The charts in [`docs/images/`](docs/images/) come from `docs/images/plot_fleet.py`. It reads the CSV files in `docs/images/data/`:

```bash
uv run --with matplotlib docs/images/plot_fleet.py
```

The diagrams are [Mermaid](https://mermaid.js.org/) blocks. GitHub draws them.
