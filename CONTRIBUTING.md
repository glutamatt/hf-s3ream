# Contributing

This repo uses [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `feat!:` for breaking.

Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): it maps the code. The reasons behind the plan, the fleet size and the brakes are in [docs/DESIGN.md](docs/DESIGN.md).

## Where things deploy from

The Space front-end lives in [`space/`](space/) and is auto-deployed by GitHub Actions on push to `main`. The `hf s3ream` extension lives in [`cli/`](cli/); `hf extensions install` takes it from `main` too. How to run the CLI tests: [`cli/README.md`](cli/README.md#development).

## Releases

Releases are manual. The release commit bumps `version` in `Cargo.toml` and `pyproject.toml`, and the image tag in `src/main.rs` (`--copier-image`), `space/app.js` and `cli/hf_s3ream_cli/__init__.py` (`cli/tests/test_pins.py` checks that the three agree). Push the tag first — `git tag vX.Y.Z && git push origin vX.Y.Z`. The tag triggers `release.yml`, which builds and pushes the container image (`ghcr.io/glutamatt/hf-s3ream:vX.Y.Z` + `:latest`) and publishes a GitHub release. Update `main` only once the image is there: the Space and the extension run the image they pin.

> Why no release automation? The crate depends on unpublished xet-core APIs via git deps, and release-bot tooling (release-plz) runs `cargo package` *with verification* to compute next versions — the verify build resolves deps from crates.io (git specs are stripped when packaging) and fails on the unpublished APIs. Revisit if xet-core publishes them or release-plz grows a `--no-verify` option for version determination.

## Docs and charts

The charts in [`docs/images/`](docs/images/) come from `docs/images/plot_fleet.py`, which reads the CSV files in `docs/images/data/`:

```bash
uv run --with matplotlib docs/images/plot_fleet.py
```

The diagrams are [Mermaid](https://mermaid.js.org/) blocks, rendered by GitHub.
