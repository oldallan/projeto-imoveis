# Repository Guidelines

## Project Structure & Module Organization

This repository implements an incremental Python pipeline for real-estate listings from OLX, Lopes, and QuintoAndar.

- `scrapers/`: source-specific discovery and listing collectors, shared HTTP/parsing utilities, and the scraper registry.
- `pipelines/`: normalization, deduplication, ZIP-code enrichment, daily snapshots, and historical storage.
- `stages/`: executable workflow-stage adapters.
- `workflow/`: orchestration, manifests, paths, models, and logging.
- `tests/`: pytest unit and lightweight integration tests.
- `imoveis_pipeline.py`: primary CLI; `main_pipeline.py` is the default full-run shortcut.

Generated data belongs under `raw/`, `processed/`, `artifacts/`, or `logs/`. These directories are intentionally ignored by Git.

The persistent pipeline data directory is `H:\dados-imoveis` (use it as the CLI `--output-path`).

## Build, Test, and Development Commands

Use the `projeto_imoveis` Conda environment when available:

```bash
conda activate projeto_imoveis
python -m pip install -r requirements.txt
python imoveis_pipeline.py list-stages
python imoveis_pipeline.py run-all --output-path . --date DD-MM-YYYY
python imoveis_pipeline.py run-stage collect_discovery --output-path . --date DD-MM-YYYY --sources olx
python -m pytest
```

Add `--verbose` when diagnosing scraper behavior. Use `--force-discovery` only when an existing successful discovery manifest must be replaced.

## Coding Style & Naming Conventions

Follow standard Python conventions: four-space indentation, `snake_case` for functions and modules, `PascalCase` for classes, and uppercase names for constants. Keep source-specific behavior in its matching module (for example, OLX discovery logic in `scrapers/olx_discovery.py`). Preserve type hints and small, focused helper functions. No formatter or linter is currently enforced; match surrounding code and keep imports organized.

## Testing Guidelines

Tests use pytest, with files named `test_*.py` and methods named `test_<behavior>`. Add regression tests for parser changes using minimal representative HTML rather than live network calls. Run a focused file while iterating, for example:

```bash
python -m pytest tests/test_olx_discovery.py
```

Run the complete suite before submitting changes. No numeric coverage threshold is configured, but new branches and failure modes should be tested.

## Commit & Pull Request Guidelines

History follows short Conventional Commit-style subjects such as `feat:`, `fix:`, `test:`, `docs:`, `refactor:`, and `chore:`. Keep commits scoped and imperative.

Pull requests should explain the problem, implementation, affected sources or stages, and verification commands. Link related issues and include relevant logs or sample output for pipeline changes. Never commit generated datasets, credentials, `.env` files, or runtime logs.
