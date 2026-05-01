# Repository Guidelines

## Project Structure & Module Organization
R-PhysGen-DB is a Python file-lake project for refrigerant data generation and validation. Core package code lives in `src/r_physgen_db/`, with data-source adapters under `src/r_physgen_db/sources/` and staged pipeline components under `src/r_physgen_db/pipeline_stages/`. Dataset build scripts are in `pipelines/`; one-off utilities and migration helpers are in `scripts/`. Table contracts and controlled vocabularies are YAML files in `schemas/`. Tests are in `tests/`, while project status, contracts, and migration notes are in `docs/`. Layered dataset artifacts are stored under `data/raw`, `data/bronze`, `data/silver`, `data/gold`, `data/extensions`, and `data/index`; treat large ZIP/workbook files in `methods/` as reference inputs unless a task states otherwise.

## Build, Test, and Development Commands
Use Python 3.11, matching CI. Typical setup and verification:

```bash
python -m pip install -r requirements.txt
python pipelines/generate_wave2_seed_catalog.py
python pipelines/build_v1_dataset.py
python pipelines/validate_v1_dataset.py
python -m pytest -q
```

`generate_wave2_seed_catalog.py` refreshes manual inventory inputs, `build_v1_dataset.py` rebuilds Parquet/DuckDB outputs, and `validate_v1_dataset.py` checks dataset quality reports. For focused work, run a single test file, for example `python -m pytest -q tests/test_schema_contract.py`.

## Coding Style & Naming Conventions
Follow existing Python style: 4-space indentation, `from __future__ import annotations`, type hints where practical, `Path` for filesystem work, and small pure helpers around pipeline logic. Use `snake_case` for functions, modules, columns, and schema fields; keep table names aligned with their YAML schema filenames. Prefer explicit source traceability fields over inferred or hidden defaults.

## Testing Guidelines
Pytest is the test framework. Add tests next to related coverage in `tests/test_*.py`; name functions `test_<behavior>()`. Contract changes should update or add schema/data tests, and pipeline changes should include rebuild/validation evidence. Keep tests deterministic and avoid network dependence unless explicitly mocked or fixture-backed.

## Commit & Pull Request Guidelines
History uses concise imperative subjects such as `Add dataset migration governance` and `Fix frontend molecule detail routing`. Keep subjects action-oriented; include a short body when constraints or rejected alternatives matter. PRs should explain the data/schema impact, list verification commands run, link related docs/issues, and include screenshots for frontend HTML changes.

## Security & Configuration Tips
Do not commit secrets, credentials, or ad hoc local state. Keep generated caches and local orchestration files out of source control. When changing raw inputs, preserve provenance and document the source in `docs/` or the relevant schema/manifest.
