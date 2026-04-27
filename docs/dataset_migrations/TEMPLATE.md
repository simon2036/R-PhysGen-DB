---
migration_id: YYYY-MM-DD-vMAJOR.MINOR.PATCH-short-description
target_version: vMAJOR.MINOR.PATCH
compatibility: additive
rebuild_required: true
affected_layers:
  - schemas
migration_script: none
review_status: draft
---

# Dataset Migration: short description

## Summary

Describe the dataset contract, pipeline, validation, or data-only change.

## Compatibility

State whether consumers must change code or queries. If this is `breaking`, list the affected tables and columns.

## Required Actions

- Update schema/config/docs as needed.
- Rebuild generated Parquet and DuckDB outputs if `rebuild_required` is `true`.
- Run migration script only when `migration_script` is not `none`.

## Verification

```powershell
.venv\Scripts\pytest.exe -q tests\test_dataset_migrations.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
```
