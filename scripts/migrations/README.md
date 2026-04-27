# Dataset Migration Scripts

This directory is reserved for one-time dataset migration scripts that cannot be replaced by rebuilding Parquet and DuckDB outputs from authoritative inputs.

Default policy:

- Prefer `pipelines/build_v1_dataset.py` plus `pipelines/validate_v1_dataset.py`.
- Set migration records to `migration_script: none` when rebuild is enough.
- Add a script here only when old data must be transformed outside the normal pipeline.
- If a migration record names a script path, `validate_dataset_migrations()` checks that the file exists.

Script requirements:

- Read explicit input paths and write explicit output paths.
- Do not mutate raw source artifacts in place.
- Preserve source traceability and audit notes.
- Include a matching migration record in `docs/dataset_migrations/`.
