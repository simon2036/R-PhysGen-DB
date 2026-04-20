# R-PhysGen-DB

R-PhysGen-DB is a file-lake-first refrigerant data foundation for AI-assisted fifth-generation refrigerant design.

Wave 2 extends the V1 base with:

- single-component neutral small molecules
- open-source-first acquisition
- layered storage in `data/raw`, `data/bronze`, `data/silver`, and `data/gold`
- Parquet as the primary persistence format
- DuckDB as the local query/index layer
- a full inventory seed catalog plus a model-ready promoted subset
- explicit `regulatory_status` and `pending_sources` tables
- NIST phase-table parsing plus EPA ODS/SNAP adapters
- explicit CoolProp alias control and transcritical `R-744` cycle handling
- `Tier A/B/C` promoted coverage waves plus `Tier D` inventory-only molecules
- explicit `entity_scope` and `model_inclusion` control in `data/raw/manual/seed_catalog.csv`

The existing files in [`methods`](methods) remain reference material only. The random data generator in `methods/refrigerant_data_project/refrigerant_data_pipeline.py` is not treated as source-of-truth.

Current local baseline after the inventory expansion:

- `seed_catalog_count`: `132`
- `resolved_molecule_count`: `128`
- `model_dataset_index_count`: `120`
- `refrigerant_count`: `70`
- `candidate_count`: `62`

## Quick Start

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe pipelines\generate_wave2_seed_catalog.py
.venv\Scripts\python.exe pipelines\build_v1_dataset.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
.venv\Scripts\pytest.exe -q
```

## Key Outputs

- `data/raw/manual/seed_catalog.csv`
- `data/raw/manual/refrigerant_inventory.csv`
- `data/raw/manual/manual_property_observations.csv`
- `data/raw/manual/observations/*.csv`
- `data/bronze/source_manifest.parquet`
- `data/bronze/pending_sources.parquet`
- `data/silver/molecule_core.parquet`
- `data/silver/molecule_alias.parquet`
- `data/silver/property_observation.parquet`
- `data/silver/regulatory_status.parquet`
- `data/gold/property_recommended.parquet`
- `data/gold/molecule_master.parquet`
- `data/gold/property_matrix.parquet`
- `data/gold/model_dataset_index.parquet`
- `data/gold/model_ready.parquet`
- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/index/r_physgen_v2.duckdb`

## Documentation

- [docs/current_status.md](docs/current_status.md)
  Current project state: completed work, incomplete work, latest coverage, known issues, active workstreams, and next priorities.
- [docs/wave2_implementation.md](docs/wave2_implementation.md)
  Wave 2 implementation notes: coverage tiers, new tables, source adapters, validation targets, and current boundaries.
- [docs/project_scope.md](docs/project_scope.md)
  Project scope and inclusion boundaries: what is in-scope, what is excluded, and the overall V1/Wave 2 target definition.
- [docs/data_contract.md](docs/data_contract.md)
  Data contract reference: core tables, field expectations, layered storage rules, and schema-level conventions.
- [docs/controlled_vocabularies.md](docs/controlled_vocabularies.md)
  Controlled vocabularies and enumerations used across properties, source types, quality levels, and related fields.
- [docs/phase2_interfaces.md](docs/phase2_interfaces.md)
  Reserved Phase 2 interfaces for quantum calculations, cycle simulation, and active-learning feedback extensions.
