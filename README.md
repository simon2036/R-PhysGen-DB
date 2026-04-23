# R-PhysGen-DB

R-PhysGen-DB is a file-lake-first refrigerant data foundation for AI-assisted fifth-generation refrigerant design.

Wave 2 extends the V1 base with:

- single-component neutral small molecules
- open-source-first acquisition
- layered storage in `data/raw`, `data/bronze`, `data/silver`, `data/gold`, and `data/extensions`
- Parquet as the primary persistence format
- DuckDB as the local query/index layer
- a full inventory seed catalog plus a model-ready promoted subset
- explicit `regulatory_status` and `pending_sources` tables
- NIST phase-table parsing plus EPA ODS/SNAP adapters
- explicit CoolProp alias control and transcritical `R-744` cycle handling
- `Tier A/B/C` promoted coverage waves plus `Tier D` inventory-only molecules
- explicit `entity_scope` and `model_inclusion` control in `data/raw/manual/seed_catalog.csv`
- `2026-04-22` property-governance bundle alignment with canonical observation/recommended tables and a strict ML-filtered canonical slice
- explicit manual adjudication of reviewed canonical queue rows without widening the legacy projection layer
- controlled proxy-only strict acceptance for governed canonical features when no non-proxy candidate exists

Most files in [`methods`](methods) remain reference material only. The exception is [`methods/refrigerant_seed_database_20260422_property_governance_bundle.zip`](methods/refrigerant_seed_database_20260422_property_governance_bundle.zip), which is now ingested into the extension and canonical-property layers. The random data generator in `methods/refrigerant_data_project/refrigerant_data_pipeline.py` is not treated as source-of-truth.

Current local baseline after the `2026-04-22` governance alignment:

- curated base `seed_catalog.csv`: `5700` rows (`70` refrigerants + `5630` candidates)
- effective build inventory (`seed_catalog.csv` plus generated governance seeds): `5707` rows (`77` refrigerants + `5630` candidates)
- `resolved_molecule_count`: `5598`
- `model_dataset_index_count`: `120`
- `property_observation_canonical_count`: `1687`
- `property_recommended_canonical_count`: `1389`
- `property_recommended_canonical_strict_count`: `1304`
- `property_recommended_canonical_review_queue_count`: `0`
- open canonical conflict/source-divergence review rows: `0`
- proxy-only rows promoted into strict via policy: `396`
- canonical review decisions applied: `120` = `35` `accept_selected_source` + `85` `accept_out_of_strict`
- property-governance extension mirror: `218/218` tables aligned

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
- `data/raw/manual/property_governance_20260422_unresolved_curations.csv`
- `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv`
- `data/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv`
- `data/raw/manual/observations/*.csv`
- `data/raw/generated/property_governance_20260422_seed_catalog.csv`
- `data/bronze/source_manifest.parquet`
- `data/bronze/pending_sources.parquet`
- `data/bronze/property_governance_20260422_substance_crosswalk.parquet`
- `data/bronze/property_governance_20260422_unresolved_substances.parquet`
- `data/bronze/property_governance_20260422_audit.json`
- `data/silver/molecule_core.parquet`
- `data/silver/molecule_alias.parquet`
- `data/silver/property_observation.parquet`
- `data/silver/property_observation_canonical.parquet`
- `data/silver/regulatory_status.parquet`
- `data/gold/property_recommended.parquet`
- `data/gold/property_dictionary.parquet`
- `data/gold/property_canonical_map.parquet`
- `data/gold/property_recommended_canonical.parquet`
- `data/gold/property_recommended_canonical_strict.parquet`
- `data/gold/property_recommended_canonical_review_queue.parquet`
- `data/gold/molecule_master.parquet`
- `data/gold/property_matrix.parquet`
- `data/gold/model_dataset_index.parquet`
- `data/gold/model_ready.parquet`
- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/extensions/property_governance_20260422/`
- `data/index/r_physgen_v2.duckdb`

## Documentation

- [docs/current_status.md](docs/current_status.md)
  Current project state: completed work, latest coverage, active risks, and next priorities.
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
