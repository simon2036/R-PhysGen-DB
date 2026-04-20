# Data Contract

## Layering

- `raw`
  Original downloaded or manually curated artifacts, including the inventory catalog and manual observation batches.
- `bronze`
  Source inventory and fetch status.
- `silver`
  Normalized entities and long-form observations.
- `gold`
  Recommended labels, inventory reports, and model-facing tables.

## Inventory Catalog

`data/raw/manual/seed_catalog.csv` is the authoritative inventory catalog.

Key fields:

- `seed_id`
- `r_number`
- `family`
- `coverage_tier`
- `selection_role`
- `entity_scope`
- `model_inclusion`
- `source_bundle`
- `coolprop_support_expected`
- `regulatory_priority`

Interpretation:

- `Tier A/B/C` rows with `model_inclusion=yes` are the promoted subset
- `Tier D` rows with `model_inclusion=no` remain inventory-only until promoted
- `source_bundle=pubchem_bulk` is reserved for generated `Tier D` candidate rows sourced from the PubChem FTP bulk intake

## Bulk Candidate Artifacts

Bulk PubChem acquisition writes three contract-level artifacts:

- `data/bronze/pubchem_candidate_pool.parquet`
  Filtered, RDKit-standardized candidate pool with structural annotations, de-duplication flags, and `volatility_status`.
- `data/bronze/pubchem_candidate_filter_audit.parquet`
  Per-CID audit log for the materialized bulk-screening universe, showing whether a record passed hard filters and which exclusion reasons were triggered.
- `data/raw/generated/pubchem_tierd_candidates.csv`
  Generated `seed_catalog`-compatible supplement used by `pipelines/generate_wave2_seed_catalog.py` to append a capped batch of `Tier D` rows.

Rules:

- The bulk candidate pool is a `Tier D`-only entry path.
- `build_v1_dataset.py` resolves `source_bundle=pubchem_bulk` rows from `pubchem_candidate_pool.parquet` and must not fall back to live per-record PubChem API requests for those rows.
- Existing curated `refrigerant` rows remain authoritative for completeness validation.
- Large local-only raw FTP bundles and regenerated bulk artifacts that should not be pushed to normal Git history are documented in `docs/local_large_artifacts.md`.

Audit semantics:

- The current `pubchem_candidate_filter_audit.parquet` is not a full `123,857,780`-row PubChem negative ledger.
- It covers the coarse-survivor universe produced by the cheap DuckDB prefilter on `CID-Mass.gz`, then records why a surviving CID was excluded by later stages such as `multi_component`, `non_neutral`, `disallowed_elements`, `screening_error:*`, or `missing_smiles`.
- This keeps the audit table operationally small enough to inspect and regenerate while still explaining why a near-candidate failed to become part of `pubchem_candidate_pool.parquet`.
- If the project later needs a full coarse-filter failure ledger for all PubChem mass rows, that should be treated as a separate artifact with a different storage and retention expectation.

## Core Tables

### `source_manifest`

Tracks source artifacts and generated source sessions.

Key fields:

- `source_id`
- `source_type`
- `source_name`
- `license`
- `retrieved_at`
- `checksum_sha256`
- `local_path`
- `parser_version`
- `upstream_url`
- `status`

### `molecule_core`

One row per standardized molecular entity.

Key fields:

- `mol_id`
- `seed_id`
- `canonical_smiles`
- `isomeric_smiles`
- `inchi`
- `inchikey`
- `inchikey_first_block`
- `formula`
- `molecular_weight`
- `charge`
- `heavy_atom_count`
- `stereo_flag`
- `ez_isomer`
- `family`
- `entity_scope`
- `model_inclusion`

For `source_bundle=pubchem_bulk`, the molecule identity is resolved from the local bulk candidate pool using the generated `seed_id=tierd_pubchem_<cid>` mapping.

### `molecule_alias`

Stores human and external identifiers as a crosswalk.

Key fields:

- `mol_id`
- `alias_type`
- `alias_value`
- `is_primary`
- `source_name`

### `property_observation`

Long table of all observed, curated, or calculated values.

Required fields:

- `observation_id`
- `mol_id`
- `property_name`
- `value`
- `value_num`
- `unit`
- `temperature`
- `pressure`
- `phase`
- `source_type`
- `source_name`
- `source_id`
- `method`
- `uncertainty`
- `quality_level`

### `property_recommended`

One selected recommended value per `mol_id` and `property_name`, plus conflict metadata.

### `structure_features`

RDKit-derived descriptors, counts, fingerprints, scaffold, and SELFIES.

### `model_dataset_index`

Split assignment, label masks, confidence, and scaffold grouping for the promoted subset only.

### `model_ready`

The final model-facing join. This table is intentionally filtered to `model_inclusion=yes`.

Bulk-only `Tier D` candidates are expected to resolve into inventory tables such as `molecule_core` and `molecule_alias`, but they are excluded from `model_dataset_index` and `model_ready` until promoted.

## Manual Observation Inputs

The pipeline merges and deduplicates:

- `data/raw/manual/manual_property_observations.csv`
- `data/raw/manual/observations/*.csv`

Each physical file is registered in `source_manifest` and mapped into `property_observation.source_id`.

## Recommended Source Priority

1. `manual_curated_reference`
2. `public_database`
3. `calculated_open_source`
4. `public_web_snapshot`
5. `placeholder`

## Canonical Units

- temperature: `degC`
- pressure: `MPa`
- density: `kg/m3`
- enthalpy: `kJ/mol`
- GWP/ODP: `dimensionless`
- COP: `dimensionless`
- volumetric cooling: `MJ/m3`
