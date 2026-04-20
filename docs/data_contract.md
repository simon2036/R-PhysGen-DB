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
