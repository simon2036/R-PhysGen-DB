# Current Status

## Snapshot

Status date: `2026-04-20`

R-PhysGen-DB now has a working Wave 2 codebase with a green build/validate/test baseline after integrating the first real PubChem bulk FTP intake into the inventory pipeline. The repository currently distinguishes between:

- full inventory: all in-scope single-component refrigerants and candidate molecules tracked in `seed_catalog.csv`
- promoted model subset: the `Tier A/B/C` molecules with `model_inclusion=yes`

Latest local rebuild summary:

- `seed_catalog_count`: `5132`
- `refrigerant_count`: `70`
- `candidate_count`: `5062`
- `resolved_molecule_count`: `5127`
- `property_observation`: `1859`
- `property_recommended`: `1132`
- `regulatory_status`: `95`
- `pending_sources`: `47`
- `qc_issue_count`: `0`
- split counts: `train=84`, `validation=18`, `test=18`

Latest validation state:

- `pipelines/build_v1_dataset.py`: passes
- `pipelines/validate_v1_dataset.py`: passes
- `.venv\Scripts\pytest.exe -q`: `32 passed`
- current validation errors: none

## Completed

### Inventory and promotion model

- `data/raw/manual/seed_catalog.csv` is no longer capped at `120` rows; it now carries the full inventory with `entity_scope` and `model_inclusion`.
- `Tier A/B/C` remain the promoted high-quality subset used for model-facing outputs.
- `Tier D` is now reserved for inventory-only molecules that are in scope but not yet promoted into the model subset.
- A curated inventory supplement now exists at `data/raw/manual/refrigerant_inventory.csv`.
- Manual labels now support two inputs: the legacy `manual_property_observations.csv` file and `data/raw/manual/observations/*.csv`.

### Pipeline behavior

- `molecule_core`, `molecule_alias`, `property_observation`, `regulatory_status`, and `molecule_master` now carry the resolved full inventory.
- `model_dataset_index` and `model_ready` are now filtered to `model_inclusion=yes`, so the promoted subset remains stable while the inventory expands.
- Duplicate resolved structures now prefer refrigerant inventory rows over candidate placeholders when assigning the canonical `seed_id` in `molecule_core`.
- Inventory reporting now exposes `refrigerant_count`, `candidate_count`, `unresolved_refrigerants`, and label-gap summaries by `entity_scope` and `coverage_tier`.

### Bulk PubChem intake

- Real PubChem FTP Extras files have been downloaded locally for `CID-SMILES.gz`, `CID-InChI-Key.gz`, `CID-Mass.gz`, `CID-Component.gz`, and `CID-Synonym-filtered.gz`.
- The large local-only FTP inputs and regenerated bulk outputs that should not be pushed to normal Git history are documented in `docs/local_large_artifacts.md`.
- The bulk candidate builder now uses a two-stage path:
  - DuckDB formula/mass prefilter on `CID-Mass.gz`
  - streaming RDKit screening only on the surviving single-component CID set
- First production bulk results:
  - PubChem total mass records scanned by DuckDB: `123,857,780`
  - coarse survivors after formula/mass/atom-count rules: `612,908`
  - single-component survivors: `555,722`
  - RDKit hard-filter passes: `554,347`
  - deduplicated `pubchem_candidate_pool.parquet` rows: `551,067`
  - full InChIKey matches against existing inventory: `83`
  - first-block matches against existing inventory: `865`
- The first generated Tier D export batch adds `5000` `source_bundle=pubchem_bulk` rows into `seed_catalog.csv`.

### Validation and resilience

- Validation now checks three layers in one report:
  - schema and integration integrity
  - refrigerant inventory completeness
  - existing `Tier A/B` quality gates
- Curated refrigerant inventory rows now all resolve to molecules in the current baseline.
- Candidate-only grouped EPA mappings are still allowed, but they no longer backfill explicit refrigerant inventory rows.

## Current Coverage

### Promoted subset

- `Tier A`: `32` anchors
- `Tier B`: `48` promoted known refrigerants / industrial candidates
- `Tier C`: `40` promoted long-tail candidates
- promoted model subset size: `120`

### Inventory-only tail

- `Tier D`: `5012` rows in the catalog
- `Tier D candidate` rows: `5007`
- `Tier D refrigerant` rows: `5`
- `Tier D` rows currently resolved into `molecule_core`: `5007`
- the first bulk PubChem Tier D batch is inventory-only and not part of the promoted model subset

### Key label coverage

- `Tier A`
  - `gwp_100yr`: `0.9688`
  - `odp`: `0.9375`
  - `ashrae_safety`: `0.9063`
  - `toxicity_class`: `0.9063`
- `Tier B`
  - `gwp_100yr`: `0.5417`
  - `odp`: `0.5417`
  - `ashrae_safety`: `0.3125`
  - `toxicity_class`: `0.3125`
- `Tier C`
  - `gwp_100yr`: `0.6000`
  - `odp`: `0.2250`
  - `ashrae_safety`: `0.0250`
  - `toxicity_class`: `0.0250`
- `Tier D`
  - `gwp_100yr`: `0.0002`
  - `odp`: `0.0000`
  - `ashrae_safety`: `0.0000`
  - `toxicity_class`: `0.0000`

## Known Issues

### Upstream source limitations

- Several NIST phase pages still return `No tables found`, including `R-1234yf`, `R-1234ze(E)`, `R-1234ze(Z)`, `R-1233zd(E)`, `R-1336mzz(E)`, and `Novec 649`.
- Some NIST snapshots parse but yield `0 parsed observations`, including `deuterium`, `methyl linolenate`, and `neon`.
- Some CoolProp fluids still fail standard-cycle calculations because the configured cycle conditions fall below supported limits, including `D4`, `methyl palmitate`, `methyl stearate`, and `p-xylene`.

### Coverage limitations

- `Tier B` environmental and safety labels are still only partially covered.
- `Tier C` and `Tier D` safety coverage remain sparse.
- Bulk PubChem Tier D rows are intentionally structure-only in the first pass. Nearly all of them still have no environmental or safety labels.
- `pending_sources` remains non-zero because the optional CompTox line is not configured with an API key.

### Audit semantics

- `data/bronze/pubchem_candidate_filter_audit.parquet` is currently a coarse-survivor audit, not a full PubChem negative log.
- The current materialized audit covers the `612,908` CIDs that passed the cheap DuckDB coarse screen and then records whether they failed on multi-component filtering or RDKit hard screening.
- The full `123,857,780` PubChem `CID-Mass.gz` rows are not materialized into a giant “failed coarse filter” audit table by default.

## Next Priorities

1. Finish the remaining `Tier A` environmental/safety holes, especially `anchor_r41`, `anchor_r365mfc`, `anchor_r1234ze(Z)`, and `anchor_r718`.
2. Continue explicit public-label completion for refrigerant rows in `Tier B` and `Tier D`, prioritizing `gwp_100yr`, `odp`, `ashrae_safety`, and `toxicity_class`.
3. Add a second-stage volatility enrichment pass for the bulk Tier D candidate pool using NIST, CoolProp, and literature-backed sources.
4. Decide whether the project needs a full coarse-filter failure audit for all `123,857,780` PubChem mass records or whether aggregate filter counts are sufficient.

## Core Output Files

- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/bronze/pubchem_candidate_pool.parquet`
- `data/bronze/pubchem_candidate_filter_audit.parquet`
- `data/raw/generated/pubchem_tierd_candidates.csv`
- `data/index/r_physgen_v2.duckdb`
- `data/raw/manual/seed_catalog.csv`
- `data/raw/manual/refrigerant_inventory.csv`
- `data/raw/manual/manual_property_observations.csv`
- `data/raw/manual/observations/`
