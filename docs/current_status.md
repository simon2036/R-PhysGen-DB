# Current Status

## Snapshot

Status date: `2026-04-19`

R-PhysGen-DB now has a working Wave 2 codebase with a green build/validate/test baseline after converting the old fixed `120`-seed catalog into a full inventory catalog plus a promoted model subset. The repository currently distinguishes between:

- full inventory: all in-scope single-component refrigerants and candidate molecules tracked in `seed_catalog.csv`
- promoted model subset: the `Tier A/B/C` molecules with `model_inclusion=yes`

Latest local rebuild summary:

- `seed_catalog_count`: `132`
- `refrigerant_count`: `70`
- `candidate_count`: `62`
- `resolved_molecule_count`: `128`
- `property_observation`: `1859`
- `property_recommended`: `1132`
- `regulatory_status`: `95`
- `pending_sources`: `47`
- `qc_issue_count`: `0`
- split counts: `train=84`, `validation=18`, `test=18`

Latest validation state:

- `pipelines/build_v1_dataset.py`: passes
- `pipelines/validate_v1_dataset.py`: passes
- `.venv\Scripts\pytest.exe -q`: `25 passed`
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

- `Tier D`: `12` inventory-only rows in the catalog
- currently resolved into `molecule_core`: `8`
- the remaining inventory-only rows are still visible in the catalog and will be promoted only after evidence-backed label completion

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
  - `gwp_100yr`: `0.1250`
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
- `pending_sources` remains non-zero because the optional CompTox line is not configured with an API key.

## Next Priorities

1. Finish the remaining `Tier A` environmental/safety holes, especially `anchor_r41`, `anchor_r365mfc`, `anchor_r1234ze(Z)`, and `anchor_r718`.
2. Continue explicit public-label completion for refrigerant rows in `Tier B` and `Tier D`, prioritizing `gwp_100yr`, `odp`, `ashrae_safety`, and `toxicity_class`.
3. Promote selected `Tier D` refrigerants into `Tier C/B` only after evidence-backed label completion.
4. Keep rebuilding and validating after each batch so `quality_report.json` and `validation_report.json` remain aligned with the actual working state.

## Core Output Files

- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/index/r_physgen_v2.duckdb`
- `data/raw/manual/seed_catalog.csv`
- `data/raw/manual/refrigerant_inventory.csv`
- `data/raw/manual/manual_property_observations.csv`
- `data/raw/manual/observations/`
