# Current Status

## Snapshot

Status date: `2026-04-21`

R-PhysGen-DB now has a working Wave 2 codebase with a green build/validate/test baseline after integrating both the first real PubChem bulk FTP intake and the curated Excel workbook `methods/制冷剂数据库202603.xlsx` into the inventory pipeline. The repository currently distinguishes between:

- full inventory: all in-scope single-component refrigerants and candidate molecules tracked in `seed_catalog.csv`
- promoted model subset: the `Tier A/B/C` molecules with `model_inclusion=yes`

Latest local rebuild summary:

- `seed_catalog_count`: `5700`
- `refrigerant_count`: `70`
- `candidate_count`: `5630`
- `resolved_molecule_count`: `5591`
- `property_observation`: `3060`
- `property_recommended`: `2330`
- `regulatory_status`: `96`
- `pending_sources`: `47`
- `qc_issue_count`: `6`
- split counts: `train=84`, `validation=18`, `test=18`

Latest validation state:

- `pipelines/build_v1_dataset.py`: passes
- `pipelines/validate_v1_dataset.py`: passes
- `.venv\Scripts\pytest.exe -q`: `37 passed`
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
- A new coarse-filter aggregate layer is now materialized as:
  - `data/bronze/coarse_filter_summary.parquet`
  - `data/bronze/coarse_filter_summary.json`
- The bulk candidate builder now uses a two-stage path:
  - DuckDB formula/mass prefilter on `CID-Mass.gz`
  - streaming RDKit screening only on the surviving single-component CID set
- First production bulk results:
  - PubChem total mass records scanned by DuckDB: `123,857,780`
  - coarse survivors after formula/mass/atom-count rules: `612,908`
  - coarse failures after the same rules: `123,244,872`
  - grouped coarse summary rows in `coarse_filter_summary.parquet`: `471,756`
  - single-component survivors: `555,722`
  - RDKit hard-filter passes: `554,347`
  - deduplicated `pubchem_candidate_pool.parquet` rows: `551,067`
  - full InChIKey matches against existing inventory: `83`
  - first-block matches against existing inventory: `865`
- Highest-volume coarse failure reasons in the aggregated summary are:
  - `total_atom_count_gt_18`: `122,294,222` reason hits
  - `carbon_count_gt_6`: `121,817,820` reason hits
  - `heavy_atom_count_gt_15`: `111,446,969` reason hits
- The first generated Tier D export batch adds `5000` `source_bundle=pubchem_bulk` rows into `seed_catalog.csv`.

### Excel 202603 intake

- Workbook intake is now materialized through:
  - `data/raw/manual/observations/excel_202603_observations.csv`
  - `data/raw/generated/excel_202603_tierd_candidates.csv`
  - `data/raw/generated/excel_202603_name_only_staging.csv`
  - `docs/excel_202603_brief_report.md`
- The Excel supplement currently contributes:
  - `1263` workbook-derived observation rows in total
  - `136` observation rows matched onto existing molecules
  - `1127` observation rows backfilled onto generated workbook-only candidates
  - `568` generated `Tier D` candidate rows from workbook-only structured entries
  - `1757` name-only staging rows spanning `936` unique workbook-only names from `2-热物性参考`
  - `127` recommended `critical_compressibility_factor` (`Zc`) labels in `property_recommended`
  - `200` candidate-property backfill rows now come from `2-热物性参考`, including `160` exact-alias second-pass enrichments
  - `11` workbook `ODP` outliers filtered out before entering the main observation tables
- Workbook import policy in the current baseline:
  - `Tb/Tc/Pc/ω/ΔvapH/ODP/Zc` are admissible into the main observation pipeline
  - bare `GWP` remains report-only because the workbook does not specify a time horizon
  - `Hv[298K]` remains report-only / staging-only because it is not treated as the same label as `vaporization_enthalpy_kjmol`
  - `2-热物性参考` can now also backfill resolved Excel candidates through exact alias matches, but only when the candidate-property value is unique

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

- `Tier D`: `5580` rows in the catalog
- `Tier D candidate` rows: `5575`
- `Tier D refrigerant` rows: `5`
- `Tier D` rows currently resolved into `molecule_core`: `5471`
- the first bulk PubChem Tier D batch and the workbook-derived Excel Tier D batch are inventory-only and not part of the promoted model subset

### Key label coverage

- `Tier A`
  - `gwp_100yr`: `0.9688`
  - `odp`: `0.9688`
  - `ashrae_safety`: `0.9063`
  - `toxicity_class`: `0.9063`
- `Tier B`
  - `gwp_100yr`: `0.5417`
  - `odp`: `0.5833`
  - `ashrae_safety`: `0.3125`
  - `toxicity_class`: `0.3125`
- `Tier C`
  - `gwp_100yr`: `0.6000`
  - `odp`: `0.4250`
  - `ashrae_safety`: `0.0250`
  - `toxicity_class`: `0.0250`
- `Tier D`
  - `gwp_100yr`: `0.0027`
  - `odp`: `0.0766`
  - `ashrae_safety`: `0.0004`
  - `toxicity_class`: `0.0004`

## Known Issues

### Upstream source limitations

- Several NIST phase pages still return `No tables found`, including `R-1234yf`, `R-1234ze(E)`, `R-1234ze(Z)`, `R-1233zd(E)`, `R-1336mzz(E)`, and `Novec 649`.
- Some NIST snapshots parse but yield `0 parsed observations`, including `deuterium`, `methyl linolenate`, and `neon`.
- Some CoolProp fluids still fail standard-cycle calculations because the configured cycle conditions fall below supported limits, including `D4`, `methyl palmitate`, `methyl stearate`, and `p-xylene`.

### Coverage limitations

- `Tier B` environmental and safety labels are still only partially covered.
- `Tier C` and `Tier D` safety coverage remain sparse.
- Bulk PubChem Tier D rows are intentionally structure-only in the first pass. Nearly all of them still have no environmental or safety labels.
- The Excel-generated Tier D candidate batch improves structural coverage, but many workbook-only CAS lookups still do not resolve through PubChem and therefore remain unresolved inventory tail rows.
- `pending_sources` remains non-zero because the optional CompTox line is not configured with an API key.
- The remaining `6` QC warnings are all historical EPA ODS `ODP > 1` halon records; the workbook-derived `ODP` outliers have been filtered out and no longer enter `property_observation`.

### Audit semantics

- `data/bronze/pubchem_candidate_filter_audit.parquet` is currently a coarse-survivor audit, not a full PubChem negative log.
- The current materialized audit covers the `612,908` CIDs that passed the cheap DuckDB coarse screen and then records whether they failed on multi-component filtering or RDKit hard screening.
- `data/bronze/coarse_filter_summary.parquet` now serves as the default coarse-filter-wide summary over all `123,857,780` PubChem `CID-Mass.gz` rows.
- `data/bronze/coarse_filter_summary.json` is the compact human-readable digest of that same pass, while the parquet file keeps the detailed grouped aggregate.
- Failure-reason counts in `coarse_filter_summary.*` are reason-hit counts rather than mutually exclusive outcomes, so a single CID may appear under multiple coarse failure reasons.

## Next Priorities

1. Finish the remaining `Tier A` environmental/safety holes, especially `anchor_r41`, `anchor_r365mfc`, `anchor_r1234ze(Z)`, and `anchor_r718`.
2. Continue explicit public-label completion for refrigerant rows in `Tier B` and `Tier D`, prioritizing `gwp_100yr`, `odp`, `ashrae_safety`, and `toxicity_class`.
3. Add a second-stage volatility enrichment pass for the bulk Tier D candidate pool using NIST, CoolProp, and literature-backed sources.
4. Revisit unresolved workbook-derived `Tier D` CAS seeds and decide whether a second pass should switch selected cases from CAS queries to structure-resolved SMILES or curated PubChem CIDs.
5. Use `coarse_filter_summary.*` to guide rule tuning and volatility-enrichment prioritization; only build a full per-CID coarse-failure ledger if future traceability requirements become stricter.

## Core Output Files

- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/bronze/coarse_filter_summary.parquet`
- `data/bronze/coarse_filter_summary.json`
- `data/bronze/pubchem_candidate_pool.parquet`
- `data/bronze/pubchem_candidate_filter_audit.parquet`
- `data/raw/generated/pubchem_tierd_candidates.csv`
- `data/raw/generated/excel_202603_tierd_candidates.csv`
- `data/raw/generated/excel_202603_name_only_staging.csv`
- `data/index/r_physgen_v2.duckdb`
- `data/raw/manual/seed_catalog.csv`
- `data/raw/manual/refrigerant_inventory.csv`
- `data/raw/manual/manual_property_observations.csv`
- `data/raw/manual/observations/`
- `docs/excel_202603_brief_report.md`
