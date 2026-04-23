# Current Status

## Snapshot

Status date: `2026-04-22`

R-PhysGen-DB has a green build/validate/test baseline after aligning the `2026-04-22` property-governance bundle with the existing Wave 2 inventory, PubChem bulk intake, and Excel supplement. The governance bundle is now fully mirrored in the extension layer and fused into canonical property outputs without widening the legacy `property_name` projection surface.

Latest local rebuild summary:

- curated base inventory in `data/raw/manual/seed_catalog.csv`: `5700` rows = `70` refrigerants + `5630` candidates
- effective build inventory after appending generated governance seeds: `5707` rows = `77` refrigerants + `5630` candidates
- `resolved_molecule_count`: `5598`
- `property_observation`: `4315`
- `property_observation_canonical`: `1687`
- `property_recommended`: `2643`
- `property_recommended_canonical`: `1389`
- `property_recommended_canonical_strict`: `1304`
- `property_recommended_canonical_review_queue`: `0`
- `regulatory_status`: `96`
- `pending_sources`: `47`
- `qc_issue_count`: `10`
- split counts: `train=84`, `validation=18`, `test=18`
- extension mirror: `218/218` tables aligned; `mixture_core=123`; `mixture_component=378`
- bundle crosswalk: `matched_existing=128`, `generated_new_seed=7`, `unresolved=0`, `external_resolution_count=2`

Latest validation state:

- `pipelines/build_v1_dataset.py`: passes
- `pipelines/validate_v1_dataset.py`: passes
- `.venv\Scripts\pytest.exe -q`: `53 passed`
- current validation errors: none

## Completed

### Governance bundle alignment

- The bundle `methods/refrigerant_seed_database_20260422_property_governance_bundle.zip` is mirrored into `data/extensions/property_governance_20260422/tables/*.parquet` with `218/218` tables, matching row counts, and preserved column order.
- DuckDB now registers bundle tables under `property_governance_ext.*` and normalized extension tables under `extensions.*`.
- `data/raw/manual/property_governance_20260422_unresolved_curations.csv` is applied before bundle crosswalking and is validated as a high-confidence, single-structure override input.
- `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv` is applied after canonical recommendation selection and now supports both `accept_selected_source` and `accept_out_of_strict`, closing reviewed queue rows without mutating the selected canonical value.
- `data/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv` is applied during strict filtering and allows explicitly approved proxy-only feature/source combinations into the ML-facing canonical strict layer.
- The two previously unresolved bundle substances, `SUB092 / R-C316 / CAS 356-18-3` and `SUB098 / R-E245ca1 / CAS 69948-24-9`, are now resolved through explicit external curation.
- The `1` top-rank conflict row and `34` source-divergence rows identified in the second-round review have now been adjudicated and removed from the open review queue.
- The `396` previously open `proxy_selected` rows are now absorbed into `property_recommended_canonical_strict` through `13` feature/source proxy-only policy rules, while preserving proxy provenance on the canonical recommendation layer.
- The `4` `environmental.ozone_depleting_flag` rows with governed `No` text values are now normalized to numeric `0.0` in the canonical overlay and enter the strict layer.
- The remaining `85` strict-rejection queue rows have been explicitly adjudicated as `accept_out_of_strict`, so the open canonical review queue is now `0` while low-quality or qualitative rows remain visible in `property_recommended_canonical`.
- Canonical property outputs are materialized as:
  - `data/silver/property_observation_canonical.parquet`
  - `data/gold/property_recommended_canonical.parquet`
  - `data/gold/property_recommended_canonical_strict.parquet`
- Legacy projection behavior stays intentionally narrow. Only the existing active canonical keys flow back into the legacy `property_name` tables.

### Inventory and promotion behavior

- `data/raw/manual/seed_catalog.csv` remains the authoritative curated base catalog.
- `data/raw/generated/property_governance_20260422_seed_catalog.csv` appends `7` build-time `Tier D` refrigerant rows sourced from the governance bundle.
- Those `7` generated bundle seeds remain `model_inclusion=no`, so they do not automatically enter `model_dataset_index` or `model_ready`.
- The promoted model subset remains the same `Tier A/B/C` block with `120` rows.

### Existing expansion paths retained

- The PubChem bulk candidate intake remains active and still feeds the first-pass `Tier D` candidate pool.
- The Excel 202603 supplement remains active through `data/raw/manual/observations/excel_202603_observations.csv` and its generated staging/candidate artifacts.
- Validation now covers schema integrity, inventory completeness, quality gates, canonical strict-filtering rules, and property-governance extension mirror checks in one report.

## Current Coverage

### Promoted subset

- `Tier A`: `32`
- `Tier B`: `48`
- `Tier C`: `40`
- promoted model subset size: `120`

### Effective inventory-only tail

- effective `Tier D` rows: `5587`
- `Tier D` candidate rows: `5575`
- `Tier D` refrigerant rows: `12`
- `Tier D` molecules currently resolved into `molecule_core`: `5478`

### Canonical layer

- canonical recommended feature keys: `30`
- strict ML-ready canonical feature keys: `24`
- proxy/screening-selected rows still visible in `property_recommended_canonical`: `466`
- proxy-only selected rows: `466`
- top-rank conflict provenance flags still visible in `property_recommended_canonical`: `1`
- lower-priority source-divergence provenance flags still visible in `property_recommended_canonical`: `35`
- open top-rank conflict rows in `property_recommended_canonical_review_queue`: `0`
- open source-divergence rows in `property_recommended_canonical_review_queue`: `0`
- open proxy-selected rows in `property_recommended_canonical_review_queue`: `0`
- canonical review decision rows applied: `120`
- `accept_selected_source` review decisions applied: `35`
- `accept_out_of_strict` review decisions applied: `85`
- proxy-only policy rules applied: `13`
- strict rows accepted through proxy-only policy: `396`
- canonical review queue rows: `0`
- bundle unresolved substances after curation: `0`

### Key label coverage on the promoted subset

- `Tier A`
  - `gwp_100yr`: `0.9688`
  - `odp`: `1.0000`
  - `ashrae_safety`: `0.9063`
  - `toxicity_class`: `0.9063`
- `Tier B`
  - `gwp_100yr`: `0.5625`
  - `odp`: `0.5833`
  - `ashrae_safety`: `0.3542`
  - `toxicity_class`: `0.3333`
- `Tier C`
  - `gwp_100yr`: `0.6750`
  - `odp`: `0.4250`
  - `ashrae_safety`: `0.1500`
  - `toxicity_class`: `0.1500`

## Known Issues

### Upstream source limitations

- Several NIST phase pages still return `No tables found`, including `R-1234yf`, `R-1234ze(E)`, `R-1234ze(Z)`, `R-1233zd(E)`, `R-1336mzz(E)`, and `Novec 649`.
- Some NIST snapshots parse but yield `0 parsed observations`, including `deuterium`, `methyl linolenate`, and `neon`.
- Some CoolProp fluids still fail the current standard-cycle setup because the configured operating point falls below supported limits, including `D4`, `methyl palmitate`, `methyl stearate`, and `p-xylene`.

### Coverage limitations

- `Tier B` environmental and safety labels are still incomplete.
- `Tier C` and `Tier D` safety coverage remain sparse.
- Bulk PubChem `Tier D` rows are still intentionally structure-first in the initial pass, so most of them do not yet carry environmental or safety labels.
- `pending_sources` remains non-zero because the optional CompTox line is not configured with an API key.
- The general canonical recommendation layer still contains proxy-only rows by design, and it preserves conflict/source-divergence provenance flags even after manual closure decisions and proxy-only strict acceptance; ML-facing work should use `property_recommended_canonical_strict`, while low-quality or qualitative rows intentionally kept out of strict are documented in `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv`.

## Next Priorities

1. Continue public-label completion for refrigerant rows in `Tier B` and `Tier D`, prioritizing `gwp_100yr`, `odp`, `ashrae_safety`, and `toxicity_class`.
2. Add second-stage property enrichment for the bulk `Tier D` candidate pool using NIST, CoolProp, and literature-backed sources.
3. Revisit the `85` `accept_out_of_strict` review decisions only when a higher-quality source or a better categorical/quantitative normalization path becomes available.
4. Keep future governance-bundle refreshes green against the `218/218` extension-mirror audit, the canonical strict-filter validation, the review-decision closure set, and the proxy-only acceptance policy set.

## Core Output Files

- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/bronze/property_governance_20260422_audit.json`
- `data/bronze/property_governance_20260422_substance_crosswalk.parquet`
- `data/raw/generated/property_governance_20260422_seed_catalog.csv`
- `data/raw/manual/property_governance_20260422_unresolved_curations.csv`
- `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv`
- `data/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv`
- `data/silver/property_observation_canonical.parquet`
- `data/gold/property_recommended_canonical.parquet`
- `data/gold/property_recommended_canonical_strict.parquet`
- `data/gold/property_recommended_canonical_review_queue.parquet`
- `data/extensions/property_governance_20260422/`
- `data/index/r_physgen_v2.duckdb`
