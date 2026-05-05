# Project Scope

## V1 Objective

Build a traceable, extensible database foundation for single-component refrigerants and candidate fifth-generation refrigerants, while explicitly separating:

- full in-scope inventory coverage
- promoted model-ready coverage

## Included in V1

- single-component, neutral, small-molecule refrigerants
- elements primarily constrained to `C/H/F/Cl/Br/I/O/N/S`
- C1-C6 halocarbons, HFOs, HFCs, HCFCs, CFCs, selected cyclic candidates, and natural refrigerants
- full inventory tracking in `data/lake/raw/manual/seed_catalog.csv`
- bulk PubChem candidate acquisition as a `Tier D` inventory-only side channel, not as a replacement for the curated refrigerant baseline
- promoted subset control through `coverage_tier` and `model_inclusion`
- source tracking, data contracts, quality control, recommended-value selection, and scaffold-aware model split
- open-source-first adapters for PubChem, NIST WebBook snapshotting, EPA references, and CoolProp-derived thermodynamic labels

## Explicitly Out of Scope in V1

- mixture refrigerants as first-class entities
- REFPROP integration as a runtime requirement
- DFT, MD, and active learning loop execution
- large-scale literature mining

## Inventory Semantics

- `entity_scope=refrigerant`
  Means the row is treated as part of the explicit refrigerant inventory and must resolve to a molecule in the validation baseline.
- `entity_scope=candidate`
  Means the row is still in scope for the broader database but is not treated as part of the mandatory refrigerant completeness gate.
- `model_inclusion=yes`
  Means the row participates in model-facing outputs.
- `model_inclusion=no`
  Means the row remains in the resolved inventory only.

## Bulk PubChem Candidate Intake

- Bulk PubChem intake is reserved for `Tier D` candidate expansion and does not change the curated `refrigerant` baseline.
- The first-pass screening envelope follows the project's small-molecule rules: single-component, neutral, allowed elements limited to `C/H/F/Cl/Br/I/O/N/S`, total atom count `<= 18`, heavy atom count `1-15`, molecular weight `16-300`, and carbon count `1-6`.
- First-pass volatility is not treated as a hard gate. Surviving candidates remain `volatility_status=unknown` until a later enrichment pass adds NIST, CoolProp, or literature support.
- Historical long-tail candidates already present in the repository, such as siloxanes or long-chain species, remain as historical records only. They are not part of the new bulk-screening intake criteria.
- The bulk candidate pool is intentionally broader than the promoted modeling subset. `Tier A/B/C` promotion logic remains unchanged.

## Mixture Reservation

Mixtures are reserved for phase 2. Schema placeholders are defined in:

- `schemas/extensions/mixture_core.yaml`
- `schemas/extensions/mixture_component.yaml`

## Methods Reference Status

The following files are reference material, not V1 truth tables:

- `archive/2026-05-05/methods/数据收集与预处理闭环实验设计.md`
- `archive/2026-05-05/methods/多尺度生成模型数据闭环设计.md`
- `archive/2026-05-05/methods/refrigerant_data_project/refrigerant_data_pipeline.py`

The V1 pipeline reuses the design intent from those files but not the random-sample generation logic.
