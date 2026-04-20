# Project Scope

## V1 Objective

Build a traceable, extensible database foundation for single-component refrigerants and candidate fifth-generation refrigerants, while explicitly separating:

- full in-scope inventory coverage
- promoted model-ready coverage

## Included in V1

- single-component, neutral, small-molecule refrigerants
- elements primarily constrained to `C/H/F/Cl/Br/I/O/N/S`
- C1-C6 halocarbons, HFOs, HFCs, HCFCs, CFCs, selected cyclic candidates, and natural refrigerants
- full inventory tracking in `data/raw/manual/seed_catalog.csv`
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

## Mixture Reservation

Mixtures are reserved for phase 2. Schema placeholders are defined in:

- `schemas/extensions/mixture_core.yaml`
- `schemas/extensions/mixture_component.yaml`

## Methods Reference Status

The following files are reference material, not V1 truth tables:

- `methods/鏁版嵁鏀堕泦涓庨澶勭悊闂幆瀹為獙璁捐.md`
- `methods/澶氬昂搴︾敓鎴愭ā鍨嬫暟鎹棴鐜璁?md`
- `methods/refrigerant_data_project/refrigerant_data_pipeline.py`

The V1 pipeline reuses the design intent from those files but not the random-sample generation logic.
