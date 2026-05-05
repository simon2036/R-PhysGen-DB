"""Production stage functions for the R-PhysGen-DB build."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db import pipeline as legacy
from r_physgen_db.active_learning import (
    ACTIVE_LEARNING_SOURCE_ID,
    ACTIVE_LEARNING_SOURCE_NAME,
    active_learning_max_entries,
    build_active_learning_queue,
    build_deterministic_active_learning_queue,
    active_learning_summary,
    production_quantum_request_target,
)
from r_physgen_db.canonical_projection import project_native_canonical_recommendations
from r_physgen_db.condition_sets import backfill_condition_sets
from r_physgen_db.constants import DATA_DIR, PARSER_VERSION
from r_physgen_db.coverage_worklist import write_promoted_coverage_outputs
from r_physgen_db.cycle_conditions import build_cycle_tables
from r_physgen_db.mixtures import (
    MIXTURE_COMPONENT_CURATION_SOURCE_ID,
    MIXTURE_COMPONENT_CURATION_SOURCE_NAME,
    MIXTURE_FRACTION_CURATION_SOURCE_ID,
    MIXTURE_FRACTION_CURATION_SOURCE_NAME,
    build_mixture_tables,
    load_mixture_component_curations,
    load_mixture_fraction_curations,
)
from r_physgen_db.pipeline_stages.artifacts import StageResult
from r_physgen_db.pipeline_stages.context import BuildState
from r_physgen_db.pipeline_stages.orchestrator import StageSpec
from r_physgen_db.proxy_features import (
    PROXY_SOURCE_ID,
    PROXY_SOURCE_NAME,
    build_proxy_feature_rows,
    proxy_feature_metadata,
)
from r_physgen_db.quantum_pilot import (
    QUANTUM_SOURCE_ID,
    QUANTUM_SOURCE_NAME,
    build_psi4_dft_request_manifest,
    build_quantum_pilot,
    build_quantum_pilot_request_manifest,
    completed_psi4_request_ids,
    completed_xtb_mol_ids,
    completed_xtb_request_ids,
)
from r_physgen_db.readiness import evaluate_research_task_readiness


def stage00_init_run(state: BuildState) -> StageResult:
    paths = legacy._paths()
    if state.data_dir == DATA_DIR:
        inferred_data_dir = _infer_data_dir_from_paths(paths)
        if inferred_data_dir is not None:
            state.data_dir = inferred_data_dir
    if state.data_dir != DATA_DIR:
        paths = {key: _remap_data_path(path, state.data_dir) for key, path in paths.items()}
    _ensure_prc_paths(paths)
    state.paths = paths
    for key, path in paths.items():
        legacy.ensure_directory(path.parent if key == "gold_version" or path.suffix else path)
    state.autofill_versions()

    state.pubchem = legacy.PubChemClient()
    state.nist = legacy.NISTWebBookClient()
    state.nist_parser = legacy.NISTThermoParser()
    state.coolprop = legacy.CoolPropSource()
    state.epa_gwp_reference_parser = legacy.EPATechnologyTransitionsGWPParser()
    state.epa_ods_parser = legacy.EPAODSParser()
    state.epa_snap_parser = legacy.EPASNAPParser()
    state.comptox = legacy.CompToxClient()
    return StageResult(
        stage_id="00",
        status="succeeded",
        outputs=[state.logical_artifact("paths"), state.logical_artifact("clients")],
        notes=f"code_version={state.code_version}; dataset_version={state.dataset_version}",
    )


def stage01_load_inventory(state: BuildState) -> StageResult:
    paths = state.paths
    state.seed_catalog = pd.read_csv(paths["seed_catalog"]).fillna("")
    state.manual_observations = legacy._load_manual_observations(paths)
    state.coolprop_aliases = legacy.load_yaml(paths["coolprop_aliases"]).get("mappings", {})
    state.bulk_pubchem_lookup = legacy._load_bulk_pubchem_candidate_lookup(paths)
    state.source_manifest_rows.extend(legacy._register_manual_sources(paths))

    coolprop_meta_path = paths["raw_coolprop_meta"]
    legacy.write_json(coolprop_meta_path, state.coolprop.session_metadata())
    state.source_manifest_rows.append(
        legacy._source_manifest_entry(
            source_id="source_coolprop_session",
            source_type="calculated_open_source",
            source_name=f"CoolProp {state.coolprop.version}",
            license_name="CoolProp open-source",
            local_path=coolprop_meta_path,
            upstream_url="https://coolprop.org/",
            status="generated",
        )
    )
    return StageResult(
        stage_id="01",
        status="succeeded",
        outputs=[
            state.logical_artifact("seed_catalog", row_count=len(state.seed_catalog)),
            state.logical_artifact("manual_observations", row_count=len(state.manual_observations)),
            state.logical_artifact("row_buffers"),
        ],
        row_count_summary={
            "seed_catalog": int(len(state.seed_catalog)),
            "manual_observations": int(len(state.manual_observations)),
        },
    )


def stage02_resolve_identity_boundary(state: BuildState) -> StageResult:
    return StageResult(
        stage_id="02",
        status="succeeded",
        outputs=[state.logical_artifact("identity_resolution_pending")],
        notes="Compatibility boundary: PubChem identity resolution remains in Stage 04 for PR-B equivalence.",
    )


def stage03_acquire_global_sources(state: BuildState) -> StageResult:
    state.global_sources = legacy._fetch_global_sources(
        paths=state.paths,
        refresh_remote=state.refresh_remote,
        source_manifest_rows=state.source_manifest_rows,
        resolution_rows=state.resolution_rows,
        epa_gwp_reference_parser=state.epa_gwp_reference_parser,
        epa_ods_parser=state.epa_ods_parser,
        epa_snap_parser=state.epa_snap_parser,
    )
    return StageResult(
        stage_id="03",
        status="succeeded",
        outputs=[state.logical_artifact("global_sources")],
        row_count_summary={
            "epa_gwp_reference": int(len(state.global_sources["epa_gwp_reference_df"])),
            "epa_ods": int(len(state.global_sources["epa_ods_df"])),
            "epa_snap_frames": int(len(state.global_sources["epa_snap_frames"])),
        },
        notes="Stage 03 caches global EPA sources only; entity mapping occurs in Stage 05.",
    )


def stage04_acquire_entity_sources(state: BuildState) -> StageResult:
    paths = state.paths
    coolprop_source_id = "source_coolprop_session"
    legacy.RDLogger.DisableLog("rdApp.warning")
    legacy.RDLogger.DisableLog("rdApp.error")
    for seed in state.seed_catalog.to_dict(orient="records"):
        seed_id = str(seed["seed_id"])
        pubchem_source_id = f"source_pubchem_{legacy.slugify(seed_id)}"
        nist_source_id = f"source_nist_{legacy.slugify(seed_id)}"
        r_number = legacy._clean_str(seed.get("r_number"))

        try:
            pubchem_snapshot = legacy._resolve_pubchem_snapshot(
                pubchem=state.pubchem,
                seed=seed,
                paths=paths,
                refresh_remote=state.refresh_remote,
                bulk_pubchem_lookup=state.bulk_pubchem_lookup,
            )
            pubchem_payload = pubchem_snapshot["payload"]
            state.source_manifest_rows.append(
                legacy._source_manifest_entry(
                    source_id=pubchem_source_id,
                    source_type=pubchem_snapshot["source_type"],
                    source_name=pubchem_snapshot["source_name"],
                    license_name=pubchem_snapshot["license_name"],
                    local_path=pubchem_snapshot["local_path"],
                    upstream_url=pubchem_payload["pubchem_record"]["url"],
                    status=pubchem_snapshot["source_status"],
                )
            )

            pubchem_record = pubchem_payload["pubchem_record"]
            synonyms_record = pubchem_payload["synonyms"]
            standardized = legacy.standardize_smiles(pubchem_record["isomeric_smiles"])
            mol_id = f"mol_{standardized['inchikey'].lower()}"
            state.seed_to_mol_id[seed_id] = mol_id

            candidate_row = {
                "mol_id": mol_id,
                "seed_id": seed_id,
                "family": legacy._clean_str(seed.get("family")),
                "canonical_smiles": standardized["canonical_smiles"],
                "isomeric_smiles": standardized["isomeric_smiles"],
                "inchi": standardized["inchi"],
                "inchikey": standardized["inchikey"],
                "inchikey_first_block": standardized["inchikey_first_block"],
                "formula": standardized["formula"],
                "molecular_weight": standardized["molecular_weight"],
                "charge": standardized["charge"],
                "heavy_atom_count": standardized["heavy_atom_count"],
                "stereo_flag": standardized["stereo_flag"],
                "ez_isomer": standardized["ez_isomer"] or "",
                "pubchem_cid": pubchem_record["cid"],
                "pubchem_query": legacy._clean_str(seed.get("query_name")),
                "entity_scope": legacy._clean_str(seed.get("entity_scope")) or "candidate",
                "model_inclusion": legacy._clean_str(seed.get("model_inclusion")) or "yes",
                "coverage_tier": legacy._clean_str(seed.get("coverage_tier")),
                "status": "resolved",
            }
            existing_row = state.molecule_rows.get(mol_id)
            if existing_row is None or legacy._prefer_seed_catalog_entry(candidate_row, existing_row):
                state.molecule_rows[mol_id] = candidate_row

            legacy._append_alias(state.alias_rows, mol_id, "seed_id", seed_id, True, "seed_catalog")
            legacy._append_alias(state.alias_rows, mol_id, "r_number", r_number, True, "seed_catalog")
            legacy._append_alias(state.alias_rows, mol_id, "query_name", legacy._clean_str(seed.get("query_name")), True, "seed_catalog")
            legacy._append_alias(state.alias_rows, mol_id, "pubchem_cid", pubchem_record["cid"], True, "PubChem")
            legacy._append_alias(state.alias_rows, mol_id, "coolprop_fluid", legacy._clean_str(seed.get("coolprop_fluid")), False, "seed_catalog")

            alias_bundle = state.pubchem.extract_aliases(synonyms_record["synonyms"])
            for cas in alias_bundle["cas_numbers"]:
                legacy._append_alias(state.alias_rows, mol_id, "cas", cas, False, "PubChem")
            for alias_r_number in alias_bundle["r_numbers"]:
                legacy._append_alias(state.alias_rows, mol_id, "r_number", alias_r_number, False, "PubChem")
            for name in alias_bundle["common_names"]:
                legacy._append_alias(state.alias_rows, mol_id, "synonym", name, False, "PubChem")
            legacy._append_family_prefixed_aliases(state.alias_rows, mol_id, r_number, legacy._clean_str(seed.get("family")))

            pubchem_resolution = {
                "seed_id": seed_id,
                "r_number": r_number,
                "stage": "pubchem",
                "status": "resolved",
                "detail": mol_id,
            }
            if pubchem_snapshot["source_status"] == "cached_fallback":
                pubchem_resolution["status"] = "cached_fallback"
                pubchem_resolution["detail"] = f"{mol_id}; {legacy._cached_fallback_detail(pubchem_snapshot)}"
            state.resolution_rows.append(pubchem_resolution)

            if legacy._clean_str(seed.get("regulatory_priority")) in {"high", "medium"} and not state.comptox.enabled:
                state.pending_rows.append(
                    {
                        "pending_id": f"pending_{legacy.slugify(seed_id)}_comptox",
                        **state.comptox.pending_record(seed_id=seed_id, r_number=r_number, mol_id=mol_id),
                    }
                )
        except Exception as exc:  # noqa: BLE001 - per-seed failure is legacy behavior
            error_path = paths["raw_pubchem"] / f"{legacy.slugify(seed_id)}.error.json"
            legacy.write_json(error_path, {"seed": seed, "error": str(exc)})
            state.source_manifest_rows.append(
                legacy._source_manifest_entry(
                    source_id=pubchem_source_id,
                    source_type="public_database",
                    source_name="PubChem PUG REST",
                    license_name="NCBI / PubChem public",
                    local_path=error_path,
                    upstream_url="",
                    status="failed",
                )
            )
            state.resolution_rows.append(
                {"seed_id": seed_id, "r_number": r_number, "stage": "pubchem", "status": "failed", "detail": str(exc)}
            )
            continue

        _acquire_nist_for_seed(state, seed, seed_id, nist_source_id, r_number)
        _acquire_coolprop_for_seed(state, seed, seed_id, r_number, coolprop_source_id)

    legacy.RDLogger.EnableLog("rdApp.warning")
    legacy.RDLogger.EnableLog("rdApp.error")
    state.molecule_core = legacy._ensure_columns(
        pd.DataFrame(sorted(state.molecule_rows.values(), key=lambda item: item["mol_id"])),
        legacy._molecule_core_columns(),
    )
    state.alias_df = legacy._ensure_columns(pd.DataFrame(state.alias_rows).drop_duplicates(), legacy._molecule_alias_columns())
    return StageResult(
        stage_id="04",
        status="succeeded",
        outputs=[
            state.logical_artifact("molecule_core_pre_governance", row_count=len(state.molecule_core)),
            state.logical_artifact("molecule_alias_pre_governance", row_count=len(state.alias_df)),
            state.logical_artifact("entity_source_rows"),
        ],
        row_count_summary={
            "molecule_core_pre_governance": int(len(state.molecule_core)),
            "molecule_alias_pre_governance": int(len(state.alias_df)),
            "property_rows": int(len(state.property_rows)),
        },
    )


def stage06_integrate_governance_bundle(state: BuildState) -> StageResult:
    state.bundle_integration = legacy.integrate_property_governance_bundle(
        bundle_path=legacy._property_governance_bundle_path(state.paths),
        output_root=legacy._output_root_from_paths(state.paths),
        seed_catalog=state.seed_catalog,
        molecule_core=state.molecule_core,
        alias_df=state.alias_df,
        parser_version=PARSER_VERSION,
        retrieved_at=legacy.now_iso(),
    )
    if state.bundle_integration["bundle_present"]:
        generated_seed_rows = state.bundle_integration["generated_seed_rows"]
        if isinstance(generated_seed_rows, pd.DataFrame) and not generated_seed_rows.empty:
            state.seed_catalog = (
                pd.concat([state.seed_catalog, generated_seed_rows], ignore_index=True)
                .drop_duplicates(subset=["seed_id"], keep="first")
                .reset_index(drop=True)
            )
        for row in state.bundle_integration["generated_molecule_rows"]:
            existing_row = state.molecule_rows.get(row["mol_id"])
            if existing_row is None or legacy._prefer_seed_catalog_entry(row, existing_row):
                state.molecule_rows[row["mol_id"]] = row
            state.seed_to_mol_id[row["seed_id"]] = row["mol_id"]
        state.alias_rows.extend(state.bundle_integration["generated_alias_rows"])
        state.property_rows.extend(state.bundle_integration["legacy_property_rows"])
        state.source_manifest_rows.extend(state.bundle_integration["source_manifest_rows"])
        state.resolution_rows.extend(state.bundle_integration["resolution_rows"])

        state.molecule_core = legacy._ensure_columns(
            pd.DataFrame(sorted(state.molecule_rows.values(), key=lambda item: item["mol_id"])),
            legacy._molecule_core_columns(),
        )
        state.alias_df = legacy._ensure_columns(pd.DataFrame(state.alias_rows).drop_duplicates(), legacy._molecule_alias_columns())

    state.canonical_observation = state.bundle_integration.get("canonical_observation", pd.DataFrame())
    state.canonical_recommended = state.bundle_integration.get("canonical_recommended", pd.DataFrame())
    state.canonical_recommended_strict = state.bundle_integration.get("canonical_recommended_strict", pd.DataFrame())
    state.canonical_review_queue = state.bundle_integration.get("canonical_review_queue", pd.DataFrame())
    state.property_governance_audit = state.bundle_integration.get("audit", {})
    component_curations = load_mixture_component_curations(state.paths["raw_mixture_component_curations"])
    fraction_curations = load_mixture_fraction_curations(state.paths["raw_mixture_fraction_curations"])
    mixture_build = build_mixture_tables(
        state.bundle_integration.get("mixture_core", pd.DataFrame()),
        state.bundle_integration.get("mixture_component", pd.DataFrame()),
        state.molecule_core,
        component_curations=component_curations,
        fraction_curations=fraction_curations,
    )
    state.mixture_core_table = mixture_build.mixture_core
    state.mixture_composition = mixture_build.mixture_composition
    state.mixture_summary = mixture_build.summary
    if not component_curations.empty:
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=MIXTURE_COMPONENT_CURATION_SOURCE_ID,
                source_type="manual_curated_reference",
                source_name=MIXTURE_COMPONENT_CURATION_SOURCE_NAME,
                license_name="project-local manual curation",
                local_path=state.paths["raw_mixture_component_curations"],
                upstream_url="",
                status="loaded",
            )
        )
    if not fraction_curations.empty:
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=MIXTURE_FRACTION_CURATION_SOURCE_ID,
                source_type="manual_curated_reference",
                source_name=MIXTURE_FRACTION_CURATION_SOURCE_NAME,
                license_name="project-local manual curation",
                local_path=state.paths["raw_mixture_fraction_curations"],
                upstream_url="",
                status="loaded",
            )
        )
    return StageResult(
        stage_id="06",
        status="succeeded",
        outputs=[
            state.logical_artifact("molecule_core", row_count=len(state.molecule_core)),
            state.logical_artifact("molecule_alias", row_count=len(state.alias_df)),
            state.logical_artifact("mixture_core", row_count=len(state.mixture_core_table)),
            state.logical_artifact("mixture_composition", row_count=len(state.mixture_composition)),
            state.logical_artifact("governance_bundle"),
        ],
        row_count_summary={
            "molecule_core": int(len(state.molecule_core)),
            "molecule_alias": int(len(state.alias_df)),
            "canonical_observation": int(len(state.canonical_observation)),
            "mixture_core": int(len(state.mixture_core_table)),
            "mixture_composition": int(len(state.mixture_composition)),
        },
    )


def stage05_harmonize_observations(state: BuildState) -> StageResult:
    state.molecule_context = legacy._build_molecule_source_context(state.molecule_core, state.seed_catalog)
    state.alias_lookup = legacy._build_alias_lookup(state.alias_df)
    state.property_rows.extend(legacy._manual_property_rows(state.manual_observations, state.seed_to_mol_id, state.alias_lookup))
    state.property_rows.extend(
        legacy._epa_gwp_reference_property_rows(
            gwp_df=state.global_sources["epa_gwp_reference_df"],
            alias_lookup=state.alias_lookup,
            molecule_context=state.molecule_context,
            source_id=state.global_sources["epa_gwp_reference_source_id"],
        )
    )
    state.property_rows.extend(
        legacy._epa_ods_property_rows(
            ods_df=state.global_sources["epa_ods_df"],
            alias_lookup=state.alias_lookup,
            source_id=state.global_sources["epa_ods_source_id"],
        )
    )
    snap_property_rows, snap_regulatory_rows = legacy._epa_snap_rows(
        snap_frames=state.global_sources["epa_snap_frames"],
        alias_lookup=state.alias_lookup,
        molecule_context=state.molecule_context,
        source_type="public_web_snapshot",
        source_name_prefix="EPA SNAP",
    )
    state.property_rows.extend(snap_property_rows)
    state.regulatory_rows.extend(snap_regulatory_rows)

    proxy_rows, state.proxy_feature_summary = build_proxy_feature_rows(state.molecule_core)
    state.property_rows.extend(proxy_rows)
    proxy_metadata_path = state.paths["raw_proxy_feature_metadata"]
    legacy.write_json(proxy_metadata_path, proxy_feature_metadata(state.proxy_feature_summary))
    state.source_manifest_rows.append(
        legacy._source_manifest_entry(
            source_id=PROXY_SOURCE_ID,
            source_type="derived_harmonized",
            source_name=PROXY_SOURCE_NAME,
            license_name="project-local deterministic heuristic",
            local_path=proxy_metadata_path,
            upstream_url="",
            status="generated",
        )
    )

    quantum_build = build_quantum_pilot(state.paths["raw_quantum_pilot_results"], state.molecule_core)
    state.property_rows.extend(quantum_build.property_rows)
    state.quantum_job = quantum_build.quantum_job
    state.quantum_artifact = quantum_build.quantum_artifact
    state.quantum_pilot_summary = quantum_build.summary
    quantum_request_manifest, _ = _write_quantum_request_outputs(state)
    if quantum_build.input_exists:
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=QUANTUM_SOURCE_ID,
                source_type="calculated_open_source",
                source_name=QUANTUM_SOURCE_NAME,
                license_name="project-local offline quantum result bundle",
                local_path=state.paths["raw_quantum_pilot_results"],
                upstream_url="",
                status="loaded",
            )
        )
    state.source_manifest_rows.append(
        legacy._source_manifest_entry(
            source_id="source_r_physgen_quantum_pilot_requests",
            source_type="derived_harmonized",
            source_name="R-PhysGen-DB Quantum Pilot Request Manifest",
            license_name="project-local deterministic queue",
            local_path=state.paths["raw_quantum_pilot_requests"],
            upstream_url="",
            status="generated",
        )
    )

    state.property_observation = legacy._ensure_columns(pd.DataFrame(state.property_rows), legacy._property_observation_columns())
    state.property_observation = legacy._assign_observation_ids(state.property_observation)
    state.property_observation, state.qc_issues = legacy._apply_qc(state.property_observation)
    (
        state.property_observation,
        state.observation_condition_set,
        state.condition_migration_progress,
    ) = backfill_condition_sets(state.property_observation, created_by_stage_id="05", parser_version=PARSER_VERSION)
    state.cycle_case, state.cycle_operating_point, state.cycle_summary = build_cycle_tables(state.property_observation)
    state.regulatory_status = legacy._ensure_columns(
        pd.DataFrame(state.regulatory_rows).drop_duplicates(),
        legacy._regulatory_status_columns(),
    )
    return StageResult(
        stage_id="05",
        status="succeeded",
        outputs=[
            state.logical_artifact("property_observation", row_count=len(state.property_observation)),
            state.logical_artifact("observation_condition_set", row_count=len(state.observation_condition_set)),
            state.logical_artifact("cycle_case", row_count=len(state.cycle_case)),
            state.logical_artifact("cycle_operating_point", row_count=len(state.cycle_operating_point)),
            state.logical_artifact("proxy_feature_observation", row_count=len(proxy_rows)),
            state.logical_artifact("quantum_pilot_observation", row_count=len(quantum_build.property_rows)),
            state.file_artifact("quantum_pilot_requests", state.paths["raw_quantum_pilot_requests"], kind="file"),
            state.file_artifact("quantum_pilot_xyz_manifest", state.paths["raw_quantum_pilot_xyz_manifest"], kind="file"),
            state.logical_artifact("quantum_job", row_count=len(state.quantum_job)),
            state.logical_artifact("quantum_artifact", row_count=len(state.quantum_artifact)),
            state.logical_artifact("regulatory_status", row_count=len(state.regulatory_status)),
            state.logical_artifact("qc_issues", row_count=len(state.qc_issues)),
        ],
        row_count_summary={
            "property_observation": int(len(state.property_observation)),
            "observation_condition_set": int(len(state.observation_condition_set)),
            "cycle_case": int(len(state.cycle_case)),
            "cycle_operating_point": int(len(state.cycle_operating_point)),
            "proxy_feature_observation": int(len(proxy_rows)),
            "quantum_pilot_observation": int(len(quantum_build.property_rows)),
            "quantum_pilot_requests": int(len(quantum_request_manifest)),
            "quantum_job": int(len(state.quantum_job)),
            "quantum_artifact": int(len(state.quantum_artifact)),
            "regulatory_status": int(len(state.regulatory_status)),
            "qc_issues": int(len(state.qc_issues)),
        },
        notes="PR-B executes governance before harmonization to preserve legacy output equivalence.",
    )


def stage07_build_feature_and_recommendation_layers(state: BuildState) -> StageResult:
    state.property_recommended = legacy._select_recommended(state.property_observation)
    readiness_rules_path = state.data_dir / "gold" / "property_modeling_readiness_rules.parquet"
    readiness_rules = pd.read_parquet(readiness_rules_path) if readiness_rules_path.exists() else pd.DataFrame()
    canonical_projection = project_native_canonical_recommendations(
        property_recommended=state.property_recommended,
        existing_canonical_recommended=state.canonical_recommended,
        existing_canonical_recommended_strict=state.canonical_recommended_strict,
        readiness_rules=readiness_rules,
    )
    state.canonical_recommended = canonical_projection.canonical_recommended
    state.canonical_recommended_strict = canonical_projection.canonical_recommended_strict
    state.property_governance_audit["native_canonical_projection"] = canonical_projection.summary
    coverage_summary = write_promoted_coverage_outputs(
        seed_catalog=state.seed_catalog,
        molecule_core=state.molecule_core,
        property_recommended=state.property_recommended,
        coverage_path=state.paths["raw_promoted_coverage_matrix"],
        worklist_path=state.paths["raw_promoted_enrichment_worklist"],
    )
    state.property_governance_audit["promoted_coverage_worklist"] = coverage_summary
    state.structure_features = legacy._build_structure_features(state.molecule_core)
    state.property_matrix = legacy._build_property_matrix(state.property_recommended)
    state.molecule_master = legacy._build_molecule_master(state.molecule_core, state.alias_df, state.structure_features)
    return StageResult(
        stage_id="07",
        status="succeeded",
        outputs=[
            state.logical_artifact("property_recommended", row_count=len(state.property_recommended)),
            state.logical_artifact("property_recommended_canonical", row_count=len(state.canonical_recommended)),
            state.logical_artifact("property_recommended_canonical_strict", row_count=len(state.canonical_recommended_strict)),
            state.file_artifact("promoted_coverage_matrix", state.paths["raw_promoted_coverage_matrix"], kind="file"),
            state.file_artifact("promoted_enrichment_worklist", state.paths["raw_promoted_enrichment_worklist"], kind="file"),
            state.logical_artifact("structure_features", row_count=len(state.structure_features)),
            state.logical_artifact("property_matrix", row_count=len(state.property_matrix)),
            state.logical_artifact("molecule_master", row_count=len(state.molecule_master)),
        ],
        row_count_summary={
            "property_recommended": int(len(state.property_recommended)),
            "property_recommended_canonical": int(len(state.canonical_recommended)),
            "property_recommended_canonical_strict": int(len(state.canonical_recommended_strict)),
            "promoted_coverage_matrix": int(coverage_summary["coverage_matrix_rows"]),
            "promoted_enrichment_worklist": int(coverage_summary["worklist_rows"]),
            "structure_features": int(len(state.structure_features)),
            "property_matrix": int(len(state.property_matrix)),
            "molecule_master": int(len(state.molecule_master)),
        },
    )


def stage08_build_model_outputs(state: BuildState) -> StageResult:
    state.model_dataset_index = legacy._build_model_dataset_index(
        state.structure_features,
        state.property_recommended,
        state.molecule_core,
    )
    state.molecule_split_definition = legacy._build_molecule_split_definition(
        state.model_dataset_index,
        state.molecule_core,
        dataset_version=state.dataset_version,
        code_version=state.code_version,
    )
    state.model_ready = legacy._build_model_ready(state.molecule_master, state.property_matrix, state.model_dataset_index)
    return StageResult(
        stage_id="08",
        status="succeeded",
        outputs=[
            state.logical_artifact("model_dataset_index", row_count=len(state.model_dataset_index)),
            state.logical_artifact("molecule_split_definition", row_count=len(state.molecule_split_definition)),
            state.logical_artifact("model_ready", row_count=len(state.model_ready)),
        ],
        row_count_summary={
            "model_dataset_index": int(len(state.model_dataset_index)),
            "molecule_split_definition": int(len(state.molecule_split_definition)),
            "model_ready": int(len(state.model_ready)),
        },
    )


def stage09_validate_and_publish(state: BuildState) -> StageResult:
    paths = state.paths
    active_learning_build = build_active_learning_queue(
        paths["raw_active_learning_queue"],
        state.molecule_core,
        decision_log_path=paths["raw_active_learning_decision_log"],
    )
    if active_learning_build.input_exists:
        state.active_learning_queue = active_learning_build.queue
        state.active_learning_decision_log = active_learning_build.decision_log
        state.active_learning_summary = active_learning_build.summary
        active_learning_local_path = (
            paths["raw_active_learning_queue"]
            if paths["raw_active_learning_queue"].exists()
            else paths["raw_active_learning_decision_log"]
        )
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=ACTIVE_LEARNING_SOURCE_ID,
                source_type="manual_catalog",
                source_name=ACTIVE_LEARNING_SOURCE_NAME,
                license_name="project-local manual active learning queue",
                local_path=active_learning_local_path,
                upstream_url="",
                status="loaded",
            )
        )
    else:
        state.active_learning_queue = build_deterministic_active_learning_queue(
            molecule_core=state.molecule_core,
            seed_catalog=state.seed_catalog,
            property_recommended=state.property_recommended,
            completed_quantum_mol_ids=completed_xtb_mol_ids(_raw_quantum_results(state)),
            max_entries=active_learning_max_entries(max(250, production_quantum_request_target())),
            min_quantum_entries=production_quantum_request_target(),
            model_version=state.dataset_version,
        )
        state.active_learning_decision_log = pd.DataFrame(columns=active_learning_build.decision_log.columns)
        state.active_learning_queue.drop(columns=["source_id"], errors="ignore").to_csv(
            paths["raw_generated_active_learning_queue"],
            index=False,
        )
        state.active_learning_summary = active_learning_summary(
            state.active_learning_queue,
            state.active_learning_decision_log,
            input_exists=True,
            input_path=paths["raw_generated_active_learning_queue"],
            input_row_count=len(state.active_learning_queue),
            decision_log_path=paths["raw_active_learning_decision_log"],
            decision_input_row_count=0,
            queue_input_exists=True,
            decision_input_exists=False,
        )
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=ACTIVE_LEARNING_SOURCE_ID,
                source_type="derived_harmonized",
                source_name=ACTIVE_LEARNING_SOURCE_NAME,
                license_name="project-local deterministic active-learning policy",
                local_path=paths["raw_generated_active_learning_queue"],
                upstream_url="",
                status="generated",
            )
        )

    _write_quantum_request_outputs(state, active_learning_queue=state.active_learning_queue)
    _write_psi4_dft_request_outputs(state, active_learning_queue=state.active_learning_queue)
    state.source_manifest_rows.append(
        legacy._source_manifest_entry(
            source_id="source_r_physgen_quantum_dft_requests",
            source_type="derived_harmonized",
            source_name="R-PhysGen-DB Psi4 DFT Request Manifest",
            license_name="project-local deterministic DFT queue",
            local_path=paths["raw_quantum_dft_requests"],
            upstream_url="",
            status="generated",
        )
    )

    state.source_manifest = legacy._ensure_columns(
        pd.DataFrame(state.source_manifest_rows).drop_duplicates(subset=["source_id"], keep="first"),
        legacy._source_manifest_columns(),
    )
    state.resolution_df = legacy._ensure_columns(
        pd.DataFrame(state.resolution_rows),
        ["seed_id", "r_number", "stage", "status", "detail"],
    )
    state.pending_sources = legacy._ensure_columns(
        pd.DataFrame(state.pending_rows).drop_duplicates(subset=["pending_id"], keep="first"),
        legacy._pending_source_columns(),
    )

    legacy._write_parquet(state.source_manifest, paths["bronze_source_manifest"])
    legacy._write_parquet(state.pending_sources, paths["bronze_pending_sources"])
    legacy._write_parquet(state.resolution_df, paths["bronze_seed_resolution"])
    legacy._write_parquet(state.molecule_core, paths["silver_molecule_core"])
    legacy._write_parquet(state.alias_df, paths["silver_molecule_alias"])
    legacy._write_parquet(state.property_observation, paths["silver_property_observation"])
    legacy._write_parquet(state.observation_condition_set, paths["silver_observation_condition_set"])
    legacy._write_parquet(state.cycle_case, paths["silver_cycle_case"])
    legacy._write_parquet(state.cycle_operating_point, paths["silver_cycle_operating_point"])
    legacy._write_parquet(state.quantum_job, paths["silver_quantum_job"])
    legacy._write_parquet(state.quantum_artifact, paths["silver_quantum_artifact"])
    legacy._write_parquet(state.mixture_core_table, paths["silver_mixture_core"])
    legacy._write_parquet(state.mixture_composition, paths["silver_mixture_composition"])
    legacy._write_parquet(state.regulatory_status, paths["silver_regulatory_status"])
    legacy._write_parquet(state.qc_issues, paths["silver_qc_issues"])
    legacy._write_parquet(state.property_recommended, paths["gold_property_recommended"])
    legacy._write_parquet(state.structure_features, paths["gold_structure_features"])
    legacy._write_parquet(state.molecule_master, paths["gold_molecule_master"])
    legacy._write_parquet(state.property_matrix, paths["gold_property_matrix"])
    legacy._write_parquet(state.model_dataset_index, paths["gold_model_index"])
    legacy._write_parquet(state.molecule_split_definition, paths["gold_molecule_split_definition"])
    legacy._write_parquet(state.model_ready, paths["gold_model_ready"])
    legacy._write_parquet(state.active_learning_queue, paths["gold_active_learning_queue"])
    legacy._write_parquet(state.active_learning_decision_log, paths["gold_active_learning_decision_log"])
    legacy._write_parquet(state.canonical_recommended, paths["gold_property_recommended_canonical"])
    legacy._write_parquet(state.canonical_recommended_strict, paths["gold_property_recommended_canonical_strict"])
    legacy._write_parquet(state.canonical_review_queue, paths["gold_property_recommended_canonical_review_queue"])
    legacy.write_text(paths["gold_version"], f"{state.dataset_version}\n")

    state.research_task_readiness_report, state.research_task_readiness_summary = evaluate_research_task_readiness(
        frames={
            "molecule_core": state.molecule_core,
            "property_recommended": state.property_recommended,
            "property_recommended_canonical": state.canonical_recommended,
            "property_recommended_canonical_strict": state.canonical_recommended_strict,
            "model_ready": state.model_ready,
            "seed_catalog": state.seed_catalog,
        }
    )
    legacy._write_parquet(state.research_task_readiness_report, paths["gold_research_task_readiness_report"])

    state.report = legacy._build_quality_report(
        seed_catalog=state.seed_catalog,
        molecule_core=state.molecule_core,
        property_observation=state.property_observation,
        property_recommended=state.property_recommended,
        model_ready=state.model_ready,
        qc_issues=state.qc_issues,
        resolution_df=state.resolution_df,
        regulatory_status=state.regulatory_status,
        pending_sources=state.pending_sources,
        property_observation_canonical=state.canonical_observation,
        property_recommended_canonical=state.canonical_recommended,
        property_recommended_canonical_strict=state.canonical_recommended_strict,
        property_recommended_canonical_review_queue=state.canonical_review_queue,
        property_governance_audit=state.property_governance_audit,
        condition_migration_progress=state.condition_migration_progress,
        research_task_readiness=state.research_task_readiness_summary,
        cycle_summary=state.cycle_summary,
        proxy_summary=state.proxy_feature_summary,
        quantum_summary=state.quantum_pilot_summary,
        mixture_summary=state.mixture_summary,
        active_learning_summary=state.active_learning_summary,
        dataset_version=state.dataset_version,
    )
    legacy.write_json(paths["gold_quality_report"], state.report)
    legacy._build_duckdb_index(paths)
    return StageResult(
        stage_id="09",
        status="succeeded",
        outputs=[
            state.file_artifact("stage_run_manifest", state.data_dir / "bronze" / "stage_run_manifest.parquet", kind="table"),
            state.file_artifact("observation_condition_set", paths["silver_observation_condition_set"], kind="table"),
            state.file_artifact("cycle_case", paths["silver_cycle_case"], kind="table"),
            state.file_artifact("cycle_operating_point", paths["silver_cycle_operating_point"], kind="table"),
            state.file_artifact("quantum_job", paths["silver_quantum_job"], kind="table"),
            state.file_artifact("quantum_artifact", paths["silver_quantum_artifact"], kind="table"),
            state.file_artifact("mixture_core", paths["silver_mixture_core"], kind="table"),
            state.file_artifact("mixture_composition", paths["silver_mixture_composition"], kind="table"),
            state.file_artifact("active_learning_queue", paths["gold_active_learning_queue"], kind="table"),
            state.file_artifact("active_learning_decision_log", paths["gold_active_learning_decision_log"], kind="table"),
            state.file_artifact("property_recommended_canonical", paths["gold_property_recommended_canonical"], kind="table"),
            state.file_artifact("property_recommended_canonical_strict", paths["gold_property_recommended_canonical_strict"], kind="table"),
            state.file_artifact("property_recommended_canonical_review_queue", paths["gold_property_recommended_canonical_review_queue"], kind="table"),
            state.file_artifact("molecule_split_definition", paths["gold_molecule_split_definition"], kind="table"),
            state.file_artifact("dataset_version", paths["gold_version"], kind="file"),
            state.file_artifact("research_task_readiness_report", paths["gold_research_task_readiness_report"], kind="table"),
            state.file_artifact("quality_report", paths["gold_quality_report"], kind="file"),
            state.file_artifact("duckdb_index", paths["duckdb_path"], kind="file"),
        ],
        row_count_summary={
            "source_manifest": int(len(state.source_manifest)),
            "model_ready": int(len(state.model_ready)),
            "cycle_case": int(len(state.cycle_case)),
            "cycle_operating_point": int(len(state.cycle_operating_point)),
            "quantum_job": int(len(state.quantum_job)),
            "quantum_artifact": int(len(state.quantum_artifact)),
            "mixture_core": int(len(state.mixture_core_table)),
            "mixture_composition": int(len(state.mixture_composition)),
            "active_learning_queue": int(len(state.active_learning_queue)),
            "active_learning_decision_log": int(len(state.active_learning_decision_log)),
            "molecule_split_definition": int(len(state.molecule_split_definition)),
            "property_recommended_canonical": int(len(state.canonical_recommended)),
            "property_recommended_canonical_strict": int(len(state.canonical_recommended_strict)),
            "property_recommended_canonical_review_queue": int(len(state.canonical_review_queue)),
            "research_task_readiness_report": int(len(state.research_task_readiness_report)),
        },
    )


def _write_quantum_request_outputs(
    state: BuildState,
    *,
    active_learning_queue: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    quantum_request_manifest, quantum_xyz_manifest, quantum_request_summary = build_quantum_pilot_request_manifest(
        state.molecule_core,
        active_learning_queue=active_learning_queue,
        completed_request_ids=completed_xtb_request_ids(_raw_quantum_results(state)),
        xyz_dir=state.paths["raw_quantum_pilot_xyz_manifest"].parent / "quantum_xyz",
    )
    quantum_request_manifest.to_csv(state.paths["raw_quantum_pilot_requests"], index=False)
    quantum_xyz_manifest.to_csv(state.paths["raw_quantum_pilot_xyz_manifest"], index=False)
    state.quantum_pilot_summary["request_manifest"] = quantum_request_summary
    return quantum_request_manifest, quantum_xyz_manifest


def _write_psi4_dft_request_outputs(
    state: BuildState,
    *,
    active_learning_queue: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_results = (
        pd.read_csv(state.paths["raw_quantum_pilot_results"]).fillna("")
        if state.paths["raw_quantum_pilot_results"].exists()
        else pd.DataFrame()
    )
    dft_request_manifest, dft_xyz_manifest, dft_request_summary = build_psi4_dft_request_manifest(
        state.molecule_core,
        xtb_results=raw_results,
        active_learning_queue=active_learning_queue,
        completed_request_ids=completed_psi4_request_ids(raw_results),
        xyz_dir=state.paths["raw_quantum_dft_xyz_manifest"].parent / "quantum_dft_xyz",
    )
    dft_request_manifest.to_csv(state.paths["raw_quantum_dft_requests"], index=False)
    dft_xyz_manifest.to_csv(state.paths["raw_quantum_dft_xyz_manifest"], index=False)
    state.quantum_pilot_summary["dft_request_manifest"] = dft_request_summary
    return dft_request_manifest, dft_xyz_manifest


def _raw_quantum_results(state: BuildState) -> pd.DataFrame:
    path = state.paths["raw_quantum_pilot_results"]
    return pd.read_csv(path).fillna("") if path.exists() else pd.DataFrame()


def _acquire_nist_for_seed(state: BuildState, seed: dict[str, Any], seed_id: str, nist_source_id: str, r_number: str) -> None:
    nist_query = legacy._clean_str(seed.get("nist_query"))
    if not nist_query:
        state.resolution_rows.append(
            {"seed_id": seed_id, "r_number": r_number, "stage": "nist", "status": "skipped", "detail": "No NIST query configured"}
        )
        return
    try:
        nist_path = state.paths["raw_nist"] / f"{legacy.slugify(seed_id)}.html"
        fallback_nist_url = state.nist.snapshot_url(
            legacy._clean_str(seed.get("nist_query")),
            legacy._clean_str(seed.get("nist_query_type")) or "name",
        )
        nist_snapshot = legacy._load_or_fetch_text_payload(
            nist_path,
            state.refresh_remote,
            lambda seed=seed: state.nist.fetch_snapshot(
                legacy._clean_str(seed.get("nist_query")),
                legacy._clean_str(seed.get("nist_query_type")) or "name",
            ),
            fallback_url=fallback_nist_url,
        )
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=nist_source_id,
                source_type="public_web_snapshot",
                source_name="NIST Chemistry WebBook",
                license_name="NIST public web snapshot",
                local_path=nist_path,
                upstream_url=nist_snapshot["url"],
                status=nist_snapshot["source_status"],
            )
        )
        if nist_snapshot["status"] == "ok":
            mol_id = state.seed_to_mol_id.get(seed_id, "")
            parsed = state.nist_parser.parse(nist_snapshot["html"])
            resolution_status = "cached_fallback" if nist_snapshot["source_status"] == "cached_fallback" else ("resolved" if parsed else "warning")
            detail = f"{len(parsed)} parsed observations"
            if nist_snapshot["source_status"] == "cached_fallback":
                detail = f"{detail}; {legacy._cached_fallback_detail(nist_snapshot)}"
            state.property_rows.extend(
                legacy._wrap_external_property_rows(
                    mol_id=mol_id,
                    source_id=nist_source_id,
                    source_type="public_web_snapshot",
                    source_name="NIST Chemistry WebBook",
                    quality_level="primary_public_reference",
                    rows=parsed,
                )
            )
            state.resolution_rows.append({"seed_id": seed_id, "r_number": r_number, "stage": "nist", "status": resolution_status, "detail": detail})
    except Exception as exc:  # noqa: BLE001
        error_path = state.paths["raw_nist"] / f"{legacy.slugify(seed_id)}.error.txt"
        legacy.write_text(error_path, str(exc))
        state.source_manifest_rows.append(
            legacy._source_manifest_entry(
                source_id=nist_source_id,
                source_type="public_web_snapshot",
                source_name="NIST Chemistry WebBook",
                license_name="NIST public web snapshot",
                local_path=error_path,
                upstream_url="",
                status="failed",
            )
        )
        state.resolution_rows.append({"seed_id": seed_id, "r_number": r_number, "stage": "nist", "status": "failed", "detail": str(exc)})


def _acquire_coolprop_for_seed(state: BuildState, seed: dict[str, Any], seed_id: str, r_number: str, coolprop_source_id: str) -> None:
    mol_id = state.seed_to_mol_id.get(seed_id, "")
    coolprop_fluid = legacy._resolve_coolprop_fluid(seed, state.coolprop_aliases)
    try:
        if coolprop_fluid:
            state.property_rows.extend(state.coolprop.generate_observations(mol_id, coolprop_fluid, coolprop_source_id))
            state.resolution_rows.append(
                {"seed_id": seed_id, "r_number": r_number, "stage": "coolprop", "status": "resolved", "detail": coolprop_fluid}
            )
        else:
            state.resolution_rows.append(
                {"seed_id": seed_id, "r_number": r_number, "stage": "coolprop", "status": "skipped", "detail": "No explicit CoolProp mapping"}
            )
    except legacy.UnsupportedCoolPropFluidError as exc:
        state.resolution_rows.append({"seed_id": seed_id, "r_number": r_number, "stage": "coolprop", "status": "warning", "detail": str(exc)})
    except Exception as exc:  # noqa: BLE001
        state.resolution_rows.append({"seed_id": seed_id, "r_number": r_number, "stage": "coolprop", "status": "failed", "detail": str(exc)})


def _remap_data_path(path: Path, data_dir: Path) -> Path:
    try:
        return data_dir / path.relative_to(DATA_DIR)
    except ValueError:
        return path


def _infer_data_dir_from_paths(paths: dict[str, Path]) -> Path | None:
    """Infer the active data root from monkeypatched legacy path maps.

    Tests often monkeypatch ``legacy._paths()`` instead of passing an explicit
    ``data_dir`` to the staged orchestrator.  Stage manifest writes use
    ``state.data_dir``, so infer the root before Stage 00 returns and the
    orchestrator persists its first manifest row.
    """

    path = paths.get("seed_catalog")
    if path is not None and path.name == "seed_catalog.csv" and path.parent.name == "manual" and path.parent.parent.name == "raw":
        return path.parent.parent.parent

    for key in ("bronze_source_manifest", "silver_molecule_core", "gold_model_ready"):
        path = paths.get(key)
        if path is not None and path.parent.name in {"bronze", "silver", "gold"}:
            return path.parent.parent

    path = paths.get("raw_generated_pubchem_tierd_candidates")
    if path is not None and path.parent.name == "generated" and path.parent.parent.name == "raw":
        return path.parent.parent.parent

    return None


def _ensure_prc_paths(paths: dict[str, Path]) -> None:
    raw_generated_base = (
        paths["raw_generated_pubchem_tierd_candidates"].parent
        if "raw_generated_pubchem_tierd_candidates" in paths
        else DATA_DIR / "raw" / "generated"
    )
    raw_manual_base = paths["seed_catalog"].parent if "seed_catalog" in paths else DATA_DIR / "raw" / "manual"
    gold_base = paths["gold_model_ready"].parent if "gold_model_ready" in paths else DATA_DIR / "gold"

    if "silver_observation_condition_set" not in paths and "silver_property_observation" in paths:
        paths["silver_observation_condition_set"] = paths["silver_property_observation"].with_name("observation_condition_set.parquet")
    if "silver_cycle_case" not in paths and "silver_property_observation" in paths:
        paths["silver_cycle_case"] = paths["silver_property_observation"].with_name("cycle_case.parquet")
    if "silver_cycle_operating_point" not in paths and "silver_property_observation" in paths:
        paths["silver_cycle_operating_point"] = paths["silver_property_observation"].with_name("cycle_operating_point.parquet")
    if "silver_quantum_job" not in paths and "silver_property_observation" in paths:
        paths["silver_quantum_job"] = paths["silver_property_observation"].with_name("quantum_job.parquet")
    if "silver_quantum_artifact" not in paths and "silver_property_observation" in paths:
        paths["silver_quantum_artifact"] = paths["silver_property_observation"].with_name("quantum_artifact.parquet")
    if "silver_mixture_core" not in paths and "silver_property_observation" in paths:
        paths["silver_mixture_core"] = paths["silver_property_observation"].with_name("mixture_core.parquet")
    if "silver_mixture_composition" not in paths and "silver_property_observation" in paths:
        paths["silver_mixture_composition"] = paths["silver_property_observation"].with_name("mixture_composition.parquet")
    if "gold_research_task_readiness_report" not in paths and "gold_model_ready" in paths:
        paths["gold_research_task_readiness_report"] = paths["gold_model_ready"].with_name("research_task_readiness_report.parquet")
    if "gold_active_learning_queue" not in paths and "gold_model_ready" in paths:
        paths["gold_active_learning_queue"] = paths["gold_model_ready"].with_name("active_learning_queue.parquet")
    if "gold_active_learning_decision_log" not in paths and "gold_model_ready" in paths:
        paths["gold_active_learning_decision_log"] = paths["gold_model_ready"].with_name("active_learning_decision_log.parquet")
    if "gold_molecule_split_definition" not in paths and "gold_model_ready" in paths:
        paths["gold_molecule_split_definition"] = paths["gold_model_ready"].with_name("molecule_split_definition.parquet")
    if "gold_version" not in paths and "gold_model_ready" in paths:
        paths["gold_version"] = paths["gold_model_ready"].with_name("VERSION")
    if "raw_proxy_feature_metadata" not in paths and "raw_generated_pubchem_tierd_candidates" in paths:
        paths["raw_proxy_feature_metadata"] = paths["raw_generated_pubchem_tierd_candidates"].with_name(
            "proxy_feature_heuristics_metadata.json"
        )
    if "raw_quantum_pilot_requests" not in paths:
        paths["raw_quantum_pilot_requests"] = raw_generated_base / "quantum_pilot_requests.csv"
    if "raw_quantum_pilot_xyz_manifest" not in paths:
        paths["raw_quantum_pilot_xyz_manifest"] = raw_generated_base / "quantum_pilot_xyz_manifest.csv"
    if "raw_quantum_dft_requests" not in paths:
        paths["raw_quantum_dft_requests"] = raw_generated_base / "quantum_dft_requests.csv"
    if "raw_quantum_dft_xyz_manifest" not in paths:
        paths["raw_quantum_dft_xyz_manifest"] = raw_generated_base / "quantum_dft_xyz_manifest.csv"
    if "raw_generated_active_learning_queue" not in paths:
        paths["raw_generated_active_learning_queue"] = raw_generated_base / "active_learning_queue.csv"
    if "raw_promoted_coverage_matrix" not in paths:
        paths["raw_promoted_coverage_matrix"] = raw_generated_base / "promoted_coverage_matrix.csv"
    if "raw_promoted_enrichment_worklist" not in paths:
        paths["raw_promoted_enrichment_worklist"] = raw_generated_base / "promoted_enrichment_worklist.csv"
    if "raw_quantum_pilot_results" not in paths:
        paths["raw_quantum_pilot_results"] = raw_manual_base / "quantum_pilot_results.csv"
    if "raw_cycle_backend_results" not in paths:
        paths["raw_cycle_backend_results"] = raw_manual_base / "cycle_backend_results.csv"
    if "raw_active_learning_queue" not in paths:
        paths["raw_active_learning_queue"] = raw_manual_base / "active_learning_queue.csv"
    if "raw_active_learning_decision_log" not in paths:
        paths["raw_active_learning_decision_log"] = raw_manual_base / "active_learning_decision_log.csv"
    if "raw_mixture_component_curations" not in paths:
        paths["raw_mixture_component_curations"] = raw_manual_base / "mixture_component_curations.csv"
    if "raw_mixture_fraction_curations" not in paths:
        paths["raw_mixture_fraction_curations"] = raw_manual_base / "mixture_fraction_curations.csv"
    if "raw_review_only_inequality_observations" not in paths:
        paths["raw_review_only_inequality_observations"] = (
            raw_manual_base / "review_only" / "review_only_inequality_observations_round2_20260501.csv"
        )
    if "gold_property_recommended_canonical" not in paths and "gold_model_ready" in paths:
        paths["gold_property_recommended_canonical"] = gold_base / "property_recommended_canonical.parquet"
    if "gold_property_recommended_canonical_strict" not in paths:
        paths["gold_property_recommended_canonical_strict"] = gold_base / "property_recommended_canonical_strict.parquet"
    if "gold_property_recommended_canonical_review_queue" not in paths:
        paths["gold_property_recommended_canonical_review_queue"] = gold_base / "property_recommended_canonical_review_queue.parquet"


STAGES: tuple[StageSpec, ...] = (
    StageSpec("00", "init_run", 0, stage00_init_run, produced_outputs=("paths", "clients")),
    StageSpec("01", "load_inventory", 1, stage01_load_inventory, required_inputs=("paths", "clients"), produced_outputs=("seed_catalog", "manual_observations", "row_buffers")),
    StageSpec("02", "resolve_identity_boundary", 2, stage02_resolve_identity_boundary, required_inputs=("seed_catalog",), produced_outputs=("identity_resolution_pending",)),
    StageSpec("03", "acquire_global_sources", 3, stage03_acquire_global_sources, required_inputs=("paths", "clients"), produced_outputs=("global_sources",)),
    StageSpec("04", "acquire_entity_sources", 4, stage04_acquire_entity_sources, required_inputs=("seed_catalog", "clients", "identity_resolution_pending"), produced_outputs=("molecule_core_pre_governance", "molecule_alias_pre_governance", "entity_source_rows")),
    StageSpec("06", "integrate_governance_bundle", 5, stage06_integrate_governance_bundle, required_inputs=("molecule_core_pre_governance", "molecule_alias_pre_governance"), produced_outputs=("molecule_core", "molecule_alias", "mixture_core", "mixture_composition", "governance_bundle")),
    StageSpec("05", "harmonize_observations", 6, stage05_harmonize_observations, required_inputs=("global_sources", "molecule_core", "molecule_alias", "governance_bundle"), produced_outputs=("property_observation", "observation_condition_set", "cycle_case", "cycle_operating_point", "proxy_feature_observation", "quantum_pilot_observation", "quantum_job", "quantum_artifact", "regulatory_status", "qc_issues")),
    StageSpec("07", "build_feature_and_recommendation_layers", 7, stage07_build_feature_and_recommendation_layers, required_inputs=("property_observation", "molecule_core", "molecule_alias"), produced_outputs=("property_recommended", "structure_features", "property_matrix", "molecule_master")),
    StageSpec("08", "build_model_outputs", 8, stage08_build_model_outputs, required_inputs=("molecule_master", "property_matrix"), produced_outputs=("model_dataset_index", "molecule_split_definition", "model_ready")),
    StageSpec("09", "validate_and_publish", 9, stage09_validate_and_publish, required_inputs=("model_ready",), produced_outputs=("stage_run_manifest", "mixture_core", "mixture_composition", "active_learning_queue", "active_learning_decision_log", "molecule_split_definition", "dataset_version", "quantum_job", "quantum_artifact", "quality_report", "duckdb_index")),
)
