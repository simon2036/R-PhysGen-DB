"""End-to-end dataset build pipeline for R-PhysGen-DB Wave 2."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from r_physgen_db.chemistry import compute_structure_features, standardize_smiles
from r_physgen_db.constants import (
    CATEGORICAL_PROPERTIES,
    DATA_DIR,
    DUCKDB_TABLES,
    EPA_TECHNOLOGY_TRANSITIONS_GWP_URL,
    GWP_PREFERENCE_ORDER,
    MODEL_TARGET_PROPERTIES,
    NUMERIC_PROPERTIES,
    PARSER_VERSION,
    PROJECT_ROOT,
    QUALITY_SCORES,
    SNAP_SOURCE_PAGES,
    SOURCE_PRIORITY,
)
from r_physgen_db.sources.comptox_client import CompToxClient
from r_physgen_db.sources.coolprop_source import CoolPropSource, UnsupportedCoolPropFluidError
from r_physgen_db.sources.epa_gwp_reference_parser import EPATechnologyTransitionsGWPParser
from r_physgen_db.sources.epa_ods_parser import EPAODSParser
from r_physgen_db.sources.epa_snap_parser import EPASNAPParser
from r_physgen_db.sources.http_utils import build_retry_session, is_transient_request_exception
from r_physgen_db.sources.nist_thermo_parser import NISTThermoParser
from r_physgen_db.sources.nist_webbook import NISTWebBookClient
from r_physgen_db.sources.property_governance_bundle import default_bundle_path, integrate_property_governance_bundle
from r_physgen_db.sources.pubchem import PubChemClient
from r_physgen_db.utils import ensure_directory, load_yaml, now_iso, sha256_file, slugify, write_json, write_text

_HTTP_SESSION = build_retry_session()


def build_dataset(refresh_remote: bool = False) -> dict[str, Any]:
    paths = _paths()
    for path in paths.values():
        ensure_directory(path.parent if path.suffix else path)

    seed_catalog = pd.read_csv(paths["seed_catalog"]).fillna("")
    manual_observations = _load_manual_observations(paths)
    coolprop_aliases = load_yaml(paths["coolprop_aliases"]).get("mappings", {})
    bulk_pubchem_lookup = _load_bulk_pubchem_candidate_lookup(paths)

    source_manifest_rows: list[dict[str, Any]] = []
    resolution_rows: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    molecule_rows: dict[str, dict[str, Any]] = {}
    seed_to_mol_id: dict[str, str] = {}
    alias_rows: list[dict[str, Any]] = []
    property_rows: list[dict[str, Any]] = []
    regulatory_rows: list[dict[str, Any]] = []

    source_manifest_rows.extend(_register_manual_sources(paths))

    pubchem = PubChemClient()
    nist = NISTWebBookClient()
    nist_parser = NISTThermoParser()
    coolprop = CoolPropSource()
    epa_gwp_reference_parser = EPATechnologyTransitionsGWPParser()
    epa_ods_parser = EPAODSParser()
    epa_snap_parser = EPASNAPParser()
    comptox = CompToxClient()

    coolprop_meta_path = paths["raw_coolprop_meta"]
    write_json(coolprop_meta_path, coolprop.session_metadata())
    coolprop_source_id = "source_coolprop_session"
    source_manifest_rows.append(
        _source_manifest_entry(
            source_id=coolprop_source_id,
            source_type="calculated_open_source",
            source_name=f"CoolProp {coolprop.version}",
            license_name="CoolProp open-source",
            local_path=coolprop_meta_path,
            upstream_url="https://coolprop.org/",
            status="generated",
        )
    )

    global_sources = _fetch_global_sources(
        paths=paths,
        refresh_remote=refresh_remote,
        source_manifest_rows=source_manifest_rows,
        resolution_rows=resolution_rows,
        epa_gwp_reference_parser=epa_gwp_reference_parser,
        epa_ods_parser=epa_ods_parser,
        epa_snap_parser=epa_snap_parser,
    )

    for seed in seed_catalog.to_dict(orient="records"):
        seed_id = str(seed["seed_id"])
        pubchem_source_id = f"source_pubchem_{slugify(seed_id)}"
        nist_source_id = f"source_nist_{slugify(seed_id)}"
        r_number = _clean_str(seed.get("r_number"))

        try:
            pubchem_snapshot = _resolve_pubchem_snapshot(
                pubchem=pubchem,
                seed=seed,
                paths=paths,
                refresh_remote=refresh_remote,
                bulk_pubchem_lookup=bulk_pubchem_lookup,
            )
            pubchem_payload = pubchem_snapshot["payload"]
            source_manifest_rows.append(
                _source_manifest_entry(
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
            standardized = standardize_smiles(pubchem_record["isomeric_smiles"])
            mol_id = f"mol_{standardized['inchikey'].lower()}"
            seed_to_mol_id[seed_id] = mol_id

            candidate_row = {
                "mol_id": mol_id,
                "seed_id": seed_id,
                "family": _clean_str(seed.get("family")),
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
                "pubchem_query": _clean_str(seed.get("query_name")),
                "entity_scope": _clean_str(seed.get("entity_scope")) or "candidate",
                "model_inclusion": _clean_str(seed.get("model_inclusion")) or "yes",
                "coverage_tier": _clean_str(seed.get("coverage_tier")),
                "status": "resolved",
            }
            existing_row = molecule_rows.get(mol_id)
            if existing_row is None or _prefer_seed_catalog_entry(candidate_row, existing_row):
                molecule_rows[mol_id] = candidate_row

            _append_alias(alias_rows, mol_id, "seed_id", seed_id, True, "seed_catalog")
            _append_alias(alias_rows, mol_id, "r_number", r_number, True, "seed_catalog")
            _append_alias(alias_rows, mol_id, "query_name", _clean_str(seed.get("query_name")), True, "seed_catalog")
            _append_alias(alias_rows, mol_id, "pubchem_cid", pubchem_record["cid"], True, "PubChem")
            _append_alias(alias_rows, mol_id, "coolprop_fluid", _clean_str(seed.get("coolprop_fluid")), False, "seed_catalog")

            alias_bundle = pubchem.extract_aliases(synonyms_record["synonyms"])
            for cas in alias_bundle["cas_numbers"]:
                _append_alias(alias_rows, mol_id, "cas", cas, False, "PubChem")
            for alias_r_number in alias_bundle["r_numbers"]:
                _append_alias(alias_rows, mol_id, "r_number", alias_r_number, False, "PubChem")
            for name in alias_bundle["common_names"]:
                _append_alias(alias_rows, mol_id, "synonym", name, False, "PubChem")
            _append_family_prefixed_aliases(alias_rows, mol_id, r_number, _clean_str(seed.get("family")))

            pubchem_resolution = {
                "seed_id": seed_id,
                "r_number": r_number,
                "stage": "pubchem",
                "status": "resolved",
                "detail": mol_id,
            }
            if pubchem_snapshot["source_status"] == "cached_fallback":
                pubchem_resolution["status"] = "cached_fallback"
                pubchem_resolution["detail"] = f"{mol_id}; {_cached_fallback_detail(pubchem_snapshot)}"
            resolution_rows.append(pubchem_resolution)

            if _clean_str(seed.get("regulatory_priority")) in {"high", "medium"} and not comptox.enabled:
                pending_rows.append(
                    {
                        "pending_id": f"pending_{slugify(seed_id)}_comptox",
                        **comptox.pending_record(seed_id=seed_id, r_number=r_number, mol_id=mol_id),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            error_path = paths["raw_pubchem"] / f"{slugify(seed_id)}.error.json"
            write_json(error_path, {"seed": seed, "error": str(exc)})
            source_manifest_rows.append(
                _source_manifest_entry(
                    source_id=pubchem_source_id,
                    source_type="public_database",
                    source_name="PubChem PUG REST",
                    license_name="NCBI / PubChem public",
                    local_path=error_path,
                    upstream_url="",
                    status="failed",
                )
            )
            resolution_rows.append(
                {
                    "seed_id": seed_id,
                    "r_number": r_number,
                    "stage": "pubchem",
                    "status": "failed",
                    "detail": str(exc),
                }
            )
            continue

        nist_query = _clean_str(seed.get("nist_query"))
        if not nist_query:
            resolution_rows.append(
                {
                    "seed_id": seed_id,
                    "r_number": r_number,
                    "stage": "nist",
                    "status": "skipped",
                    "detail": "No NIST query configured",
                }
            )
        else:
            try:
                nist_path = paths["raw_nist"] / f"{slugify(seed_id)}.html"
                fallback_nist_url = nist.snapshot_url(_clean_str(seed.get("nist_query")), _clean_str(seed.get("nist_query_type")) or "name")
                nist_snapshot = _load_or_fetch_text_payload(
                    nist_path,
                    refresh_remote,
                    lambda seed=seed: nist.fetch_snapshot(_clean_str(seed.get("nist_query")), _clean_str(seed.get("nist_query_type")) or "name"),
                    fallback_url=fallback_nist_url,
                )
                source_manifest_rows.append(
                    _source_manifest_entry(
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
                    mol_id = seed_to_mol_id.get(seed_id, "")
                    parsed = nist_parser.parse(nist_snapshot["html"])
                    resolution_status = "cached_fallback" if nist_snapshot["source_status"] == "cached_fallback" else ("resolved" if parsed else "warning")
                    detail = f"{len(parsed)} parsed observations"
                    if nist_snapshot["source_status"] == "cached_fallback":
                        detail = f"{detail}; {_cached_fallback_detail(nist_snapshot)}"
                    property_rows.extend(
                        _wrap_external_property_rows(
                            mol_id=mol_id,
                            source_id=nist_source_id,
                            source_type="public_web_snapshot",
                            source_name="NIST Chemistry WebBook",
                            quality_level="primary_public_reference",
                            rows=parsed,
                        )
                    )
                    resolution_rows.append(
                        {
                            "seed_id": seed_id,
                            "r_number": r_number,
                            "stage": "nist",
                            "status": resolution_status,
                            "detail": detail,
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                error_path = paths["raw_nist"] / f"{slugify(seed_id)}.error.txt"
                write_text(error_path, str(exc))
                source_manifest_rows.append(
                    _source_manifest_entry(
                        source_id=nist_source_id,
                        source_type="public_web_snapshot",
                        source_name="NIST Chemistry WebBook",
                        license_name="NIST public web snapshot",
                        local_path=error_path,
                        upstream_url="",
                        status="failed",
                    )
                )
                resolution_rows.append(
                    {
                        "seed_id": seed_id,
                        "r_number": r_number,
                        "stage": "nist",
                        "status": "failed",
                        "detail": str(exc),
                    }
                )

        mol_id = seed_to_mol_id.get(seed_id, "")
        coolprop_fluid = _resolve_coolprop_fluid(seed, coolprop_aliases)
        try:
            if coolprop_fluid:
                property_rows.extend(coolprop.generate_observations(mol_id, coolprop_fluid, coolprop_source_id))
                resolution_rows.append(
                    {
                        "seed_id": seed_id,
                        "r_number": r_number,
                        "stage": "coolprop",
                        "status": "resolved",
                        "detail": coolprop_fluid,
                    }
                )
            else:
                resolution_rows.append(
                    {
                        "seed_id": seed_id,
                        "r_number": r_number,
                        "stage": "coolprop",
                        "status": "skipped",
                        "detail": "No explicit CoolProp mapping",
                    }
                )
        except UnsupportedCoolPropFluidError as exc:
            resolution_rows.append(
                {
                    "seed_id": seed_id,
                    "r_number": r_number,
                    "stage": "coolprop",
                    "status": "warning",
                    "detail": str(exc),
                }
            )
        except Exception as exc:  # noqa: BLE001
            resolution_rows.append(
                {
                    "seed_id": seed_id,
                    "r_number": r_number,
                    "stage": "coolprop",
                    "status": "failed",
                    "detail": str(exc),
                }
            )

    molecule_core = _ensure_columns(
        pd.DataFrame(sorted(molecule_rows.values(), key=lambda item: item["mol_id"])),
        _molecule_core_columns(),
    )
    alias_df = _ensure_columns(pd.DataFrame(alias_rows).drop_duplicates(), _molecule_alias_columns())

    bundle_integration = integrate_property_governance_bundle(
        bundle_path=_property_governance_bundle_path(paths),
        output_root=_output_root_from_paths(paths),
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        alias_df=alias_df,
        parser_version=PARSER_VERSION,
        retrieved_at=now_iso(),
    )
    if bundle_integration["bundle_present"]:
        generated_seed_rows = bundle_integration["generated_seed_rows"]
        if isinstance(generated_seed_rows, pd.DataFrame) and not generated_seed_rows.empty:
            seed_catalog = (
                pd.concat([seed_catalog, generated_seed_rows], ignore_index=True)
                .drop_duplicates(subset=["seed_id"], keep="first")
                .reset_index(drop=True)
            )
        for row in bundle_integration["generated_molecule_rows"]:
            existing_row = molecule_rows.get(row["mol_id"])
            if existing_row is None or _prefer_seed_catalog_entry(row, existing_row):
                molecule_rows[row["mol_id"]] = row
            seed_to_mol_id[row["seed_id"]] = row["mol_id"]
        alias_rows.extend(bundle_integration["generated_alias_rows"])
        property_rows.extend(bundle_integration["legacy_property_rows"])
        source_manifest_rows.extend(bundle_integration["source_manifest_rows"])
        resolution_rows.extend(bundle_integration["resolution_rows"])

        molecule_core = _ensure_columns(
            pd.DataFrame(sorted(molecule_rows.values(), key=lambda item: item["mol_id"])),
            _molecule_core_columns(),
        )
        alias_df = _ensure_columns(pd.DataFrame(alias_rows).drop_duplicates(), _molecule_alias_columns())

    canonical_observation = bundle_integration.get("canonical_observation", pd.DataFrame())
    canonical_recommended = bundle_integration.get("canonical_recommended", pd.DataFrame())
    canonical_recommended_strict = bundle_integration.get("canonical_recommended_strict", pd.DataFrame())
    canonical_review_queue = bundle_integration.get("canonical_review_queue", pd.DataFrame())
    property_governance_audit = bundle_integration.get("audit", {})

    molecule_context = _build_molecule_source_context(molecule_core, seed_catalog)
    alias_lookup = _build_alias_lookup(alias_df)
    property_rows.extend(_manual_property_rows(manual_observations, seed_to_mol_id, alias_lookup))

    property_rows.extend(
        _epa_gwp_reference_property_rows(
            gwp_df=global_sources["epa_gwp_reference_df"],
            alias_lookup=alias_lookup,
            molecule_context=molecule_context,
            source_id=global_sources["epa_gwp_reference_source_id"],
        )
    )

    property_rows.extend(
        _epa_ods_property_rows(
            ods_df=global_sources["epa_ods_df"],
            alias_lookup=alias_lookup,
            source_id=global_sources["epa_ods_source_id"],
        )
    )
    snap_property_rows, snap_regulatory_rows = _epa_snap_rows(
        snap_frames=global_sources["epa_snap_frames"],
        alias_lookup=alias_lookup,
        molecule_context=molecule_context,
        source_type="public_web_snapshot",
        source_name_prefix="EPA SNAP",
    )
    property_rows.extend(snap_property_rows)
    regulatory_rows.extend(snap_regulatory_rows)

    property_observation = _ensure_columns(pd.DataFrame(property_rows), _property_observation_columns())
    property_observation = _assign_observation_ids(property_observation)
    property_observation, qc_issues = _apply_qc(property_observation)

    regulatory_status = _ensure_columns(pd.DataFrame(regulatory_rows).drop_duplicates(), _regulatory_status_columns())
    property_recommended = _select_recommended(property_observation)
    structure_features = _build_structure_features(molecule_core)
    property_matrix = _build_property_matrix(property_recommended)
    model_dataset_index = _build_model_dataset_index(structure_features, property_recommended, molecule_core)
    molecule_master = _build_molecule_master(molecule_core, alias_df, structure_features)
    model_ready = _build_model_ready(molecule_master, property_matrix, model_dataset_index)

    source_manifest = _ensure_columns(pd.DataFrame(source_manifest_rows).drop_duplicates(subset=["source_id"], keep="first"), _source_manifest_columns())
    resolution_df = _ensure_columns(pd.DataFrame(resolution_rows), ["seed_id", "r_number", "stage", "status", "detail"])
    pending_sources = _ensure_columns(pd.DataFrame(pending_rows).drop_duplicates(subset=["pending_id"], keep="first"), _pending_source_columns())

    _write_parquet(source_manifest, paths["bronze_source_manifest"])
    _write_parquet(pending_sources, paths["bronze_pending_sources"])
    _write_parquet(resolution_df, paths["bronze_seed_resolution"])
    _write_parquet(molecule_core, paths["silver_molecule_core"])
    _write_parquet(alias_df, paths["silver_molecule_alias"])
    _write_parquet(property_observation, paths["silver_property_observation"])
    _write_parquet(regulatory_status, paths["silver_regulatory_status"])
    _write_parquet(qc_issues, paths["silver_qc_issues"])
    _write_parquet(property_recommended, paths["gold_property_recommended"])
    _write_parquet(structure_features, paths["gold_structure_features"])
    _write_parquet(molecule_master, paths["gold_molecule_master"])
    _write_parquet(property_matrix, paths["gold_property_matrix"])
    _write_parquet(model_dataset_index, paths["gold_model_index"])
    _write_parquet(model_ready, paths["gold_model_ready"])

    report = _build_quality_report(
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        property_observation=property_observation,
        property_recommended=property_recommended,
        model_ready=model_ready,
        qc_issues=qc_issues,
        resolution_df=resolution_df,
        regulatory_status=regulatory_status,
        pending_sources=pending_sources,
        property_observation_canonical=canonical_observation,
        property_recommended_canonical=canonical_recommended,
        property_recommended_canonical_strict=canonical_recommended_strict,
        property_recommended_canonical_review_queue=canonical_review_queue,
        property_governance_audit=property_governance_audit,
    )
    write_json(paths["gold_quality_report"], report)
    _build_duckdb_index(paths)
    return report


def _paths() -> dict[str, Path]:
    return {
        "raw_root": DATA_DIR / "raw",
        "raw_pubchem": DATA_DIR / "raw" / "pubchem",
        "raw_nist": DATA_DIR / "raw" / "nist_webbook",
        "raw_epa": DATA_DIR / "raw" / "epa",
        "raw_coolprop_meta": DATA_DIR / "raw" / "coolprop" / "session_metadata.json",
        "seed_catalog": DATA_DIR / "raw" / "manual" / "seed_catalog.csv",
        "property_governance_bundle": default_bundle_path(PROJECT_ROOT),
        "refrigerant_inventory": DATA_DIR / "raw" / "manual" / "refrigerant_inventory.csv",
        "manual_observations": DATA_DIR / "raw" / "manual" / "manual_property_observations.csv",
        "manual_observations_dir": DATA_DIR / "raw" / "manual" / "observations",
        "coolprop_aliases": DATA_DIR / "raw" / "manual" / "coolprop_aliases.yaml",
        "raw_generated_pubchem_tierd_candidates": DATA_DIR / "raw" / "generated" / "pubchem_tierd_candidates.csv",
        "bronze_source_manifest": DATA_DIR / "bronze" / "source_manifest.parquet",
        "bronze_pending_sources": DATA_DIR / "bronze" / "pending_sources.parquet",
        "bronze_seed_resolution": DATA_DIR / "bronze" / "seed_resolution.parquet",
        "bronze_pubchem_candidate_pool": DATA_DIR / "bronze" / "pubchem_candidate_pool.parquet",
        "bronze_pubchem_candidate_filter_audit": DATA_DIR / "bronze" / "pubchem_candidate_filter_audit.parquet",
        "silver_molecule_core": DATA_DIR / "silver" / "molecule_core.parquet",
        "silver_molecule_alias": DATA_DIR / "silver" / "molecule_alias.parquet",
        "silver_property_observation": DATA_DIR / "silver" / "property_observation.parquet",
        "silver_regulatory_status": DATA_DIR / "silver" / "regulatory_status.parquet",
        "silver_qc_issues": DATA_DIR / "silver" / "qc_issues.parquet",
        "gold_property_recommended": DATA_DIR / "gold" / "property_recommended.parquet",
        "gold_structure_features": DATA_DIR / "gold" / "structure_features.parquet",
        "gold_molecule_master": DATA_DIR / "gold" / "molecule_master.parquet",
        "gold_property_matrix": DATA_DIR / "gold" / "property_matrix.parquet",
        "gold_model_index": DATA_DIR / "gold" / "model_dataset_index.parquet",
        "gold_model_ready": DATA_DIR / "gold" / "model_ready.parquet",
        "gold_property_recommended_canonical_strict": DATA_DIR / "gold" / "property_recommended_canonical_strict.parquet",
        "gold_property_recommended_canonical_review_queue": DATA_DIR / "gold" / "property_recommended_canonical_review_queue.parquet",
        "gold_quality_report": DATA_DIR / "gold" / "quality_report.json",
        "duckdb_path": DATA_DIR / "index" / "r_physgen_v2.duckdb",
    }


def _property_governance_bundle_path(paths: dict[str, Path]) -> Path:
    bundle_path = paths.get("property_governance_bundle")
    if isinstance(bundle_path, Path):
        return bundle_path
    raw_root = paths.get("raw_root", DATA_DIR / "raw")
    return raw_root / "__missing_property_governance_bundle__.zip"


def _output_root_from_paths(paths: dict[str, Path]) -> Path:
    raw_root = paths.get("raw_root")
    if isinstance(raw_root, Path):
        try:
            return raw_root.parents[1]
        except IndexError:
            pass
    return PROJECT_ROOT


def _fetch_global_sources(
    *,
    paths: dict[str, Path],
    refresh_remote: bool,
    source_manifest_rows: list[dict[str, Any]],
    resolution_rows: list[dict[str, Any]],
    epa_gwp_reference_parser: EPATechnologyTransitionsGWPParser,
    epa_ods_parser: EPAODSParser,
    epa_snap_parser: EPASNAPParser,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "epa_gwp_reference_df": pd.DataFrame(columns=EPATechnologyTransitionsGWPParser.columns()),
        "epa_gwp_reference_source_id": "source_epa_technology_transitions_gwp",
        "epa_ods_df": pd.DataFrame(columns=EPAODSParser.columns()),
        "epa_ods_source_id": "source_epa_ods",
        "epa_snap_frames": [],
    }

    gwp_source_id = "source_epa_technology_transitions_gwp"
    gwp_path = paths["raw_epa"] / "technology_transitions_gwp_reference.html"
    try:
        gwp_snapshot = _load_or_fetch_text(
            gwp_path,
            refresh_remote,
            lambda: _fetch_url_text(EPA_TECHNOLOGY_TRANSITIONS_GWP_URL),
            upstream_url=EPA_TECHNOLOGY_TRANSITIONS_GWP_URL,
        )
        source_manifest_rows.append(
            _source_manifest_entry(
                source_id=gwp_source_id,
                source_type="public_web_snapshot",
                source_name="EPA Technology Transitions GWP Reference Table",
                license_name="EPA public web snapshot",
                local_path=gwp_path,
                upstream_url=EPA_TECHNOLOGY_TRANSITIONS_GWP_URL,
                status=gwp_snapshot["source_status"],
            )
        )
        result["epa_gwp_reference_df"] = epa_gwp_reference_parser.parse(gwp_snapshot["text"])
        if gwp_snapshot["source_status"] == "cached_fallback":
            resolution_rows.append(
                {
                    "seed_id": "",
                    "r_number": "",
                    "stage": "epa_technology_transitions_gwp",
                    "status": "cached_fallback",
                    "detail": _cached_fallback_detail(gwp_snapshot),
                }
            )
    except Exception as exc:  # noqa: BLE001
        error_path = paths["raw_epa"] / "technology_transitions_gwp_reference.error.txt"
        write_text(error_path, str(exc))
        source_manifest_rows.append(
            _source_manifest_entry(
                source_id=gwp_source_id,
                source_type="public_web_snapshot",
                source_name="EPA Technology Transitions GWP Reference Table",
                license_name="EPA public web snapshot",
                local_path=error_path,
                upstream_url=EPA_TECHNOLOGY_TRANSITIONS_GWP_URL,
                status="failed",
            )
        )
        resolution_rows.append(
            {
                "seed_id": "",
                "r_number": "",
                "stage": "epa_technology_transitions_gwp",
                "status": "failed",
                "detail": str(exc),
            }
        )

    ods_source_id = "source_epa_ods"
    ods_url = "https://www.epa.gov/ozone-layer-protection/ozone-depleting-substances"
    ods_path = paths["raw_epa"] / "ods.html"
    try:
        ods_snapshot = _load_or_fetch_text(ods_path, refresh_remote, lambda: _fetch_url_text(ods_url), upstream_url=ods_url)
        source_manifest_rows.append(
            _source_manifest_entry(
                source_id=ods_source_id,
                source_type="public_web_snapshot",
                source_name="EPA ODS table",
                license_name="EPA public web snapshot",
                local_path=ods_path,
                upstream_url=ods_url,
                status=ods_snapshot["source_status"],
            )
        )
        result["epa_ods_df"] = epa_ods_parser.parse(ods_snapshot["text"])
        if ods_snapshot["source_status"] == "cached_fallback":
            resolution_rows.append(
                {
                    "seed_id": "",
                    "r_number": "",
                    "stage": "epa_ods",
                    "status": "cached_fallback",
                    "detail": _cached_fallback_detail(ods_snapshot),
                }
            )
    except Exception as exc:  # noqa: BLE001
        error_path = paths["raw_epa"] / "ods.error.txt"
        write_text(error_path, str(exc))
        source_manifest_rows.append(
            _source_manifest_entry(
                source_id=ods_source_id,
                source_type="public_web_snapshot",
                source_name="EPA ODS table",
                license_name="EPA public web snapshot",
                local_path=error_path,
                upstream_url=ods_url,
                status="failed",
            )
        )
        resolution_rows.append({"seed_id": "", "r_number": "", "stage": "epa_ods", "status": "failed", "detail": str(exc)})

    snap_frames: list[dict[str, Any]] = []
    for page in SNAP_SOURCE_PAGES:
        source_id = f"source_epa_snap_{page['key']}"
        html_path = paths["raw_epa"] / f"snap_{page['key']}.html"
        try:
            snapshot = _load_or_fetch_text(
                html_path,
                refresh_remote,
                lambda url=page["url"]: _fetch_url_text(url),
                upstream_url=page["url"],
            )
            source_manifest_rows.append(
                _source_manifest_entry(
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name=f"EPA SNAP {page['end_use']}",
                    license_name="EPA public web snapshot",
                    local_path=html_path,
                    upstream_url=page["url"],
                    status=snapshot["source_status"],
                )
            )
            parsed = epa_snap_parser.parse(snapshot["text"], end_use=page["end_use"])
            snap_frames.append({"source_id": source_id, "source_name": f"EPA SNAP {page['end_use']}", "df": parsed})
            if snapshot["source_status"] == "cached_fallback":
                resolution_rows.append(
                    {
                        "seed_id": "",
                        "r_number": "",
                        "stage": f"epa_snap:{page['key']}",
                        "status": "cached_fallback",
                        "detail": _cached_fallback_detail(snapshot),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            error_path = paths["raw_epa"] / f"snap_{page['key']}.error.txt"
            write_text(error_path, str(exc))
            source_manifest_rows.append(
                _source_manifest_entry(
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name=f"EPA SNAP {page['end_use']}",
                    license_name="EPA public web snapshot",
                    local_path=error_path,
                    upstream_url=page["url"],
                    status="failed",
                )
            )
            resolution_rows.append({"seed_id": "", "r_number": "", "stage": f"epa_snap:{page['key']}", "status": "failed", "detail": str(exc)})
    result["epa_snap_frames"] = snap_frames
    return result


def _resolve_pubchem_snapshot(
    *,
    pubchem: PubChemClient,
    seed: dict[str, Any],
    paths: dict[str, Path],
    refresh_remote: bool,
    bulk_pubchem_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if _is_bulk_pubchem_seed(seed):
        payload = _fetch_pubchem_payload(pubchem, seed, bulk_pubchem_lookup=bulk_pubchem_lookup)
        return {
            "payload": payload,
            "source_status": "registered",
            "source_type": "public_database",
            "source_name": "PubChem FTP Bulk Candidate Pool",
            "license_name": "NCBI / PubChem public",
            "local_path": paths["bronze_pubchem_candidate_pool"],
        }

    pubchem_path = paths["raw_pubchem"] / f"{slugify(str(seed.get('seed_id', '')))}.json"
    snapshot = _load_or_fetch_json(
        pubchem_path,
        refresh_remote,
        lambda seed=seed: _fetch_pubchem_payload(pubchem, seed),
    )
    snapshot["source_type"] = "public_database"
    snapshot["source_name"] = "PubChem PUG REST"
    snapshot["license_name"] = "NCBI / PubChem public"
    snapshot["local_path"] = pubchem_path
    return snapshot


def _fetch_pubchem_payload(
    pubchem: PubChemClient,
    seed: dict[str, Any],
    *,
    bulk_pubchem_lookup: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if _is_bulk_pubchem_seed(seed):
        seed_id = _clean_str(seed.get("seed_id"))
        record = (bulk_pubchem_lookup or {}).get(seed_id)
        if not record:
            raise KeyError(f"Missing PubChem bulk candidate pool record for {seed_id}")
        cid = _clean_str(record.get("cid"))
        synonyms = _normalize_bulk_synonyms(record.get("synonyms"))
        return {
            "seed": seed,
            "pubchem_record": {
                "cid": cid,
                "query": _clean_str(seed.get("query_name")) or cid,
                "query_type": "cid",
                "molecular_formula": _clean_str(record.get("formula")),
                "molecular_weight": float(record.get("molecular_weight")),
                "canonical_smiles": _clean_str(record.get("canonical_smiles")),
                "isomeric_smiles": _clean_str(record.get("isomeric_smiles")),
                "inchi": _clean_str(record.get("inchi")),
                "inchikey": _clean_str(record.get("inchikey")),
                "raw": record,
                "url": f"https://pubchem.ncbi.nlm.nih.gov/compound/{cid}",
            },
            "synonyms": {
                "cid": cid,
                "synonyms": synonyms,
                "url": "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-Synonym-filtered.gz",
            },
        }

    pubchem_record = pubchem.resolve_compound(_clean_str(seed.get("query_name")), _clean_str(seed.get("pubchem_query_type")) or "name")
    synonyms_record = pubchem.fetch_synonyms(pubchem_record["cid"])
    return {
        "seed": seed,
        "pubchem_record": pubchem_record,
        "synonyms": synonyms_record,
    }


def _load_or_fetch_json(path: Path, refresh_remote: bool, fetcher: Any) -> dict[str, Any]:
    if path.exists() and not refresh_remote:
        return {
            "payload": json.loads(path.read_text(encoding="utf-8")),
            "source_status": "ok",
            "refresh_error": "",
            "refresh_error_path": "",
        }
    try:
        payload = fetcher()
    except Exception as exc:  # noqa: BLE001
        if _should_use_cached_snapshot(path, exc):
            error_path = _write_refresh_error(path, exc)
            return {
                "payload": json.loads(path.read_text(encoding="utf-8")),
                "source_status": "cached_fallback",
                "refresh_error": str(exc),
                "refresh_error_path": _relpath(error_path),
            }
        raise
    write_json(path, payload)
    return {
        "payload": payload,
        "source_status": "fetched",
        "refresh_error": "",
        "refresh_error_path": "",
    }


def _load_or_fetch_text_payload(path: Path, refresh_remote: bool, fetcher: Any, *, fallback_url: str) -> dict[str, Any]:
    if path.exists() and not refresh_remote:
        return {
            "url": fallback_url,
            "status_code": 200,
            "title": "",
            "status": "ok",
            "html": path.read_text(encoding="utf-8"),
            "source_status": "ok",
            "refresh_error": "",
            "refresh_error_path": "",
        }
    try:
        payload = fetcher()
    except Exception as exc:  # noqa: BLE001
        if _should_use_cached_snapshot(path, exc):
            error_path = _write_refresh_error(path, exc)
            return {
                "url": fallback_url,
                "status_code": 0,
                "title": "cached_fallback",
                "status": "ok",
                "html": path.read_text(encoding="utf-8"),
                "source_status": "cached_fallback",
                "refresh_error": str(exc),
                "refresh_error_path": _relpath(error_path),
            }
        raise
    write_text(path, payload["html"])
    payload["source_status"] = payload.get("status", "ok")
    payload["refresh_error"] = ""
    payload["refresh_error_path"] = ""
    return payload


def _load_or_fetch_text(path: Path, refresh_remote: bool, fetcher: Any, *, upstream_url: str) -> dict[str, Any]:
    if path.exists() and not refresh_remote:
        return {
            "text": path.read_text(encoding="utf-8"),
            "source_status": "fetched",
            "upstream_url": upstream_url,
            "refresh_error": "",
            "refresh_error_path": "",
        }
    try:
        text = fetcher()
    except Exception as exc:  # noqa: BLE001
        if _should_use_cached_snapshot(path, exc):
            error_path = _write_refresh_error(path, exc)
            return {
                "text": path.read_text(encoding="utf-8"),
                "source_status": "cached_fallback",
                "upstream_url": upstream_url,
                "refresh_error": str(exc),
                "refresh_error_path": _relpath(error_path),
            }
        raise
    write_text(path, text)
    return {
        "text": text,
        "source_status": "fetched",
        "upstream_url": upstream_url,
        "refresh_error": "",
        "refresh_error_path": "",
    }


def _fetch_url_text(url: str) -> str:
    response = _HTTP_SESSION.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def _should_use_cached_snapshot(path: Path, exc: Exception) -> bool:
    return path.exists() and path.stat().st_size > 0 and is_transient_request_exception(exc)


def _write_refresh_error(path: Path, exc: Exception) -> Path:
    error_path = path.with_suffix(".refresh_error.txt")
    write_text(error_path, f"{now_iso()}\n{type(exc).__name__}: {exc}")
    return error_path


def _cached_fallback_detail(snapshot: dict[str, Any]) -> str:
    detail = f"used cached snapshot after transient refresh failure: {snapshot.get('refresh_error', '').strip()}".strip()
    if snapshot.get("refresh_error_path"):
        detail = f"{detail} | {snapshot['refresh_error_path']}"
    return detail


def _source_manifest_entry(
    *,
    source_id: str,
    source_type: str,
    source_name: str,
    license_name: str,
    local_path: Path,
    upstream_url: str,
    status: str,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "source_type": source_type,
        "source_name": source_name,
        "license": license_name,
        "retrieved_at": now_iso(),
        "checksum_sha256": sha256_file(local_path) if local_path.exists() else "",
        "local_path": _relpath(local_path) if local_path.exists() else "",
        "parser_version": PARSER_VERSION,
        "upstream_url": upstream_url,
        "status": status,
    }


def _register_manual_sources(paths: dict[str, Path]) -> list[dict[str, Any]]:
    entries = []
    source_rows = [
        ("source_seed_catalog", "manual_catalog", "Manual Seed Catalog", paths["seed_catalog"]),
        ("source_refrigerant_inventory", "manual_catalog", "Curated Refrigerant Inventory", paths["refrigerant_inventory"]),
        ("source_manual_property_observations", "manual_curated_reference", "Manual Property Observations", paths["manual_observations"]),
        ("source_coolprop_aliases", "manual_catalog", "Explicit CoolProp Alias Mappings", paths["coolprop_aliases"]),
        ("source_pubchem_tierd_generated", "manual_catalog", "Generated PubChem Tier D Candidates", paths["raw_generated_pubchem_tierd_candidates"]),
        ("source_pubchem_candidate_pool", "derived_harmonized", "PubChem Bulk Candidate Pool", paths["bronze_pubchem_candidate_pool"]),
        ("source_pubchem_candidate_filter_audit", "derived_harmonized", "PubChem Bulk Candidate Filter Audit", paths["bronze_pubchem_candidate_filter_audit"]),
    ]
    for extra_path in sorted(paths["manual_observations_dir"].glob("*.csv")) if paths["manual_observations_dir"].exists() else []:
        source_rows.append(
            (
                f"source_manual_property_observations_{slugify(extra_path.stem)}",
                "manual_curated_reference",
                f"Manual Property Observations ({extra_path.stem})",
                extra_path,
            )
        )
    for source_id, source_type, name, path in source_rows:
        if not path.exists():
            continue
        entries.append(
            {
                "source_id": source_id,
                "source_type": source_type,
                "source_name": name,
                "license": "project-local manual curation",
                "retrieved_at": now_iso(),
                "checksum_sha256": sha256_file(path),
                "local_path": _relpath(path),
                "parser_version": PARSER_VERSION,
                "upstream_url": "",
                "status": "registered",
            }
        )
    return entries


def _load_manual_observations(paths: dict[str, Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    source_specs = [(paths["manual_observations"], "source_manual_property_observations")]
    if paths["manual_observations_dir"].exists():
        source_specs.extend(
            (path, f"source_manual_property_observations_{slugify(path.stem)}")
            for path in sorted(paths["manual_observations_dir"].glob("*.csv"))
        )

    for path, source_id in source_specs:
        if not path.exists():
            continue
        frame = pd.read_csv(path).fillna("")
        if frame.empty:
            continue
        frame["manual_source_id"] = source_id
        frame["manual_source_path"] = _relpath(path)
        frames.append(frame)

    if not frames:
        return pd.DataFrame(
            columns=[
                "seed_id",
                "r_number",
                "property_name",
                "value",
                "value_num",
                "unit",
                "temperature",
                "pressure",
                "phase",
                "source_type",
                "source_name",
                "source_url",
                "method",
                "uncertainty",
                "quality_level",
                "assessment_version",
                "time_horizon",
                "year",
                "notes",
                "manual_source_id",
                "manual_source_path",
            ]
        )

    combined = pd.concat(frames, ignore_index=True)
    dedupe_columns = [column for column in combined.columns if column not in {"manual_source_id", "manual_source_path"}]
    combined = combined.drop_duplicates(subset=dedupe_columns).reset_index(drop=True)
    return combined


def _epa_ods_property_rows(ods_df: pd.DataFrame, alias_lookup: dict[str, set[str]], source_id: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in ods_df.to_dict(orient="records"):
        mol_ids = _match_alias_candidates(alias_lookup, [record.get("chemical_name", ""), record.get("cas_number", "")])
        if len(mol_ids) != 1:
            continue
        mol_id = next(iter(mol_ids))
        chemical_name = _clean_str(record.get("chemical_name"))
        notes = f"EPA ODS: {chemical_name}" if chemical_name else "EPA ODS"
        if pd.notna(record.get("atmospheric_lifetime_yr")):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="atmospheric_lifetime_yr",
                    value_num=float(record["atmospheric_lifetime_yr"]),
                    unit="yr",
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name="EPA ODS table",
                    quality_level="primary_public_reference",
                    method="EPA ODS table parse",
                    notes=notes,
                )
            )
        if pd.notna(record.get("odp_montreal_protocol")):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="odp",
                    value_num=float(record["odp_montreal_protocol"]),
                    unit="dimensionless",
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name="EPA ODS table",
                    quality_level="primary_public_reference",
                    method="EPA ODS table parse",
                    assessment_version="Montreal Protocol",
                    notes=notes,
                )
            )
        if pd.notna(record.get("odp_wmo_2011")):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="odp",
                    value_num=float(record["odp_wmo_2011"]),
                    unit="dimensionless",
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name="EPA ODS table",
                    quality_level="primary_public_reference",
                    method="EPA ODS table parse",
                    assessment_version="WMO 2011",
                    notes=notes,
                )
            )
        for property_name in ["gwp_ar4_100yr", "gwp_ar5_100yr"]:
            if pd.notna(record.get(property_name)):
                rows.append(
                    _property_row(
                        mol_id=mol_id,
                        property_name=property_name,
                        value_num=float(record[property_name]),
                        unit="dimensionless",
                        source_id=source_id,
                        source_type="public_web_snapshot",
                        source_name="EPA ODS table",
                        quality_level="primary_public_reference",
                        method="EPA ODS table parse",
                        assessment_version=property_name.split("_")[1].upper(),
                        time_horizon="100",
                        notes=notes,
                    )
                )
    return rows


def _epa_gwp_reference_property_rows(
    gwp_df: pd.DataFrame,
    alias_lookup: dict[str, set[str]],
    molecule_context: pd.DataFrame,
    source_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in gwp_df.to_dict(orient="records"):
        substance_name = _clean_str(record.get("substance_name"))
        reference = _clean_str(record.get("reference"))
        mol_ids = _match_epa_alias_candidates(alias_lookup, [substance_name])
        if len(mol_ids) == 1 and pd.notna(record.get("gwp_100yr")):
            mol_id = next(iter(mol_ids))
            notes = f"EPA Technology Transitions GWP table: {substance_name}".strip()
            if reference:
                notes = f"{notes} | reference: {reference}"
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="gwp_100yr",
                    value_num=float(record["gwp_100yr"]),
                    unit="dimensionless",
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name="EPA Technology Transitions GWP Reference Table",
                    quality_level="primary_public_reference",
                    method="EPA Technology Transitions GWP table parse",
                    time_horizon="100",
                    assessment_version=reference,
                    notes=notes,
                )
            )
            continue

        group_targets = _tier_c_epa_gwp_group_targets(molecule_context, substance_name)
        if not group_targets or pd.isna(record.get("gwp_range_max")):
            continue
        notes = f"EPA Technology Transitions grouped class entry: {substance_name} | conservative upper-bound mapping"
        reference = _clean_str(record.get("reference"))
        if reference:
            notes = f"{notes} | reference: {reference}"
        for mol_id in sorted(group_targets):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="gwp_100yr",
                    value_num=float(record["gwp_range_max"]),
                    unit="dimensionless",
                    source_id=source_id,
                    source_type="public_web_snapshot",
                    source_name="EPA Technology Transitions GWP Reference Table",
                    quality_level="derived_harmonized",
                    method="EPA Technology Transitions grouped class upper-bound parse",
                    time_horizon="100",
                    assessment_version=reference,
                    notes=notes,
                )
            )
    return rows


def _epa_snap_rows(
    *,
    snap_frames: list[dict[str, Any]],
    alias_lookup: dict[str, set[str]],
    molecule_context: pd.DataFrame,
    source_type: str,
    source_name_prefix: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    property_rows: list[dict[str, Any]] = []
    regulatory_rows: list[dict[str, Any]] = []
    for frame in snap_frames:
        source_id = frame["source_id"]
        source_name = frame["source_name"]
        for record in frame["df"].to_dict(orient="records"):
            raw_substitute = _clean_str(record.get("substitute"))
            mol_ids = _match_epa_alias_candidates(alias_lookup, [raw_substitute, record.get("trade_names", "")])
            if len(mol_ids) == 1:
                mol_id = next(iter(mol_ids))
                notes = f"{record.get('end_use', '')} | {record.get('listing_status', '')}".strip(" |")
                regulatory_rows.append(
                    {
                        "regulatory_status_id": f"reg_{slugify(mol_id)}_{slugify(source_id)}_{slugify(_clean_str(record.get('end_use')))}",
                        "mol_id": mol_id,
                        "end_use": _clean_str(record.get("end_use")),
                        "jurisdiction": "EPA SNAP (United States)",
                        "acceptability": _clean_str(record.get("acceptability")),
                        "listing_status": _clean_str(record.get("listing_status")),
                        "retrofit_new": _clean_str(record.get("retrofit_new")),
                        "use_conditions": _clean_str(record.get("use_conditions")),
                        "effective_date": _clean_str(record.get("effective_date")) or _clean_str(record.get("listing_date")),
                        "trade_names": _clean_str(record.get("trade_names")),
                        "raw_substitute": raw_substitute,
                        "source_id": source_id,
                        "source_name": source_name,
                        "source_type": source_type,
                    }
                )
                property_rows.extend(
                    _epa_snap_property_rows_for_targets(
                        [mol_id],
                        record,
                        source_id=source_id,
                        source_name=source_name,
                        source_type=source_type,
                        source_name_prefix=source_name_prefix,
                        quality_level="primary_public_reference",
                        notes=notes,
                    )
                )
                continue

            group_targets = _tier_c_epa_snap_group_targets(molecule_context, raw_substitute)
            if not group_targets:
                continue
            notes = (
                f"{record.get('end_use', '')} | {record.get('listing_status', '')} | "
                f"EPA SNAP grouped class mapping: {raw_substitute}"
            ).strip(" |")
            property_rows.extend(
                _epa_snap_property_rows_for_targets(
                    sorted(group_targets),
                    record,
                    source_id=source_id,
                    source_name=source_name,
                    source_type=source_type,
                    source_name_prefix=source_name_prefix,
                    quality_level="derived_harmonized",
                    notes=notes,
                )
            )
    return property_rows, regulatory_rows


def _epa_snap_property_rows_for_targets(
    mol_ids: list[str],
    record: dict[str, Any],
    *,
    source_id: str,
    source_name: str,
    source_type: str,
    source_name_prefix: str,
    quality_level: str,
    notes: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for mol_id in mol_ids:
        if _clean_str(record.get("ashrae_safety")):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="ashrae_safety",
                    value=_clean_str(record["ashrae_safety"]),
                    unit="class",
                    source_id=source_id,
                    source_type=source_type,
                    source_name=source_name,
                    quality_level=quality_level,
                    method=f"{source_name_prefix} table parse",
                    notes=notes,
                )
            )
        if pd.notna(record.get("gwp")):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="gwp_100yr",
                    value_num=float(record["gwp"]),
                    unit="dimensionless",
                    source_id=source_id,
                    source_type=source_type,
                    source_name=source_name,
                    quality_level=quality_level,
                    method=f"{source_name_prefix} table parse",
                    time_horizon="100",
                    notes=notes,
                )
            )
        if pd.notna(record.get("odp")):
            rows.append(
                _property_row(
                    mol_id=mol_id,
                    property_name="odp",
                    value_num=float(record["odp"]),
                    unit="dimensionless",
                    source_id=source_id,
                    source_type=source_type,
                    source_name=source_name,
                    quality_level=quality_level,
                    method=f"{source_name_prefix} table parse",
                    notes=notes,
                )
            )
    return rows


def _wrap_external_property_rows(
    *,
    mol_id: str,
    source_id: str,
    source_type: str,
    source_name: str,
    quality_level: str,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    wrapped = []
    for row in rows:
        wrapped.append(
            _property_row(
                mol_id=mol_id,
                property_name=row["property_name"],
                value=row.get("value", ""),
                value_num=row.get("value_num"),
                unit=row["unit"],
                source_id=source_id,
                source_type=source_type,
                source_name=source_name,
                quality_level=quality_level,
                method=row.get("method", ""),
                temperature=row.get("temperature", ""),
                pressure=row.get("pressure", ""),
                phase=row.get("phase", ""),
                notes=row.get("notes", ""),
            )
        )
    return wrapped


def _manual_property_rows(
    manual_observations: pd.DataFrame,
    seed_to_mol_id: dict[str, str],
    alias_lookup: dict[str, set[str]],
) -> list[dict[str, Any]]:
    rows = []
    for obs in manual_observations.to_dict(orient="records"):
        mol_id = seed_to_mol_id.get(_clean_str(obs.get("seed_id")), "")
        if not mol_id:
            candidates = _match_alias_candidates(alias_lookup, [_clean_str(obs.get("r_number"))])
            mol_id = next(iter(candidates)) if len(candidates) == 1 else ""
        if not mol_id:
            continue
        rows.append(
            {
                "observation_id": None,
                "mol_id": mol_id,
                "property_name": _clean_str(obs.get("property_name")),
                "value": _clean_str(obs.get("value")),
                "value_num": _optional_float(obs.get("value_num")),
                "unit": _clean_str(obs.get("unit")),
                "temperature": _clean_str(obs.get("temperature")),
                "pressure": _clean_str(obs.get("pressure")),
                "phase": _clean_str(obs.get("phase")),
                "source_type": _clean_str(obs.get("source_type")),
                "source_name": _clean_str(obs.get("source_name")),
                "source_id": _clean_str(obs.get("manual_source_id")) or "source_manual_property_observations",
                "method": _clean_str(obs.get("method")),
                "uncertainty": _clean_str(obs.get("uncertainty")),
                "quality_level": _clean_str(obs.get("quality_level")),
                "assessment_version": _clean_str(obs.get("assessment_version")),
                "time_horizon": _clean_str(obs.get("time_horizon")),
                "year": _clean_str(obs.get("year")),
                "notes": _clean_str(obs.get("notes")),
                "qc_status": "pass",
                "qc_flags": "",
            }
        )
    return rows


def _property_row(
    *,
    mol_id: str,
    property_name: str,
    unit: str,
    source_id: str,
    source_type: str,
    source_name: str,
    quality_level: str,
    method: str,
    value: str = "",
    value_num: float | None = None,
    temperature: str = "",
    pressure: str = "",
    phase: str = "",
    assessment_version: str = "",
    time_horizon: str = "",
    year: str = "",
    notes: str = "",
) -> dict[str, Any]:
    text_value = value if value else (f"{float(value_num):.8g}" if value_num is not None else "")
    return {
        "observation_id": None,
        "mol_id": mol_id,
        "property_name": property_name,
        "value": text_value,
        "value_num": value_num,
        "unit": unit,
        "temperature": temperature,
        "pressure": pressure,
        "phase": phase,
        "source_type": source_type,
        "source_name": source_name,
        "source_id": source_id,
        "method": method,
        "uncertainty": "",
        "quality_level": quality_level,
        "assessment_version": assessment_version,
        "time_horizon": time_horizon,
        "year": year,
        "notes": notes,
        "qc_status": "pass",
        "qc_flags": "",
    }


def _relpath(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def _append_alias(rows: list[dict[str, Any]], mol_id: str, alias_type: str, alias_value: str, is_primary: bool, source_name: str) -> None:
    cleaned = _clean_str(alias_value)
    if not cleaned:
        return
    rows.append(
        {
            "mol_id": mol_id,
            "alias_type": alias_type,
            "alias_value": cleaned,
            "is_primary": bool(is_primary),
            "source_name": source_name,
        }
    )


def _append_family_prefixed_aliases(rows: list[dict[str, Any]], mol_id: str, r_number: str, family: str) -> None:
    if not r_number:
        return
    suffix = r_number.replace("R-", "").replace("R", "")
    prefix_map = {
        "HFC": "HFC-",
        "HFO": "HFO-",
        "HCFO": "HCFO-",
        "HCFC": "HCFC-",
        "CFC": "CFC-",
    }
    prefix = prefix_map.get(family)
    if prefix:
        _append_alias(rows, mol_id, "synonym", f"{prefix}{suffix}", False, "family_derived")


def _is_bulk_pubchem_seed(seed: dict[str, Any]) -> bool:
    return _clean_str(seed.get("source_bundle")) == "pubchem_bulk"


def _load_bulk_pubchem_candidate_lookup(paths: dict[str, Path]) -> dict[str, dict[str, Any]]:
    pool_path = paths.get("bronze_pubchem_candidate_pool")
    if not pool_path or not pool_path.exists():
        return {}

    df = pd.read_parquet(pool_path).fillna("")
    lookup: dict[str, dict[str, Any]] = {}
    for record in df.to_dict(orient="records"):
        cid = _clean_str(record.get("cid"))
        if not cid:
            continue
        lookup[f"tierd_pubchem_{cid}"] = record
    return lookup


def _normalize_bulk_synonyms(value: Any) -> list[str]:
    if value is None:
        return []
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
    if isinstance(value, list):
        return [_clean_str(item) for item in value if _clean_str(item)]
    if isinstance(value, tuple):
        return [_clean_str(item) for item in value if _clean_str(item)]
    if isinstance(value, set):
        return [_clean_str(item) for item in value if _clean_str(item)]
    if pd.isna(value):
        return []
    cleaned = _clean_str(value)
    return [cleaned] if cleaned else []


def _clean_str(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _optional_float(value: Any) -> float | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    return float(value)


def _prefer_seed_catalog_entry(candidate_row: dict[str, Any], existing_row: dict[str, Any]) -> bool:
    return _seed_catalog_priority(candidate_row) < _seed_catalog_priority(existing_row)


def _seed_catalog_priority(row: dict[str, Any]) -> tuple[int, int, int, str]:
    entity_scope_rank = 0 if _clean_str(row.get("entity_scope")) == "refrigerant" else 1
    model_inclusion_rank = 0 if _clean_str(row.get("model_inclusion")) == "yes" else 1
    coverage_rank = {"A": 0, "B": 1, "C": 2, "D": 3}.get(_clean_str(row.get("coverage_tier")), 9)
    return (entity_scope_rank, model_inclusion_rank, coverage_rank, _clean_str(row.get("seed_id")))


def _resolve_coolprop_fluid(seed: dict[str, Any], coolprop_aliases: dict[str, str]) -> str:
    explicit = _clean_str(seed.get("coolprop_fluid"))
    if explicit:
        return explicit
    for key in [_clean_str(seed.get("r_number")), _clean_str(seed.get("query_name")), _clean_str(seed.get("seed_id"))]:
        if key and key in coolprop_aliases:
            return _clean_str(coolprop_aliases[key])
    return ""


def _build_molecule_source_context(molecule_core: pd.DataFrame, seed_catalog: pd.DataFrame) -> pd.DataFrame:
    seed_fields = seed_catalog[["seed_id", "coverage_tier", "selection_role", "entity_scope", "model_inclusion"]].copy()
    merged = pd.merge(molecule_core, seed_fields, on="seed_id", how="left")
    return _ensure_columns(
        merged,
        ["mol_id", "seed_id", "family", "formula", "pubchem_query", "coverage_tier", "selection_role", "entity_scope", "model_inclusion"],
    )


def _expand_epa_alias_variants(value: str) -> set[str]:
    cleaned = _clean_str(value)
    if not cleaned:
        return set()

    variants = {cleaned}
    stripped_marks = re.sub(r"[®℠™]", "", cleaned)
    stripped_marks = re.sub(r"(?<=[A-Za-z])TM(?=\s*\d|\b)", "", stripped_marks, flags=re.IGNORECASE)
    stripped_marks = " ".join(stripped_marks.split())
    if stripped_marks:
        variants.add(stripped_marks)

    direct_match = re.match(r"(?i)^direct\s+(.+?)\s+expansion$", stripped_marks)
    if direct_match:
        variants.add(direct_match.group(1).strip())

    if "(" in stripped_marks and ")" in stripped_marks:
        prefix, remainder = stripped_marks.split("(", 1)
        inside, suffix = remainder.split(")", 1)
        for candidate in [prefix.strip(), inside.strip(), suffix.strip(), f"{prefix.strip()} {suffix.strip()}".strip()]:
            if candidate:
                variants.add(candidate)

    for candidate in list(variants):
        for part in re.split(r"[;,]", candidate):
            part = part.strip("() ")
            if part:
                variants.add(part)

    return {item for item in variants if item}


def _normalize_alias(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _expand_alias_variants(value: str) -> set[str]:
    cleaned = _clean_str(value)
    if not cleaned:
        return set()

    variants = {cleaned}
    lowered = cleaned.lower()
    for marker in [
        " absorption",
        " vapor compression",
        " technology",
        " systems",
        " system",
        " with secondary loop",
        " secondary loop",
        " primary heat transfer fluid",
    ]:
        if marker in lowered:
            variants.add(cleaned[: lowered.index(marker)].strip())

    for part in re.split(r"[;,]", cleaned):
        part = part.strip("() ")
        if part:
            variants.add(part)

    if "(" in cleaned and ")" in cleaned:
        prefix, remainder = cleaned.split("(", 1)
        inside, suffix = remainder.split(")", 1)
        for candidate in [prefix.strip(), inside.strip(), suffix.strip(), f"{prefix.strip()} {suffix.strip()}".strip()]:
            if candidate:
                variants.add(candidate)

    for candidate in list(variants):
        for part in re.split(r"[;,]", candidate):
            part = part.strip("() ")
            if part:
                variants.add(part)

    for candidate in list(variants):
        if not re.search(r"\d+(?:\.\d+)?/\d+(?:\.\d+)?", candidate):
            for part in candidate.split("/"):
                part = part.strip("() ")
                if part:
                    variants.add(part)

    for candidate in list(variants):
        for part in re.split(r"\bor\b", candidate, flags=re.IGNORECASE):
            part = part.strip(" -")
            if part:
                variants.add(part)

    return {item for item in variants if item}


def _build_alias_lookup(alias_df: pd.DataFrame) -> dict[str, set[str]]:
    lookup: dict[str, set[str]] = defaultdict(set)
    for row in alias_df.to_dict(orient="records"):
        for variant in _expand_alias_variants(_clean_str(row["alias_value"])):
            normalized = _normalize_alias(variant)
            if normalized:
                lookup[normalized].add(row["mol_id"])
    return lookup


def _match_alias_candidates(alias_lookup: dict[str, set[str]], values: list[str]) -> set[str]:
    candidates: set[str] = set()
    for value in values:
        for variant in _expand_alias_variants(value):
            normalized = _normalize_alias(variant)
            if normalized in alias_lookup:
                candidates.update(alias_lookup[normalized])
    return candidates


def _match_epa_alias_candidates(alias_lookup: dict[str, set[str]], values: list[str]) -> set[str]:
    candidates: set[str] = set()
    for value in values:
        value_candidates: set[str] = set()
        for variant in _expand_epa_alias_variants(value):
            normalized = _normalize_alias(variant)
            if normalized in alias_lookup:
                value_candidates.update(alias_lookup[normalized])
        if len(value_candidates) == 1:
            return value_candidates
        candidates.update(value_candidates)
    return candidates


def _tier_c_epa_gwp_group_targets(molecule_context: pd.DataFrame, substance_name: str) -> set[str]:
    normalized = _normalize_alias(substance_name)
    if normalized == _normalize_alias("Hydrocarbons (C5-C20)"):
        return set(
            molecule_context.loc[
                molecule_context.apply(_is_inventory_candidate_hydrocarbon, axis=1),
                "mol_id",
            ]
        )
    if normalized == _normalize_alias("Oxygenated organic solvents (esters, ethers, alcohols, ketones)"):
        return set(
            molecule_context.loc[
                molecule_context.apply(_is_inventory_candidate_oxygenated_organic, axis=1),
                "mol_id",
            ]
        )
    return set()


def _tier_c_epa_snap_group_targets(molecule_context: pd.DataFrame, substitute: str) -> set[str]:
    normalized = _normalize_alias(substitute)
    if normalized == _normalize_alias("Volatile Methyl Siloxanes"):
        return set(
            molecule_context.loc[
                molecule_context.apply(_is_inventory_candidate_siloxane, axis=1),
                "mol_id",
            ]
        )
    return set()


def _is_inventory_candidate_hydrocarbon(row: pd.Series) -> bool:
    if _clean_str(row.get("coverage_tier")) not in {"C", "D"}:
        return False
    if _clean_str(row.get("entity_scope")) not in {"", "candidate"}:
        return False
    counts = _formula_element_counts(_clean_str(row.get("formula")))
    if not counts or set(counts) != {"C", "H"}:
        return False
    return 5 <= counts.get("C", 0) <= 20


def _is_inventory_candidate_oxygenated_organic(row: pd.Series) -> bool:
    if _clean_str(row.get("coverage_tier")) not in {"C", "D"}:
        return False
    if _clean_str(row.get("entity_scope")) not in {"", "candidate"}:
        return False
    counts = _formula_element_counts(_clean_str(row.get("formula")))
    if not counts or "C" not in counts or "O" not in counts:
        return False
    return not (set(counts) - {"C", "H", "O"})


def _is_inventory_candidate_siloxane(row: pd.Series) -> bool:
    return (
        _clean_str(row.get("coverage_tier")) in {"C", "D"}
        and _clean_str(row.get("entity_scope")) in {"", "candidate"}
        and _clean_str(row.get("family")) == "Siloxane"
    )


def _formula_element_counts(formula: str) -> dict[str, int]:
    if not formula:
        return {}
    counts: dict[str, int] = {}
    for element, count_text in re.findall(r"([A-Z][a-z]?)(\d*)", formula):
        counts[element] = counts.get(element, 0) + (int(count_text) if count_text else 1)
    return counts


def _assign_observation_ids(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _ensure_columns(df.copy(), _property_observation_columns())
    df = df.copy()
    df["observation_id"] = [
        f"obs_{slugify(mol_id)}_{slugify(prop)}_{idx + 1}"
        for idx, (mol_id, prop) in enumerate(zip(df["mol_id"], df["property_name"], strict=True))
    ]
    return df


def _apply_qc(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return _ensure_columns(df.copy(), _property_observation_columns()), pd.DataFrame(columns=["mol_id", "issue_type", "detail"])

    issues: list[dict[str, Any]] = []
    checked = df.copy()
    gwp_names = {"gwp_20yr", "gwp_100yr", "gwp_ar4_100yr", "gwp_ar5_100yr", "gwp_ar6_100yr"}

    for idx, row in checked.iterrows():
        flags: list[str] = []
        prop = row["property_name"]
        value_num = row["value_num"]
        if prop in gwp_names and value_num is not None and value_num < 0:
            flags.append("negative_gwp")
        if prop == "odp" and value_num is not None and not (0 <= value_num <= 1.5):
            flags.append("odp_out_of_range")
        if prop == "atmospheric_lifetime_yr" and value_num is not None and value_num < 0:
            flags.append("negative_lifetime")
        if prop == "vaporization_enthalpy_kjmol" and value_num is not None and value_num < 0:
            flags.append("negative_vaporization_enthalpy")
        if prop == "ashrae_safety" and row["value"] not in {"A1", "A2", "A2L", "A3", "B1", "B2", "B2L", "B3"}:
            flags.append("invalid_ashrae_class")
        checked.at[idx, "qc_status"] = "pass" if not flags else "warning"
        checked.at[idx, "qc_flags"] = ";".join(flags)
        for flag in flags:
            issues.append({"mol_id": row["mol_id"], "issue_type": flag, "detail": row["observation_id"]})

    numeric_view = checked.loc[checked["value_num"].notna()]
    if not numeric_view.empty:
        pair_lookup = numeric_view.pivot_table(index="mol_id", columns="property_name", values="value_num", aggfunc="first")
        for mol_id, pair in pair_lookup.iterrows():
            tb = pair.get("boiling_point_c")
            tc = pair.get("critical_temp_c")
            if pd.notna(tb) and pd.notna(tc) and tb >= tc:
                issues.append({"mol_id": mol_id, "issue_type": "tb_not_below_tc", "detail": f"Tb={tb}, Tc={tc}"})

    return checked, pd.DataFrame(issues)


def _select_recommended(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_property_recommended_columns())

    working = df.copy()
    working = working.loc[(working["value_num"].notna()) | (working["value"].astype(str).str.strip() != "")]
    if working.empty:
        return pd.DataFrame(columns=_property_recommended_columns())

    working["source_priority"] = working["source_type"].map(SOURCE_PRIORITY).fillna(0).astype(int)
    working["quality_score"] = working["quality_level"].map(QUALITY_SCORES).fillna(0.0)
    working["source_priority_rank_value"] = pd.to_numeric(working.get("source_priority_rank"), errors="coerce")
    working["data_quality_score_value"] = pd.to_numeric(working.get("data_quality_score_100"), errors="coerce")
    working["proxy_sort"] = pd.to_numeric(working.get("is_proxy_or_screening"), errors="coerce").fillna(0).astype(int)
    working["source_priority_effective"] = working.apply(_effective_source_priority, axis=1)
    working["quality_score_effective"] = working.apply(_effective_quality_score, axis=1)

    rows: list[dict[str, Any]] = []
    for (mol_id, property_name), group in working.groupby(["mol_id", "property_name"], sort=True):
        rows.append(_select_group_recommended(mol_id, property_name, group))

    recommended = pd.DataFrame(rows)
    existing_pairs = {(row["mol_id"], row["property_name"]) for row in rows}

    for mol_id in sorted(working["mol_id"].unique()):
        if (mol_id, "gwp_100yr") not in existing_pairs:
            for source_property in GWP_PREFERENCE_ORDER:
                group = working.loc[(working["mol_id"] == mol_id) & (working["property_name"] == source_property)]
                if not group.empty:
                    item = _select_group_recommended(mol_id, "gwp_100yr", group)
                    item["conflict_detail"] = f"harmonized_from:{source_property}"
                    rows.append(item)
                    existing_pairs.add((mol_id, "gwp_100yr"))
                    break
        if (mol_id, "toxicity_class") not in existing_pairs:
            ashrae = recommended.loc[
                (recommended["mol_id"] == mol_id) & (recommended["property_name"] == "ashrae_safety"),
                :,
            ]
            if not ashrae.empty:
                selected = ashrae.iloc[0]
                toxicity = _clean_str(selected["value"])[:1]
                if toxicity in {"A", "B"}:
                    rows.append(
                        {
                            "mol_id": mol_id,
                            "property_name": "toxicity_class",
                            "value": toxicity,
                            "value_num": None,
                            "unit": "class",
                            "selected_source_id": selected["selected_source_id"],
                            "selected_source_name": selected["selected_source_name"],
                            "selected_quality_level": "derived_harmonized",
                            "source_priority": SOURCE_PRIORITY["derived_harmonized"],
                            "source_count": 1,
                            "conflict_flag": False,
                            "conflict_detail": "derived_from:ashrae_safety",
                        }
                    )
                    existing_pairs.add((mol_id, "toxicity_class"))

    return _ensure_columns(pd.DataFrame(rows), _property_recommended_columns()).sort_values(["mol_id", "property_name"]).reset_index(drop=True)


def _select_group_recommended(mol_id: str, property_name: str, group: pd.DataFrame) -> dict[str, Any]:
    group = group.sort_values(
        by=["proxy_sort", "source_priority_effective", "quality_score_effective"],
        ascending=[True, False, False],
        kind="stable",
    )
    selected = group.iloc[0]
    conflict_flag = False
    conflict_detail = ""
    if len(group) > 1:
        if property_name in NUMERIC_PROPERTIES or group["value_num"].notna().any():
            numeric_values = [v for v in group["value_num"].tolist() if pd.notna(v)]
            if len(numeric_values) > 1:
                vmin = min(numeric_values)
                vmax = max(numeric_values)
                scale = max(abs(vmax), abs(vmin), 1.0)
                if abs(vmax - vmin) / scale > 0.05:
                    conflict_flag = True
                    conflict_detail = f"numeric spread {vmin}..{vmax}"
        else:
            text_values = sorted(set(group["value"].astype(str).tolist()))
            if len(text_values) > 1:
                conflict_flag = True
                conflict_detail = " | ".join(text_values)
    return {
        "mol_id": mol_id,
        "property_name": property_name,
        "value": selected["value"],
        "value_num": selected["value_num"],
        "unit": selected["unit"],
        "selected_source_id": selected["source_id"],
        "selected_source_name": selected["source_name"],
        "selected_quality_level": selected["quality_level"],
        "source_priority": int(selected["source_priority_effective"]),
        "source_count": int(len(group)),
        "conflict_flag": bool(conflict_flag),
        "conflict_detail": conflict_detail,
    }


def _effective_source_priority(row: pd.Series) -> int:
    rank = row.get("source_priority_rank_value")
    if pd.notna(rank):
        return max(0, 10000 - int(rank))
    return int(row.get("source_priority", 0) or 0)


def _effective_quality_score(row: pd.Series) -> float:
    score = row.get("data_quality_score_value")
    if pd.notna(score):
        return float(score) / 100.0
    return float(row.get("quality_score", 0.0) or 0.0)


def _build_structure_features(molecule_core: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in molecule_core.to_dict(orient="records"):
        features = compute_structure_features(row["isomeric_smiles"])
        features["mol_id"] = row["mol_id"]
        rows.append(features)
    return pd.DataFrame(rows).sort_values("mol_id") if rows else pd.DataFrame(columns=["mol_id"])


def _build_property_matrix(property_recommended: pd.DataFrame) -> pd.DataFrame:
    if property_recommended.empty:
        return pd.DataFrame(columns=["mol_id"])
    numeric = (
        property_recommended.loc[property_recommended["property_name"].isin(NUMERIC_PROPERTIES), ["mol_id", "property_name", "value_num"]]
        .pivot(index="mol_id", columns="property_name", values="value_num")
        .reset_index()
    )
    categorical = (
        property_recommended.loc[property_recommended["property_name"].isin(CATEGORICAL_PROPERTIES), ["mol_id", "property_name", "value"]]
        .pivot(index="mol_id", columns="property_name", values="value")
        .reset_index()
    )
    merged = pd.merge(numeric, categorical, on="mol_id", how="outer")
    merged.columns.name = None
    return merged


def _build_model_dataset_index(
    structure_features: pd.DataFrame,
    property_recommended: pd.DataFrame,
    molecule_core: pd.DataFrame,
) -> pd.DataFrame:
    if structure_features.empty:
        return pd.DataFrame(columns=["mol_id", "scaffold_key", "split"])

    model_eligible = set(
        molecule_core.loc[molecule_core["model_inclusion"].astype(str).str.strip().str.lower() == "yes", "mol_id"].tolist()
    )
    structure_features = structure_features.loc[structure_features["mol_id"].isin(model_eligible)].copy()
    if structure_features.empty:
        return pd.DataFrame(columns=["mol_id", "scaffold_key", "split"])

    usable = property_recommended.loc[
        (property_recommended["mol_id"].isin(model_eligible))
        & (property_recommended["value_num"].notna() | (property_recommended["value"].astype(str).str.strip() != ""))
    ]
    coverage = usable.groupby("mol_id")["property_name"].nunique().to_dict()
    avg_quality = (
        usable.assign(quality_score=usable["selected_quality_level"].map(QUALITY_SCORES).fillna(0.0))
        .groupby("mol_id")["quality_score"]
        .mean()
        .to_dict()
    )

    split_map = _assign_scaffold_splits(structure_features[["mol_id", "scaffold_key"]])
    available_properties = defaultdict(set)
    for row in usable[["mol_id", "property_name"]].to_dict(orient="records"):
        available_properties[row["mol_id"]].add(row["property_name"])

    rows = []
    for row in structure_features.to_dict(orient="records"):
        mol_id = row["mol_id"]
        observed = available_properties.get(mol_id, set())
        completeness = sum(1 for prop in MODEL_TARGET_PROPERTIES if prop in observed) / max(len(MODEL_TARGET_PROPERTIES), 1)
        mean_quality = float(avg_quality.get(mol_id, 0.0))
        confidence = round(0.6 * mean_quality + 0.4 * completeness, 6)
        item = {
            "mol_id": mol_id,
            "split": split_map[mol_id],
            "scaffold_key": row["scaffold_key"],
            "confidence_score": confidence,
            "source_coverage_count": int(coverage.get(mol_id, 0)),
        }
        for prop in MODEL_TARGET_PROPERTIES:
            item[f"has_{prop}"] = prop in observed
        rows.append(item)
    return pd.DataFrame(rows).sort_values("mol_id")


def _assign_scaffold_splits(df: pd.DataFrame, train_ratio: float = 0.7, val_ratio: float = 0.15) -> dict[str, str]:
    grouped = df.groupby("scaffold_key")["mol_id"].apply(list).sort_values(key=lambda series: series.map(len), ascending=False)
    total = len(df)
    targets = {
        "train": total * train_ratio,
        "validation": total * val_ratio,
        "test": total * (1.0 - train_ratio - val_ratio),
    }
    counts = {key: 0 for key in targets}
    assignment: dict[str, str] = {}
    for _, mol_ids in grouped.items():
        ordered_splits = sorted(targets.keys(), key=lambda split: (targets[split] - counts[split], -counts[split]), reverse=True)
        chosen = ordered_splits[0]
        counts[chosen] += len(mol_ids)
        for mol_id in mol_ids:
            assignment[mol_id] = chosen
    return assignment


def _build_molecule_master(molecule_core: pd.DataFrame, alias_df: pd.DataFrame, structure_features: pd.DataFrame) -> pd.DataFrame:
    primary_r = (
        alias_df.loc[(alias_df["alias_type"] == "r_number") & (alias_df["is_primary"]), ["mol_id", "alias_value"]]
        .drop_duplicates("mol_id")
        .rename(columns={"alias_value": "r_number_primary"})
    )
    master = pd.merge(molecule_core, primary_r, on="mol_id", how="left")
    master = pd.merge(master, structure_features, on="mol_id", how="left")
    if "formula_x" in master.columns:
        master["formula"] = master["formula_x"].fillna(master.get("formula_y"))
        master = master.drop(columns=[col for col in ["formula_x", "formula_y"] if col in master.columns])
    if "heavy_atom_count_x" in master.columns:
        master["heavy_atom_count"] = master["heavy_atom_count_x"].fillna(master.get("heavy_atom_count_y"))
        master = master.drop(columns=[col for col in ["heavy_atom_count_x", "heavy_atom_count_y"] if col in master.columns])
    if "mol_weight" in master.columns:
        master["mol_weight"] = master["mol_weight"].fillna(master.get("molecular_weight"))
    return master


def _build_model_ready(molecule_master: pd.DataFrame, property_matrix: pd.DataFrame, model_dataset_index: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(molecule_master, property_matrix, on="mol_id", how="left")
    if model_dataset_index.empty:
        return merged.iloc[0:0].copy()
    merged = pd.merge(merged, model_dataset_index, on=["mol_id", "scaffold_key"], how="left")
    return merged.loc[merged["mol_id"].isin(model_dataset_index["mol_id"])].reset_index(drop=True)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    df.to_parquet(path, index=False)


def _build_quality_report(
    *,
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    property_observation: pd.DataFrame,
    property_recommended: pd.DataFrame,
    model_ready: pd.DataFrame,
    qc_issues: pd.DataFrame,
    resolution_df: pd.DataFrame,
    regulatory_status: pd.DataFrame,
    pending_sources: pd.DataFrame,
    property_observation_canonical: pd.DataFrame | None = None,
    property_recommended_canonical: pd.DataFrame | None = None,
    property_recommended_canonical_strict: pd.DataFrame | None = None,
    property_recommended_canonical_review_queue: pd.DataFrame | None = None,
    property_governance_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    property_observation_canonical = property_observation_canonical if property_observation_canonical is not None else pd.DataFrame()
    property_recommended_canonical = property_recommended_canonical if property_recommended_canonical is not None else pd.DataFrame()
    property_recommended_canonical_strict = (
        property_recommended_canonical_strict if property_recommended_canonical_strict is not None else pd.DataFrame()
    )
    property_recommended_canonical_review_queue = (
        property_recommended_canonical_review_queue
        if property_recommended_canonical_review_queue is not None
        else pd.DataFrame()
    )
    property_governance_audit = property_governance_audit or {}
    recommended_props = property_recommended.groupby("property_name")["mol_id"].nunique().to_dict() if not property_recommended.empty else {}
    split_counts = model_ready["split"].value_counts(dropna=False).to_dict() if "split" in model_ready.columns else {}
    unresolved = resolution_df.loc[resolution_df["status"].isin(["failed", "warning"]), ["seed_id", "stage", "detail"]].to_dict(orient="records")
    tier_coverage = _tier_coverage(seed_catalog, molecule_core, property_recommended)
    unresolved_refrigerants = _unresolved_refrigerants(seed_catalog, resolution_df, molecule_core)
    inventory_property_gaps = _inventory_property_gaps(seed_catalog, molecule_core, property_recommended)
    canonical_metrics = _build_canonical_metrics(
        property_observation_canonical=property_observation_canonical,
        property_recommended_canonical=property_recommended_canonical,
        property_recommended_canonical_strict=property_recommended_canonical_strict,
        property_recommended_canonical_review_queue=property_recommended_canonical_review_queue,
        property_governance_audit=property_governance_audit,
    )
    return {
        "seed_catalog_count": int(len(seed_catalog)),
        "refrigerant_count": int((seed_catalog["entity_scope"].astype(str) == "refrigerant").sum()) if "entity_scope" in seed_catalog.columns else 0,
        "candidate_count": int((seed_catalog["entity_scope"].astype(str) == "candidate").sum()) if "entity_scope" in seed_catalog.columns else 0,
        "resolved_molecule_count": int(len(molecule_core)),
        "observation_count": int(len(property_observation)),
        "recommended_count": int(len(property_recommended)),
        "regulatory_status_count": int(len(regulatory_status)),
        "pending_source_count": int(len(pending_sources)),
        "qc_issue_count": int(len(qc_issues)),
        "recommended_property_coverage": recommended_props,
        "tier_coverage": tier_coverage,
        "inventory_property_gaps": inventory_property_gaps,
        "split_counts": split_counts,
        "unresolved_refrigerants": unresolved_refrigerants,
        "unresolved_events": unresolved,
        "canonical_metrics": canonical_metrics,
        "property_governance_bundle": property_governance_audit,
    }


def _build_canonical_metrics(
    *,
    property_observation_canonical: pd.DataFrame,
    property_recommended_canonical: pd.DataFrame,
    property_recommended_canonical_strict: pd.DataFrame,
    property_recommended_canonical_review_queue: pd.DataFrame,
    property_governance_audit: dict[str, Any],
) -> dict[str, Any]:
    crosswalk_audit = property_governance_audit.get("crosswalk", {}) if isinstance(property_governance_audit, dict) else {}
    review_reason_counts = (
        property_recommended_canonical_review_queue["review_reason"].astype(str).value_counts().to_dict()
        if not property_recommended_canonical_review_queue.empty
        else {}
    )
    return {
        "canonical_observation_count": int(len(property_observation_canonical)),
        "canonical_recommended_count": int(len(property_recommended_canonical)),
        "canonical_recommended_strict_count": int(len(property_recommended_canonical_strict)),
        "canonical_review_queue_count": int(len(property_recommended_canonical_review_queue)),
        "canonical_proxy_selected_count": int(property_recommended_canonical["is_proxy_or_screening"].fillna(False).astype(bool).sum())
        if not property_recommended_canonical.empty
        else 0,
        "canonical_proxy_only_count": int(property_recommended_canonical["proxy_only_flag"].fillna(False).astype(bool).sum())
        if not property_recommended_canonical.empty
        else 0,
        "canonical_conflict_count": int(property_recommended_canonical["conflict_flag"].fillna(False).astype(bool).sum())
        if not property_recommended_canonical.empty
        else 0,
        "canonical_source_divergence_count": int(property_recommended_canonical["source_divergence_flag"].fillna(False).astype(bool).sum())
        if not property_recommended_canonical.empty
        else 0,
        "canonical_conflict_open_count": int(
            property_governance_audit.get("canonical_conflict_open_count", review_reason_counts.get("top_rank_conflict", 0))
            or 0
        ),
        "canonical_source_divergence_open_count": int(
            property_governance_audit.get("canonical_source_divergence_open_count", review_reason_counts.get("source_divergence", 0))
            or 0
        ),
        "canonical_review_decision_count": int(property_governance_audit.get("canonical_review_decision_count", 0) or 0),
        "canonical_proxy_policy_count": int(property_governance_audit.get("canonical_proxy_policy_count", 0) or 0),
        "canonical_strict_proxy_accept_count": int(property_governance_audit.get("canonical_strict_proxy_accept_count", 0) or 0),
        "canonical_feature_key_count": int(property_recommended_canonical["canonical_feature_key"].nunique())
        if not property_recommended_canonical.empty
        else 0,
        "canonical_strict_feature_key_count": int(property_recommended_canonical_strict["canonical_feature_key"].nunique())
        if not property_recommended_canonical_strict.empty
        else 0,
        "canonical_review_reason_counts": review_reason_counts,
        "canonical_review_decision_reason_counts": property_governance_audit.get("canonical_review_decision_reason_counts", {}),
        "canonical_review_decision_action_counts": property_governance_audit.get("canonical_review_decision_action_counts", {}),
        "canonical_proxy_policy_feature_counts": property_governance_audit.get("canonical_proxy_policy_feature_counts", {}),
        "unresolved_bundle_substance_count": int(crosswalk_audit.get("unresolved", 0) or 0),
        "external_resolution_count": int(crosswalk_audit.get("external_resolution_count", 0) or 0),
        "row_count_audit_status": property_governance_audit.get("row_count_audit", {}).get("status", ""),
    }


def _tier_coverage(seed_catalog: pd.DataFrame, molecule_core: pd.DataFrame, property_recommended: pd.DataFrame) -> dict[str, Any]:
    if molecule_core.empty:
        return {}
    tier_map = seed_catalog[["seed_id", "coverage_tier"]].copy()
    merged = pd.merge(molecule_core[["mol_id", "seed_id"]], tier_map, on="seed_id", how="left")
    coverage: dict[str, Any] = {}
    for tier in sorted(merged["coverage_tier"].dropna().unique()):
        tier_mols = merged.loc[merged["coverage_tier"] == tier, "mol_id"].unique().tolist()
        if not tier_mols:
            continue
        subset = property_recommended.loc[property_recommended["mol_id"].isin(tier_mols)]
        prop_counts = subset.groupby("property_name")["mol_id"].nunique().to_dict() if not subset.empty else {}
        coverage[tier] = {
            "molecule_count": len(tier_mols),
            "property_coverage": {prop: count / len(tier_mols) for prop, count in sorted(prop_counts.items())},
        }
    return coverage


def _unresolved_refrigerants(seed_catalog: pd.DataFrame, resolution_df: pd.DataFrame, molecule_core: pd.DataFrame) -> list[dict[str, Any]]:
    if seed_catalog.empty or "entity_scope" not in seed_catalog.columns:
        return []

    refrigerant_seed_ids = set(
        seed_catalog.loc[seed_catalog["entity_scope"].astype(str) == "refrigerant", "seed_id"].tolist()
    )
    resolved_seed_ids = set(molecule_core["seed_id"].tolist()) if "seed_id" in molecule_core.columns else set()

    issues = []
    for seed_id in sorted(refrigerant_seed_ids - resolved_seed_ids):
        failed_rows = resolution_df.loc[
            (resolution_df["seed_id"] == seed_id) & (resolution_df["status"].isin(["failed", "warning"])),
            ["stage", "detail"],
        ]
        if failed_rows.empty:
            issues.append({"seed_id": seed_id, "stage": "resolution", "detail": "No resolved molecule row"})
            continue
        for row in failed_rows.to_dict(orient="records"):
            issues.append({"seed_id": seed_id, "stage": row["stage"], "detail": row["detail"]})

    seen = set()
    deduped = []
    for item in issues:
        key = (item["seed_id"], item["stage"], item["detail"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _inventory_property_gaps(
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    property_recommended: pd.DataFrame,
) -> dict[str, dict[str, dict[str, dict[str, int]]]]:
    tracked_properties = ["gwp_100yr", "odp", "ashrae_safety", "toxicity_class"]
    if seed_catalog.empty:
        return {}

    resolved = pd.merge(
        seed_catalog[["seed_id", "entity_scope", "coverage_tier"]],
        molecule_core[["seed_id", "mol_id"]] if not molecule_core.empty else pd.DataFrame(columns=["seed_id", "mol_id"]),
        on="seed_id",
        how="left",
    )
    available = (
        property_recommended.groupby("mol_id")["property_name"].apply(set).to_dict()
        if not property_recommended.empty
        else {}
    )

    report: dict[str, dict[str, dict[str, dict[str, int]]]] = {}
    for (entity_scope, coverage_tier), group in resolved.groupby(["entity_scope", "coverage_tier"], dropna=False):
        scope_key = _clean_str(entity_scope) or "unknown"
        tier_key = _clean_str(coverage_tier) or "unknown"
        scope_bucket = report.setdefault(scope_key, {})
        total = int(len(group))
        tier_report: dict[str, dict[str, int]] = {}
        for property_name in tracked_properties:
            present = 0
            for mol_id in group["mol_id"].tolist():
                if mol_id and property_name in available.get(mol_id, set()):
                    present += 1
            tier_report[property_name] = {
                "present_count": present,
                "missing_count": total - present,
                "total_count": total,
            }
        scope_bucket[tier_key] = tier_report
    return report


def _build_duckdb_index(paths: dict[str, Path]) -> None:
    db_path = paths["duckdb_path"]
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    parquet_map = {
        "source_manifest": paths["bronze_source_manifest"],
        "pending_sources": paths["bronze_pending_sources"],
        "seed_resolution": paths["bronze_seed_resolution"],
        "molecule_core": paths["silver_molecule_core"],
        "molecule_alias": paths["silver_molecule_alias"],
        "property_observation": paths["silver_property_observation"],
        "regulatory_status": paths["silver_regulatory_status"],
        "property_recommended": paths["gold_property_recommended"],
        "structure_features": paths["gold_structure_features"],
        "molecule_master": paths["gold_molecule_master"],
        "property_matrix": paths["gold_property_matrix"],
        "model_dataset_index": paths["gold_model_index"],
        "model_ready": paths["gold_model_ready"],
    }

    for table_name in DUCKDB_TABLES:
        parquet_path = parquet_map[table_name].resolve().as_posix()
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")

    supplemental_tables = {
        "property_dictionary": PROJECT_ROOT / "data" / "gold" / "property_dictionary.parquet",
        "property_canonical_map": PROJECT_ROOT / "data" / "gold" / "property_canonical_map.parquet",
        "unit_conversion_rules": PROJECT_ROOT / "data" / "gold" / "unit_conversion_rules.parquet",
        "property_source_priority_rules": PROJECT_ROOT / "data" / "gold" / "property_source_priority_rules.parquet",
        "property_modeling_readiness_rules": PROJECT_ROOT / "data" / "gold" / "property_modeling_readiness_rules.parquet",
        "property_governance_issues": PROJECT_ROOT / "data" / "gold" / "property_governance_issues.parquet",
        "property_observation_canonical": PROJECT_ROOT / "data" / "silver" / "property_observation_canonical.parquet",
        "property_recommended_canonical": PROJECT_ROOT / "data" / "gold" / "property_recommended_canonical.parquet",
        "property_recommended_canonical_strict": PROJECT_ROOT / "data" / "gold" / "property_recommended_canonical_strict.parquet",
        "property_recommended_canonical_review_queue": PROJECT_ROOT / "data" / "gold" / "property_recommended_canonical_review_queue.parquet",
    }
    for table_name, parquet_path in supplemental_tables.items():
        if parquet_path.exists():
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path.resolve().as_posix()}')")

    extension_manifest = PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422" / "extension_manifest.parquet"
    extension_root = PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422" / "tables"
    if extension_manifest.exists():
        con.execute("CREATE SCHEMA IF NOT EXISTS property_governance_ext")
        manifest_df = pd.read_parquet(extension_manifest).fillna("")
        for row in manifest_df.to_dict(orient="records"):
            table_name = _clean_str(row.get("table_name"))
            if not table_name:
                continue
            parquet_path = extension_root / f"{table_name}.parquet"
            if parquet_path.exists():
                con.execute(
                    f"CREATE TABLE property_governance_ext.{table_name} AS SELECT * FROM read_parquet('{parquet_path.resolve().as_posix()}')"
                )

    normalized_extension_tables = {
        "mixture_core": PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422" / "mixture_core.parquet",
        "mixture_component": PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422" / "mixture_component.parquet",
    }
    if any(path.exists() for path in normalized_extension_tables.values()):
        con.execute("CREATE SCHEMA IF NOT EXISTS extensions")
        for table_name, parquet_path in normalized_extension_tables.items():
            if parquet_path.exists():
                con.execute(
                    f"CREATE TABLE extensions.{table_name} AS SELECT * FROM read_parquet('{parquet_path.resolve().as_posix()}')"
                )
    con.close()


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def _property_observation_columns() -> list[str]:
    return [
        "observation_id",
        "mol_id",
        "property_name",
        "value",
        "value_num",
        "unit",
        "temperature",
        "pressure",
        "phase",
        "source_type",
        "source_name",
        "source_id",
        "method",
        "uncertainty",
        "quality_level",
        "assessment_version",
        "time_horizon",
        "year",
        "notes",
        "qc_status",
        "qc_flags",
        "canonical_feature_key",
        "standard_unit",
        "bundle_record_id",
        "source_priority_rank",
        "data_quality_score_100",
        "is_proxy_or_screening",
        "ml_use_status",
    ]


def _regulatory_status_columns() -> list[str]:
    return [
        "regulatory_status_id",
        "mol_id",
        "end_use",
        "jurisdiction",
        "acceptability",
        "listing_status",
        "retrofit_new",
        "use_conditions",
        "effective_date",
        "trade_names",
        "raw_substitute",
        "source_id",
        "source_name",
        "source_type",
    ]


def _pending_source_columns() -> list[str]:
    return [
        "pending_id",
        "seed_id",
        "r_number",
        "mol_id",
        "requested_source",
        "status",
        "detail",
        "required_env_var",
    ]


def _source_manifest_columns() -> list[str]:
    return [
        "source_id",
        "source_type",
        "source_name",
        "license",
        "retrieved_at",
        "checksum_sha256",
        "local_path",
        "parser_version",
        "upstream_url",
        "status",
    ]


def _property_recommended_columns() -> list[str]:
    return [
        "mol_id",
        "property_name",
        "value",
        "value_num",
        "unit",
        "selected_source_id",
        "selected_source_name",
        "selected_quality_level",
        "source_priority",
        "source_count",
        "conflict_flag",
        "conflict_detail",
    ]


def _molecule_core_columns() -> list[str]:
    return [
        "mol_id",
        "seed_id",
        "family",
        "canonical_smiles",
        "isomeric_smiles",
        "inchi",
        "inchikey",
        "inchikey_first_block",
        "formula",
        "molecular_weight",
        "charge",
        "heavy_atom_count",
        "stereo_flag",
        "ez_isomer",
        "pubchem_cid",
        "pubchem_query",
        "entity_scope",
        "model_inclusion",
        "status",
    ]


def _molecule_alias_columns() -> list[str]:
    return ["mol_id", "alias_type", "alias_value", "is_primary", "source_name"]
