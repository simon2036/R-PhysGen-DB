"""Helpers for the 2026-04-22 property governance bundle."""

from __future__ import annotations

import io
import json
import re
import zipfile
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.chemistry import standardize_smiles
from r_physgen_db.utils import ensure_directory, sha256_file, slugify, write_json

BUNDLE_FILE_NAME = "refrigerant_seed_database_20260422_property_governance_bundle.zip"
INNER_TABLE_ZIP_NAME = "refrigerant_seed_database_20260422_property_governance_csv_tables.zip"

FILE_MANIFEST_NAME = "file_manifest_20260422_property_governance.csv"
ROW_MANIFEST_NAME = "manifest_row_counts_20260422_property_governance.csv"
SUMMARY_NAME = "property_governance_update_summary_20260422.md"
WORKBOOK_QA_NAME = "workbook_QA_property_governance_20260422.txt"
UNRESOLVED_CURATION_FILE_NAME = "property_governance_20260422_unresolved_curations.csv"
CANONICAL_REVIEW_DECISION_FILE_NAME = "property_governance_20260422_canonical_review_decisions.csv"
PROXY_ACCEPTANCE_FILE_NAME = "property_governance_20260422_proxy_acceptance_rules.csv"

GOVERNANCE_TABLE_MAP = {
    "property_dictionary": "tbl_property_dictionary_v1",
    "property_canonical_map": "tbl_property_canonical_map_v1",
    "unit_conversion_rules": "tbl_unit_conversion_rules_v1",
    "property_source_priority_rules": "tbl_property_source_priority_rules_v1",
    "property_modeling_readiness_rules": "tbl_property_modeling_readiness_rules_v1",
    "property_governance_issues": "tbl_property_governance_issues_v1",
}

LEGACY_CANONICAL_MAP: dict[str, dict[str, Any]] = {
    "thermodynamic.normal_boiling_temperature": {
        "property_name": "boiling_point_c",
        "unit": "degC",
        "numeric_transform": lambda value: value - 273.15,
    },
    "thermodynamic.critical_temperature": {
        "property_name": "critical_temp_c",
        "unit": "degC",
        "numeric_transform": lambda value: value - 273.15,
    },
    "thermodynamic.critical_pressure": {
        "property_name": "critical_pressure_mpa",
        "unit": "MPa",
        "numeric_transform": lambda value: value / 1000.0,
    },
    "thermodynamic.critical_density": {
        "property_name": "critical_density_kgm3",
        "unit": "kg/m3",
        "numeric_transform": lambda value: value,
    },
    "molecular_descriptor.acentric_factor": {
        "property_name": "acentric_factor",
        "unit": "dimensionless",
        "numeric_transform": lambda value: value,
    },
    "environmental.gwp_20yr": {
        "property_name": "gwp_20yr",
        "unit": "dimensionless",
        "numeric_transform": lambda value: value,
    },
    "environmental.gwp_100yr": {
        "property_name": "gwp_100yr",
        "unit": "dimensionless",
        "numeric_transform": lambda value: value,
    },
    "environmental.odp": {
        "property_name": "odp",
        "unit": "dimensionless",
        "numeric_transform": lambda value: value,
    },
    "environmental.atmospheric_lifetime": {
        "property_name": "atmospheric_lifetime_yr",
        "unit": "yr",
        "numeric_transform": lambda value: value,
    },
    "safety.safety_group": {
        "property_name": "ashrae_safety",
        "unit": "class",
        "numeric_transform": None,
    },
}


@dataclass(frozen=True)
class PropertyGovernanceBundle:
    bundle_path: Path
    bundle_sha256: str
    file_manifest: pd.DataFrame
    row_manifest: pd.DataFrame
    tables: dict[str, pd.DataFrame]
    texts: dict[str, str]


def default_bundle_path(project_root: Path) -> Path:
    return project_root / "methods" / BUNDLE_FILE_NAME


def default_unresolved_curation_path(project_root: Path) -> Path:
    return project_root / "data" / "raw" / "manual" / UNRESOLVED_CURATION_FILE_NAME


def default_canonical_review_decision_path(project_root: Path) -> Path:
    return project_root / "data" / "raw" / "manual" / CANONICAL_REVIEW_DECISION_FILE_NAME


def default_proxy_acceptance_path(project_root: Path) -> Path:
    return project_root / "data" / "raw" / "manual" / PROXY_ACCEPTANCE_FILE_NAME


def integrate_property_governance_bundle(
    *,
    bundle_path: Path,
    output_root: Path,
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    alias_df: pd.DataFrame,
    parser_version: str,
    retrieved_at: str,
    unresolved_curation_path: Path | None = None,
    canonical_review_decision_path: Path | None = None,
    proxy_acceptance_path: Path | None = None,
) -> dict[str, Any]:
    if not bundle_path.exists():
        return _empty_integration()

    bundle = load_property_governance_bundle(bundle_path)
    curation_path = unresolved_curation_path or default_unresolved_curation_path(output_root)
    unresolved_curations = load_property_governance_unresolved_curations(curation_path=curation_path, bundle=bundle)
    review_decision_path = canonical_review_decision_path or default_canonical_review_decision_path(output_root)
    proxy_policy_path = proxy_acceptance_path or default_proxy_acceptance_path(output_root)
    outputs = _output_paths(output_root)
    written_files: list[tuple[str, str, str, Path, str]] = []

    extension_manifest = _mirror_extension_tables(bundle, outputs["extension_tables"], written_files)
    _write_frame_parquet(extension_manifest, outputs["extension_manifest"])
    written_files.append(
        (
            "source_property_governance_extension_manifest",
            "derived_harmonized",
            "Property Governance Extension Manifest",
            outputs["extension_manifest"],
            "",
        )
    )

    crosswalk = _build_substance_crosswalk(
        bundle=bundle,
        molecule_core=molecule_core,
        alias_df=alias_df,
        unresolved_curations=unresolved_curations,
    )
    _write_frame_parquet(crosswalk, outputs["crosswalk"])
    written_files.append(
        (
            "source_property_governance_crosswalk",
            "derived_harmonized",
            "Property Governance Substance Crosswalk",
            outputs["crosswalk"],
            "",
        )
    )

    unresolved = crosswalk.loc[crosswalk["match_status"] == "unresolved"].copy()
    _write_frame_parquet(unresolved, outputs["unresolved"])
    written_files.append(
        (
            "source_property_governance_unresolved_substances",
            "derived_harmonized",
            "Property Governance Unresolved Pure Substances",
            outputs["unresolved"],
            "",
        )
    )

    generated_seed_rows = _build_generated_seed_rows(crosswalk)
    _write_frame_csv(generated_seed_rows, outputs["generated_seed_catalog"])
    written_files.append(
        (
            "source_property_governance_seed_supplement",
            "manual_catalog",
            "Generated Property Governance Seed Supplement",
            outputs["generated_seed_catalog"],
            "",
        )
    )

    generated_molecule_rows = _build_generated_molecule_rows(crosswalk)
    generated_alias_rows = _build_generated_alias_rows(crosswalk, alias_df)

    sources_df = bundle.tables["tbl_sources"].copy()
    canonical_observation = _build_canonical_observation(bundle=bundle, crosswalk=crosswalk, sources_df=sources_df)
    _write_frame_parquet(canonical_observation, outputs["canonical_observation"])
    written_files.append(
        (
            "source_property_governance_canonical_observation",
            "derived_harmonized",
            "Property Governance Canonical Observation Layer",
            outputs["canonical_observation"],
            "",
        )
    )

    canonical_recommended = select_canonical_recommended(canonical_observation)
    _write_frame_parquet(canonical_recommended, outputs["canonical_recommended"])
    written_files.append(
        (
            "source_property_governance_canonical_recommended",
            "derived_harmonized",
            "Property Governance Canonical Recommended Values",
            outputs["canonical_recommended"],
            "",
        )
    )

    legacy_property_rows = build_legacy_property_rows(canonical_observation)
    governance_outputs: dict[str, pd.DataFrame] = {}

    for output_name, table_name in GOVERNANCE_TABLE_MAP.items():
        frame = _prepare_governance_output(output_name, bundle.tables[table_name].copy())
        governance_outputs[output_name] = frame
        _write_frame_parquet(frame, outputs[output_name])
        written_files.append(
            (
                f"source_{output_name}",
                "derived_harmonized",
                output_name.replace("_", " ").title(),
                outputs[output_name],
                "",
            )
        )

    canonical_review_decisions = load_property_governance_canonical_review_decisions(
        decision_path=review_decision_path,
        canonical_recommended=canonical_recommended,
    )
    proxy_acceptance_rules = load_property_governance_proxy_acceptance_rules(
        rule_path=proxy_policy_path,
        canonical_recommended=canonical_recommended,
    )

    canonical_recommended_strict = select_canonical_recommended_strict(
        canonical_recommended=canonical_recommended,
        readiness_rules=governance_outputs.get("property_modeling_readiness_rules", pd.DataFrame()),
        proxy_acceptance_rules=proxy_acceptance_rules,
    )
    _write_frame_parquet(canonical_recommended_strict, outputs["canonical_recommended_strict"])
    written_files.append(
        (
            "source_property_governance_canonical_recommended_strict",
            "derived_harmonized",
            "Property Governance Canonical Recommended Values Strict",
            outputs["canonical_recommended_strict"],
            "",
        )
    )

    canonical_review_queue = build_canonical_recommended_review_queue(
        canonical_recommended=canonical_recommended,
        readiness_rules=governance_outputs.get("property_modeling_readiness_rules", pd.DataFrame()),
        review_decisions=canonical_review_decisions,
        proxy_acceptance_rules=proxy_acceptance_rules,
    )
    _write_frame_parquet(canonical_review_queue, outputs["canonical_recommended_review_queue"])
    written_files.append(
        (
            "source_property_governance_canonical_review_queue",
            "derived_harmonized",
            "Property Governance Canonical Review Queue",
            outputs["canonical_recommended_review_queue"],
            "",
        )
    )

    mixture_core, mixture_component = _build_normalized_mixture_tables(bundle=bundle, alias_df=alias_df, crosswalk=crosswalk)
    _write_frame_parquet(mixture_core, outputs["mixture_core"])
    _write_frame_parquet(mixture_component, outputs["mixture_component"])
    written_files.extend(
        [
            (
                "source_property_governance_mixture_core",
                "derived_harmonized",
                "Property Governance Normalized Mixture Core",
                outputs["mixture_core"],
                "",
            ),
            (
                "source_property_governance_mixture_component",
                "derived_harmonized",
                "Property Governance Normalized Mixture Component",
                outputs["mixture_component"],
                "",
            ),
        ]
    )

    audit_payload = _build_audit_payload(
        bundle=bundle,
        extension_manifest=extension_manifest,
        crosswalk=crosswalk,
        canonical_observation=canonical_observation,
        canonical_recommended=canonical_recommended,
        canonical_recommended_strict=canonical_recommended_strict,
        canonical_review_queue=canonical_review_queue,
        canonical_review_decisions=canonical_review_decisions,
        proxy_acceptance_rules=proxy_acceptance_rules,
    )
    write_json(outputs["audit"], audit_payload)
    written_files.append(
        (
            "source_property_governance_bundle_audit",
            "derived_harmonized",
            "Property Governance Bundle Audit",
            outputs["audit"],
            "",
        )
    )

    source_manifest_rows = _bundle_source_manifest_entries(
        bundle=bundle,
        parser_version=parser_version,
        retrieved_at=retrieved_at,
        written_files=written_files,
        unresolved_curation_path=curation_path,
        canonical_review_decision_path=review_decision_path,
        proxy_acceptance_path=proxy_policy_path,
    )

    resolution_rows = _build_resolution_rows(crosswalk)

    return {
        "bundle_present": True,
        "generated_seed_rows": generated_seed_rows,
        "generated_molecule_rows": generated_molecule_rows,
        "generated_alias_rows": generated_alias_rows,
        "legacy_property_rows": legacy_property_rows,
        "canonical_observation": canonical_observation,
        "canonical_recommended": canonical_recommended,
        "canonical_recommended_strict": canonical_recommended_strict,
        "canonical_review_queue": canonical_review_queue,
        "crosswalk": crosswalk,
        "unresolved": unresolved,
        "source_manifest_rows": source_manifest_rows,
        "resolution_rows": resolution_rows,
        "audit": audit_payload,
        "extension_manifest": extension_manifest,
        "mixture_core": mixture_core,
        "mixture_component": mixture_component,
        "unresolved_curations": unresolved_curations,
        "canonical_review_decisions": canonical_review_decisions,
        "proxy_acceptance_rules": proxy_acceptance_rules,
    }


def load_property_governance_bundle(bundle_path: Path) -> PropertyGovernanceBundle:
    if not bundle_path.exists():
        raise FileNotFoundError(bundle_path)

    with zipfile.ZipFile(bundle_path) as outer:
        file_manifest = pd.read_csv(io.BytesIO(outer.read(FILE_MANIFEST_NAME)))
        row_manifest = pd.read_csv(io.BytesIO(outer.read(ROW_MANIFEST_NAME)))
        texts = {
            SUMMARY_NAME: outer.read(SUMMARY_NAME).decode("utf-8"),
            WORKBOOK_QA_NAME: outer.read(WORKBOOK_QA_NAME).decode("utf-8"),
            FILE_MANIFEST_NAME: outer.read(FILE_MANIFEST_NAME).decode("utf-8"),
            ROW_MANIFEST_NAME: outer.read(ROW_MANIFEST_NAME).decode("utf-8"),
        }
        inner = zipfile.ZipFile(io.BytesIO(outer.read(INNER_TABLE_ZIP_NAME)))
        tables: dict[str, pd.DataFrame] = {}
        for member_name in inner.namelist():
            table_name = member_name.removesuffix(".csv")
            tables[table_name] = pd.read_csv(inner.open(member_name))

    _validate_row_manifest(row_manifest=row_manifest, tables=tables)
    return PropertyGovernanceBundle(
        bundle_path=bundle_path,
        bundle_sha256=sha256_file(bundle_path),
        file_manifest=file_manifest,
        row_manifest=row_manifest,
        tables=tables,
        texts=texts,
    )


def load_property_governance_unresolved_curations(*, curation_path: Path, bundle: PropertyGovernanceBundle) -> pd.DataFrame:
    if not curation_path.exists():
        return pd.DataFrame(columns=_unresolved_curation_columns())

    frame = pd.read_csv(curation_path).fillna("")
    missing = [column for column in _unresolved_curation_csv_columns() if column not in frame.columns]
    if missing:
        raise ValueError(f"Property governance unresolved curations missing columns: {missing}")

    bundle_lookup = {
        _clean_str(record.get("substance_id")): record
        for record in _bundle_substance_records(bundle).to_dict(orient="records")
        if _clean_str(record.get("substance_id"))
    }

    normalized_rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        substance_id = _clean_str(record.get("substance_id"))
        if not substance_id:
            raise ValueError("Property governance unresolved curation rows require substance_id")
        bundle_record = bundle_lookup.get(substance_id)
        if bundle_record is None:
            raise ValueError(f"Property governance unresolved curation references unknown substance_id: {substance_id}")
        if _record_has_structure(bundle_record):
            raise ValueError(f"Property governance unresolved curation targets already structured bundle row: {substance_id}")

        confidence = _clean_str(record.get("resolution_confidence")).lower()
        if confidence != "high":
            raise ValueError(f"Property governance unresolved curation must use resolution_confidence=high: {substance_id}")

        curation_r_number = _clean_str(record.get("refrigerant_number"))
        bundle_r_number = _clean_str(bundle_record.get("refrigerant_number"))
        if curation_r_number and bundle_r_number and curation_r_number != bundle_r_number:
            raise ValueError(
                f"Property governance unresolved curation refrigerant_number mismatch for {substance_id}: {curation_r_number} != {bundle_r_number}"
            )

        curation_cas = _clean_str(record.get("cas_number"))
        bundle_cas = _clean_str(bundle_record.get("cas_number"))
        if curation_cas and bundle_cas and curation_cas != bundle_cas:
            raise ValueError(f"Property governance unresolved curation CAS mismatch for {substance_id}: {curation_cas} != {bundle_cas}")

        standardized = _validate_curation_structure(record=record, bundle_record=bundle_record, substance_id=substance_id)
        normalized_rows.append(
            {
                "substance_id": substance_id,
                "refrigerant_number": bundle_r_number or curation_r_number,
                "cas_number": bundle_cas or curation_cas,
                "canonical_smiles": standardized["canonical_smiles"],
                "isomeric_smiles": standardized["isomeric_smiles"],
                "inchi": standardized["inchi"],
                "inchikey": standardized["inchikey"],
                "resolution_source": _clean_str(record.get("resolution_source")),
                "resolution_source_url": _clean_str(record.get("resolution_source_url")),
                "resolution_confidence": "high",
                "notes": _clean_str(record.get("notes")),
                "standardized_formula": standardized["formula"],
                "standardized_molecular_weight": standardized["molecular_weight"],
            }
        )
    return pd.DataFrame(normalized_rows, columns=_unresolved_curation_columns()).sort_values("substance_id").reset_index(drop=True)


def load_property_governance_canonical_review_decisions(
    *,
    decision_path: Path,
    canonical_recommended: pd.DataFrame,
) -> pd.DataFrame:
    if not decision_path.exists():
        return pd.DataFrame(columns=_canonical_review_decision_columns())

    frame = pd.read_csv(decision_path).fillna("")
    missing = [column for column in _canonical_review_decision_csv_columns() if column not in frame.columns]
    if missing:
        raise ValueError(f"Property governance canonical review decisions missing columns: {missing}")
    if frame.duplicated(subset=["mol_id", "canonical_feature_key"], keep=False).any():
        duplicates = (
            frame.loc[frame.duplicated(subset=["mol_id", "canonical_feature_key"], keep=False), ["mol_id", "canonical_feature_key"]]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        raise ValueError(f"Property governance canonical review decisions contain duplicate keys: {duplicates}")

    recommendation_lookup = {
        (_clean_str(record.get("mol_id")), _clean_str(record.get("canonical_feature_key"))): record
        for record in canonical_recommended.fillna("").to_dict(orient="records")
        if _clean_str(record.get("mol_id")) and _clean_str(record.get("canonical_feature_key"))
    }
    normalized_rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        mol_id = _clean_str(record.get("mol_id"))
        canonical_feature_key = _clean_str(record.get("canonical_feature_key"))
        if not mol_id or not canonical_feature_key:
            raise ValueError("Property governance canonical review decision rows require mol_id and canonical_feature_key")

        selected = recommendation_lookup.get((mol_id, canonical_feature_key))
        if selected is None:
            raise ValueError(
                "Property governance canonical review decision references unknown canonical recommendation: "
                f"{mol_id} / {canonical_feature_key}"
            )

        review_reason = _clean_str(record.get("review_reason"))
        decision_action = _clean_str(record.get("decision_action"))
        allowed_reason_map = {
            "accept_selected_source": {"top_rank_conflict", "source_divergence"},
            "accept_out_of_strict": {"below_minimum_quality", "non_numeric_selected_value", "not_ml_relevant"},
        }
        if decision_action not in allowed_reason_map:
            raise ValueError(
                "Property governance canonical review decision decision_action must be accept_selected_source or accept_out_of_strict: "
                f"{mol_id} / {canonical_feature_key}"
            )
        if review_reason not in allowed_reason_map[decision_action]:
            raise ValueError(
                "Property governance canonical review decision review_reason is not compatible with decision_action "
                f"{decision_action}: {mol_id} / {canonical_feature_key}"
            )
        if review_reason == "top_rank_conflict" and not _coerce_bool_flag(selected.get("conflict_flag")):
            raise ValueError(
                "Property governance canonical review decision targets a row without top-rank conflict: "
                f"{mol_id} / {canonical_feature_key}"
            )
        if review_reason == "source_divergence" and not _coerce_bool_flag(selected.get("source_divergence_flag")):
            raise ValueError(
                "Property governance canonical review decision targets a row without source divergence: "
                f"{mol_id} / {canonical_feature_key}"
            )

        expected_source_id = _clean_str(record.get("expected_selected_source_id"))
        actual_source_id = _clean_str(selected.get("selected_source_id"))
        if not expected_source_id or expected_source_id != actual_source_id:
            raise ValueError(
                "Property governance canonical review decision selected source mismatch for "
                f"{mol_id} / {canonical_feature_key}: {expected_source_id} != {actual_source_id}"
            )

        expected_value = _clean_str(record.get("expected_selected_value"))
        if not _matches_expected_selected_value(
            expected_value=expected_value,
            actual_value=_clean_str(selected.get("value")),
            actual_value_num=_optional_float(selected.get("value_num")),
        ):
            raise ValueError(
                "Property governance canonical review decision selected value mismatch for "
                f"{mol_id} / {canonical_feature_key}: expected {expected_value} but found {_clean_str(selected.get('value'))}"
            )

        resolution_basis = _clean_str(record.get("resolution_basis"))
        if not resolution_basis:
            raise ValueError(
                "Property governance canonical review decision resolution_basis is required: "
                f"{mol_id} / {canonical_feature_key}"
            )

        normalized_rows.append(
            {
                "mol_id": mol_id,
                "canonical_feature_key": canonical_feature_key,
                "review_reason": review_reason,
                "decision_action": decision_action,
                "expected_selected_source_id": expected_source_id,
                "expected_selected_value": expected_value,
                "resolution_basis": resolution_basis,
                "resolution_source_url": _clean_str(record.get("resolution_source_url")),
                "notes": _clean_str(record.get("notes")),
            }
        )
    return (
        pd.DataFrame(normalized_rows, columns=_canonical_review_decision_columns())
        .sort_values(["review_reason", "mol_id", "canonical_feature_key"])
        .reset_index(drop=True)
    )


def load_property_governance_proxy_acceptance_rules(
    *,
    rule_path: Path,
    canonical_recommended: pd.DataFrame,
) -> pd.DataFrame:
    if not rule_path.exists():
        return pd.DataFrame(columns=_proxy_acceptance_rule_columns())

    frame = pd.read_csv(rule_path).fillna("")
    missing = [column for column in _proxy_acceptance_rule_csv_columns() if column not in frame.columns]
    if missing:
        raise ValueError(f"Property governance proxy acceptance rules missing columns: {missing}")
    if frame.duplicated(subset=["canonical_feature_key", "selected_source_id"], keep=False).any():
        duplicates = (
            frame.loc[
                frame.duplicated(subset=["canonical_feature_key", "selected_source_id"], keep=False),
                ["canonical_feature_key", "selected_source_id"],
            ]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        raise ValueError(f"Property governance proxy acceptance rules contain duplicate feature/source pairs: {duplicates}")

    normalized_rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        policy_id = _clean_str(record.get("proxy_policy_id"))
        canonical_feature_key = _clean_str(record.get("canonical_feature_key"))
        selected_source_id = _clean_str(record.get("selected_source_id"))
        if not policy_id or not canonical_feature_key or not selected_source_id:
            raise ValueError(
                "Property governance proxy acceptance rules require proxy_policy_id, canonical_feature_key, and selected_source_id"
            )
        allow_flag = _coerce_int(record.get("allow_in_strict_if_proxy_only"), 0)
        if allow_flag != 1:
            raise ValueError(
                "Property governance proxy acceptance rules currently require allow_in_strict_if_proxy_only=1: "
                f"{policy_id}"
            )
        rationale = _clean_str(record.get("rationale"))
        if not rationale:
            raise ValueError(f"Property governance proxy acceptance rule rationale is required: {policy_id}")

        matched_rows = canonical_recommended.loc[
            canonical_recommended["canonical_feature_key"].astype(str).eq(canonical_feature_key)
            & canonical_recommended["selected_source_id"].astype(str).eq(selected_source_id)
        ].copy()
        if matched_rows.empty:
            raise ValueError(
                "Property governance proxy acceptance rule references no current canonical recommendations: "
                f"{policy_id}"
            )
        if not matched_rows["is_proxy_or_screening"].fillna(False).astype(bool).all():
            raise ValueError(
                "Property governance proxy acceptance rule targets non-proxy selected rows: "
                f"{policy_id}"
            )
        if not matched_rows["proxy_only_flag"].fillna(False).astype(bool).all():
            raise ValueError(
                "Property governance proxy acceptance rule targets rows that still have non-proxy candidates: "
                f"{policy_id}"
            )

        normalized_rows.append(
            {
                "proxy_policy_id": policy_id,
                "canonical_feature_key": canonical_feature_key,
                "selected_source_id": selected_source_id,
                "allow_in_strict_if_proxy_only": 1,
                "rationale": rationale,
                "notes": _clean_str(record.get("notes")),
            }
        )
    return (
        pd.DataFrame(normalized_rows, columns=_proxy_acceptance_rule_columns())
        .sort_values(["canonical_feature_key", "selected_source_id"])
        .reset_index(drop=True)
    )


def select_canonical_recommended(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_canonical_recommended_columns())

    rows: list[dict[str, Any]] = []
    working = df.loc[(df["value_num"].notna()) | (df["value"].astype(str).str.strip() != "")].copy()
    if working.empty:
        return pd.DataFrame(columns=_canonical_recommended_columns())

    for (mol_id, canonical_feature_key), group in working.groupby(["mol_id", "canonical_feature_key"], sort=True):
        group = group.copy()
        group["proxy_sort"] = group["is_proxy_or_screening"].fillna(0).astype(int)
        group["source_priority_sort"] = pd.to_numeric(group["source_priority_rank"], errors="coerce").fillna(9999).astype(int)
        group["quality_sort"] = pd.to_numeric(group["data_quality_score_100"], errors="coerce").fillna(0.0)
        group = group.sort_values(
            by=["proxy_sort", "source_priority_sort", "quality_sort"],
            ascending=[True, True, False],
            kind="stable",
        )
        selected = group.iloc[0]
        top_rank_group = _top_rank_candidate_group(group)
        conflict_flag, conflict_detail = _conflict_detail_for_group(top_rank_group, value_col="value_num", text_col="value")
        source_divergence_flag, source_divergence_detail = _conflict_detail_for_group(group, value_col="value_num", text_col="value")
        nonproxy_candidate_count = int(group["proxy_sort"].eq(0).sum())
        proxy_only_flag = bool(_coerce_bool_flag(selected.get("is_proxy_or_screening")) and nonproxy_candidate_count == 0)
        rows.append(
            {
                "mol_id": mol_id,
                "canonical_feature_key": canonical_feature_key,
                "canonical_property_id": _clean_str(selected.get("canonical_property_id")),
                "canonical_property_group": _clean_str(selected.get("canonical_property_group")),
                "canonical_property_name": _clean_str(selected.get("canonical_property_name")),
                "value": _clean_str(selected.get("value")),
                "value_num": _optional_float(selected.get("value_num")),
                "unit": _clean_str(selected.get("unit")),
                "selected_source_id": _clean_str(selected.get("source_id")),
                "selected_source_name": _clean_str(selected.get("source_name")),
                "selected_quality_level": _clean_str(selected.get("quality_level")),
                "source_priority_rank": _coerce_int(selected.get("source_priority_rank"), 9999),
                "data_quality_score_100": _optional_float(selected.get("data_quality_score_100")),
                "is_proxy_or_screening": _coerce_bool_flag(selected.get("is_proxy_or_screening")),
                "ml_use_status": _clean_str(selected.get("ml_use_status")),
                "proxy_only_flag": proxy_only_flag,
                "nonproxy_candidate_count": nonproxy_candidate_count,
                "top_rank_source_count": int(len(top_rank_group)),
                "source_divergence_flag": source_divergence_flag,
                "source_divergence_detail": source_divergence_detail,
                "source_count": int(len(group)),
                "conflict_flag": conflict_flag,
                "conflict_detail": conflict_detail,
            }
        )
    return pd.DataFrame(rows, columns=_canonical_recommended_columns()).sort_values(["mol_id", "canonical_feature_key"]).reset_index(drop=True)


def select_canonical_recommended_strict(
    *,
    canonical_recommended: pd.DataFrame,
    readiness_rules: pd.DataFrame,
    proxy_acceptance_rules: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if canonical_recommended.empty:
        return pd.DataFrame(columns=_canonical_recommended_strict_columns())

    merged = _merge_canonical_readiness(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness_rules,
        proxy_acceptance_rules=proxy_acceptance_rules,
    )
    filtered = merged.loc[merged["strict_accept"]].copy()
    if filtered.empty:
        return pd.DataFrame(columns=_canonical_recommended_strict_columns())

    filtered["strict_accept"] = True
    return filtered[_canonical_recommended_strict_columns()].sort_values(["mol_id", "canonical_feature_key"]).reset_index(drop=True)


def build_canonical_recommended_review_queue(
    *,
    canonical_recommended: pd.DataFrame,
    readiness_rules: pd.DataFrame,
    review_decisions: pd.DataFrame | None = None,
    proxy_acceptance_rules: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if canonical_recommended.empty:
        return pd.DataFrame(columns=_canonical_review_queue_columns())

    merged = _merge_canonical_readiness(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness_rules,
        proxy_acceptance_rules=proxy_acceptance_rules,
    )
    merged["review_triggers"] = merged.apply(_review_triggers, axis=1)
    merged["review_reason"] = merged["review_triggers"].astype(str).str.split(";").str[0].fillna("")
    merged["review_priority"] = merged.apply(_review_priority, axis=1)
    queue = merged.loc[merged["review_reason"].astype(str) != ""].copy()
    if queue.empty:
        return pd.DataFrame(columns=_canonical_review_queue_columns())
    if review_decisions is not None and not review_decisions.empty:
        decision_keys = review_decisions[["mol_id", "canonical_feature_key", "review_reason"]].drop_duplicates().copy()
        open_keys = queue[["mol_id", "canonical_feature_key", "review_triggers"]].drop_duplicates().copy()
        stale_decisions = decision_keys.merge(open_keys, on=["mol_id", "canonical_feature_key"], how="left", indicator=True)
        stale_decisions = stale_decisions.loc[stale_decisions["_merge"] != "both", ["mol_id", "canonical_feature_key"]]
        if not stale_decisions.empty:
            raise ValueError(
                "Property governance canonical review decisions reference rows that are no longer in the open review queue: "
                f"{stale_decisions.to_dict(orient='records')}"
            )
        stale_reason_rows: list[dict[str, str]] = []
        decision_state = decision_keys.merge(open_keys, on=["mol_id", "canonical_feature_key"], how="left")
        for record in decision_state.to_dict(orient="records"):
            triggers = {
                _clean_str(item)
                for item in _clean_str(record.get("review_triggers")).split(";")
                if _clean_str(item)
            }
            review_reason = _clean_str(record.get("review_reason"))
            if review_reason and review_reason not in triggers:
                stale_reason_rows.append(
                    {
                        "mol_id": _clean_str(record.get("mol_id")),
                        "canonical_feature_key": _clean_str(record.get("canonical_feature_key")),
                        "review_reason": review_reason,
                    }
                )
        if stale_reason_rows:
            raise ValueError(
                "Property governance canonical review decisions reference queue rows with changed review reasons: "
                f"{stale_reason_rows}"
            )
        queue = queue.merge(
            review_decisions[["mol_id", "canonical_feature_key"]].drop_duplicates().assign(_closed_by_review_decision=True),
            on=["mol_id", "canonical_feature_key"],
            how="left",
        )
        queue = queue.loc[~queue["_closed_by_review_decision"].fillna(False).astype(bool)].drop(columns=["_closed_by_review_decision"])
    if queue.empty:
        return pd.DataFrame(columns=_canonical_review_queue_columns())
    queue["review_priority_rank"] = queue["review_priority"].map(_review_priority_rank).fillna(999).astype(int)
    return (
        queue[_canonical_review_queue_columns() + ["review_priority_rank"]]
        .sort_values(["review_priority_rank", "mol_id", "canonical_feature_key"])
        .drop(columns=["review_priority_rank"])
        .reset_index(drop=True)
    )


def _merge_canonical_readiness(
    *,
    canonical_recommended: pd.DataFrame,
    readiness_rules: pd.DataFrame,
    proxy_acceptance_rules: pd.DataFrame | None = None,
) -> pd.DataFrame:
    readiness = readiness_rules.copy().fillna("")
    defaults: dict[str, Any] = {
        "readiness_rule_id": "",
        "use_as_ml_feature": 0,
        "use_as_ml_target": 0,
        "minimum_quality_score": None,
        "exclude_if_proxy_or_screening": 0,
        "preferred_standard_unit": "",
        "normalization_recommendation": "",
        "missing_value_strategy": "",
        "notes": "",
    }
    for column, default in defaults.items():
        if column not in readiness.columns:
            readiness[column] = default
    readiness["readiness_notes"] = readiness["notes"].astype(str)
    readiness = readiness[
        [
            "canonical_feature_key",
            "canonical_property_id",
            "readiness_rule_id",
            "use_as_ml_feature",
            "use_as_ml_target",
            "minimum_quality_score",
            "exclude_if_proxy_or_screening",
            "preferred_standard_unit",
            "normalization_recommendation",
            "missing_value_strategy",
            "readiness_notes",
        ]
    ].drop_duplicates(subset=["canonical_feature_key", "canonical_property_id"], keep="first")

    merged = canonical_recommended.merge(
        readiness,
        on=["canonical_feature_key", "canonical_property_id"],
        how="left",
    )
    proxy_rules = proxy_acceptance_rules.copy() if proxy_acceptance_rules is not None else pd.DataFrame()
    if proxy_rules.empty:
        proxy_rules = pd.DataFrame(columns=_proxy_acceptance_rule_columns())
    else:
        for column, default in {
            "proxy_policy_id": "",
            "allow_in_strict_if_proxy_only": 0,
            "rationale": "",
            "notes": "",
        }.items():
            if column not in proxy_rules.columns:
                proxy_rules[column] = default
        proxy_rules["allow_in_strict_if_proxy_only"] = (
            pd.to_numeric(proxy_rules["allow_in_strict_if_proxy_only"], errors="coerce").fillna(0).astype(int)
        )
        proxy_rules = proxy_rules[
            [
                "canonical_feature_key",
                "selected_source_id",
                "proxy_policy_id",
                "allow_in_strict_if_proxy_only",
                "rationale",
                "notes",
            ]
        ].drop_duplicates(subset=["canonical_feature_key", "selected_source_id"], keep="first")
    merged = merged.merge(
        proxy_rules,
        on=["canonical_feature_key", "selected_source_id"],
        how="left",
    )
    for column in [
        "use_as_ml_feature",
        "use_as_ml_target",
        "exclude_if_proxy_or_screening",
    ]:
        merged[column] = pd.to_numeric(merged[column], errors="coerce").fillna(0).astype(int)
    merged["minimum_quality_score"] = pd.to_numeric(merged["minimum_quality_score"], errors="coerce")
    merged["allow_in_strict_if_proxy_only"] = pd.to_numeric(
        merged["allow_in_strict_if_proxy_only"], errors="coerce"
    ).fillna(0).astype(int)
    merged["proxy_policy_id"] = merged["proxy_policy_id"].fillna("").astype(str)
    merged["proxy_policy_rationale"] = merged["rationale"].fillna("").astype(str)
    merged["proxy_policy_notes"] = merged["notes"].fillna("").astype(str)
    merged = merged.drop(columns=["rationale", "notes"], errors="ignore")
    standard_strict_accept = (
        merged["value_num"].notna()
        & (merged["use_as_ml_feature"].eq(1) | merged["use_as_ml_target"].eq(1))
        & merged["minimum_quality_score"].notna()
        & (pd.to_numeric(merged["data_quality_score_100"], errors="coerce") >= merged["minimum_quality_score"])
        & (~merged["exclude_if_proxy_or_screening"].eq(1) | ~merged["is_proxy_or_screening"].fillna(False).astype(bool))
    )
    proxy_policy_accept = (
        merged["value_num"].notna()
        & (merged["use_as_ml_feature"].eq(1) | merged["use_as_ml_target"].eq(1))
        & merged["minimum_quality_score"].notna()
        & (pd.to_numeric(merged["data_quality_score_100"], errors="coerce") >= merged["minimum_quality_score"])
        & merged["exclude_if_proxy_or_screening"].eq(1)
        & merged["is_proxy_or_screening"].fillna(False).astype(bool)
        & merged["proxy_only_flag"].fillna(False).astype(bool)
        & merged["allow_in_strict_if_proxy_only"].eq(1)
    )
    merged["strict_accept_basis"] = ""
    merged.loc[standard_strict_accept, "strict_accept_basis"] = "standard"
    merged.loc[~standard_strict_accept & proxy_policy_accept, "strict_accept_basis"] = "proxy_only_policy"
    merged["strict_accept"] = standard_strict_accept | proxy_policy_accept
    merged["strict_rejection_reason"] = merged.apply(_strict_rejection_reason, axis=1)
    return merged


def _strict_rejection_reason(row: pd.Series) -> str:
    strict_accept = row.get("strict_accept")
    if not pd.isna(strict_accept) and bool(strict_accept):
        return ""
    if pd.isna(row.get("value_num")):
        return "non_numeric_selected_value"
    if not (_coerce_int(row.get("use_as_ml_feature"), 0) == 1 or _coerce_int(row.get("use_as_ml_target"), 0) == 1):
        return "not_ml_relevant"
    if pd.isna(row.get("minimum_quality_score")):
        return "missing_readiness_rule"
    quality = pd.to_numeric(row.get("data_quality_score_100"), errors="coerce")
    minimum = pd.to_numeric(row.get("minimum_quality_score"), errors="coerce")
    if pd.notna(minimum) and (pd.isna(quality) or float(quality) < float(minimum)):
        return "below_minimum_quality"
    if _coerce_int(row.get("exclude_if_proxy_or_screening"), 0) == 1 and _coerce_bool_flag(row.get("is_proxy_or_screening")):
        return "proxy_selected"
    return "strict_filter_rejected"


def _review_triggers(row: pd.Series) -> str:
    triggers: list[str] = []
    if _coerce_bool_flag(row.get("conflict_flag")):
        triggers.append("top_rank_conflict")
    if _coerce_bool_flag(row.get("source_divergence_flag")):
        triggers.append("source_divergence")
    rejection_reason = _clean_str(row.get("strict_rejection_reason"))
    if rejection_reason:
        triggers.append(rejection_reason)
    return ";".join(dict.fromkeys(triggers))


def _review_priority(row: pd.Series) -> str:
    if _coerce_bool_flag(row.get("conflict_flag")):
        return "critical"
    if _coerce_bool_flag(row.get("source_divergence_flag")):
        return "high"
    rejection_reason = _clean_str(row.get("strict_rejection_reason"))
    if rejection_reason in {"proxy_selected", "below_minimum_quality"}:
        return "medium"
    if rejection_reason:
        return "low"
    return ""


def _review_priority_rank(value: Any) -> int:
    return {
        "critical": 1,
        "high": 2,
        "medium": 3,
        "low": 4,
    }.get(_clean_str(value).lower(), 999)


def _top_rank_candidate_group(group: pd.DataFrame) -> pd.DataFrame:
    if group.empty:
        return group
    min_proxy = int(group["proxy_sort"].min())
    candidates = group.loc[group["proxy_sort"].eq(min_proxy)].copy()
    min_rank = int(candidates["source_priority_sort"].min())
    return candidates.loc[candidates["source_priority_sort"].eq(min_rank)].copy()


def build_legacy_property_rows(canonical_observation: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if canonical_observation.empty:
        return rows

    for item in canonical_observation.to_dict(orient="records"):
        mapping = LEGACY_CANONICAL_MAP.get(_clean_str(item.get("canonical_feature_key")))
        if mapping is None:
            continue

        value_num = _optional_float(item.get("value_num"))
        transform = mapping["numeric_transform"]
        legacy_value_num = transform(value_num) if value_num is not None and transform is not None else None
        if legacy_value_num is None:
            legacy_value = _clean_str(item.get("value"))
        else:
            legacy_value = _format_numeric_text(legacy_value_num)

        rows.append(
            {
                "observation_id": None,
                "mol_id": _clean_str(item.get("mol_id")),
                "property_name": mapping["property_name"],
                "value": legacy_value,
                "value_num": legacy_value_num,
                "unit": mapping["unit"],
                "temperature": "",
                "pressure": "",
                "phase": "",
                "source_type": _legacy_source_type_from_rank(item.get("source_priority_rank")),
                "source_name": _clean_str(item.get("source_name")),
                "source_id": _clean_str(item.get("source_id")),
                "method": "property_governance_bundle_canonical_overlay",
                "uncertainty": "",
                "quality_level": _quality_level_from_score(item.get("data_quality_score_100")),
                "assessment_version": "property_governance_bundle_20260422",
                "time_horizon": "100" if mapping["property_name"] == "gwp_100yr" else "",
                "year": "2026",
                "notes": _legacy_note(item),
                "qc_status": "pass",
                "qc_flags": "",
                "canonical_feature_key": _clean_str(item.get("canonical_feature_key")),
                "standard_unit": _clean_str(item.get("standard_unit")),
                "bundle_record_id": _clean_str(item.get("bundle_record_id")),
                "source_priority_rank": pd.to_numeric(item.get("source_priority_rank"), errors="coerce"),
                "data_quality_score_100": _optional_float(item.get("data_quality_score_100")),
                "is_proxy_or_screening": _coerce_int(item.get("is_proxy_or_screening"), 0),
                "ml_use_status": _clean_str(item.get("ml_use_status")),
            }
        )
    return rows


def _empty_integration() -> dict[str, Any]:
    empty_df = pd.DataFrame()
    return {
        "bundle_present": False,
        "generated_seed_rows": empty_df,
        "generated_molecule_rows": [],
        "generated_alias_rows": [],
        "legacy_property_rows": [],
        "canonical_observation": empty_df,
        "canonical_recommended": empty_df,
        "canonical_recommended_strict": empty_df,
        "canonical_review_queue": empty_df,
        "crosswalk": empty_df,
        "unresolved": empty_df,
        "source_manifest_rows": [],
        "resolution_rows": [],
        "audit": {},
        "extension_manifest": empty_df,
        "mixture_core": empty_df,
        "mixture_component": empty_df,
        "unresolved_curations": empty_df,
        "canonical_review_decisions": empty_df,
        "proxy_acceptance_rules": empty_df,
    }


def _output_paths(output_root: Path) -> dict[str, Path]:
    data_root = output_root / "data"
    extension_root = data_root / "extensions" / "property_governance_20260422"
    return {
        "extension_tables": extension_root / "tables",
        "extension_manifest": extension_root / "extension_manifest.parquet",
        "mixture_core": extension_root / "mixture_core.parquet",
        "mixture_component": extension_root / "mixture_component.parquet",
        "generated_seed_catalog": data_root / "raw" / "generated" / "property_governance_20260422_seed_catalog.csv",
        "crosswalk": data_root / "bronze" / "property_governance_20260422_substance_crosswalk.parquet",
        "unresolved": data_root / "bronze" / "property_governance_20260422_unresolved_substances.parquet",
        "audit": data_root / "bronze" / "property_governance_20260422_audit.json",
        "canonical_observation": data_root / "silver" / "property_observation_canonical.parquet",
        "canonical_recommended": data_root / "gold" / "property_recommended_canonical.parquet",
        "canonical_recommended_strict": data_root / "gold" / "property_recommended_canonical_strict.parquet",
        "canonical_recommended_review_queue": data_root / "gold" / "property_recommended_canonical_review_queue.parquet",
        "property_dictionary": data_root / "gold" / "property_dictionary.parquet",
        "property_canonical_map": data_root / "gold" / "property_canonical_map.parquet",
        "unit_conversion_rules": data_root / "gold" / "unit_conversion_rules.parquet",
        "property_source_priority_rules": data_root / "gold" / "property_source_priority_rules.parquet",
        "property_modeling_readiness_rules": data_root / "gold" / "property_modeling_readiness_rules.parquet",
        "property_governance_issues": data_root / "gold" / "property_governance_issues.parquet",
    }


def _mirror_extension_tables(
    bundle: PropertyGovernanceBundle,
    output_dir: Path,
    written_files: list[tuple[str, str, str, Path, str]],
) -> pd.DataFrame:
    ensure_directory(output_dir)
    rows = []
    for table_name, frame in sorted(bundle.tables.items()):
        path = output_dir / f"{table_name}.parquet"
        _write_frame_parquet(frame, path)
        source_id = f"source_property_governance_ext_{slugify(table_name)}"
        written_files.append(
            (
                source_id,
                "derived_harmonized",
                f"Property Governance Extension Mirror: {table_name}",
                path,
                "",
            )
        )
        rows.append(
            {
                "table_name": table_name,
                "row_count": int(len(frame)),
                "domain": _classify_bundle_domain(table_name),
                "local_path": str(path).replace("\\", "/"),
                "source_id": source_id,
            }
        )
    return pd.DataFrame(rows).sort_values("table_name").reset_index(drop=True)


def _write_frame_parquet(frame: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    frame.to_parquet(path, index=False)


def _write_frame_csv(frame: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    frame.to_csv(path, index=False, encoding="utf-8")


def _bundle_substance_records(bundle: PropertyGovernanceBundle) -> pd.DataFrame:
    molecular_info = bundle.tables["tbl_molecular_info"].copy()
    substances = bundle.tables["tbl_substances"].copy()
    return pd.merge(
        molecular_info,
        substances[["substance_id", "refrigerant_number", "family", "common_name", "chemical_name", "chemical_formula", "cas_number"]],
        on="substance_id",
        how="left",
        suffixes=("", "_substance"),
    ).fillna("")


def _build_substance_crosswalk(
    bundle: PropertyGovernanceBundle,
    molecule_core: pd.DataFrame,
    alias_df: pd.DataFrame,
    unresolved_curations: pd.DataFrame,
) -> pd.DataFrame:
    merged = _bundle_substance_records(bundle)

    inchikey_lookup = _build_unique_lookup(molecule_core, "inchikey", "mol_id")
    smiles_lookup = _build_unique_lookup(molecule_core, "canonical_smiles", "mol_id")
    cas_lookup = _build_alias_lookup(alias_df, "cas")
    r_lookup = _build_alias_lookup(alias_df, "r_number")
    curation_lookup = {
        _clean_str(item.get("substance_id")): item
        for item in unresolved_curations.fillna("").to_dict(orient="records")
        if _clean_str(item.get("substance_id"))
    }

    rows = []
    for record in merged.to_dict(orient="records"):
        substance_id = _clean_str(record.get("substance_id"))
        curation_row = curation_lookup.get(substance_id, {})
        if curation_row:
            record = _apply_unresolved_curation(record, curation_row)
        refrigerant_number = _clean_str(record.get("refrigerant_number"))
        cas_number = _clean_str(record.get("cas_number"))
        inchikey = _clean_str(record.get("inchikey"))
        canonical_smiles = _clean_str(record.get("canonical_smiles"))
        has_structure = _record_has_structure(record)

        mol_id = ""
        match_status = "unresolved"
        matched_via = ""
        matched_value = ""
        generated_seed_id = ""
        if inchikey and inchikey in inchikey_lookup:
            mol_id = inchikey_lookup[inchikey]
            match_status = "matched_existing"
            matched_via = "inchikey"
            matched_value = inchikey
        elif cas_number and cas_number in cas_lookup:
            mol_id = cas_lookup[cas_number]
            match_status = "matched_existing"
            matched_via = "cas"
            matched_value = cas_number
        elif refrigerant_number and refrigerant_number in r_lookup:
            mol_id = r_lookup[refrigerant_number]
            match_status = "matched_existing"
            matched_via = "r_number"
            matched_value = refrigerant_number
        elif canonical_smiles and canonical_smiles in smiles_lookup:
            mol_id = smiles_lookup[canonical_smiles]
            match_status = "matched_existing"
            matched_via = "canonical_smiles"
            matched_value = canonical_smiles
        elif has_structure:
            generated_seed_id = f"tierd_pgov20260422_{slugify(substance_id)}"
            mol_id = f"mol_{_bundle_standardized(record)['inchikey'].lower()}"
            match_status = "generated_new_seed"
            matched_via = "generated_new_seed"
            matched_value = generated_seed_id
        if match_status != "generated_new_seed":
            generated_seed_id = ""

        rows.append(
            {
                "substance_id": substance_id,
                "refrigerant_number": refrigerant_number,
                "family": _normalize_family(_clean_str(record.get("family")) or _clean_str(record.get("family_substance"))),
                "common_name": _clean_str(record.get("common_name")),
                "chemical_name": _clean_str(record.get("chemical_name")),
                "chemical_formula": _clean_str(record.get("chemical_formula")) or _clean_str(record.get("molecular_formula")),
                "cas_number": cas_number,
                "canonical_smiles": canonical_smiles,
                "isomeric_smiles": _clean_str(record.get("isomeric_smiles")),
                "smiles": _clean_str(record.get("smiles")),
                "inchi": _clean_str(record.get("inchi")),
                "inchikey": inchikey,
                "molecular_weight_g_mol": _optional_float(record.get("molecular_weight_g_mol")),
                "scope_status": _clean_str(record.get("scope_status")),
                "match_status": match_status,
                "matched_via": matched_via,
                "matched_value": matched_value,
                "generated_seed_id": generated_seed_id,
                "mol_id": mol_id,
                "has_structure": has_structure,
                "external_resolution_applied": bool(curation_row),
                "external_resolution_source": _clean_str(curation_row.get("resolution_source")),
                "external_resolution_source_url": _clean_str(curation_row.get("resolution_source_url")),
                "external_resolution_confidence": _clean_str(curation_row.get("resolution_confidence")),
            }
        )
    return pd.DataFrame(rows).sort_values(["match_status", "substance_id"]).reset_index(drop=True)


def _apply_unresolved_curation(record: dict[str, Any], curation_row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(record)
    updated["canonical_smiles"] = _clean_str(curation_row.get("canonical_smiles"))
    updated["isomeric_smiles"] = _clean_str(curation_row.get("isomeric_smiles"))
    updated["smiles"] = _clean_str(curation_row.get("isomeric_smiles")) or _clean_str(curation_row.get("canonical_smiles"))
    updated["inchi"] = _clean_str(curation_row.get("inchi"))
    updated["inchikey"] = _clean_str(curation_row.get("inchikey"))
    if not _clean_str(updated.get("chemical_formula")):
        updated["chemical_formula"] = _clean_str(curation_row.get("standardized_formula"))
    if not _clean_str(updated.get("molecular_formula")):
        updated["molecular_formula"] = _clean_str(curation_row.get("standardized_formula"))
    if _optional_float(updated.get("molecular_weight_g_mol")) is None:
        updated["molecular_weight_g_mol"] = _optional_float(curation_row.get("standardized_molecular_weight"))
    return updated


def _record_has_structure(record: dict[str, Any]) -> bool:
    return bool(
        _clean_str(record.get("inchikey"))
        or _clean_str(record.get("canonical_smiles"))
        or _clean_str(record.get("isomeric_smiles"))
        or _clean_str(record.get("smiles"))
    )


def _validate_curation_structure(*, record: dict[str, Any], bundle_record: dict[str, Any], substance_id: str) -> dict[str, Any]:
    standardized_records: list[dict[str, Any]] = []
    for field_name in ["isomeric_smiles", "canonical_smiles"]:
        smiles = _clean_str(record.get(field_name))
        if smiles:
            standardized_records.append(standardize_smiles(smiles))

    if not standardized_records:
        raise ValueError(f"Property governance unresolved curation requires canonical_smiles or isomeric_smiles: {substance_id}")

    standardized = standardized_records[0]
    for other in standardized_records[1:]:
        if other["inchikey"] != standardized["inchikey"]:
            raise ValueError(f"Property governance unresolved curation has inconsistent SMILES for {substance_id}")

    provided_inchi = _clean_str(record.get("inchi"))
    if provided_inchi and provided_inchi != standardized["inchi"]:
        raise ValueError(f"Property governance unresolved curation InChI mismatch for {substance_id}")

    provided_inchikey = _clean_str(record.get("inchikey")).upper()
    if provided_inchikey and provided_inchikey != standardized["inchikey"]:
        raise ValueError(f"Property governance unresolved curation InChIKey mismatch for {substance_id}")

    bundle_formula = _clean_str(bundle_record.get("chemical_formula")) or _clean_str(bundle_record.get("molecular_formula"))
    if bundle_formula and bundle_formula != standardized["formula"]:
        raise ValueError(f"Property governance unresolved curation formula mismatch for {substance_id}: {bundle_formula} != {standardized['formula']}")

    return standardized


def _build_generated_seed_rows(crosswalk: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for record in crosswalk.loc[crosswalk["match_status"] == "generated_new_seed"].to_dict(orient="records"):
        query_name = (
            _clean_str(record.get("cas_number"))
            or _clean_str(record.get("refrigerant_number"))
            or _clean_str(record.get("chemical_name"))
            or _clean_str(record.get("common_name"))
        )
        nist_query = _clean_str(record.get("cas_number")) or _clean_str(record.get("refrigerant_number")) or query_name
        rows.append(
            {
                "seed_id": _clean_str(record.get("generated_seed_id")),
                "r_number": _clean_str(record.get("refrigerant_number")),
                "family": _clean_str(record.get("family")) or "Candidate",
                "query_name": query_name,
                "pubchem_query_type": "name",
                "nist_query": nist_query,
                "nist_query_type": "name",
                "coolprop_fluid": "",
                "priority_tier": "4",
                "selection_role": "inventory",
                "coverage_tier": "D",
                "source_bundle": "property_governance_20260422",
                "coolprop_support_expected": "no",
                "regulatory_priority": "low",
                "entity_scope": "refrigerant",
                "model_inclusion": "no",
                "notes": f"generated_from_property_governance_bundle:{_clean_str(record.get('substance_id'))}",
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "seed_id",
            "r_number",
            "family",
            "query_name",
            "pubchem_query_type",
            "nist_query",
            "nist_query_type",
            "coolprop_fluid",
            "priority_tier",
            "selection_role",
            "coverage_tier",
            "source_bundle",
            "coolprop_support_expected",
            "regulatory_priority",
            "entity_scope",
            "model_inclusion",
            "notes",
        ],
    )


def _build_generated_molecule_rows(crosswalk: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in crosswalk.loc[crosswalk["match_status"] == "generated_new_seed"].to_dict(orient="records"):
        standardized = _bundle_standardized(record)
        rows.append(
            {
                "mol_id": _clean_str(record.get("mol_id")),
                "seed_id": _clean_str(record.get("generated_seed_id")),
                "family": _clean_str(record.get("family")) or "Candidate",
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
                "pubchem_cid": "",
                "pubchem_query": _clean_str(record.get("cas_number")) or _clean_str(record.get("chemical_name")) or _clean_str(record.get("common_name")),
                "entity_scope": "refrigerant",
                "model_inclusion": "no",
                "coverage_tier": "D",
                "status": "resolved",
            }
        )
    return rows


def _build_generated_alias_rows(crosswalk: pd.DataFrame, alias_df: pd.DataFrame) -> list[dict[str, Any]]:
    existing_primary_r = set(
        alias_df.loc[(alias_df["alias_type"] == "r_number") & (alias_df["is_primary"]), "mol_id"].astype(str).tolist()
    )
    rows: list[dict[str, Any]] = []
    for record in crosswalk.to_dict(orient="records"):
        mol_id = _clean_str(record.get("mol_id"))
        if not mol_id:
            continue
        is_generated = _clean_str(record.get("match_status")) == "generated_new_seed"
        primary_r_number = is_generated and mol_id not in existing_primary_r
        for alias_type, alias_value, is_primary in [
            ("bundle_substance_id", _clean_str(record.get("substance_id")), False),
            ("r_number", _clean_str(record.get("refrigerant_number")), primary_r_number),
            ("cas", _clean_str(record.get("cas_number")), False),
            ("synonym", _clean_str(record.get("common_name")), False),
            ("synonym", _clean_str(record.get("chemical_name")), False),
        ]:
            cleaned = _clean_str(alias_value)
            if not cleaned:
                continue
            rows.append(
                {
                    "mol_id": mol_id,
                    "alias_type": alias_type,
                    "alias_value": cleaned,
                    "is_primary": bool(is_primary),
                    "source_name": "property_governance_bundle",
                }
            )
    return rows


def _build_canonical_observation(bundle: PropertyGovernanceBundle, crosswalk: pd.DataFrame, sources_df: pd.DataFrame) -> pd.DataFrame:
    overlay = bundle.tables["tbl_pure_properties_canonical_overlay_v1"].copy().fillna("")
    source_lookup = {
        _clean_str(item.get("source_id")): item
        for item in sources_df.fillna("").to_dict(orient="records")
    }
    crosswalk_lookup = {
        _clean_str(item.get("substance_id")): item
        for item in crosswalk.loc[crosswalk["mol_id"].astype(str).str.strip() != ""].to_dict(orient="records")
    }

    rows = []
    for record in overlay.to_dict(orient="records"):
        crosswalk_row = crosswalk_lookup.get(_clean_str(record.get("substance_id")))
        if crosswalk_row is None:
            continue
        mol_id = _clean_str(crosswalk_row.get("mol_id"))
        if not mol_id:
            continue
        source_id = _clean_str(record.get("source_id"))
        source_meta = source_lookup.get(source_id, {})
        value_num = _optional_float(record.get("standard_value_numeric"))
        value = _clean_str(record.get("standard_value_text")) or (
            _format_numeric_text(value_num) if value_num is not None else _clean_str(record.get("raw_value_text"))
        )
        value_num = _coerce_governed_value_num(
            canonical_feature_key=_clean_str(record.get("canonical_feature_key")),
            value=value,
            value_num=value_num,
        )
        rows.append(
            {
                "observation_id": "",
                "mol_id": mol_id,
                "canonical_property_id": _clean_str(record.get("canonical_property_id")),
                "canonical_feature_key": _clean_str(record.get("canonical_feature_key")),
                "canonical_property_group": _clean_str(record.get("canonical_property_group")),
                "canonical_property_name": _clean_str(record.get("canonical_property_name")),
                "value": value,
                "value_num": value_num,
                "unit": _clean_str(record.get("standard_unit")),
                "standard_unit": _clean_str(record.get("standard_unit")),
                "temperature": "",
                "pressure": "",
                "phase": "",
                "source_type": _legacy_source_type_from_rank(record.get("source_priority_rank")),
                "source_name": _clean_str(source_meta.get("title")) or source_id or "Property Governance Bundle",
                "source_id": source_id,
                "method": "property_governance_bundle_canonical_overlay",
                "uncertainty": "",
                "quality_level": _quality_level_from_score(record.get("data_quality_score_100")),
                "assessment_version": "property_governance_bundle_20260422",
                "time_horizon": _canonical_time_horizon(record),
                "year": "2026",
                "notes": _clean_str(record.get("notes")),
                "bundle_record_id": _clean_str(record.get("record_id")) or _clean_str(record.get("overlay_id")),
                "source_priority_rank": _optional_float(record.get("source_priority_rank")),
                "data_quality_score_100": _optional_float(record.get("data_quality_score_100")),
                "is_proxy_or_screening": _coerce_int(record.get("is_proxy_or_screening"), 0),
                "ml_use_status": _clean_str(record.get("ml_use_status")),
            }
        )
    frame = pd.DataFrame(rows, columns=_canonical_observation_columns())
    if frame.empty:
        return frame
    frame["observation_id"] = [
        f"can_{slugify(mol_id)}_{slugify(feature)}_{idx + 1}"
        for idx, (mol_id, feature) in enumerate(zip(frame["mol_id"], frame["canonical_feature_key"], strict=True))
    ]
    return frame.sort_values(["mol_id", "canonical_feature_key", "source_id"]).reset_index(drop=True)


def _build_normalized_mixture_tables(
    bundle: PropertyGovernanceBundle,
    alias_df: pd.DataFrame,
    crosswalk: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mixtures = bundle.tables["tbl_mixtures"].copy().fillna("")
    components = bundle.tables["tbl_mixture_components"].copy().fillna("")
    r_lookup = _build_alias_lookup(alias_df, "r_number")
    crosswalk_r_lookup = {
        _clean_str(item.get("refrigerant_number")): _clean_str(item.get("mol_id"))
        for item in crosswalk.to_dict(orient="records")
        if _clean_str(item.get("refrigerant_number")) and _clean_str(item.get("mol_id"))
    }

    mixture_core = pd.DataFrame(
        [
            {
                "mixture_id": _clean_str(row.get("mixture_id")),
                "mixture_name": _clean_str(row.get("refrigerant_number")),
                "ashrae_blend_designation": _clean_str(row.get("refrigerant_number")),
                "notes": _clean_str(row.get("notes")),
            }
            for row in mixtures.to_dict(orient="records")
        ],
        columns=["mixture_id", "mixture_name", "ashrae_blend_designation", "notes"],
    )

    component_rows = []
    for row in components.to_dict(orient="records"):
        component_name = _clean_str(row.get("component_refrigerant"))
        mol_id = r_lookup.get(component_name, "") or crosswalk_r_lookup.get(component_name, "")
        component_rows.append(
            {
                "mixture_id": _clean_str(row.get("mixture_id")),
                "mol_id": mol_id,
                "composition_basis": "mass_pct",
                "fraction_value": _optional_float(row.get("mass_pct")),
            }
        )
    mixture_component = pd.DataFrame(component_rows, columns=["mixture_id", "mol_id", "composition_basis", "fraction_value"])
    return mixture_core, mixture_component


def _build_audit_payload(
    *,
    bundle: PropertyGovernanceBundle,
    extension_manifest: pd.DataFrame,
    crosswalk: pd.DataFrame,
    canonical_observation: pd.DataFrame,
    canonical_recommended: pd.DataFrame,
    canonical_recommended_strict: pd.DataFrame,
    canonical_review_queue: pd.DataFrame,
    canonical_review_decisions: pd.DataFrame,
    proxy_acceptance_rules: pd.DataFrame,
) -> dict[str, Any]:
    status_counts = crosswalk["match_status"].value_counts(dropna=False).to_dict() if not crosswalk.empty else {}
    matched_existing = int(status_counts.get("matched_existing", 0))
    generated_new_seed = int(status_counts.get("generated_new_seed", 0))
    unresolved = int(status_counts.get("unresolved", 0))
    external_resolution_count = int(crosswalk["external_resolution_applied"].fillna(False).astype(bool).sum()) if not crosswalk.empty else 0
    row_count_audit = _extension_manifest_row_audit(bundle=bundle, extension_manifest=extension_manifest)
    review_reason_counts = (
        canonical_review_queue["review_reason"].astype(str).value_counts().to_dict() if not canonical_review_queue.empty else {}
    )
    review_decision_reason_counts = (
        canonical_review_decisions["review_reason"].astype(str).value_counts().to_dict()
        if not canonical_review_decisions.empty
        else {}
    )
    review_decision_action_counts = (
        canonical_review_decisions["decision_action"].astype(str).value_counts().to_dict()
        if not canonical_review_decisions.empty
        else {}
    )
    proxy_acceptance_feature_counts = (
        proxy_acceptance_rules["canonical_feature_key"].astype(str).value_counts().to_dict()
        if not proxy_acceptance_rules.empty
        else {}
    )
    strict_proxy_accept_count = int(
        canonical_recommended_strict["strict_accept_basis"].astype(str).eq("proxy_only_policy").sum()
    ) if not canonical_recommended_strict.empty and "strict_accept_basis" in canonical_recommended_strict.columns else 0
    return {
        "bundle_file": str(bundle.bundle_path).replace("\\", "/"),
        "bundle_sha256": bundle.bundle_sha256,
        "bundle_table_count": len(bundle.tables),
        "extension_table_count": int(len(extension_manifest)),
        "crosswalk": {
            "matched_existing": matched_existing,
            "generated_new_seed": generated_new_seed,
            "unresolved": unresolved,
            "inchikey_matches": int((crosswalk["matched_via"] == "inchikey").sum()) if not crosswalk.empty else 0,
            "cas_matches": int((crosswalk["matched_via"] == "cas").sum()) if not crosswalk.empty else 0,
            "r_number_matches": int((crosswalk["matched_via"] == "r_number").sum()) if not crosswalk.empty else 0,
            "canonical_smiles_matches": int((crosswalk["matched_via"] == "canonical_smiles").sum()) if not crosswalk.empty else 0,
            "external_resolution_count": external_resolution_count,
        },
        "canonical_observation_count": int(len(canonical_observation)),
        "canonical_recommended_count": int(len(canonical_recommended)),
        "canonical_recommended_strict_count": int(len(canonical_recommended_strict)),
        "canonical_review_queue_count": int(len(canonical_review_queue)),
        "canonical_proxy_selected_count": int(canonical_recommended["is_proxy_or_screening"].fillna(False).astype(bool).sum())
        if not canonical_recommended.empty
        else 0,
        "canonical_proxy_only_count": int(canonical_recommended["proxy_only_flag"].fillna(False).astype(bool).sum())
        if not canonical_recommended.empty
        else 0,
        "canonical_conflict_count": int(canonical_recommended["conflict_flag"].fillna(False).astype(bool).sum())
        if not canonical_recommended.empty
        else 0,
        "canonical_source_divergence_count": int(canonical_recommended["source_divergence_flag"].fillna(False).astype(bool).sum())
        if not canonical_recommended.empty
        else 0,
        "canonical_conflict_open_count": int(
            canonical_review_queue["review_triggers"].astype(str).str.contains(r"(?:^|;)top_rank_conflict(?:;|$)", regex=True).sum()
        )
        if not canonical_review_queue.empty
        else 0,
        "canonical_source_divergence_open_count": int(
            canonical_review_queue["review_triggers"].astype(str).str.contains(r"(?:^|;)source_divergence(?:;|$)", regex=True).sum()
        )
        if not canonical_review_queue.empty
        else 0,
        "canonical_review_decision_count": int(len(canonical_review_decisions)),
        "canonical_proxy_policy_count": int(len(proxy_acceptance_rules)),
        "canonical_strict_proxy_accept_count": strict_proxy_accept_count,
        "canonical_review_reason_counts": review_reason_counts,
        "canonical_review_decision_reason_counts": review_decision_reason_counts,
        "canonical_review_decision_action_counts": review_decision_action_counts,
        "canonical_proxy_policy_feature_counts": proxy_acceptance_feature_counts,
        "row_count_audit": row_count_audit,
    }


def _extension_manifest_row_audit(*, bundle: PropertyGovernanceBundle, extension_manifest: pd.DataFrame) -> dict[str, Any]:
    expected = {str(row["table_name"]): int(row["row_count"]) for row in bundle.row_manifest.fillna("").to_dict(orient="records")}
    actual = {str(row["table_name"]): int(row["row_count"]) for row in extension_manifest.fillna("").to_dict(orient="records")}
    missing_tables = sorted(set(expected) - set(actual))
    extra_tables = sorted(set(actual) - set(expected))
    row_mismatch_tables = [
        {"table_name": table_name, "expected": expected[table_name], "actual": actual[table_name]}
        for table_name in sorted(set(expected) & set(actual))
        if expected[table_name] != actual[table_name]
    ]
    return {
        "status": "ok" if not missing_tables and not extra_tables and not row_mismatch_tables else "error",
        "expected_table_count": int(len(expected)),
        "actual_table_count": int(len(actual)),
        "missing_tables": missing_tables,
        "extra_tables": extra_tables,
        "row_mismatch_tables": row_mismatch_tables,
    }


def _build_resolution_rows(crosswalk: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for record in crosswalk.to_dict(orient="records"):
        detail_parts = [
            _clean_str(record.get("match_status")),
            _clean_str(record.get("matched_via")),
            _clean_str(record.get("mol_id")),
        ]
        if _coerce_bool_flag(record.get("external_resolution_applied")):
            detail_parts.extend(
                [
                    "external_resolution",
                    _clean_str(record.get("external_resolution_source")),
                    _clean_str(record.get("external_resolution_source_url")),
                ]
            )
        rows.append(
            {
                "seed_id": _clean_str(record.get("generated_seed_id")),
                "r_number": _clean_str(record.get("refrigerant_number")),
                "stage": "property_governance_bundle",
                "status": _resolution_status(record),
                "detail": ":".join(part for part in detail_parts if part),
            }
        )
    return rows


def _bundle_source_manifest_entries(
    *,
    bundle: PropertyGovernanceBundle,
    parser_version: str,
    retrieved_at: str,
    written_files: list[tuple[str, str, str, Path, str]],
    unresolved_curation_path: Path,
    canonical_review_decision_path: Path,
    proxy_acceptance_path: Path,
) -> list[dict[str, Any]]:
    rows = [
        {
            "source_id": "source_property_governance_bundle_zip",
            "source_type": "manual_catalog",
            "source_name": "Property Governance Bundle Zip",
            "license": "project-local curated bundle",
            "retrieved_at": retrieved_at,
            "checksum_sha256": bundle.bundle_sha256,
            "local_path": str(bundle.bundle_path).replace("\\", "/"),
            "parser_version": parser_version,
            "upstream_url": "",
            "status": "registered",
        }
    ]
    if unresolved_curation_path.exists():
        rows.append(
            {
                "source_id": "source_property_governance_unresolved_curations_csv",
                "source_type": "manual_catalog",
                "source_name": "Property Governance Unresolved Curations",
                "license": "project-local manual curation referencing authoritative external sources",
                "retrieved_at": retrieved_at,
                "checksum_sha256": sha256_file(unresolved_curation_path),
                "local_path": str(unresolved_curation_path).replace("\\", "/"),
                "parser_version": parser_version,
                "upstream_url": "",
                "status": "registered",
            }
        )
    if canonical_review_decision_path.exists():
        rows.append(
            {
                "source_id": "source_property_governance_canonical_review_decisions_csv",
                "source_type": "manual_catalog",
                "source_name": "Property Governance Canonical Review Decisions",
                "license": "project-local manual adjudication of governed canonical review queue",
                "retrieved_at": retrieved_at,
                "checksum_sha256": sha256_file(canonical_review_decision_path),
                "local_path": str(canonical_review_decision_path).replace("\\", "/"),
                "parser_version": parser_version,
                "upstream_url": "",
                "status": "registered",
            }
        )
    if proxy_acceptance_path.exists():
        rows.append(
            {
                "source_id": "source_property_governance_proxy_acceptance_rules_csv",
                "source_type": "manual_catalog",
                "source_name": "Property Governance Proxy Acceptance Rules",
                "license": "project-local manual policy for proxy-only strict acceptance",
                "retrieved_at": retrieved_at,
                "checksum_sha256": sha256_file(proxy_acceptance_path),
                "local_path": str(proxy_acceptance_path).replace("\\", "/"),
                "parser_version": parser_version,
                "upstream_url": "",
                "status": "registered",
            }
        )
    for member_name in [FILE_MANIFEST_NAME, ROW_MANIFEST_NAME, SUMMARY_NAME, WORKBOOK_QA_NAME]:
        rows.append(
            {
                "source_id": f"source_{slugify(member_name)}",
                "source_type": "manual_catalog",
                "source_name": member_name,
                "license": "project-local curated bundle",
                "retrieved_at": retrieved_at,
                "checksum_sha256": _sha256_text(bundle.texts[member_name]),
                "local_path": f"{bundle.bundle_path.as_posix()}::{member_name}",
                "parser_version": parser_version,
                "upstream_url": "",
                "status": "registered",
            }
        )
    for source_row in bundle.tables.get("tbl_sources", pd.DataFrame()).fillna("").to_dict(orient="records"):
        source_id = _clean_str(source_row.get("source_id"))
        if not source_id:
            continue
        rows.append(
            {
                "source_id": source_id,
                "source_type": _clean_str(source_row.get("source_type")) or "manual_curated_reference",
                "source_name": _clean_str(source_row.get("title")) or source_id,
                "license": "bundle-curated external source metadata",
                "retrieved_at": retrieved_at,
                "checksum_sha256": _sha256_text(json.dumps(source_row, sort_keys=True, ensure_ascii=False)),
                "local_path": f"{bundle.bundle_path.as_posix()}::tbl_sources#{source_id}",
                "parser_version": parser_version,
                "upstream_url": _clean_str(source_row.get("source_url")),
                "status": "registered",
            }
        )
    for source_id, source_type, source_name, path, upstream_url in written_files:
        rows.append(
            {
                "source_id": source_id,
                "source_type": source_type,
                "source_name": source_name,
                "license": "project-local derived from property governance bundle",
                "retrieved_at": retrieved_at,
                "checksum_sha256": sha256_file(path),
                "local_path": str(path).replace("\\", "/"),
                "parser_version": parser_version,
                "upstream_url": upstream_url,
                "status": "generated",
            }
        )
    return rows


def _prepare_governance_output(output_name: str, frame: pd.DataFrame) -> pd.DataFrame:
    prepared = frame.copy()
    if output_name == "unit_conversion_rules":
        if "standard_unit" not in prepared.columns and "to_standard_unit" in prepared.columns:
            prepared["standard_unit"] = prepared["to_standard_unit"]
        if "to_standard_unit" not in prepared.columns and "standard_unit" in prepared.columns:
            prepared["to_standard_unit"] = prepared["standard_unit"]
    return prepared


def _validate_row_manifest(*, row_manifest: pd.DataFrame, tables: dict[str, pd.DataFrame]) -> None:
    expected = {str(row["table_name"]): int(row["row_count"]) for row in row_manifest.to_dict(orient="records")}
    actual = {table_name: int(len(frame)) for table_name, frame in tables.items()}
    if set(expected) != set(actual):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise ValueError(f"Property governance row manifest mismatch: missing={missing}, extra={extra}")
    mismatches = [
        {"table_name": table_name, "expected": expected[table_name], "actual": actual[table_name]}
        for table_name in sorted(expected)
        if expected[table_name] != actual[table_name]
    ]
    if mismatches:
        raise ValueError(f"Property governance row manifest mismatches: {json.dumps(mismatches, ensure_ascii=False)}")


def _build_unique_lookup(df: pd.DataFrame, key_column: str, value_column: str) -> dict[str, str]:
    bucket: dict[str, set[str]] = {}
    for row in df.fillna("").to_dict(orient="records"):
        key = _clean_str(row.get(key_column))
        value = _clean_str(row.get(value_column))
        if not key or not value:
            continue
        bucket.setdefault(key, set()).add(value)
    return {key: next(iter(values)) for key, values in bucket.items() if len(values) == 1}


def _build_alias_lookup(alias_df: pd.DataFrame, alias_type: str) -> dict[str, str]:
    filtered = alias_df.loc[alias_df["alias_type"].astype(str) == alias_type].copy()
    return _build_unique_lookup(filtered, "alias_value", "mol_id")


def _bundle_standardized(record: dict[str, Any]) -> dict[str, Any]:
    smiles = _clean_str(record.get("isomeric_smiles")) or _clean_str(record.get("canonical_smiles")) or _clean_str(record.get("smiles"))
    if not smiles:
        raise ValueError(f"No structure available for property governance record: {_clean_str(record.get('substance_id'))}")
    return standardize_smiles(smiles)


def _unresolved_curation_csv_columns() -> list[str]:
    return [
        "substance_id",
        "refrigerant_number",
        "cas_number",
        "canonical_smiles",
        "isomeric_smiles",
        "inchi",
        "inchikey",
        "resolution_source",
        "resolution_source_url",
        "resolution_confidence",
        "notes",
    ]


def _unresolved_curation_columns() -> list[str]:
    return _unresolved_curation_csv_columns() + [
        "standardized_formula",
        "standardized_molecular_weight",
    ]


def _canonical_review_decision_csv_columns() -> list[str]:
    return [
        "mol_id",
        "canonical_feature_key",
        "review_reason",
        "decision_action",
        "expected_selected_source_id",
        "expected_selected_value",
        "resolution_basis",
        "resolution_source_url",
        "notes",
    ]


def _canonical_review_decision_columns() -> list[str]:
    return _canonical_review_decision_csv_columns()


def _proxy_acceptance_rule_csv_columns() -> list[str]:
    return [
        "proxy_policy_id",
        "canonical_feature_key",
        "selected_source_id",
        "allow_in_strict_if_proxy_only",
        "rationale",
        "notes",
    ]


def _proxy_acceptance_rule_columns() -> list[str]:
    return _proxy_acceptance_rule_csv_columns()


def _canonical_observation_columns() -> list[str]:
    return [
        "observation_id",
        "mol_id",
        "canonical_property_id",
        "canonical_feature_key",
        "canonical_property_group",
        "canonical_property_name",
        "value",
        "value_num",
        "unit",
        "standard_unit",
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
        "bundle_record_id",
        "source_priority_rank",
        "data_quality_score_100",
        "is_proxy_or_screening",
        "ml_use_status",
    ]


def _canonical_recommended_columns() -> list[str]:
    return [
        "mol_id",
        "canonical_feature_key",
        "canonical_property_id",
        "canonical_property_group",
        "canonical_property_name",
        "value",
        "value_num",
        "unit",
        "selected_source_id",
        "selected_source_name",
        "selected_quality_level",
        "source_priority_rank",
        "data_quality_score_100",
        "is_proxy_or_screening",
        "ml_use_status",
        "proxy_only_flag",
        "nonproxy_candidate_count",
        "top_rank_source_count",
        "source_divergence_flag",
        "source_divergence_detail",
        "source_count",
        "conflict_flag",
        "conflict_detail",
    ]


def _canonical_recommended_strict_columns() -> list[str]:
    return _canonical_recommended_columns() + [
        "readiness_rule_id",
        "use_as_ml_feature",
        "use_as_ml_target",
        "minimum_quality_score",
        "exclude_if_proxy_or_screening",
        "preferred_standard_unit",
        "normalization_recommendation",
        "missing_value_strategy",
        "readiness_notes",
        "strict_accept",
        "strict_accept_basis",
        "proxy_policy_id",
        "proxy_policy_rationale",
    ]


def _canonical_review_queue_columns() -> list[str]:
    return _canonical_recommended_strict_columns() + [
        "strict_rejection_reason",
        "review_reason",
        "review_triggers",
        "review_priority",
    ]


def _canonical_time_horizon(record: dict[str, Any]) -> str:
    feature_key = _clean_str(record.get("canonical_feature_key"))
    if feature_key.endswith("gwp_100yr"):
        return "100"
    if feature_key.endswith("gwp_20yr"):
        return "20"
    if feature_key.endswith("gtp_50yr"):
        return "50"
    return ""


def _classify_bundle_domain(table_name: str) -> str:
    name = table_name.lower()
    if name.startswith("summary_"):
        return "summary"
    if name.startswith("tbl_mixture") or name == "tbl_mixtures":
        return "mixture"
    if name.startswith("tbl_qm") or name.startswith("tbl_recommended_qm"):
        return "qm"
    if "experimental" in name or name.startswith("tbl_exp"):
        return "experimental"
    if name.startswith("tbl_system") or "system_application" in name or name.startswith("tbl_oil_material"):
        return "system_application"
    if name.endswith("_qa") or name == "tbl_quality_checks":
        return "qa"
    return "property_governance"


def _quality_level_from_score(value: Any) -> str:
    score = _optional_float(value)
    if score is None:
        return "derived_harmonized"
    if score >= 90:
        return "manual_curated_reference"
    if score >= 80:
        return "primary_public_reference"
    if score >= 70:
        return "derived_harmonized"
    return "snapshot_only"


def _legacy_source_type_from_rank(value: Any) -> str:
    rank = _coerce_int(value, 9999)
    if rank <= 3:
        return "manual_curated_reference"
    if rank <= 5:
        return "derived_harmonized"
    return "placeholder"


def _legacy_note(item: dict[str, Any]) -> str:
    parts = [
        "property_governance_bundle_20260422",
        f"bundle_record_id={_clean_str(item.get('bundle_record_id'))}",
        f"source_priority_rank={_clean_str(item.get('source_priority_rank'))}",
        f"data_quality_score_100={_clean_str(item.get('data_quality_score_100'))}",
        f"is_proxy_or_screening={_clean_str(item.get('is_proxy_or_screening'))}",
    ]
    return "; ".join(part for part in parts if part)


def _conflict_detail_for_group(group: pd.DataFrame, *, value_col: str, text_col: str) -> tuple[bool, str]:
    conflict_flag = False
    conflict_detail = ""
    numeric_values = [value for value in pd.to_numeric(group[value_col], errors="coerce").tolist() if pd.notna(value)]
    if len(numeric_values) > 1:
        vmin = min(numeric_values)
        vmax = max(numeric_values)
        scale = max(abs(vmax), abs(vmin), 1.0)
        if abs(vmax - vmin) / scale > 0.05:
            conflict_flag = True
            conflict_detail = f"numeric spread {vmin}..{vmax}"
    elif len(set(group[text_col].astype(str).tolist())) > 1:
        conflict_flag = True
        conflict_detail = " | ".join(sorted(set(group[text_col].astype(str).tolist())))
    return conflict_flag, conflict_detail


def _normalize_family(value: str) -> str:
    cleaned = _clean_str(value)
    if not cleaned:
        return "Candidate"
    direct = {
        "hfc": "HFC",
        "hfo": "HFO",
        "hcfo": "HCFO",
        "hcfc": "HCFC",
        "cfc": "CFC",
        "natural": "Natural",
        "inorganic": "Inorganic",
        "organic": "Candidate",
        "amine": "Candidate",
        "halon": "Halon",
        "pfc": "PFC",
        "hfe": "HFE",
        "hc": "Hydrocarbon",
        "chlorocarbon": "Candidate",
        "iodofluorocarbon": "Candidate",
        "iodo-fluorocarbon": "Candidate",
        "fluoroketone": "Ketone",
        "halogenated cyclic": "Candidate",
    }
    return direct.get(cleaned.lower(), cleaned)


def _resolution_status(record: dict[str, Any]) -> str:
    match_status = _clean_str(record.get("match_status"))
    if match_status == "matched_existing":
        return "resolved"
    if match_status == "generated_new_seed":
        return "generated"
    return "warning"


def _format_numeric_text(value: float) -> str:
    return format(float(value), ".15g")


def _sha256_text(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()


def _clean_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_governed_value_num(*, canonical_feature_key: str, value: str, value_num: float | None) -> float | None:
    if value_num is not None:
        return value_num
    feature_key = _clean_str(canonical_feature_key)
    value_clean = _clean_str(value).lower()
    if feature_key == "environmental.ozone_depleting_flag":
        if value_clean in {"yes", "true"}:
            return 1.0
        if value_clean in {"no", "false"}:
            return 0.0
    return None


def _coerce_int(value: Any, default: int) -> int:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return default
    return int(numeric)


def _coerce_bool_flag(value: Any) -> bool:
    return bool(_coerce_int(value, 0))


def _matches_expected_selected_value(*, expected_value: str, actual_value: str, actual_value_num: float | None) -> bool:
    expected_clean = _clean_str(expected_value)
    if not expected_clean:
        return False
    expected_num = _optional_float(expected_clean)
    if expected_num is not None and actual_value_num is not None:
        scale = max(abs(expected_num), abs(actual_value_num), 1.0)
        return abs(expected_num - actual_value_num) / scale <= 1e-12
    if expected_clean == actual_value:
        return True
    if actual_value_num is not None:
        return expected_clean == _format_numeric_text(actual_value_num)
    return False
