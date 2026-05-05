"""Import helpers for the curated Excel workbook ``制冷剂数据库202603.xlsx``."""

from __future__ import annotations

import hashlib
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

from r_physgen_db.chemistry import compute_screening_features
from r_physgen_db.utils import slugify

_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

_MAIN = f"{{{_MAIN_NS}}}"
_REL = f"{{{_REL_NS}}}"
_PKG_REL = f"{{{_PKG_REL_NS}}}"

WORKBOOK_FILE_NAME = "制冷剂数据库202603.xlsx"
WORKBOOK_TAG = "excel_202603"
EXCEL_202603_SEED_PREFIX = "tierd_excel202603_"

SHEET_ODS = "1-ODS八维数据库"
SHEET_NIST_ASPEN = "1-1-NIST热物性实验数据-Aspen"
SHEET_NIST_HVAP = "1-2-NIST热物性实验数据-蒸发潜热"
SHEET_NIST_PAID = "1-NIST付费数据"
SHEET_GWP_ODP = "2-GWP和ODP"
SHEET_THERMO_REFERENCE = "2-热物性参考"

STRUCTURED_SHEETS = [
    SHEET_ODS,
    SHEET_NIST_ASPEN,
    SHEET_NIST_HVAP,
    SHEET_GWP_ODP,
]

EXPECTED_SHEETS = [
    SHEET_ODS,
    SHEET_NIST_ASPEN,
    SHEET_NIST_HVAP,
    SHEET_NIST_PAID,
    SHEET_GWP_ODP,
    SHEET_THERMO_REFERENCE,
]

THERMO_REFERENCE_SPECS = [
    ("name", "Tb", "boiling_point_c", "degC", "Tb_K"),
    ("name.1", "Tc", "critical_temp_c", "degC", "Tc_K"),
    ("name.2", "Pc/MPa", "critical_pressure_mpa", "MPa", "Pc_MPa"),
    ("name.3", "Hv[298K]", "vaporization_enthalpy_kjmol_at_298k_candidate", "kJ/mol", "Hv_298K_kJmol"),
    ("name.4", "ω", "acentric_factor", "dimensionless", "omega"),
]

SUPPLEMENT_COLUMNS = [
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
]

STAGING_COLUMNS = [
    "name",
    "property_code",
    "property_name_candidate",
    "value",
    "value_num",
    "unit",
    "source_sheet",
    "match_status",
    "next_action",
    "notes",
]

SEED_FIELDNAMES = [
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
]


@dataclass(frozen=True)
class MatchRecord:
    mol_id: str | None
    route: str


def find_default_workbook_path(root: Path) -> Path:
    source_dir = root / "data" / "sources" / "excel"
    legacy_methods_dir = root / "methods"
    for directory in [source_dir, legacy_methods_dir]:
        candidates = sorted(path for path in directory.glob("*.xlsx") if "202603" in path.stem)
        if candidates:
            return candidates[0]
        fallback = directory / WORKBOOK_FILE_NAME
        if fallback.exists():
            return fallback
    raise FileNotFoundError(f"Could not locate {WORKBOOK_FILE_NAME} under {source_dir} or {legacy_methods_dir}")


def parse_excel_202603_workbook(path: Path) -> dict[str, list[dict[str, object]]]:
    workbook = _read_xlsx_workbook(path)
    missing = [sheet for sheet in EXPECTED_SHEETS if sheet not in workbook]
    if missing:
        raise ValueError(f"Workbook is missing expected sheets: {missing}")
    return workbook


def normalize_excel_202603_workbook(path: Path) -> dict[str, object]:
    workbook = parse_excel_202603_workbook(path)
    structured_rows = _merge_structured_rows(workbook)
    thermo_reference_rows = _normalize_thermo_reference_rows(workbook[SHEET_THERMO_REFERENCE])
    return {
        "workbook_name": path.name,
        "workbook_rows": workbook,
        "structured_rows": structured_rows,
        "thermo_reference_rows": thermo_reference_rows,
        "paid_nist_rows": workbook[SHEET_NIST_PAID],
    }


def build_excel_202603_outputs(
    *,
    workbook_path: Path,
    molecule_core: pd.DataFrame,
    molecule_alias: pd.DataFrame,
    property_recommended: pd.DataFrame,
) -> dict[str, object]:
    normalized = normalize_excel_202603_workbook(workbook_path)
    workbook_rows = normalized["workbook_rows"]
    structured_rows = normalized["structured_rows"]
    thermo_reference_rows = normalized["thermo_reference_rows"]

    match_context = _build_match_context(molecule_core, molecule_alias)
    candidate_alias_context = _build_excel_candidate_alias_context(molecule_core, molecule_alias)
    current_property_lookup = _build_current_property_lookup(property_recommended)

    existing_supplement_rows, supplement_summary, used_existing_pairs = _build_supplement_rows(
        workbook_rows=workbook_rows,
        thermo_reference_rows=thermo_reference_rows,
        match_context=match_context,
        current_property_lookup=current_property_lookup,
    )
    candidate_rows, candidate_summary, candidate_contexts = _build_structured_candidate_rows(
        structured_rows=structured_rows,
        match_context=match_context,
    )
    candidate_observation_rows, candidate_observation_summary = _build_candidate_observation_rows(
        candidate_contexts=candidate_contexts,
        thermo_reference_rows=thermo_reference_rows,
        candidate_alias_context=candidate_alias_context,
    )
    staging_rows, staging_summary = _build_name_only_staging_rows(
        thermo_reference_rows=thermo_reference_rows,
        match_context=match_context,
    )
    supplement_rows = [*existing_supplement_rows, *candidate_observation_rows]
    report_markdown = _build_brief_report(
        workbook_name=str(normalized["workbook_name"]),
        workbook_rows=workbook_rows,
        structured_rows=structured_rows,
        thermo_reference_rows=thermo_reference_rows,
        supplement_summary=supplement_summary,
        candidate_summary=candidate_summary,
        candidate_observation_summary=candidate_observation_summary,
        staging_summary=staging_summary,
    )

    return {
        "supplement_rows": pd.DataFrame(supplement_rows, columns=SUPPLEMENT_COLUMNS),
        "existing_supplement_rows": pd.DataFrame(existing_supplement_rows, columns=SUPPLEMENT_COLUMNS),
        "candidate_observation_rows": pd.DataFrame(candidate_observation_rows, columns=SUPPLEMENT_COLUMNS),
        "candidate_rows": pd.DataFrame(candidate_rows, columns=SEED_FIELDNAMES),
        "name_only_staging_rows": pd.DataFrame(staging_rows, columns=STAGING_COLUMNS),
        "report_markdown": report_markdown,
        "summary": {
            "workbook_name": normalized["workbook_name"],
            "supplement": supplement_summary,
            "candidate": candidate_summary,
            "candidate_observation": candidate_observation_summary,
            "name_only_staging": staging_summary,
            "structured_row_count": len(structured_rows),
            "thermo_reference_row_count": len(thermo_reference_rows),
            "used_existing_pairs": len(used_existing_pairs),
        },
    }


def _read_xlsx_workbook(path: Path) -> dict[str, list[dict[str, object]]]:
    with zipfile.ZipFile(path) as archive:
        shared_strings = _load_shared_strings(archive)
        sheet_targets = _load_sheet_targets(archive)
        workbook: dict[str, list[dict[str, object]]] = {}
        for sheet_name, target in sheet_targets.items():
            workbook[sheet_name] = _read_sheet_rows(archive, target, shared_strings)
        return workbook


def _load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for item in root.findall(f"{_MAIN}si"):
        parts = [node.text or "" for node in item.iter(f"{_MAIN}t")]
        values.append("".join(parts))
    return values


def _load_sheet_targets(archive: zipfile.ZipFile) -> dict[str, str]:
    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    rel_map = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rel_root.findall(f"{_PKG_REL}Relationship")
    }
    targets: dict[str, str] = {}
    for sheet in workbook_root.findall(f"{_MAIN}sheets/{_MAIN}sheet"):
        name = sheet.attrib["name"]
        rel_id = sheet.attrib[f"{_REL}id"]
        target = rel_map[rel_id]
        targets[name] = f"xl/{target}".replace("\\", "/")
    return targets


def _read_sheet_rows(archive: zipfile.ZipFile, target: str, shared_strings: list[str]) -> list[dict[str, object]]:
    root = ET.fromstring(archive.read(target))
    sheet_rows: list[list[object]] = []
    for row in root.findall(f"{_MAIN}sheetData/{_MAIN}row"):
        values: dict[int, object] = {}
        for cell in row.findall(f"{_MAIN}c"):
            ref = cell.attrib.get("r", "")
            column_index = _column_index_from_ref(ref)
            values[column_index] = _cell_value(cell, shared_strings)
        if not values:
            continue
        width = max(values) + 1
        ordered = [values.get(index, "") for index in range(width)]
        while ordered and ordered[-1] == "":
            ordered.pop()
        if ordered:
            sheet_rows.append(ordered)

    if not sheet_rows:
        return []

    headers = _dedupe_headers([_clean_header(cell) for cell in sheet_rows[0]])
    records: list[dict[str, object]] = []
    for raw_row in sheet_rows[1:]:
        if not any(str(value).strip() for value in raw_row):
            continue
        padded = raw_row + [""] * max(len(headers) - len(raw_row), 0)
        record = {headers[index]: padded[index] if index < len(padded) else "" for index in range(len(headers))}
        records.append(record)
    return records


def _column_index_from_ref(ref: str) -> int:
    letters = "".join(character for character in ref if character.isalpha())
    index = 0
    for character in letters:
        index = index * 26 + (ord(character.upper()) - ord("A") + 1)
    return max(index - 1, 0)


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> object:
    cell_type = cell.attrib.get("t", "")
    value_node = cell.find(f"{_MAIN}v")
    if cell_type == "s" and value_node is not None:
        index = int(value_node.text or 0)
        return shared_strings[index] if 0 <= index < len(shared_strings) else ""
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.iter(f"{_MAIN}t"))
    if cell_type == "b" and value_node is not None:
        return (value_node.text or "") == "1"
    if cell_type in {"str", "e"} and value_node is not None:
        return value_node.text or ""
    if value_node is None:
        return ""
    text = (value_node.text or "").strip()
    return _coerce_scalar(text)


def _coerce_scalar(text: str) -> object:
    if not text:
        return ""
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    try:
        return float(text)
    except ValueError:
        return text


def _clean_header(value: object) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text)


def _dedupe_headers(headers: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    deduped: list[str] = []
    for header in headers:
        base = header or "column"
        count = counts.get(base, 0)
        deduped.append(base if count == 0 else f"{base}.{count}")
        counts[base] = count + 1
    return deduped


def _merge_structured_rows(workbook: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for sheet_name in STRUCTURED_SHEETS:
        for row in workbook[sheet_name]:
            key = _structured_key(row)
            if not key:
                continue
            bucket = merged.setdefault(
                key,
                {
                    "primary_name": "",
                    "cas": "",
                    "formula": "",
                    "smiles": "",
                    "source_sheets": [],
                    "structured_key": key,
                },
            )
            bucket["source_sheets"] = sorted(set([*bucket["source_sheets"], sheet_name]))
            bucket["primary_name"] = _first_non_empty(bucket["primary_name"], row.get("Industrial Designation or Chemical Name", ""))
            bucket["cas"] = _first_non_empty(bucket["cas"], row.get("CAS Registry Number", ""))
            bucket["formula"] = _first_non_empty(bucket["formula"], row.get("Chemical Formula", ""))
            bucket["smiles"] = _first_non_empty(bucket["smiles"], row.get("SMILES", ""))
            for column, value in row.items():
                if column not in bucket or bucket[column] in {"", None}:
                    bucket[column] = value
    return list(merged.values())


def _normalize_thermo_reference_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        for name_column, value_column, property_name, unit, property_code in THERMO_REFERENCE_SPECS:
            name = _clean_text(row.get(name_column, ""))
            value = row.get(value_column, "")
            if not name or value in {"", None}:
                continue
            value_num = _optional_float(value)
            if value_num is None:
                continue
            if property_code in {"Tb_K", "Tc_K"}:
                value_num = value_num - 273.15
            normalized.append(
                {
                    "name": name,
                    "property_name_candidate": property_name,
                    "property_code": property_code,
                    "value_num": value_num,
                    "value": f"{value_num:.8g}",
                    "unit": unit,
                    "source_sheet": SHEET_THERMO_REFERENCE,
                }
            )
    return normalized


def _structured_key(row: dict[str, object]) -> str:
    cas = _clean_text(row.get("CAS Registry Number", ""))
    smiles = _clean_text(row.get("SMILES", ""))
    name = _clean_text(row.get("Industrial Designation or Chemical Name", ""))
    if cas:
        return f"cas:{cas}"
    if smiles:
        return f"smiles:{smiles}"
    if name:
        return f"name:{_normalized_name(name)}"
    return ""


def _build_match_context(molecule_core: pd.DataFrame, molecule_alias: pd.DataFrame) -> dict[str, object]:
    seed_series = molecule_core.get("seed_id", pd.Series(dtype="object")).astype(str)
    base_molecule_core = molecule_core.loc[~seed_series.str.startswith(EXCEL_202603_SEED_PREFIX)].copy()

    cas_map: dict[str, set[str]] = defaultdict(set)
    name_map: dict[str, set[str]] = defaultdict(set)

    alias_enriched = molecule_alias.merge(
        base_molecule_core[["mol_id", "seed_id", "entity_scope", "inchikey"]],
        on="mol_id",
        how="left",
    )
    seed_id_text = alias_enriched["seed_id"].astype(str).str.strip()
    alias_enriched = alias_enriched.loc[
        alias_enriched["seed_id"].notna() & (seed_id_text != "") & (seed_id_text.str.lower() != "nan")
    ].copy()
    for row in alias_enriched.to_dict(orient="records"):
        alias_type = _clean_text(row.get("alias_type", ""))
        alias_value = _clean_text(row.get("alias_value", ""))
        mol_id = _clean_text(row.get("mol_id", ""))
        if not alias_value or not mol_id:
            continue
        if alias_type == "cas":
            cas_map[alias_value].add(mol_id)
        if alias_type in {"query_name", "synonym", "r_number", "coolprop_fluid"}:
            name_map[_normalized_name(alias_value)].add(mol_id)

    r_numbers = (
        alias_enriched.loc[alias_enriched["alias_type"].astype(str) == "r_number", ["mol_id", "alias_value"]]
        .drop_duplicates("mol_id")
        .rename(columns={"alias_value": "r_number"})
    )
    context_df = base_molecule_core[["mol_id", "seed_id", "entity_scope", "inchikey"]].drop_duplicates("mol_id")
    context_df = context_df.merge(r_numbers, on="mol_id", how="left")
    context_by_mol = {row["mol_id"]: row for row in context_df.fillna("").to_dict(orient="records")}

    return {
        "cas_map": cas_map,
        "name_map": name_map,
        "context_by_mol": context_by_mol,
        "existing_inchikeys": {
            _clean_text(value)
            for value in base_molecule_core.get("inchikey", pd.Series(dtype="object")).tolist()
            if _clean_text(value)
        },
    }


def _build_excel_candidate_alias_context(
    molecule_core: pd.DataFrame,
    molecule_alias: pd.DataFrame,
) -> dict[str, dict[str, dict[str, set[str]]]]:
    seed_series = molecule_core.get("seed_id", pd.Series(dtype="object")).astype(str)
    excel_molecule_core = molecule_core.loc[
        seed_series.str.startswith(EXCEL_202603_SEED_PREFIX),
        ["mol_id", "seed_id"],
    ].drop_duplicates("mol_id")
    if excel_molecule_core.empty or molecule_alias.empty:
        return {}

    alias_enriched = molecule_alias.merge(excel_molecule_core, on="mol_id", how="inner")
    allowed_alias_types = {"query_name", "synonym", "r_number", "coolprop_fluid"}
    aliases_by_seed: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(dict)

    for row in alias_enriched.to_dict(orient="records"):
        alias_type = _clean_text(row.get("alias_type", ""))
        alias_value = _clean_text(row.get("alias_value", ""))
        seed_id = _clean_text(row.get("seed_id", ""))
        if alias_type not in allowed_alias_types or not alias_value or not seed_id:
            continue
        normalized = _normalized_name(alias_value)
        if not normalized:
            continue
        bucket = aliases_by_seed[seed_id].setdefault(
            normalized,
            {"display_names": set(), "alias_types": set()},
        )
        bucket["display_names"].add(alias_value)
        bucket["alias_types"].add(alias_type)

    return {seed_id: names for seed_id, names in aliases_by_seed.items()}


def _build_current_property_lookup(property_recommended: pd.DataFrame) -> dict[str, set[str]]:
    if property_recommended.empty:
        return {}
    working = property_recommended.copy()
    if "selected_source_name" in working.columns:
        workbook_mask = working["selected_source_name"].astype(str).str.contains(WORKBOOK_FILE_NAME, regex=False, na=False)
        working = working.loc[~workbook_mask].copy()
    if working.empty:
        return {}
    return (
        working.groupby("mol_id")["property_name"]
        .agg(lambda series: {str(value).strip() for value in series.dropna().tolist() if str(value).strip()})
        .to_dict()
    )


def _resolve_molecule_id(
    row: dict[str, object],
    match_context: dict[str, object],
    *,
    name_field: str = "Industrial Designation or Chemical Name",
    allow_cas: bool = True,
    allow_name: bool = True,
) -> MatchRecord:
    cas = _clean_text(row.get("CAS Registry Number", "")) if allow_cas else ""
    name = _clean_text(row.get(name_field, "")) if allow_name else ""

    if cas:
        cas_hits = match_context["cas_map"].get(cas, set())
        if len(cas_hits) == 1:
            return MatchRecord(next(iter(cas_hits)), "cas")
        if len(cas_hits) > 1:
            return MatchRecord(None, "cas_ambiguous")

    if name:
        name_hits = match_context["name_map"].get(_normalized_name(name), set())
        if len(name_hits) == 1:
            return MatchRecord(next(iter(name_hits)), "name")
        if len(name_hits) > 1:
            return MatchRecord(None, "name_ambiguous")

    return MatchRecord(None, "unmatched")


def _build_supplement_rows(
    *,
    workbook_rows: dict[str, list[dict[str, object]]],
    thermo_reference_rows: list[dict[str, object]],
    match_context: dict[str, object],
    current_property_lookup: dict[str, set[str]],
) -> tuple[list[dict[str, object]], dict[str, object], set[tuple[str, str]]]:
    rows: list[dict[str, object]] = []
    summary = {
        "matched_rows_by_sheet": {},
        "imported_rows_by_property": defaultdict(int),
        "skipped_existing_property": defaultdict(int),
        "skipped_unmatched_rows": defaultdict(int),
        "skipped_name_ambiguous_rows": defaultdict(int),
        "skipped_out_of_range_rows": defaultdict(int),
        "new_dimension_rows": defaultdict(int),
        "gwp_report_only_rows": 0,
        "hvap_298k_report_only_rows": 0,
    }
    used_pairs: set[tuple[str, str]] = set()

    aspen_map = [
        ("Tb/K", "boiling_point_c", "degC"),
        ("Tc/K", "critical_temp_c", "degC"),
        ("Pc/MPa", "critical_pressure_mpa", "MPa"),
        ("ω", "acentric_factor", "dimensionless"),
    ]
    for row in workbook_rows[SHEET_NIST_ASPEN]:
        match = _resolve_molecule_id(row, match_context)
        if match.mol_id is None:
            bucket = "skipped_name_ambiguous_rows" if match.route == "name_ambiguous" else "skipped_unmatched_rows"
            summary[bucket][SHEET_NIST_ASPEN] += 1
            continue
        summary["matched_rows_by_sheet"][SHEET_NIST_ASPEN] = summary["matched_rows_by_sheet"].get(SHEET_NIST_ASPEN, 0) + 1
        for column, property_name, unit in aspen_map:
            value_num = _optional_float(row.get(column, ""))
            if value_num is None:
                continue
            if unit == "degC":
                value_num = value_num - 273.15
            mol_properties = current_property_lookup.get(match.mol_id, set())
            pair = (match.mol_id, property_name)
            if property_name in mol_properties or pair in used_pairs:
                summary["skipped_existing_property"][property_name] += 1
                continue
            context = match_context["context_by_mol"][match.mol_id]
            rows.append(
                _supplement_row(
                    seed_id=context["seed_id"],
                    r_number=context.get("r_number", ""),
                    property_name=property_name,
                    value_num=value_num,
                    unit=unit,
                    source_name=f"{WORKBOOK_FILE_NAME} / {SHEET_NIST_ASPEN}",
                    notes=f"matched_by={match.route}; workbook_sheet={SHEET_NIST_ASPEN}",
                    pressure="0.101325 MPa" if property_name == "boiling_point_c" else "",
                    phase="vapor-liquid_equilibrium" if property_name == "boiling_point_c" else "",
                )
            )
            used_pairs.add(pair)
            summary["imported_rows_by_property"][property_name] += 1

    for row in workbook_rows[SHEET_NIST_HVAP]:
        match = _resolve_molecule_id(row, match_context)
        if match.mol_id is None:
            bucket = "skipped_name_ambiguous_rows" if match.route == "name_ambiguous" else "skipped_unmatched_rows"
            summary[bucket][SHEET_NIST_HVAP] += 1
            continue
        summary["matched_rows_by_sheet"][SHEET_NIST_HVAP] = summary["matched_rows_by_sheet"].get(SHEET_NIST_HVAP, 0) + 1
        value_num = _optional_float(row.get("ΔvapH/kJ·mol-1", ""))
        if value_num is None:
            continue
        pair = (match.mol_id, "vaporization_enthalpy_kjmol")
        mol_properties = current_property_lookup.get(match.mol_id, set())
        if "vaporization_enthalpy_kjmol" in mol_properties or pair in used_pairs:
            summary["skipped_existing_property"]["vaporization_enthalpy_kjmol"] += 1
            continue
        context = match_context["context_by_mol"][match.mol_id]
        tb_k = _optional_float(row.get("Tb/K", ""))
        temperature = f"{tb_k:.6g} K" if tb_k is not None else ""
        rows.append(
            _supplement_row(
                seed_id=context["seed_id"],
                r_number=context.get("r_number", ""),
                property_name="vaporization_enthalpy_kjmol",
                value_num=value_num,
                unit="kJ/mol",
                source_name=f"{WORKBOOK_FILE_NAME} / {SHEET_NIST_HVAP}",
                notes=f"matched_by={match.route}; workbook_sheet={SHEET_NIST_HVAP}",
                temperature=temperature,
                phase="vapor-liquid_equilibrium",
            )
        )
        used_pairs.add(pair)
        summary["imported_rows_by_property"]["vaporization_enthalpy_kjmol"] += 1

    for row in workbook_rows[SHEET_GWP_ODP]:
        match = _resolve_molecule_id(row, match_context)
        if match.mol_id is None:
            bucket = "skipped_name_ambiguous_rows" if match.route == "name_ambiguous" else "skipped_unmatched_rows"
            summary[bucket][SHEET_GWP_ODP] += 1
            continue
        summary["matched_rows_by_sheet"][SHEET_GWP_ODP] = summary["matched_rows_by_sheet"].get(SHEET_GWP_ODP, 0) + 1
        odp = _optional_float(row.get("ODP", ""))
        if odp is not None:
            if not 0 <= odp <= 1:
                summary["skipped_out_of_range_rows"]["odp"] += 1
            else:
                pair = (match.mol_id, "odp")
                mol_properties = current_property_lookup.get(match.mol_id, set())
                if "odp" in mol_properties or pair in used_pairs:
                    summary["skipped_existing_property"]["odp"] += 1
                else:
                    context = match_context["context_by_mol"][match.mol_id]
                    rows.append(
                        _supplement_row(
                            seed_id=context["seed_id"],
                            r_number=context.get("r_number", ""),
                            property_name="odp",
                            value_num=odp,
                            unit="dimensionless",
                            source_name=f"{WORKBOOK_FILE_NAME} / {SHEET_GWP_ODP}",
                            notes=f"matched_by={match.route}; workbook_sheet={SHEET_GWP_ODP}",
                        )
                    )
                    used_pairs.add(pair)
                    summary["imported_rows_by_property"]["odp"] += 1

        gwp_value = _optional_float(row.get("GWP", ""))
        if gwp_value is not None:
            summary["gwp_report_only_rows"] += 1

    for row in workbook_rows[SHEET_ODS]:
        match = _resolve_molecule_id(row, match_context)
        if match.mol_id is None:
            bucket = "skipped_name_ambiguous_rows" if match.route == "name_ambiguous" else "skipped_unmatched_rows"
            summary[bucket][SHEET_ODS] += 1
            continue
        summary["matched_rows_by_sheet"][SHEET_ODS] = summary["matched_rows_by_sheet"].get(SHEET_ODS, 0) + 1
        zc_value = _optional_float(row.get("Zc", ""))
        if zc_value is None:
            continue
        pair = (match.mol_id, "critical_compressibility_factor")
        if pair in used_pairs:
            continue
        context = match_context["context_by_mol"][match.mol_id]
        rows.append(
            _supplement_row(
                seed_id=context["seed_id"],
                r_number=context.get("r_number", ""),
                property_name="critical_compressibility_factor",
                value_num=zc_value,
                unit="dimensionless",
                source_name=f"{WORKBOOK_FILE_NAME} / {SHEET_ODS}",
                notes=f"matched_by={match.route}; workbook_sheet={SHEET_ODS}",
            )
        )
        used_pairs.add(pair)
        summary["imported_rows_by_property"]["critical_compressibility_factor"] += 1
        summary["new_dimension_rows"]["critical_compressibility_factor"] += 1

    thermo_priority = {"boiling_point_c", "critical_temp_c", "critical_pressure_mpa", "acentric_factor"}
    for row in thermo_reference_rows:
        if row["property_name_candidate"] not in thermo_priority:
            if row["property_code"] == "Hv_298K_kJmol":
                summary["hvap_298k_report_only_rows"] += 1
            continue
        match = _resolve_molecule_id(
            {"Industrial Designation or Chemical Name": row["name"], "CAS Registry Number": ""},
            match_context,
            allow_cas=False,
            allow_name=True,
        )
        if match.route != "name" or match.mol_id is None:
            continue
        pair = (match.mol_id, str(row["property_name_candidate"]))
        mol_properties = current_property_lookup.get(match.mol_id, set())
        if row["property_name_candidate"] in mol_properties or pair in used_pairs:
            summary["skipped_existing_property"][str(row["property_name_candidate"])] += 1
            continue
        context = match_context["context_by_mol"][match.mol_id]
        rows.append(
            _supplement_row(
                seed_id=context["seed_id"],
                r_number=context.get("r_number", ""),
                property_name=str(row["property_name_candidate"]),
                value_num=float(row["value_num"]),
                unit=str(row["unit"]),
                source_name=f"{WORKBOOK_FILE_NAME} / {SHEET_THERMO_REFERENCE}",
                notes="matched_by=name; workbook_sheet=2-热物性参考; source_tier=name_only_reference",
            )
        )
        used_pairs.add(pair)
        summary["imported_rows_by_property"][str(row["property_name_candidate"])] += 1

    summary["matched_rows_by_sheet"] = dict(sorted(summary["matched_rows_by_sheet"].items()))
    summary["imported_rows_by_property"] = dict(sorted(summary["imported_rows_by_property"].items()))
    summary["skipped_existing_property"] = dict(sorted(summary["skipped_existing_property"].items()))
    summary["skipped_unmatched_rows"] = dict(sorted(summary["skipped_unmatched_rows"].items()))
    summary["skipped_name_ambiguous_rows"] = dict(sorted(summary["skipped_name_ambiguous_rows"].items()))
    summary["skipped_out_of_range_rows"] = dict(sorted(summary["skipped_out_of_range_rows"].items()))
    summary["new_dimension_rows"] = dict(sorted(summary["new_dimension_rows"].items()))
    summary["imported_row_count"] = len(rows)
    return rows, summary, used_pairs


def _supplement_row(
    *,
    seed_id: str,
    r_number: str,
    property_name: str,
    value_num: float,
    unit: str,
    source_name: str,
    notes: str,
    temperature: str = "",
    pressure: str = "",
    phase: str = "",
) -> dict[str, object]:
    return {
        "seed_id": seed_id,
        "r_number": r_number,
        "property_name": property_name,
        "value": f"{float(value_num):.8g}",
        "value_num": float(value_num),
        "unit": unit,
        "temperature": temperature,
        "pressure": pressure,
        "phase": phase,
        "source_type": "derived_harmonized",
        "source_name": source_name,
        "source_url": "",
        "method": "Excel 202603 import (CAS/name exact match)",
        "uncertainty": "",
        "quality_level": "derived_harmonized",
        "assessment_version": "",
        "time_horizon": "",
        "year": "",
        "notes": notes,
    }


def _build_structured_candidate_rows(
    *,
    structured_rows: list[dict[str, object]],
    match_context: dict[str, object],
) -> tuple[list[dict[str, str]], dict[str, object], list[dict[str, object]]]:
    rows: list[dict[str, str]] = []
    contexts: list[dict[str, object]] = []
    summary = {
        "structured_rows": len(structured_rows),
        "skipped_existing_alias_match": 0,
        "skipped_existing_structure_match": 0,
        "skipped_no_smiles": 0,
        "skipped_screening_error": 0,
        "skipped_hard_filter": 0,
        "exported_rows": 0,
        "route_counts": defaultdict(int),
    }
    exported_inchikeys: set[str] = set()
    existing_inchikeys = match_context["existing_inchikeys"]

    for row in structured_rows:
        match = _resolve_molecule_id(
            {
                "Industrial Designation or Chemical Name": row.get("primary_name", ""),
                "CAS Registry Number": row.get("cas", ""),
            },
            match_context,
        )
        if match.mol_id is not None:
            summary["skipped_existing_alias_match"] += 1
            continue

        raw_smiles = _clean_text(row.get("smiles", ""))
        if not raw_smiles:
            summary["skipped_no_smiles"] += 1
            continue

        try:
            screening = compute_screening_features(raw_smiles)
        except Exception:  # noqa: BLE001
            summary["skipped_screening_error"] += 1
            continue

        if screening["inchikey"] in existing_inchikeys or screening["inchikey"] in exported_inchikeys:
            summary["skipped_existing_structure_match"] += 1
            continue

        if not _passes_project_hard_filters(screening):
            summary["skipped_hard_filter"] += 1
            continue

        query_name = _clean_text(row.get("cas", "")) or screening["isomeric_smiles"]
        query_type = "name" if _clean_text(row.get("cas", "")) else "smiles"
        route_key = "cas" if query_type == "name" else "smiles"
        summary["route_counts"][route_key] += 1

        seed_id = _structured_candidate_seed_id(query_name, query_type)
        notes = (
            f"generated from {WORKBOOK_FILE_NAME} structured sheets={','.join(row.get('source_sheets', []))}; "
            f"primary_name={_clean_text(row.get('primary_name', '')) or screening['isomeric_smiles']}"
        )
        rows.append(
            {
                "seed_id": seed_id,
                "r_number": "",
                "family": _infer_candidate_family(row, screening),
                "query_name": query_name,
                "pubchem_query_type": query_type,
                "nist_query": "",
                "nist_query_type": "name",
                "coolprop_fluid": "",
                "priority_tier": "4",
                "selection_role": "inventory",
                "coverage_tier": "D",
                "source_bundle": "excel_202603_structured",
                "coolprop_support_expected": "no",
                "regulatory_priority": "low",
                "entity_scope": "candidate",
                "model_inclusion": "no",
                "notes": notes,
            }
        )
        contexts.append(
            {
                "seed_id": seed_id,
                "structured_row": row,
                "screening": screening,
            }
        )
        exported_inchikeys.add(screening["inchikey"])

    summary["route_counts"] = dict(sorted(summary["route_counts"].items()))
    summary["exported_rows"] = len(rows)
    return rows, summary, contexts


def _build_candidate_observation_rows(
    *,
    candidate_contexts: list[dict[str, object]],
    thermo_reference_rows: list[dict[str, object]],
    candidate_alias_context: dict[str, dict[str, dict[str, set[str]]]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    rows: list[dict[str, object]] = []
    summary = {
        "candidate_count": len(candidate_contexts),
        "imported_row_count": 0,
        "imported_rows_by_property": defaultdict(int),
        "thermo_reference_backfill_rows": 0,
        "thermo_reference_alias_backfill_rows": 0,
        "skipped_thermo_reference_conflict_rows": defaultdict(int),
        "skipped_out_of_range_rows": defaultdict(int),
    }
    thermo_by_name_property: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in thermo_reference_rows:
        property_name = str(row.get("property_name_candidate", ""))
        if property_name not in {"boiling_point_c", "critical_temp_c", "critical_pressure_mpa", "acentric_factor"}:
            continue
        key = (_normalized_name(str(row.get("name", ""))), property_name)
        thermo_by_name_property[key].append(row)

    property_specs = [
        ("Tb/K", "boiling_point_c", "degC"),
        ("Tc/K", "critical_temp_c", "degC"),
        ("Pc/MPa", "critical_pressure_mpa", "MPa"),
        ("ω", "acentric_factor", "dimensionless"),
        ("ΔvapH/kJ·mol-1", "vaporization_enthalpy_kjmol", "kJ/mol"),
        ("ODP", "odp", "dimensionless"),
        ("Zc", "critical_compressibility_factor", "dimensionless"),
    ]

    for item in candidate_contexts:
        seed_id = str(item["seed_id"])
        structured_row = dict(item["structured_row"])
        primary_name = _clean_text(structured_row.get("primary_name", ""))
        normalized_name = _normalized_name(primary_name)
        tb_k_for_hvap = _optional_float(structured_row.get("Tb/K", ""))
        name_candidates: list[tuple[str, str, set[str]]] = []
        if normalized_name:
            name_candidates.append((normalized_name, primary_name, {"primary_name"}))
        for alias_name, alias_meta in sorted(candidate_alias_context.get(seed_id, {}).items()):
            if alias_name == normalized_name:
                continue
            display_names = sorted(alias_meta.get("display_names", set()))
            alias_types = {str(value) for value in alias_meta.get("alias_types", set()) if str(value)}
            name_candidates.append((alias_name, display_names[0] if display_names else alias_name, alias_types))

        for column, property_name, unit in property_specs:
            value_num = _optional_float(structured_row.get(column, ""))
            source_name = ""
            notes = ""
            temperature = ""
            phase = ""
            pressure = ""

            if value_num is not None:
                source_name = f"{WORKBOOK_FILE_NAME} / {_source_sheet_for_property(property_name)}"
                notes = f"generated_candidate_seed={seed_id}; workbook_property_source={column}"
            elif property_name in {"boiling_point_c", "critical_temp_c", "critical_pressure_mpa", "acentric_factor"} and name_candidates:
                thermo_match = _resolve_candidate_thermo_reference_match(
                    property_name=property_name,
                    name_candidates=name_candidates,
                    thermo_by_name_property=thermo_by_name_property,
                )
                if thermo_match is None:
                    continue
                if thermo_match["conflict"]:
                    summary["skipped_thermo_reference_conflict_rows"][property_name] += 1
                    continue
                thermo_row = thermo_match["row"]
                value_num = float(thermo_row["value_num"])
                source_name = f"{WORKBOOK_FILE_NAME} / {SHEET_THERMO_REFERENCE}"
                notes = (
                    f"generated_candidate_seed={seed_id}; workbook_property_source={thermo_row['property_code']}; "
                    f"bridged_from=2-热物性参考; matched_name={thermo_match['matched_name']}; "
                    f"matched_via={','.join(sorted(thermo_match['matched_via']))}"
                )
                summary["thermo_reference_backfill_rows"] += 1
                if thermo_match["matched_via_alias"]:
                    summary["thermo_reference_alias_backfill_rows"] += 1
            else:
                continue

            if property_name == "odp" and not 0 <= float(value_num) <= 1:
                summary["skipped_out_of_range_rows"]["odp"] += 1
                continue

            if property_name in {"boiling_point_c", "critical_temp_c"} and column in {"Tb/K", "Tc/K"} and _optional_float(structured_row.get(column, "")) is not None:
                value_num = value_num - 273.15

            if property_name == "boiling_point_c":
                pressure = "0.101325 MPa"
                phase = "vapor-liquid_equilibrium"
            if property_name == "vaporization_enthalpy_kjmol":
                temperature = f"{tb_k_for_hvap:.6g} K" if tb_k_for_hvap is not None else ""
                phase = "vapor-liquid_equilibrium"

            rows.append(
                _supplement_row(
                    seed_id=seed_id,
                    r_number="",
                    property_name=property_name,
                    value_num=float(value_num),
                    unit=unit,
                    source_name=source_name,
                    notes=notes,
                    temperature=temperature,
                    pressure=pressure,
                    phase=phase,
                )
            )
            summary["imported_rows_by_property"][property_name] += 1

    summary["imported_rows_by_property"] = dict(sorted(summary["imported_rows_by_property"].items()))
    summary["skipped_thermo_reference_conflict_rows"] = dict(
        sorted(summary["skipped_thermo_reference_conflict_rows"].items())
    )
    summary["skipped_out_of_range_rows"] = dict(sorted(summary["skipped_out_of_range_rows"].items()))
    summary["imported_row_count"] = len(rows)
    return rows, summary


def _resolve_candidate_thermo_reference_match(
    *,
    property_name: str,
    name_candidates: list[tuple[str, str, set[str]]],
    thermo_by_name_property: dict[tuple[str, str], list[dict[str, object]]],
) -> dict[str, object] | None:
    matches: list[dict[str, object]] = []
    seen: set[tuple[str, str, float, str]] = set()
    for normalized_name, display_name, matched_via in name_candidates:
        if not normalized_name:
            continue
        for thermo_row in thermo_by_name_property.get((normalized_name, property_name), []):
            value_num = _optional_float(thermo_row.get("value_num", ""))
            if value_num is None:
                continue
            dedupe_key = (
                normalized_name,
                property_name,
                float(value_num),
                _clean_text(thermo_row.get("property_code", "")),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            matches.append(
                {
                    "row": thermo_row,
                    "value_num": float(value_num),
                    "matched_name": display_name or _clean_text(thermo_row.get("name", "")),
                    "matched_via": matched_via,
                    "matched_via_alias": "primary_name" not in matched_via,
                }
            )

    if not matches:
        return None

    unique_values = {match["value_num"] for match in matches}
    if len(unique_values) > 1:
        return {"conflict": True}

    def _priority(match: dict[str, object]) -> tuple[int, str]:
        matched_via = {str(value) for value in match["matched_via"]}
        return (0 if "primary_name" in matched_via else 1, str(match["matched_name"]))

    best = sorted(matches, key=_priority)[0]
    return {
        "conflict": False,
        "row": best["row"],
        "matched_name": best["matched_name"],
        "matched_via": best["matched_via"],
        "matched_via_alias": best["matched_via_alias"],
    }


def _passes_project_hard_filters(screening: dict[str, object]) -> bool:
    return bool(
        screening["charge"] == 0
        and screening["allowed_elements_only"]
        and int(screening["total_atom_count"]) <= 18
        and 1 <= int(screening["heavy_atom_count"]) <= 15
        and 16 <= float(screening["molecular_weight"]) <= 300
        and 1 <= int(screening["carbon_count"]) <= 6
    )


def _structured_candidate_seed_id(query_name: str, query_type: str) -> str:
    cleaned = slugify(query_name)
    if cleaned and len(cleaned) <= 32:
        return f"tierd_excel202603_{query_type}_{cleaned}"
    digest = hashlib.sha1(query_name.encode("utf-8")).hexdigest()[:12]
    return f"tierd_excel202603_{query_type}_{digest}"


def _infer_candidate_family(row: dict[str, object], screening: dict[str, object]) -> str:
    name_upper = _clean_text(row.get("primary_name", "")).upper()
    if name_upper.startswith("HCFC-"):
        return "HCFC"
    if name_upper.startswith("HCFO-"):
        return "HCFO"
    if name_upper.startswith("HFO-"):
        return "HFO"
    if name_upper.startswith("HFC-"):
        return "HFC"
    if name_upper.startswith("CFC-"):
        return "CFC"
    if screening["has_halogen"] and screening["has_c_c_double_bond"]:
        if int(screening["atom_count_cl"]) or int(screening["atom_count_br"]) or int(screening["atom_count_i"]):
            return "HCFO"
        return "HFO"
    if screening["has_halogen"]:
        if int(screening["atom_count_cl"]) or int(screening["atom_count_br"]) or int(screening["atom_count_i"]):
            return "HCFC" if int(screening["atom_count_h"]) else "CFC"
        return "HFC"
    if screening["has_ether"]:
        return "Ether"
    if screening["has_carbonyl"]:
        return "Ketone"
    return "Candidate"


def _build_name_only_staging_rows(
    *,
    thermo_reference_rows: list[dict[str, object]],
    match_context: dict[str, object],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    rows: list[dict[str, object]] = []
    summary = {
        "input_rows": len(thermo_reference_rows),
        "staged_rows": 0,
        "staged_unique_names": 0,
        "property_counts": defaultdict(int),
    }
    seen_keys: set[tuple[str, str]] = set()
    staged_names: set[str] = set()

    for row in thermo_reference_rows:
        match = _resolve_molecule_id(
            {"Industrial Designation or Chemical Name": row["name"], "CAS Registry Number": ""},
            match_context,
            allow_cas=False,
            allow_name=True,
        )
        if match.mol_id is not None or match.route == "name_ambiguous":
            continue
        key = (str(row["name"]), str(row["property_code"]))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        staged_names.add(str(row["name"]))
        rows.append(
            {
                "name": row["name"],
                "property_code": row["property_code"],
                "property_name_candidate": row["property_name_candidate"],
                "value": row["value"],
                "value_num": row["value_num"],
                "unit": row["unit"],
                "source_sheet": row["source_sheet"],
                "match_status": "unmatched_name_only",
                "next_action": "resolve_structure_or_external_id_before_seed_catalog",
                "notes": "thermo reference workbook-only name staged for later identity resolution",
            }
        )
        summary["property_counts"][str(row["property_code"])] += 1

    summary["property_counts"] = dict(sorted(summary["property_counts"].items()))
    summary["staged_rows"] = len(rows)
    summary["staged_unique_names"] = len(staged_names)
    return rows, summary


def _build_brief_report(
    *,
    workbook_name: str,
    workbook_rows: dict[str, list[dict[str, object]]],
    structured_rows: list[dict[str, object]],
    thermo_reference_rows: list[dict[str, object]],
    supplement_summary: dict[str, object],
    candidate_summary: dict[str, object],
    candidate_observation_summary: dict[str, object],
    staging_summary: dict[str, object],
) -> str:
    paid_rows = workbook_rows.get(SHEET_NIST_PAID, [])
    unique_paid_names = len({_clean_text(row.get("Industrial Designation or Chemical Name", "")) for row in paid_rows if _clean_text(row.get("Industrial Designation or Chemical Name", ""))})
    lines = [
        "# Excel `制冷剂数据库202603.xlsx` 简短报告",
        "",
        "## 结论",
        "",
        "- 这份 workbook 适合同时承担两件事：补当前数据库缺失标签，以及扩一批新的 `Tier D` inventory-only 候选。",
        "- 本批已按保守口径落地：正式补 `Tb/Tc/Pc/ω/ΔvapH/ODP` 与新维度 `Zc`，不把未标明时间尺度的裸 `GWP` 和 `Hv[298K]` 直接并入主表。",
        "- `2-热物性参考` 已按名字级弱证据处理：现有库只做唯一精确名称补充，workbook-only 条目只进入 staging，不直接进 `seed_catalog`。",
        "",
        "## Workbook 结构判断",
        "",
        f"- `{SHEET_ODS}`: {len(workbook_rows[SHEET_ODS])} 行，适合作为结构化交叉校验和 `Zc` 来源。",
        f"- `{SHEET_NIST_ASPEN}`: {len(workbook_rows[SHEET_NIST_ASPEN])} 行，是 `Tb/Tc/Pc/ω` 的主补库来源。",
        f"- `{SHEET_NIST_HVAP}`: {len(workbook_rows[SHEET_NIST_HVAP])} 行，是 `ΔvapH` 的主补库来源。",
        f"- `{SHEET_GWP_ODP}`: {len(workbook_rows[SHEET_GWP_ODP])} 行，当前只正式吸收 `ODP`。",
        f"- `{SHEET_THERMO_REFERENCE}`: {len(workbook_rows[SHEET_THERMO_REFERENCE])} 行，经拆长后为 {len(thermo_reference_rows)} 条 name-only 参考记录。",
        f"- `{SHEET_NIST_PAID}`: {len(paid_rows)} 行，但只有 {unique_paid_names} 个唯一名称，格式异常且证据链不完整，本批只做风险提示，不自动入库。",
        "",
        "## 对现有数据库的补充",
        "",
        f"- 新写入 observation 行数: `{supplement_summary['imported_row_count']}`。",
    ]
    for property_name, count in supplement_summary["imported_rows_by_property"].items():
        lines.append(f"- `{property_name}` 新补充 `{count}` 行。")
    lines.extend(
        [
            f"- 裸 `GWP` 命中 `{supplement_summary['gwp_report_only_rows']}` 行，但因时间尺度未明确，当前只保留在报告分析里。",
            f"- `Hv[298K]` 命中 `{supplement_summary['hvap_298k_report_only_rows']}` 行，但与现有 `vaporization_enthalpy_kjmol` 口径不同，当前不并表。",
            "",
            "## 新维度与扩库存",
            "",
            "- 已正式新增 `critical_compressibility_factor` (`Zc`) 为 numeric property，但不进入模型目标集合。",
            f"- 结构化 workbook-only 条目合并后共有 `{candidate_summary['structured_rows']}` 条候选视图。",
            f"- 其中导出到 generated `Tier D` 候选补充文件的共有 `{candidate_summary['exported_rows']}` 条。",
            f"- 这些 workbook-only 候选额外贡献了 `{candidate_observation_summary['imported_row_count']}` 行 workbook property observations。",
            f"- 因现库别名已命中而跳过 `{candidate_summary['skipped_existing_alias_match']}` 条，因结构重复而跳过 `{candidate_summary['skipped_existing_structure_match']}` 条。",
            f"- 其中 `{candidate_observation_summary['thermo_reference_backfill_rows']}` 行候选属性来自 `2-热物性参考` 的精确名字桥接回填。",
            f"- 上述桥接里有 `{candidate_observation_summary['thermo_reference_alias_backfill_rows']}` 行来自已解析 alias 的二次增强。",
            f"- 另有 `{sum(candidate_observation_summary['skipped_thermo_reference_conflict_rows'].values())}` 行因同一候选属性存在冲突值而被跳过。",
            f"- 另外过滤掉 `{candidate_observation_summary['skipped_out_of_range_rows'].get('odp', 0)}` 行超范围 `ODP` 值，避免把明显异常值写入主库。",
            f"- `2-热物性参考` workbook-only 名称已写入 staging: `{staging_summary['staged_rows']}` 行 / `{staging_summary['staged_unique_names']}` 个唯一名称。",
            "",
            "## 风险说明",
            "",
            "- `2-热物性参考` 只有名字，没有 CAS/SMILES；因此它适合做现有库补缺或后续人工/程序解析前的暂存，不适合直接当作结构化 seed 源。",
            "- `1-NIST付费数据` 当前表内重复和结构都不稳定，自动入库风险高，本批不使用。",
            "- generated `Tier D` 候选默认全部按 `candidate` 处理，不自动提升为 `refrigerant`。",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _normalized_name(value: str) -> str:
    text = _clean_text(value).casefold()
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def _first_non_empty(current: object, candidate: object) -> str:
    return _clean_text(current) or _clean_text(candidate)


def _source_sheet_for_property(property_name: str) -> str:
    mapping = {
        "boiling_point_c": SHEET_NIST_ASPEN,
        "critical_temp_c": SHEET_NIST_ASPEN,
        "critical_pressure_mpa": SHEET_NIST_ASPEN,
        "acentric_factor": SHEET_NIST_ASPEN,
        "vaporization_enthalpy_kjmol": SHEET_NIST_HVAP,
        "odp": SHEET_GWP_ODP,
        "critical_compressibility_factor": SHEET_ODS,
    }
    return mapping[property_name]


def _optional_float(value: object) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    text = text.replace(",", "")
    if text.startswith("<"):
        return None
    try:
        return float(text)
    except ValueError:
        return None
