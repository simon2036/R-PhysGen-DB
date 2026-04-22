from __future__ import annotations

import csv
import importlib.util
from pathlib import Path
from xml.sax.saxutils import escape
import zipfile

import pandas as pd

from r_physgen_db.constants import MODEL_TARGET_PROPERTIES, NUMERIC_PROPERTIES
from r_physgen_db.chemistry import compute_screening_features
from r_physgen_db.sources.excel_202603 import (
    SHEET_GWP_ODP,
    SHEET_NIST_ASPEN,
    SHEET_NIST_HVAP,
    SHEET_NIST_PAID,
    SHEET_ODS,
    SHEET_THERMO_REFERENCE,
    WORKBOOK_FILE_NAME,
    build_excel_202603_outputs,
    normalize_excel_202603_workbook,
    parse_excel_202603_workbook,
)


ROOT = Path(__file__).resolve().parents[1]


def _load_seed_generator_module():
    path = ROOT / "pipelines" / "generate_wave2_seed_catalog.py"
    spec = importlib.util.spec_from_file_location("generate_wave2_seed_catalog", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _column_letter(index: int) -> str:
    value = ""
    current = index + 1
    while current:
        current, remainder = divmod(current - 1, 26)
        value = chr(ord("A") + remainder) + value
    return value


def _cell_xml(ref: str, value: object) -> str:
    if value in {"", None}:
        return ""
    if isinstance(value, bool):
        return f'<c r="{ref}" t="b"><v>{"1" if value else "0"}</v></c>'
    if isinstance(value, (int, float)):
        return f'<c r="{ref}"><v>{value}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _sheet_xml(rows: list[list[object]]) -> str:
    body: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cells = []
        for column_index, value in enumerate(row):
            ref = f"{_column_letter(column_index)}{row_index}"
            cell = _cell_xml(ref, value)
            if cell:
                cells.append(cell)
        body.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(body)}</sheetData>'
        "</worksheet>"
    )


def _write_test_workbook(path: Path, sheets: dict[str, list[list[object]]]) -> None:
    workbook_xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">',
        "<sheets>",
    ]
    rels_xml = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">',
    ]
    worksheet_items: list[tuple[str, str]] = []
    for index, (name, rows) in enumerate(sheets.items(), start=1):
        workbook_xml.append(f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>')
        rels_xml.append(
            f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
        )
        worksheet_items.append((f"xl/worksheets/sheet{index}.xml", _sheet_xml(rows)))
    workbook_xml.extend(["</sheets>", "</workbook>"])
    rels_xml.append("</Relationships>")

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("xl/workbook.xml", "".join(workbook_xml).encode("utf-8"))
        archive.writestr("xl/_rels/workbook.xml.rels", "".join(rels_xml).encode("utf-8"))
        for worksheet_path, xml_text in worksheet_items:
            archive.writestr(worksheet_path, xml_text.encode("utf-8"))


def _test_workbook_sheets() -> dict[str, list[list[object]]]:
    return {
        SHEET_ODS: [
            ["Industrial Designation or Chemical Name", "CAS Registry Number", "Chemical Formula", "SMILES", "Zc"],
            ["R-1234yf", "754-12-1", "C3H2F4", "C=C(C(F)(F)F)F", 0.258],
            ["Novel Candidate", "111-11-1", "C2H2F2", "F/C=C/F", 0.21],
            ["Alias Candidate", "222-22-2", "C2H2Cl2", "Cl/C=C/Cl", 0.22],
        ],
        SHEET_NIST_ASPEN: [
            ["Industrial Designation or Chemical Name", "CAS Registry Number", "Tb/K", "Tc/K", "Pc/MPa", "ω"],
            ["R-1234yf", "754-12-1", 243.7, 367.85, 3.382, 0.276],
            ["Novel Candidate", "111-11-1", 220.0, 320.0, 4.1, 0.11],
        ],
        SHEET_NIST_HVAP: [
            ["Industrial Designation or Chemical Name", "CAS Registry Number", "Tb/K", "ΔvapH/kJ·mol-1"],
            ["R-1234yf", "754-12-1", 243.7, 18.7],
        ],
        SHEET_NIST_PAID: [
            ["Industrial Designation or Chemical Name", "Misc"],
            ["Paid Only", "unstable format"],
        ],
        SHEET_GWP_ODP: [
            ["Industrial Designation or Chemical Name", "CAS Registry Number", "GWP", "ODP"],
            ["R-1234yf", "754-12-1", 4.0, 0.0],
        ],
        SHEET_THERMO_REFERENCE: [
            ["name", "Tb", "name", "Tc", "name", "Pc/MPa", "name", "Hv[298K]", "name", "ω"],
            ["R-1234yf", 243.7, "R-1234yf", 367.85, "R-1234yf", 3.382, "R-1234yf", 16.2, "R-1234yf", 0.276],
            ["Workbook Only Name", 250.0, "", "", "", "", "", "", "", ""],
        ],
    }


def test_parse_excel_202603_workbook_preserves_unicode_headers(tmp_path: Path) -> None:
    workbook_path = tmp_path / WORKBOOK_FILE_NAME
    _write_test_workbook(workbook_path, _test_workbook_sheets())

    parsed = parse_excel_202603_workbook(workbook_path)
    normalized = normalize_excel_202603_workbook(workbook_path)

    assert "ω" in parsed[SHEET_NIST_ASPEN][0]
    assert "ΔvapH/kJ·mol-1" in parsed[SHEET_NIST_HVAP][0]
    assert {"name", "name.1", "name.2", "name.3", "name.4"}.issubset(set(parsed[SHEET_THERMO_REFERENCE][0]))
    assert any(row["property_code"] == "omega" for row in normalized["thermo_reference_rows"])


def test_build_excel_202603_outputs_emits_supplements_candidates_and_staging(tmp_path: Path) -> None:
    workbook_path = tmp_path / WORKBOOK_FILE_NAME
    _write_test_workbook(workbook_path, _test_workbook_sheets())

    existing = compute_screening_features("C=C(C(F)(F)F)F")
    generated_candidate = compute_screening_features("F/C=C/F")
    alias_candidate = compute_screening_features("Cl/C=C/Cl")

    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_existing",
                "seed_id": "anchor_r1234yf",
                "entity_scope": "refrigerant",
                "inchikey": existing["inchikey"],
            },
            {
                "mol_id": "mol_existing_excel_candidate",
                "seed_id": "tierd_excel202603_smiles_generated",
                "entity_scope": "candidate",
                "inchikey": generated_candidate["inchikey"],
            },
            {
                "mol_id": "mol_alias_excel_candidate",
                "seed_id": "tierd_excel202603_name_222_22_2",
                "entity_scope": "candidate",
                "inchikey": alias_candidate["inchikey"],
            },
        ]
    )
    molecule_alias = pd.DataFrame(
        [
            {"mol_id": "mol_existing", "alias_type": "cas", "alias_value": "754-12-1"},
            {"mol_id": "mol_existing", "alias_type": "query_name", "alias_value": "2,3,3,3-tetrafluoroprop-1-ene"},
            {"mol_id": "mol_existing", "alias_type": "synonym", "alias_value": "R-1234yf"},
            {"mol_id": "mol_existing", "alias_type": "r_number", "alias_value": "R-1234yf"},
            {"mol_id": "mol_alias_excel_candidate", "alias_type": "synonym", "alias_value": "Workbook Only Name"},
        ]
    )
    property_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_existing",
                "property_name": "critical_temp_c",
                "selected_source_name": "Other Source",
            },
            {
                "mol_id": "mol_existing",
                "property_name": "boiling_point_c",
                "selected_source_name": f"{WORKBOOK_FILE_NAME} / {SHEET_NIST_ASPEN}",
            },
        ]
    )

    outputs = build_excel_202603_outputs(
        workbook_path=workbook_path,
        molecule_core=molecule_core,
        molecule_alias=molecule_alias,
        property_recommended=property_recommended,
    )

    supplement = outputs["supplement_rows"]
    existing_supplement = outputs["existing_supplement_rows"]
    candidate_observations = outputs["candidate_observation_rows"]
    candidate_rows = outputs["candidate_rows"]
    staging = outputs["name_only_staging_rows"]

    assert set(existing_supplement["property_name"]) >= {
        "boiling_point_c",
        "critical_pressure_mpa",
        "acentric_factor",
        "vaporization_enthalpy_kjmol",
        "odp",
        "critical_compressibility_factor",
    }
    assert "critical_temp_c" not in set(existing_supplement["property_name"])
    assert "gwp_100yr" not in set(supplement["property_name"])
    assert "vaporization_enthalpy_kjmol_at_298k_candidate" not in set(supplement["property_name"])

    assert len(candidate_rows) == 2
    assert set(candidate_rows["query_name"]) == {"111-11-1", "222-22-2"}
    assert set(candidate_rows["pubchem_query_type"]) == {"name"}
    assert set(candidate_rows["coverage_tier"]) == {"D"}
    assert set(candidate_rows["source_bundle"]) == {"excel_202603_structured"}
    assert set(candidate_observations["property_name"]) >= {
        "boiling_point_c",
        "critical_temp_c",
        "critical_pressure_mpa",
        "acentric_factor",
        "critical_compressibility_factor",
    }
    assert set(candidate_observations["seed_id"]) == set(candidate_rows["seed_id"])

    alias_seed_rows = candidate_observations.loc[
        candidate_observations["seed_id"] == "tierd_excel202603_name_222_22_2"
    ]
    assert "boiling_point_c" in set(alias_seed_rows["property_name"])
    assert any("matched_via=synonym" in str(value) for value in alias_seed_rows["notes"].tolist())
    assert outputs["summary"]["candidate_observation"]["thermo_reference_alias_backfill_rows"] == 1

    assert staging["name"].tolist() == ["Workbook Only Name"]
    assert "critical_compressibility_factor" in outputs["report_markdown"]


def test_excel_202603_property_contract_adds_zc_without_model_promotion() -> None:
    assert "critical_compressibility_factor" in NUMERIC_PROPERTIES
    assert "critical_compressibility_factor" not in MODEL_TARGET_PROPERTIES


def test_generate_wave2_seed_catalog_merges_excel_202603_generated_candidates(tmp_path: Path) -> None:
    module = _load_seed_generator_module()
    generated_path = tmp_path / "excel_202603_tierd_candidates.csv"
    with generated_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "seed_id": "tierd_excel202603_name_111_11_1",
                "r_number": "",
                "family": "Candidate",
                "query_name": "111-11-1",
                "pubchem_query_type": "name",
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
                "notes": "test row",
            }
        )

    module.GENERATED_EXCEL_202603_TIERD_CANDIDATES = generated_path
    rows = module.build_rows()

    assert any(row["seed_id"] == "tierd_excel202603_name_111_11_1" for row in rows)
