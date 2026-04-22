"""Generate the full inventory seed catalog from anchors, CoolProp metadata, and curated refrigerant additions."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from CoolProp.CoolProp import FluidsList, get_fluid_param_string

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


OUTPUT = ROOT / "data" / "raw" / "manual" / "seed_catalog.csv"
PUBLIC_REFRIGERANT_INVENTORY = ROOT / "data" / "raw" / "manual" / "refrigerant_inventory.csv"
GENERATED_PUBCHEM_TIERD_CANDIDATES = ROOT / "data" / "raw" / "generated" / "pubchem_tierd_candidates.csv"
GENERATED_EXCEL_202603_TIERD_CANDIDATES = ROOT / "data" / "raw" / "generated" / "excel_202603_tierd_candidates.csv"

BASELINE_TIER_B_COUNT = 48
BASELINE_TIER_C_COUNT = 40


FIELDNAMES = [
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


ANCHORS = [
    ("anchor_r11", "R-11", "CFC", "75-69-4", "R11", "legacy baseline refrigerant"),
    ("anchor_r12", "R-12", "CFC", "75-71-8", "R12", "legacy baseline refrigerant"),
    ("anchor_r13", "R-13", "CFC", "75-72-9", "R13", "high-gwp benchmark"),
    ("anchor_r22", "R-22", "HCFC", "75-45-6", "R22", "legacy transition refrigerant"),
    ("anchor_r23", "R-23", "HFC", "75-46-7", "R23", "high-gwp HFC"),
    ("anchor_r32", "R-32", "HFC", "75-10-5", "R32", "mandatory validation entity"),
    ("anchor_r41", "R-41", "HFC", "593-53-3", "R41", "light fluorocarbon"),
    ("anchor_r113", "R-113", "CFC", "76-13-1", "R113", "legacy solvent and refrigerant"),
    ("anchor_r114", "R-114", "CFC", "76-14-2", "R114", "legacy high-odp refrigerant"),
    ("anchor_r115", "R-115", "CFC", "76-15-3", "R115", "legacy high-gwp refrigerant"),
    ("anchor_r123", "R-123", "HCFC", "306-83-2", "R123", "low-pressure HCFC"),
    ("anchor_r124", "R-124", "HCFC", "2837-89-0", "R124", "HCFC benchmark"),
    ("anchor_r125", "R-125", "HFC", "354-33-6", "R125", "common blend component"),
    ("anchor_r134a", "R-134a", "HFC", "811-97-2", "R134a", "mandatory validation entity"),
    ("anchor_r143a", "R-143a", "HFC", "420-46-2", "R143a", "blend component benchmark"),
    ("anchor_r152a", "R-152a", "HFC", "75-37-6", "R152A", "low-gwp flammable HFC"),
    ("anchor_r227ea", "R-227ea", "HFC", "431-89-0", "R227EA", "fire suppression and refrigerant candidate"),
    ("anchor_r236fa", "R-236fa", "HFC", "690-39-1", "R236FA", "heavy HFC benchmark"),
    ("anchor_r245fa", "R-245fa", "HFC", "460-73-1", "R245fa", "organic rankine and foam blowing benchmark"),
    ("anchor_r365mfc", "R-365mfc", "HFC", "406-58-6", "R365MFC", "heavy HFC with explicit CoolProp alias"),
    ("anchor_r1234yf", "R-1234yf", "HFO", "754-12-1", "R1234yf", "mandatory validation entity"),
    ("anchor_r1234zee", "R-1234ze(E)", "HFO", "29118-24-9", "R1234ze(E)", "mandatory ez-isomer separation entity"),
    ("anchor_r1234zez", "R-1234ze(Z)", "HFO", "29118-25-0", "R1234ze(Z)", "mandatory ez-isomer separation entity"),
    ("anchor_r1233zde", "R-1233zd(E)", "HCFO", "102687-65-0", "R1233zd(E)", "low-gwp HCFO benchmark"),
    ("anchor_r744", "R-744", "Natural", "124-38-9", "CarbonDioxide", "mandatory validation entity"),
    ("anchor_r717", "R-717", "Natural", "7664-41-7", "Ammonia", "mandatory validation entity"),
    ("anchor_r290", "R-290", "Natural", "74-98-6", "n-Propane", "natural low-gwp benchmark"),
    ("anchor_r600a", "R-600a", "Natural", "75-28-5", "IsoButane", "domestic refrigeration benchmark"),
    ("anchor_r600", "R-600", "Natural", "106-97-8", "n-Butane", "natural refrigerant benchmark"),
    ("anchor_r1270", "R-1270", "Natural", "115-07-1", "Propylene", "olefin natural refrigerant"),
    ("anchor_r718", "R-718", "Natural", "7732-18-5", "Water", "steam and high-temperature heat pump benchmark"),
    ("anchor_rc318", "RC318", "c-HFC", "115-25-3", "RC318", "cyclic fluorocarbon benchmark"),
]


R_NUMBER_BY_FLUID = {
    "Ammonia": "R-717",
    "Argon": "R-740",
    "CarbonDioxide": "R-744",
    "Ethane": "R-170",
    "Ethylene": "R-1150",
    "Helium": "R-704",
    "Hydrogen": "R-702",
    "IsoButane": "R-600a",
    "Methane": "R-50",
    "Neon": "R-720",
    "Nitrogen": "R-728",
    "NitrousOxide": "R-744A",
    "Oxygen": "R-732",
    "Propylene": "R-1270",
    "SulfurDioxide": "R-764",
    "Water": "R-718",
    "n-Butane": "R-600",
    "n-Propane": "R-290",
    "R11": "R-11",
    "R113": "R-113",
    "R114": "R-114",
    "R115": "R-115",
    "R116": "R-116",
    "R12": "R-12",
    "R123": "R-123",
    "R1233zd(E)": "R-1233zd(E)",
    "R1234yf": "R-1234yf",
    "R1234ze(E)": "R-1234ze(E)",
    "R1234ze(Z)": "R-1234ze(Z)",
    "R124": "R-124",
    "R1243zf": "R-1243zf",
    "R125": "R-125",
    "R13": "R-13",
    "R1336mzz(E)": "R-1336mzz(E)",
    "R134a": "R-134a",
    "R13I1": "R-13I1",
    "R14": "R-14",
    "R141b": "R-141b",
    "R142b": "R-142b",
    "R143a": "R-143a",
    "R152A": "R-152a",
    "R161": "R-161",
    "R21": "R-21",
    "R218": "R-218",
    "R22": "R-22",
    "R227EA": "R-227ea",
    "R23": "R-23",
    "R236EA": "R-236ea",
    "R236FA": "R-236fa",
    "R245ca": "R-245ca",
    "R245fa": "R-245fa",
    "R32": "R-32",
    "R365MFC": "R-365mfc",
    "R40": "R-40",
    "R41": "R-41",
    "RC318": "RC318",
}


FAMILY_BY_FLUID = {
    "Ammonia": "Natural",
    "Argon": "Inorganic",
    "CarbonDioxide": "Natural",
    "Ethane": "Natural",
    "Ethylene": "Natural",
    "Helium": "Inorganic",
    "Hydrogen": "Inorganic",
    "IsoButane": "Natural",
    "Methane": "Natural",
    "Neon": "Inorganic",
    "Nitrogen": "Inorganic",
    "NitrousOxide": "Inorganic",
    "Oxygen": "Inorganic",
    "Propylene": "Natural",
    "SulfurDioxide": "Inorganic",
    "Water": "Natural",
    "n-Butane": "Natural",
    "n-Propane": "Natural",
    "R11": "CFC",
    "R113": "CFC",
    "R114": "CFC",
    "R115": "CFC",
    "R116": "CFC",
    "R12": "CFC",
    "R123": "HCFC",
    "R1233zd(E)": "HCFO",
    "R1234yf": "HFO",
    "R1234ze(E)": "HFO",
    "R1234ze(Z)": "HFO",
    "R124": "HCFC",
    "R1243zf": "HFO",
    "R125": "HFC",
    "R13": "CFC",
    "R1336mzz(E)": "HFO",
    "R134a": "HFC",
    "R13I1": "Candidate",
    "R14": "CFC",
    "R141b": "HCFC",
    "R142b": "HCFC",
    "R143a": "HFC",
    "R152A": "HFC",
    "R161": "HFC",
    "R21": "HCFC",
    "R218": "HFC",
    "R22": "HCFC",
    "R227EA": "HFC",
    "R23": "HFC",
    "R236EA": "HFC",
    "R236FA": "HFC",
    "R245ca": "HFC",
    "R245fa": "HFC",
    "R32": "HFC",
    "R365MFC": "HFC",
    "R40": "HCFC",
    "R41": "HFC",
    "RC318": "c-HFC",
    "D4": "Siloxane",
    "D5": "Siloxane",
    "D6": "Siloxane",
    "MD2M": "Siloxane",
    "MD3M": "Siloxane",
    "MD4M": "Siloxane",
    "MDM": "Siloxane",
    "MM": "Siloxane",
    "Acetone": "Ketone",
    "Novec649": "Ketone",
    "DiethylEther": "Ether",
    "DimethylEther": "Ether",
    "HFE143m": "Ether",
    "Benzene": "Aromatic",
    "EthylBenzene": "Aromatic",
    "Toluene": "Aromatic",
    "m-Xylene": "Aromatic",
    "o-Xylene": "Aromatic",
    "p-Xylene": "Aromatic",
    "CarbonMonoxide": "Inorganic",
    "CarbonylSulfide": "Inorganic",
    "Fluorine": "Inorganic",
    "HydrogenChloride": "Inorganic",
    "HydrogenSulfide": "Inorganic",
    "SulfurHexafluoride": "Inorganic",
}


EXPLICIT_REFRIGERANT_FLUIDS = {
    "Acetone",
    "CarbonMonoxide",
    "CycloHexane",
    "CycloPropane",
    "Cyclopentane",
    "DiethylEther",
    "DimethylEther",
    "Ethanol",
    "Methanol",
    "NitrousOxide",
    "SulfurDioxide",
}


FLUID_EXCLUSIONS = {
    "Air",
    "R404A",
    "R407C",
    "R410A",
    "R507A",
    "OrthoDeuterium",
    "OrthoHydrogen",
    "ParaDeuterium",
    "ParaHydrogen",
    "SES36",
}


MANUAL_CANDIDATES = [
    {
        "seed_id": "tierc_candidate_r1123",
        "r_number": "R-1123",
        "family": "HFO",
        "query_name": "FC=C(F)F",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "trifluoroethene candidate",
    },
    {
        "seed_id": "tierc_candidate_r1132a",
        "r_number": "R-1132a",
        "family": "HFO",
        "query_name": "C=C(F)F",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "1,1-difluoroethene candidate",
    },
    {
        "seed_id": "tierc_candidate_dfe_e",
        "r_number": "",
        "family": "Candidate",
        "query_name": "(E)-1,2-difluoroethene",
        "pubchem_query_type": "name",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "trans-1,2-difluoroethene candidate",
    },
    {
        "seed_id": "tierc_candidate_dfe_z",
        "r_number": "",
        "family": "Candidate",
        "query_name": "(Z)-1,2-difluoroethene",
        "pubchem_query_type": "name",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "cis-1,2-difluoroethene candidate",
    },
    {
        "seed_id": "tierc_candidate_1225yee",
        "r_number": "R-1225ye(E)",
        "family": "HFO",
        "query_name": "(E)-1,2,3,3,3-pentafluoroprop-1-ene",
        "pubchem_query_type": "name",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "pentafluoropropene candidate",
    },
    {
        "seed_id": "tierc_candidate_1225yez",
        "r_number": "R-1225ye(Z)",
        "family": "HFO",
        "query_name": "(Z)-1,2,3,3,3-pentafluoroprop-1-ene",
        "pubchem_query_type": "name",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "pentafluoropropene candidate isomer",
    },
    {
        "seed_id": "tierc_candidate_1224ydz",
        "r_number": "R-1224yd(Z)",
        "family": "HCFO",
        "query_name": "F/C(Cl)=C(\\F)C(F)F",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "chlorotetrafluoropropene candidate",
    },
    {
        "seed_id": "tierc_candidate_1336mzzz",
        "r_number": "R-1336mzz(Z)",
        "family": "HFO",
        "query_name": "FC(F)(F)/C=C\\C(F)(F)F",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "hexafluorobutene candidate isomer",
    },
    {
        "seed_id": "tierc_candidate_tfecl",
        "r_number": "",
        "family": "HCFO",
        "query_name": "FC(F)=CCl",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "chlorotrifluoroethylene candidate",
    },
    {
        "seed_id": "tierc_candidate_tfebr",
        "r_number": "",
        "family": "Candidate",
        "query_name": "FC(F)=CBr",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "bromotrifluoroethylene candidate",
    },
    {
        "seed_id": "tierc_candidate_tfpropene",
        "r_number": "",
        "family": "HFO",
        "query_name": "C=CC(F)(F)F",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "trifluoropropene candidate",
    },
    {
        "seed_id": "tierc_candidate_tetrafluoroethane",
        "r_number": "",
        "family": "HFC",
        "query_name": "FC(F)C(F)F",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "tetrafluoroethane structural isomer candidate",
    },
    {
        "seed_id": "tierc_candidate_pentafluoropropane",
        "r_number": "",
        "family": "HFC",
        "query_name": "FC(F)(F)C(F)CF",
        "pubchem_query_type": "smiles",
        "nist_query": "",
        "nist_query_type": "name",
        "coolprop_fluid": "",
        "notes": "pentafluoropropane isomer candidate",
    },
]


TIER_B_PREFERRED = [
    "R14",
    "R21",
    "R40",
    "R116",
    "R141b",
    "R142b",
    "R161",
    "R218",
    "R236EA",
    "R245ca",
    "R1243zf",
    "R1336mzz(E)",
    "R13I1",
    "Ethane",
    "Ethylene",
    "Methane",
    "CycloPropane",
    "Cyclopentane",
    "CycloHexane",
    "DimethylEther",
    "DiethylEther",
    "HFE143m",
    "Novec649",
    "Acetone",
    "Methanol",
    "Ethanol",
    "NitrousOxide",
    "SulfurDioxide",
    "SulfurHexafluoride",
    "CarbonMonoxide",
    "CarbonylSulfide",
    "HydrogenSulfide",
    "HydrogenChloride",
    "Fluorine",
    "1-Butene",
    "cis-2-Butene",
    "trans-2-Butene",
    "IsoButene",
    "Propyne",
    "Neopentane",
    "Isopentane",
    "Isohexane",
    "n-Pentane",
    "n-Hexane",
    "n-Heptane",
    "n-Octane",
    "n-Decane",
    "n-Nonane",
]


def family_for(fluid: str) -> str:
    if fluid in FAMILY_BY_FLUID:
        return FAMILY_BY_FLUID[fluid]
    return "Candidate"


def priority_for(family: str, tier: str) -> str:
    if tier == "A":
        return "high"
    if tier == "B":
        return "medium" if family in {"CFC", "HCFC", "HFC", "HFO", "HCFO", "Natural", "c-HFC"} else "low"
    return "low"


def metadata_string(fluid: str, key: str) -> str:
    try:
        value = get_fluid_param_string(fluid, key)
    except Exception:  # noqa: BLE001
        return ""
    return value.strip() if value else ""


def build_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    used_seed_ids: set[str] = set()
    used_r_numbers: set[str] = set()
    used_fluids: set[str] = set()

    def append(row: dict[str, str]) -> None:
        seed_id = row["seed_id"]
        if seed_id in used_seed_ids:
            return
        r_number = row["r_number"]
        if r_number and r_number in used_r_numbers:
            return
        coolprop_fluid = row["coolprop_fluid"]
        if coolprop_fluid and coolprop_fluid in used_fluids:
            return
        rows.append(row)
        used_seed_ids.add(seed_id)
        if r_number:
            used_r_numbers.add(r_number)
        if coolprop_fluid:
            used_fluids.add(coolprop_fluid)

    for seed_id, r_number, family, cas_query, coolprop_fluid, notes in ANCHORS:
        append(
            {
                "seed_id": seed_id,
                "r_number": r_number,
                "family": family,
                "query_name": cas_query,
                "pubchem_query_type": "name",
                "nist_query": cas_query,
                "nist_query_type": "name",
                "coolprop_fluid": coolprop_fluid,
                "priority_tier": "1",
                "selection_role": "anchor",
                "coverage_tier": "A",
                "source_bundle": "pubchem+nist+coolprop+epa",
                "coolprop_support_expected": "yes",
                "regulatory_priority": "high",
                "entity_scope": "refrigerant",
                "model_inclusion": "yes",
                "notes": notes,
            }
        )

    auto_candidates = [fluid for fluid in sorted(FluidsList()) if fluid not in FLUID_EXCLUSIONS and fluid not in used_fluids]
    ordered_auto = [fluid for fluid in TIER_B_PREFERRED if fluid in auto_candidates] + [fluid for fluid in auto_candidates if fluid not in TIER_B_PREFERRED]
    baseline_b_fluids = ordered_auto[:BASELINE_TIER_B_COUNT]
    baseline_c_auto_fluids = ordered_auto[BASELINE_TIER_B_COUNT : BASELINE_TIER_B_COUNT + BASELINE_TIER_C_COUNT]
    inventory_auto_fluids = ordered_auto[BASELINE_TIER_B_COUNT + BASELINE_TIER_C_COUNT :]

    for fluid, tier, selection_role in (
        [(fluid, "B", "expansion") for fluid in baseline_b_fluids]
        + [(fluid, "C", "candidate") for fluid in baseline_c_auto_fluids]
        + [(fluid, "D", "inventory") for fluid in inventory_auto_fluids]
    ):
        append(_auto_fluid_row(fluid=fluid, tier=tier, selection_role=selection_role))

    promote_candidates_to_c = max(BASELINE_TIER_C_COUNT - len(baseline_c_auto_fluids), 0)
    for index, item in enumerate(MANUAL_CANDIDATES):
        tier = "C" if index < promote_candidates_to_c else "D"
        append(
            _manual_candidate_row(
                item=item,
                tier=tier,
                selection_role="candidate" if tier == "C" else "inventory",
            )
        )

    for item in _load_public_refrigerant_inventory():
        append(
            {
                "seed_id": item["seed_id"],
                "r_number": item["r_number"],
                "family": item["family"],
                "query_name": item["query_name"],
                "pubchem_query_type": item["pubchem_query_type"],
                "nist_query": item["nist_query"],
                "nist_query_type": item["nist_query_type"],
                "coolprop_fluid": item["coolprop_fluid"],
                "priority_tier": "4",
                "selection_role": "inventory",
                "coverage_tier": "D",
                "source_bundle": "pubchem+nist+epa",
                "coolprop_support_expected": "yes" if item["coolprop_fluid"] else "no",
                "regulatory_priority": "low",
                "entity_scope": "refrigerant",
                "model_inclusion": "no",
                "notes": item["notes"],
            }
        )

    for item in _load_generated_seed_rows(GENERATED_PUBCHEM_TIERD_CANDIDATES):
        append(item)

    for item in _load_generated_seed_rows(GENERATED_EXCEL_202603_TIERD_CANDIDATES):
        append(item)

    return rows


def _auto_fluid_row(*, fluid: str, tier: str, selection_role: str) -> dict[str, str]:
    r_number = R_NUMBER_BY_FLUID.get(fluid, "")
    family = family_for(fluid)
    cas_query = metadata_string(fluid, "CAS")
    query = cas_query if cas_query and cas_query != "N/A" else fluid
    entity_scope = "refrigerant" if (r_number or fluid in EXPLICIT_REFRIGERANT_FLUIDS) else "candidate"
    return {
        "seed_id": f"seed_{fluid.lower().replace('(', '').replace(')', '').replace('-', '_').replace(' ', '_')}",
        "r_number": r_number,
        "family": family,
        "query_name": query,
        "pubchem_query_type": "name",
        "nist_query": query,
        "nist_query_type": "name",
        "coolprop_fluid": fluid,
        "priority_tier": "2" if tier == "B" else ("3" if tier == "C" else "4"),
        "selection_role": selection_role,
        "coverage_tier": tier,
        "source_bundle": "pubchem+nist+coolprop+epa" if priority_for(family, tier) != "low" or entity_scope == "refrigerant" else "pubchem+nist+coolprop",
        "coolprop_support_expected": "yes",
        "regulatory_priority": priority_for(family, tier),
        "entity_scope": entity_scope,
        "model_inclusion": "yes" if tier in {"A", "B", "C"} else "no",
        "notes": f"auto-generated from CoolProp fluid metadata: {fluid}",
    }


def _manual_candidate_row(*, item: dict[str, str], tier: str, selection_role: str) -> dict[str, str]:
    return {
        **item,
        "priority_tier": "3" if tier == "C" else "4",
        "selection_role": selection_role,
        "coverage_tier": tier,
        "source_bundle": "pubchem" if not item["coolprop_fluid"] else "pubchem+nist+coolprop",
        "coolprop_support_expected": "yes" if item["coolprop_fluid"] else "no",
        "regulatory_priority": "low",
        "entity_scope": "candidate",
        "model_inclusion": "yes" if tier == "C" else "no",
    }


def _load_public_refrigerant_inventory() -> list[dict[str, str]]:
    if not PUBLIC_REFRIGERANT_INVENTORY.exists():
        return []
    with PUBLIC_REFRIGERANT_INVENTORY.open("r", encoding="utf-8", newline="") as handle:
        return [{key: value.strip() for key, value in row.items()} for row in csv.DictReader(handle)]


def _load_generated_seed_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [{key: value.strip() for key, value in row.items()} for row in csv.DictReader(handle)]


def main() -> None:
    rows = build_rows()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
