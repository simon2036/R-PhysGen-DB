"""RDKit-based standardization and feature generation."""

from __future__ import annotations

import math
import re
from typing import Any

import selfies as sf
from rdkit import Chem, DataStructs
from rdkit.Chem import Crippen, Descriptors, MACCSkeys, QED, rdFingerprintGenerator, rdMolDescriptors, rdmolops
from rdkit.Chem.MolStandardize import rdMolStandardize
from rdkit.Chem.Scaffolds import MurckoScaffold


def _bitvect_to_hex(bitvect: DataStructs.ExplicitBitVect) -> str:
    bitstring = bitvect.ToBitString()
    pad_bits = math.ceil(len(bitstring) / 4) * 4
    padded = bitstring.ljust(pad_bits, "0")
    return hex(int(padded, 2))[2:].zfill(pad_bits // 4)


def _parse_formula(formula: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for element, raw_count in re.findall(r"([A-Z][a-z]?)(\d*)", formula):
        counts[element] = int(raw_count) if raw_count else 1
    return counts


def _generic_topology_smiles(mol: Chem.Mol) -> str:
    generic = Chem.Mol(mol)
    try:
        for atom in generic.GetAtoms():
            if atom.GetAtomicNum() > 1:
                atom.SetAtomicNum(6)
            atom.SetFormalCharge(0)
            atom.SetIsotope(0)
            atom.SetChiralTag(Chem.ChiralType.CHI_UNSPECIFIED)
            atom.SetNoImplicit(False)
            atom.SetNumExplicitHs(0)
            atom.SetAtomMapNum(0)
            atom.SetIsAromatic(False)
        for bond in generic.GetBonds():
            bond.SetStereo(Chem.BondStereo.STEREONONE)
            if bond.GetIsAromatic():
                bond.SetBondType(Chem.BondType.SINGLE)
            bond.SetIsAromatic(False)
        Chem.SanitizeMol(generic)
        return Chem.MolToSmiles(generic, canonical=True, isomericSmiles=False)
    except Exception:  # noqa: BLE001
        fallback = Chem.Mol(mol)
        for atom in fallback.GetAtoms():
            atom.SetIsotope(0)
            atom.SetChiralTag(Chem.ChiralType.CHI_UNSPECIFIED)
            atom.SetAtomMapNum(0)
        for bond in fallback.GetBonds():
            bond.SetStereo(Chem.BondStereo.STEREONONE)
        return Chem.MolToSmiles(fallback, canonical=True, isomericSmiles=False)


def _detect_ez_isomer(mol: Chem.Mol) -> str | None:
    for bond in mol.GetBonds():
        stereo = bond.GetStereo()
        if stereo == Chem.BondStereo.STEREOE:
            return "E"
        if stereo == Chem.BondStereo.STEREOZ:
            return "Z"
    return None


def standardize_smiles(smiles: str) -> dict[str, Any]:
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        raise ValueError(f"Could not parse SMILES: {smiles}")

    cleaned = rdMolStandardize.Cleanup(mol)
    parent = rdMolStandardize.FragmentParent(cleaned)
    parent = rdMolStandardize.Uncharger().uncharge(parent)

    canonical_smiles = Chem.MolToSmiles(parent, canonical=True, isomericSmiles=False)
    isomeric_smiles = Chem.MolToSmiles(parent, canonical=True, isomericSmiles=True)
    inchi = Chem.MolToInchi(parent)
    inchikey = Chem.MolToInchiKey(parent)
    formula = rdMolDescriptors.CalcMolFormula(parent)
    ez_isomer = _detect_ez_isomer(parent)

    return {
        "mol": parent,
        "canonical_smiles": canonical_smiles,
        "isomeric_smiles": isomeric_smiles,
        "inchi": inchi,
        "inchikey": inchikey,
        "inchikey_first_block": inchikey.split("-")[0],
        "formula": formula,
        "molecular_weight": float(Descriptors.MolWt(parent)),
        "charge": int(rdmolops.GetFormalCharge(parent)),
        "heavy_atom_count": int(parent.GetNumHeavyAtoms()),
        "stereo_flag": bool(Chem.FindMolChiralCenters(parent, includeUnassigned=True) or ez_isomer),
        "ez_isomer": ez_isomer,
    }


def compute_structure_features(smiles: str) -> dict[str, Any]:
    standardized = standardize_smiles(smiles)
    mol = standardized["mol"]
    formula = standardized["formula"]
    counts = _parse_formula(formula)
    murcko = MurckoScaffold.MurckoScaffoldSmiles(mol=mol)
    scaffold_key = murcko or _generic_topology_smiles(mol)

    morgan_generator = rdFingerprintGenerator.GetMorganGenerator(radius=2, fpSize=2048)
    morgan_fp = morgan_generator.GetFingerprint(mol)
    maccs_fp = MACCSkeys.GenMACCSKeys(mol)

    carbonyl = Chem.MolFromSmarts("[CX3]=[OX1]")
    ether = Chem.MolFromSmarts("[OD2]([#6])[#6]")
    cf3 = Chem.MolFromSmarts("[CX4](F)(F)F")

    return {
        "mol_id": None,
        "formula": formula,
        "mol_weight": float(Descriptors.MolWt(mol)),
        "logp": float(Crippen.MolLogP(mol)),
        "tpsa": float(rdMolDescriptors.CalcTPSA(mol)),
        "qed": float(QED.qed(mol)),
        "heavy_atom_count": int(mol.GetNumHeavyAtoms()),
        "ring_count": int(rdMolDescriptors.CalcNumRings(mol)),
        "double_bond_count": int(sum(1 for bond in mol.GetBonds() if bond.GetBondType() == Chem.BondType.DOUBLE)),
        "atom_count_c": int(counts.get("C", 0)),
        "atom_count_h": int(counts.get("H", 0)),
        "atom_count_f": int(counts.get("F", 0)),
        "atom_count_cl": int(counts.get("Cl", 0)),
        "atom_count_br": int(counts.get("Br", 0)),
        "atom_count_i": int(counts.get("I", 0)),
        "atom_count_o": int(counts.get("O", 0)),
        "atom_count_n": int(counts.get("N", 0)),
        "atom_count_s": int(counts.get("S", 0)),
        "has_c_c_double_bond": bool(mol.HasSubstructMatch(Chem.MolFromSmarts("C=C"))),
        "has_cf3": bool(mol.HasSubstructMatch(cf3)),
        "has_ether": bool(mol.HasSubstructMatch(ether)),
        "has_carbonyl": bool(mol.HasSubstructMatch(carbonyl)),
        "murcko_scaffold": murcko,
        "scaffold_key": scaffold_key,
        "selfies": sf.encoder(standardized["isomeric_smiles"]),
        "morgan_fp_hex": _bitvect_to_hex(morgan_fp),
        "maccs_fp_hex": _bitvect_to_hex(maccs_fp),
    }


def scaffold_key_from_smiles(smiles: str) -> str:
    return compute_structure_features(smiles)["scaffold_key"]
