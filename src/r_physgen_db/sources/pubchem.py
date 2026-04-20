"""PubChem adapter."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

import requests


class PubChemClient:
    base_url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"

    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = requests.Session()

    def resolve_compound(self, query: str, query_type: str = "name") -> dict[str, Any]:
        endpoint = (
            f"{self.base_url}/compound/{query_type}/{quote(query)}/property/"
            "CanonicalSMILES,IsomericSMILES,InChI,InChIKey,MolecularFormula,MolecularWeight/JSON"
        )
        response = self.session.get(endpoint, timeout=self.timeout)
        response.raise_for_status()
        record = response.json()["PropertyTable"]["Properties"][0]
        return {
            "cid": str(record["CID"]),
            "query": query,
            "query_type": query_type,
            "molecular_formula": record["MolecularFormula"],
            "molecular_weight": float(record["MolecularWeight"]),
            "canonical_smiles": record.get("ConnectivitySMILES") or record.get("CanonicalSMILES") or record.get("SMILES"),
            "isomeric_smiles": record.get("SMILES") or record.get("IsomericSMILES") or record.get("ConnectivitySMILES"),
            "inchi": record["InChI"],
            "inchikey": record["InChIKey"],
            "raw": record,
            "url": endpoint,
        }

    def fetch_synonyms(self, cid: str) -> dict[str, Any]:
        endpoint = f"{self.base_url}/compound/cid/{cid}/synonyms/JSON"
        response = self.session.get(endpoint, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        synonyms = payload["InformationList"]["Information"][0].get("Synonym", [])
        return {"cid": cid, "synonyms": synonyms, "url": endpoint}

    @staticmethod
    def extract_aliases(synonyms: list[str]) -> dict[str, list[str]]:
        cas_pattern = re.compile(r"^\d{2,7}-\d{2}-\d$")
        r_pattern = re.compile(r"^(R[- ]?\d+[A-Za-z0-9()\-]*|RC\d+)$", re.IGNORECASE)

        cas_numbers = []
        r_numbers = []
        common_names = []
        for synonym in synonyms:
            cleaned = synonym.strip()
            if not cleaned:
                continue
            if cas_pattern.fullmatch(cleaned):
                cas_numbers.append(cleaned)
            elif r_pattern.fullmatch(cleaned):
                r_numbers.append(cleaned.replace(" ", "-").upper())
            elif len(common_names) < 12:
                common_names.append(cleaned)

        return {
            "cas_numbers": sorted(set(cas_numbers)),
            "r_numbers": sorted(set(r_numbers)),
            "common_names": common_names,
        }
