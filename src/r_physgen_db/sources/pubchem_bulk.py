"""Bulk PubChem candidate acquisition helpers."""

from __future__ import annotations

import csv
import gzip
import sys
import time
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pandas as pd
import requests

from rdkit import RDLogger

from r_physgen_db.chemistry import compute_screening_features
from r_physgen_db.sources.http_utils import build_retry_session


PUBCHEM_EXTRAS_BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras"
DEFAULT_EXTRAS_FILES = {
    "smiles": "CID-SMILES.gz",
    "inchi_key": "CID-InChI-Key.gz",
    "mass": "CID-Mass.gz",
    "component": "CID-Component.gz",
    "synonym": "CID-Synonym-filtered.gz",
}


def download_pubchem_extras(
    target_dir: Path,
    *,
    refresh_remote: bool = False,
    session: requests.Session | None = None,
    timeout: int = 180,
    chunk_size: int = 1024 * 1024,
    max_retries: int = 4,
    retry_sleep_seconds: float = 8.0,
    file_sleep_seconds: float = 3.0,
    max_bytes_per_second: int | None = None,
) -> dict[str, Path]:
    target_dir.mkdir(parents=True, exist_ok=True)
    http = session or build_retry_session()
    http.headers.setdefault("User-Agent", "R-PhysGen-DB/1.0 (sequential PubChem bulk download)")
    paths: dict[str, Path] = {}
    items = list(DEFAULT_EXTRAS_FILES.items())
    for index, (key, filename) in enumerate(items):
        output_path = target_dir / filename
        if output_path.exists() and not refresh_remote:
            paths[key] = output_path
            continue

        _download_with_resume(
            http=http,
            url=f"{PUBCHEM_EXTRAS_BASE_URL}/{filename}",
            output_path=output_path,
            refresh_remote=refresh_remote,
            timeout=timeout,
            chunk_size=chunk_size,
            max_retries=max_retries,
            retry_sleep_seconds=retry_sleep_seconds,
            max_bytes_per_second=max_bytes_per_second,
        )
        paths[key] = output_path
        if index < len(items) - 1 and file_sleep_seconds > 0:
            time.sleep(file_sleep_seconds)
    return paths


def build_pubchem_candidate_pool(
    *,
    smiles_path: Path,
    inchikey_path: Path,
    mass_path: Path,
    component_path: Path,
    synonym_path: Path,
    existing_molecule_core: pd.DataFrame | None = None,
    cid_limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    existing_molecule_core = existing_molecule_core if existing_molecule_core is not None else pd.DataFrame()
    existing_inchikeys = {
        str(value).strip()
        for value in existing_molecule_core.get("inchikey", pd.Series(dtype="object")).tolist()
        if str(value).strip()
    }
    existing_first_blocks = {
        str(value).strip()
        for value in existing_molecule_core.get("inchikey_first_block", pd.Series(dtype="object")).tolist()
        if str(value).strip()
    }

    audit_rows: list[dict[str, Any]] = []
    preliminary_rows: list[dict[str, Any]] = []
    candidate_cids: set[str] = set()

    for index, (cid, smiles) in enumerate(_iter_simple_pairs(smiles_path), start=1):
        if cid_limit is not None and index > cid_limit:
            break

        failure_reasons: list[str] = []
        component_count = smiles.count(".") + 1

        try:
            screening = compute_screening_features(smiles)
        except Exception as exc:  # noqa: BLE001
            audit_rows.append(
                {
                    "cid": cid,
                    "raw_smiles": smiles,
                    "passed_hard_filters": False,
                    "failure_reasons": f"screening_error:{type(exc).__name__}",
                }
            )
            continue

        if screening["charge"] != 0:
            failure_reasons.append("non_neutral")
        if not screening["allowed_elements_only"]:
            failure_reasons.append("disallowed_elements")
        if component_count > 1:
            failure_reasons.append("multi_component")
        if screening["total_atom_count"] > 18:
            failure_reasons.append("total_atom_count_gt_18")
        if screening["heavy_atom_count"] < 1:
            failure_reasons.append("heavy_atom_count_lt_1")
        if screening["heavy_atom_count"] > 15:
            failure_reasons.append("heavy_atom_count_gt_15")
        if screening["molecular_weight"] < 16:
            failure_reasons.append("molecular_weight_lt_16")
        if screening["molecular_weight"] > 300:
            failure_reasons.append("molecular_weight_gt_300")
        if screening["carbon_count"] < 1:
            failure_reasons.append("carbon_count_lt_1")
        if screening["carbon_count"] > 6:
            failure_reasons.append("carbon_count_gt_6")

        row = {
            "cid": cid,
            "raw_smiles": smiles,
            "component_count": component_count,
            **screening,
        }
        row["passed_hard_filters"] = not failure_reasons
        row["failure_reasons"] = "|".join(failure_reasons)
        preliminary_rows.append(row)
        audit_rows.append(
            {
                "cid": cid,
                "raw_smiles": smiles,
                "passed_hard_filters": not failure_reasons,
                "failure_reasons": "|".join(failure_reasons),
                "component_count": component_count,
                "formula": screening["formula"],
                "molecular_weight": screening["molecular_weight"],
                "charge": screening["charge"],
                "total_atom_count": screening["total_atom_count"],
                "heavy_atom_count": screening["heavy_atom_count"],
                "carbon_count": screening["carbon_count"],
                "disallowed_elements": "|".join(screening["disallowed_elements"]),
            }
        )
        if not failure_reasons:
            candidate_cids.add(cid)

    component_counts = _load_component_counts(component_path, candidate_cids)
    inchi_payload = _load_inchi_payload(inchikey_path, candidate_cids)
    mass_payload = _load_mass_payload(mass_path, candidate_cids)
    synonym_payload = _load_synonym_payload(synonym_path, candidate_cids)

    candidate_rows: list[dict[str, Any]] = []
    audit_index = {row["cid"]: row for row in audit_rows}
    for row in preliminary_rows:
        cid = row["cid"]
        reasons = [item for item in row["failure_reasons"].split("|") if item]
        component_count = component_counts.get(cid, row["component_count"])
        if component_count > 1 and "multi_component" not in reasons:
            reasons.append("multi_component")

        if reasons:
            audit_index[cid]["passed_hard_filters"] = False
            audit_index[cid]["failure_reasons"] = "|".join(reasons)
            audit_index[cid]["component_count"] = component_count
            continue

        inchi_record = inchi_payload.get(cid, {})
        mass_record = mass_payload.get(cid, {})
        synonyms = synonym_payload.get(cid, [])

        inchikey = str(inchi_record.get("inchikey") or row["inchikey"]).strip()
        inchikey_first_block = str(inchikey.split("-")[0]).strip() if inchikey else ""

        candidate_row = {
            **row,
            "component_count": component_count,
            "inchi": str(inchi_record.get("inchi") or row["inchi"]).strip(),
            "inchikey": inchikey,
            "inchikey_first_block": inchikey_first_block or row["inchikey_first_block"],
            "formula": str(mass_record.get("formula") or row["formula"]).strip(),
            "molecular_weight": float(mass_record.get("exact_mass") or row["molecular_weight"]),
            "primary_name": synonyms[0] if synonyms else "",
            "title": synonyms[0] if synonyms else f"CID {cid}",
            "synonyms": synonyms,
            "existing_full_inchikey_match": inchikey in existing_inchikeys,
            "existing_first_block_match": inchikey_first_block in existing_first_blocks,
            "volatility_status": "unknown",
            "passed_hard_filters": True,
            "failure_reasons": "",
        }
        audit_index[cid].update(
            {
                "passed_hard_filters": True,
                "failure_reasons": "",
                "component_count": component_count,
                "formula": candidate_row["formula"],
                "molecular_weight": candidate_row["molecular_weight"],
                "inchi": candidate_row["inchi"],
                "inchikey": candidate_row["inchikey"],
            }
        )
        candidate_rows.append(candidate_row)

    if not candidate_rows:
        return pd.DataFrame(columns=["cid"]), pd.DataFrame(audit_rows).sort_values("cid").reset_index(drop=True)

    pool = pd.DataFrame(candidate_rows)
    pool = (
        pool.sort_values(
            by=[
                "existing_full_inchikey_match",
                "existing_first_block_match",
                "has_halogen",
                "has_c_c_double_bond",
                "total_atom_count",
                "molecular_weight",
                "cid",
            ],
            ascending=[True, True, False, False, True, True, True],
        )
        .drop_duplicates(subset=["inchikey"], keep="first")
        .reset_index(drop=True)
    )
    audit = pd.DataFrame(audit_rows).sort_values("cid").reset_index(drop=True)
    return pool, audit


def build_pubchem_candidate_pool_streaming(
    *,
    smiles_path: Path,
    inchikey_path: Path,
    mass_path: Path,
    component_path: Path,
    synonym_path: Path,
    existing_molecule_core: pd.DataFrame | None = None,
    cid_limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    existing_molecule_core = existing_molecule_core if existing_molecule_core is not None else pd.DataFrame()
    existing_inchikeys = {
        str(value).strip()
        for value in existing_molecule_core.get("inchikey", pd.Series(dtype="object")).tolist()
        if str(value).strip()
    }
    existing_first_blocks = {
        str(value).strip()
        for value in existing_molecule_core.get("inchikey_first_block", pd.Series(dtype="object")).tolist()
        if str(value).strip()
    }

    mass_prefilter = _load_mass_prefilter_candidates(mass_path, cid_limit=cid_limit)
    if mass_prefilter.empty:
        return pd.DataFrame(columns=["cid"]), pd.DataFrame(columns=["cid"])

    candidate_cids = set(mass_prefilter["cid"].astype(str).tolist())
    component_counts = _load_component_counts(component_path, candidate_cids)
    mass_prefilter["component_count"] = mass_prefilter["cid"].map(component_counts).fillna(1).astype(int)

    eligible_smiles_cids = {
        cid
        for cid in mass_prefilter.loc[mass_prefilter["component_count"] <= 1, "cid"].astype(str).tolist()
    }
    candidate_pool, screening_audit, processed_smiles_cids = _screen_prefiltered_smiles(
        smiles_path=smiles_path,
        candidate_cids=eligible_smiles_cids,
        existing_inchikeys=existing_inchikeys,
        existing_first_blocks=existing_first_blocks,
    )

    if not candidate_pool.empty:
        final_candidate_cids = set(candidate_pool["cid"].astype(str).tolist())
        inchi_payload = _load_inchi_payload(inchikey_path, final_candidate_cids)
        synonym_payload = _load_primary_synonym_payload(synonym_path, final_candidate_cids)
        candidate_pool = _enrich_streamed_candidate_pool(
            candidate_pool,
            mass_prefilter=mass_prefilter,
            inchi_payload=inchi_payload,
            synonym_payload=synonym_payload,
            existing_inchikeys=existing_inchikeys,
            existing_first_blocks=existing_first_blocks,
        )

    audit = _build_streaming_audit(
        mass_prefilter=mass_prefilter,
        screening_audit=screening_audit,
        processed_smiles_cids=processed_smiles_cids,
        eligible_smiles_cids=eligible_smiles_cids,
    )
    return candidate_pool, audit


def build_pubchem_coarse_filter_summary(
    mass_path: Path,
    *,
    cid_limit: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    con = duckdb.connect()
    try:
        summary = con.execute(
            f"""
            {_build_mass_screen_cte_sql(mass_path, cid_limit=cid_limit)},
            reason_hits AS (
                SELECT
                    cid,
                    formula,
                    element_pattern,
                    carbon_bucket,
                    carbon_bucket_order,
                    mass_bucket,
                    mass_bucket_order,
                    0 AS reason_order,
                    'passed_coarse_filter' AS coarse_filter_reason
                FROM mass_scored
                WHERE passed_coarse_filter
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 1, 'disallowed_formula_pattern'
                FROM mass_scored
                WHERE fail_disallowed_formula_pattern
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 2, 'molecular_weight_lt_16'
                FROM mass_scored
                WHERE fail_molecular_weight_lt_16
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 3, 'molecular_weight_gt_300'
                FROM mass_scored
                WHERE fail_molecular_weight_gt_300
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 4, 'carbon_count_lt_1'
                FROM mass_scored
                WHERE fail_carbon_count_lt_1
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 5, 'carbon_count_gt_6'
                FROM mass_scored
                WHERE fail_carbon_count_gt_6
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 6, 'total_atom_count_gt_18'
                FROM mass_scored
                WHERE fail_total_atom_count_gt_18
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 7, 'heavy_atom_count_lt_1'
                FROM mass_scored
                WHERE fail_heavy_atom_count_lt_1
                UNION ALL
                SELECT cid, formula, element_pattern, carbon_bucket, carbon_bucket_order, mass_bucket, mass_bucket_order, 8, 'heavy_atom_count_gt_15'
                FROM mass_scored
                WHERE fail_heavy_atom_count_gt_15
            )
            SELECT
                coarse_filter_reason,
                reason_order,
                element_pattern,
                carbon_bucket,
                carbon_bucket_order,
                mass_bucket,
                mass_bucket_order,
                COUNT(*) AS cid_count,
                COUNT(DISTINCT formula) AS formula_count,
                MIN(formula) AS sample_formula
            FROM reason_hits
            GROUP BY
                coarse_filter_reason,
                reason_order,
                element_pattern,
                carbon_bucket,
                carbon_bucket_order,
                mass_bucket,
                mass_bucket_order
            ORDER BY
                reason_order,
                cid_count DESC,
                element_pattern,
                carbon_bucket_order,
                mass_bucket_order
            """
        ).fetchdf()
        totals = con.execute(
            f"""
            {_build_mass_screen_cte_sql(mass_path, cid_limit=cid_limit)}
            SELECT
                COUNT(*) AS total_mass_records,
                SUM(CASE WHEN passed_coarse_filter THEN 1 ELSE 0 END) AS passed_coarse_filter_count,
                SUM(CASE WHEN NOT passed_coarse_filter THEN 1 ELSE 0 END) AS failed_coarse_filter_count
            FROM mass_scored
            """
        ).fetchone()
    finally:
        con.close()

    metadata = {
        "total_mass_records": int(totals[0] or 0),
        "passed_coarse_filter_count": int(totals[1] or 0),
        "failed_coarse_filter_count": int(totals[2] or 0),
        "cid_limit": int(cid_limit) if cid_limit is not None else None,
        "reason_count_semantics": (
            "`passed_coarse_filter` is mutually exclusive; failure reasons are reason-hit counts, "
            "so one CID may contribute to multiple failure reasons."
        ),
    }
    return summary, metadata


def export_tierd_seed_rows(candidate_pool: pd.DataFrame, *, limit: int = 5000) -> list[dict[str, str]]:
    if candidate_pool.empty:
        return []

    eligible = candidate_pool.copy()
    for column, default in {
        "has_halogen": False,
        "has_c_c_double_bond": False,
        "existing_first_block_match": False,
        "existing_full_inchikey_match": False,
        "ring_count": 0,
        "total_atom_count": 0,
        "molecular_weight": 0.0,
        "cid": "",
    }.items():
        if column not in eligible.columns:
            eligible[column] = default
    if "passed_hard_filters" in eligible.columns:
        eligible = eligible.loc[eligible["passed_hard_filters"].astype(bool)]
    if "existing_full_inchikey_match" in eligible.columns:
        eligible = eligible.loc[~eligible["existing_full_inchikey_match"].astype(bool)]

    eligible = (
        eligible.sort_values(
            by=[
                "has_halogen",
                "has_c_c_double_bond",
                "existing_first_block_match",
                "ring_count",
                "total_atom_count",
                "molecular_weight",
                "cid",
            ],
            ascending=[False, False, True, True, True, True, True],
        )
        .head(limit)
    )
    dedupe_column = "inchikey" if "inchikey" in eligible.columns else "cid"
    eligible = eligible.drop_duplicates(subset=[dedupe_column], keep="first")

    rows: list[dict[str, str]] = []
    for record in eligible.to_dict(orient="records"):
        cid = str(record["cid"]).strip()
        title = str(record.get("title") or record.get("primary_name") or f"CID {cid}").strip()
        rows.append(
            {
                "seed_id": f"tierd_pubchem_{cid}",
                "r_number": "",
                "family": _infer_family(record),
                "query_name": cid,
                "pubchem_query_type": "cid",
                "nist_query": "",
                "nist_query_type": "name",
                "coolprop_fluid": "",
                "priority_tier": "4",
                "selection_role": "inventory",
                "coverage_tier": "D",
                "source_bundle": "pubchem_bulk",
                "coolprop_support_expected": "no",
                "regulatory_priority": "low",
                "entity_scope": "candidate",
                "model_inclusion": "no",
                "notes": _build_seed_note(title, record),
            }
        )
    return rows


def _infer_family(record: dict[str, Any]) -> str:
    if bool(record.get("has_halogen")) and bool(record.get("has_c_c_double_bond")):
        return "HFO"
    if bool(record.get("has_halogen")):
        return "HFC"
    if bool(record.get("has_ether")):
        return "Ether"
    if bool(record.get("has_carbonyl")):
        return "Ketone"
    return "Candidate"


def _build_seed_note(title: str, record: dict[str, Any]) -> str:
    first_block = "yes" if bool(record.get("existing_first_block_match")) else "no"
    return f"generated from PubChem bulk pool: {title}; existing_first_block_match={first_block}"


def _set_csv_field_size_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit = limit // 10


def _build_mass_screen_cte_sql(path: Path, *, cid_limit: int | None = None) -> str:
    limit_clause = f"LIMIT {int(cid_limit)}" if cid_limit is not None else ""
    return f"""
    WITH mass_raw AS (
        SELECT *
        FROM read_csv(
            '{path.as_posix()}',
            delim='\\t',
            header=false,
            compression='gzip',
            columns={{'cid':'VARCHAR','formula':'VARCHAR','monoisotopic_mass':'DOUBLE','exact_mass':'DOUBLE'}}
        )
        {limit_clause}
    ),
    mass_counts AS (
        SELECT
            cid,
            formula,
            exact_mass AS molecular_weight,
            CASE WHEN regexp_matches(formula, '^C') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, '^C([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS carbon_count,
            CASE WHEN regexp_matches(formula, '^C[0-9]*H') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, '^C[0-9]*H([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_h,
            CASE WHEN regexp_matches(formula, 'Br') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'Br([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_br,
            CASE WHEN regexp_matches(formula, 'Cl') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'Cl([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_cl,
            CASE WHEN regexp_matches(formula, 'F') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'F([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_f,
            CASE WHEN regexp_matches(formula, 'I') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'I([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_i,
            CASE WHEN regexp_matches(formula, 'N') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'N([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_n,
            CASE WHEN regexp_matches(formula, 'O') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'O([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_o,
            CASE WHEN regexp_matches(formula, 'S') THEN COALESCE(TRY_CAST(NULLIF(regexp_extract(formula, 'S([0-9]*)', 1), '') AS INTEGER), 1) ELSE 0 END AS atom_count_s,
            regexp_matches(formula, '^C[0-9]*(H[0-9]*)?(Br[0-9]*)?(Cl[0-9]*)?(F[0-9]*)?(I[0-9]*)?(N[0-9]*)?(O[0-9]*)?(S[0-9]*)?$') AS allowed_formula
        FROM mass_raw
    ),
    mass_scored AS (
        SELECT
            cid,
            formula,
            molecular_weight,
            carbon_count,
            atom_count_h,
            atom_count_f,
            atom_count_cl,
            atom_count_br,
            atom_count_i,
            atom_count_n,
            atom_count_o,
            atom_count_s,
            carbon_count + atom_count_h + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s AS total_atom_count,
            carbon_count + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s AS heavy_atom_count,
            COALESCE(
                NULLIF(
                    regexp_replace(
                        regexp_replace(formula, '[0-9]+', '', 'g'),
                        '[^A-Za-z]',
                        '',
                        'g'
                    ),
                    ''
                ),
                'UNKNOWN'
            ) AS element_pattern,
            CASE
                WHEN carbon_count <= 0 THEN 'C0'
                WHEN carbon_count = 1 THEN 'C1'
                WHEN carbon_count = 2 THEN 'C2'
                WHEN carbon_count = 3 THEN 'C3'
                WHEN carbon_count = 4 THEN 'C4'
                WHEN carbon_count = 5 THEN 'C5'
                WHEN carbon_count = 6 THEN 'C6'
                ELSE 'C7+'
            END AS carbon_bucket,
            CASE
                WHEN carbon_count <= 0 THEN 0
                WHEN carbon_count = 1 THEN 1
                WHEN carbon_count = 2 THEN 2
                WHEN carbon_count = 3 THEN 3
                WHEN carbon_count = 4 THEN 4
                WHEN carbon_count = 5 THEN 5
                WHEN carbon_count = 6 THEN 6
                ELSE 7
            END AS carbon_bucket_order,
            CASE
                WHEN molecular_weight < 16 THEN '<16'
                WHEN molecular_weight < 50 THEN '16-49.999'
                WHEN molecular_weight < 100 THEN '50-99.999'
                WHEN molecular_weight < 150 THEN '100-149.999'
                WHEN molecular_weight < 200 THEN '150-199.999'
                WHEN molecular_weight < 250 THEN '200-249.999'
                WHEN molecular_weight <= 300 THEN '250-300'
                ELSE '>300'
            END AS mass_bucket,
            CASE
                WHEN molecular_weight < 16 THEN 0
                WHEN molecular_weight < 50 THEN 1
                WHEN molecular_weight < 100 THEN 2
                WHEN molecular_weight < 150 THEN 3
                WHEN molecular_weight < 200 THEN 4
                WHEN molecular_weight < 250 THEN 5
                WHEN molecular_weight <= 300 THEN 6
                ELSE 7
            END AS mass_bucket_order,
            NOT allowed_formula AS fail_disallowed_formula_pattern,
            molecular_weight < 16 AS fail_molecular_weight_lt_16,
            molecular_weight > 300 AS fail_molecular_weight_gt_300,
            carbon_count < 1 AS fail_carbon_count_lt_1,
            carbon_count > 6 AS fail_carbon_count_gt_6,
            carbon_count + atom_count_h + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s > 18 AS fail_total_atom_count_gt_18,
            carbon_count + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s < 1 AS fail_heavy_atom_count_lt_1,
            carbon_count + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s > 15 AS fail_heavy_atom_count_gt_15,
            (
                allowed_formula
                AND molecular_weight BETWEEN 16 AND 300
                AND carbon_count BETWEEN 1 AND 6
                AND carbon_count + atom_count_h + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s <= 18
                AND carbon_count + atom_count_f + atom_count_cl + atom_count_br + atom_count_i + atom_count_n + atom_count_o + atom_count_s BETWEEN 1 AND 15
            ) AS passed_coarse_filter
        FROM mass_counts
    )
    """


def _load_mass_prefilter_candidates(path: Path, *, cid_limit: int | None = None) -> pd.DataFrame:
    sql = f"""
    {_build_mass_screen_cte_sql(path, cid_limit=cid_limit)}
    SELECT
        cid,
        formula,
        molecular_weight,
        carbon_count,
        atom_count_h,
        atom_count_f,
        atom_count_cl,
        atom_count_br,
        atom_count_i,
        atom_count_n,
        atom_count_o,
        atom_count_s,
        total_atom_count,
        heavy_atom_count
    FROM mass_scored
    WHERE passed_coarse_filter
    ORDER BY cid
    """
    con = duckdb.connect()
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def _screen_prefiltered_smiles(
    *,
    smiles_path: Path,
    candidate_cids: set[str],
    existing_inchikeys: set[str],
    existing_first_blocks: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, set[str]]:
    pass_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    processed_cids: set[str] = set()

    RDLogger.DisableLog("rdApp.warning")
    RDLogger.DisableLog("rdApp.error")
    try:
        for cid, smiles in _iter_simple_pairs(smiles_path):
            if cid not in candidate_cids:
                continue
            processed_cids.add(cid)

            try:
                screening = compute_screening_features(smiles)
            except Exception as exc:  # noqa: BLE001
                audit_rows.append(
                    {
                        "cid": cid,
                        "raw_smiles": smiles,
                        "passed_hard_filters": False,
                        "failure_reasons": f"screening_error:{type(exc).__name__}",
                        "charge": None,
                        "disallowed_elements": "",
                    }
                )
                continue

            failure_reasons: list[str] = []
            if screening["charge"] != 0:
                failure_reasons.append("non_neutral")
            if not screening["allowed_elements_only"]:
                failure_reasons.append("disallowed_elements")

            row = {
                "cid": cid,
                "raw_smiles": smiles,
                **screening,
                "component_count": 1,
                "existing_full_inchikey_match": screening["inchikey"] in existing_inchikeys,
                "existing_first_block_match": screening["inchikey_first_block"] in existing_first_blocks,
                "volatility_status": "unknown",
                "passed_hard_filters": not failure_reasons,
                "failure_reasons": "|".join(failure_reasons),
            }
            audit_rows.append(
                {
                    "cid": cid,
                    "raw_smiles": smiles,
                    "passed_hard_filters": not failure_reasons,
                    "failure_reasons": "|".join(failure_reasons),
                    "charge": screening["charge"],
                    "disallowed_elements": "|".join(screening["disallowed_elements"]),
                }
            )
            if not failure_reasons:
                pass_rows.append(row)
    finally:
        RDLogger.EnableLog("rdApp.warning")
        RDLogger.EnableLog("rdApp.error")

    candidate_pool = pd.DataFrame(pass_rows)
    screening_audit = pd.DataFrame(audit_rows)
    return candidate_pool, screening_audit, processed_cids


def _enrich_streamed_candidate_pool(
    candidate_pool: pd.DataFrame,
    *,
    mass_prefilter: pd.DataFrame,
    inchi_payload: dict[str, dict[str, str]],
    synonym_payload: dict[str, list[str]],
    existing_inchikeys: set[str],
    existing_first_blocks: set[str],
) -> pd.DataFrame:
    pool = candidate_pool.copy()
    mass_subset = mass_prefilter[
        ["cid", "formula", "molecular_weight", "total_atom_count", "heavy_atom_count", "carbon_count"]
    ].drop_duplicates(subset=["cid"])
    pool = pool.merge(mass_subset, on="cid", how="left", suffixes=("", "_mass"))

    for column in ["formula", "molecular_weight", "total_atom_count", "heavy_atom_count", "carbon_count"]:
        mass_column = f"{column}_mass"
        if mass_column in pool.columns:
            pool[column] = pool[mass_column].where(pool[mass_column].notna(), pool[column])
            pool = pool.drop(columns=[mass_column])

    if inchi_payload:
        inchi_df = pd.DataFrame(
            [
                {"cid": cid, "inchi_ext": record.get("inchi", ""), "inchikey_ext": record.get("inchikey", "")}
                for cid, record in inchi_payload.items()
            ]
        )
        pool = pool.merge(inchi_df, on="cid", how="left")
        pool["inchi"] = pool["inchi_ext"].where(pool["inchi_ext"].astype(str).str.strip() != "", pool["inchi"])
        pool["inchikey"] = pool["inchikey_ext"].where(pool["inchikey_ext"].astype(str).str.strip() != "", pool["inchikey"])
        pool = pool.drop(columns=["inchi_ext", "inchikey_ext"])

    pool["inchikey_first_block"] = pool["inchikey"].astype(str).map(lambda value: value.split("-")[0] if value else "")
    pool["existing_full_inchikey_match"] = pool["inchikey"].astype(str).isin(existing_inchikeys)
    pool["existing_first_block_match"] = pool["inchikey_first_block"].astype(str).isin(existing_first_blocks)

    pool["synonyms"] = pool["cid"].map(lambda cid: synonym_payload.get(str(cid), []))
    pool["primary_name"] = pool["cid"].map(
        lambda cid: synonym_payload.get(str(cid), [""])[0] if synonym_payload.get(str(cid)) else ""
    )
    pool["title"] = pool["primary_name"].where(pool["primary_name"].astype(str).str.strip() != "", pool["cid"].map(lambda cid: f"CID {cid}"))
    pool["passed_hard_filters"] = True
    pool["failure_reasons"] = ""
    pool["volatility_status"] = "unknown"

    pool = (
        pool.sort_values(
            by=[
                "existing_full_inchikey_match",
                "existing_first_block_match",
                "has_halogen",
                "has_c_c_double_bond",
                "total_atom_count",
                "molecular_weight",
                "cid",
            ],
            ascending=[True, True, False, False, True, True, True],
        )
        .drop_duplicates(subset=["inchikey"], keep="first")
        .reset_index(drop=True)
    )
    return pool


def _build_streaming_audit(
    *,
    mass_prefilter: pd.DataFrame,
    screening_audit: pd.DataFrame,
    processed_smiles_cids: set[str],
    eligible_smiles_cids: set[str],
) -> pd.DataFrame:
    audit = mass_prefilter[
        ["cid", "formula", "molecular_weight", "total_atom_count", "heavy_atom_count", "carbon_count", "component_count"]
    ].copy()
    audit["raw_smiles"] = ""
    audit["passed_hard_filters"] = False
    audit["failure_reasons"] = ""
    audit["charge"] = None
    audit["disallowed_elements"] = ""
    audit.loc[audit["component_count"] > 1, "failure_reasons"] = "multi_component"

    if not screening_audit.empty:
        updates = screening_audit.set_index("cid")
        audit = audit.set_index("cid")
        for column in ["raw_smiles", "passed_hard_filters", "failure_reasons", "charge", "disallowed_elements"]:
            audit.loc[updates.index, column] = updates[column]
        audit = audit.reset_index()

    missing_smiles_cids = sorted(eligible_smiles_cids - processed_smiles_cids)
    if missing_smiles_cids:
        audit.loc[audit["cid"].isin(missing_smiles_cids), "failure_reasons"] = "missing_smiles"
        audit.loc[audit["cid"].isin(missing_smiles_cids), "passed_hard_filters"] = False

    return audit.sort_values("cid").reset_index(drop=True)


def _download_with_resume(
    *,
    http: requests.Session,
    url: str,
    output_path: Path,
    refresh_remote: bool,
    timeout: int,
    chunk_size: int,
    max_retries: int,
    retry_sleep_seconds: float,
    max_bytes_per_second: int | None,
) -> None:
    partial_path = output_path.with_suffix(f"{output_path.suffix}.part")
    if refresh_remote:
        if output_path.exists():
            output_path.unlink()
        if partial_path.exists():
            partial_path.unlink()

    attempt = 0
    while True:
        attempt += 1
        resume_from = partial_path.stat().st_size if partial_path.exists() else 0
        headers: dict[str, str] = {}
        write_mode = "wb"
        if resume_from > 0:
            headers["Range"] = f"bytes={resume_from}-"
            write_mode = "ab"

        try:
            response = http.get(url, stream=True, timeout=timeout, headers=headers)
            if resume_from > 0 and response.status_code == 200:
                partial_path.unlink(missing_ok=True)
                resume_from = 0
                write_mode = "wb"
            response.raise_for_status()

            transfer_start = time.monotonic()
            transferred_bytes = 0
            with partial_path.open(write_mode) as handle:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        handle.write(chunk)
                        if max_bytes_per_second and max_bytes_per_second > 0:
                            transferred_bytes += len(chunk)
                            expected_elapsed = transferred_bytes / max_bytes_per_second
                            actual_elapsed = time.monotonic() - transfer_start
                            if expected_elapsed > actual_elapsed:
                                time.sleep(expected_elapsed - actual_elapsed)

            partial_path.replace(output_path)
            return
        except (requests.RequestException, OSError):
            if attempt >= max_retries:
                raise
            time.sleep(retry_sleep_seconds)


def _iter_simple_pairs(path: Path) -> Iterable[tuple[str, str]]:
    _set_csv_field_size_limit()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 2:
                continue
            cid = str(row[0]).strip()
            value = str(row[1]).strip()
            if cid and value:
                yield cid, value


def _load_component_counts(path: Path, cids: set[str]) -> dict[str, int]:
    component_counts: dict[str, int] = {}
    if not cids:
        return component_counts

    _set_csv_field_size_limit()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if not row:
                continue
            cid = str(row[0]).strip()
            if cid not in cids:
                continue
            component_counts[cid] = max(len([item for item in row[1:] if str(item).strip()]), 1)
    return component_counts


def _load_inchi_payload(path: Path, cids: set[str]) -> dict[str, dict[str, str]]:
    payload: dict[str, dict[str, str]] = {}
    if not cids:
        return payload

    _set_csv_field_size_limit()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 3:
                continue
            cid = str(row[0]).strip()
            if cid not in cids:
                continue
            payload[cid] = {
                "inchi": str(row[1]).strip(),
                "inchikey": str(row[2]).strip(),
            }
    return payload


def _load_mass_payload(path: Path, cids: set[str]) -> dict[str, dict[str, str]]:
    payload: dict[str, dict[str, str]] = {}
    if not cids:
        return payload

    _set_csv_field_size_limit()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 4:
                continue
            cid = str(row[0]).strip()
            if cid not in cids:
                continue
            payload[cid] = {
                "formula": str(row[1]).strip(),
                "monoisotopic_mass": str(row[2]).strip(),
                "exact_mass": str(row[3]).strip(),
            }
    return payload


def _load_synonym_payload(path: Path, cids: set[str], *, max_synonyms_per_cid: int = 12) -> dict[str, list[str]]:
    payload: dict[str, list[str]] = {}
    if not cids:
        return payload

    _set_csv_field_size_limit()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 2:
                continue
            cid = str(row[0]).strip()
            if cid not in cids:
                continue
            synonym = str(row[1]).strip()
            if not synonym:
                continue
            bucket = payload.setdefault(cid, [])
            if synonym not in bucket and len(bucket) < max_synonyms_per_cid:
                bucket.append(synonym)
    return payload


def _load_primary_synonym_payload(path: Path, cids: set[str]) -> dict[str, list[str]]:
    payload: dict[str, list[str]] = {}
    if not cids:
        return payload

    _set_csv_field_size_limit()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if len(row) < 2:
                continue
            cid = str(row[0]).strip()
            if cid not in cids or cid in payload:
                continue
            synonym = str(row[1]).strip()
            if synonym:
                payload[cid] = [synonym]
    return payload
