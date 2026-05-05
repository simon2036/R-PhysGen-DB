#!/usr/bin/env python3
"""Embed the latest layered dataset snapshot into the static frontend."""

from __future__ import annotations

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ROOT_HTML = ROOT / "R-PhysGen-DB.html"
LAN_HTML = ROOT / "deploy" / "lan" / "index.html"


def clean(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 10)
    if hasattr(value, "item"):
        return clean(value.item())
    return value


def js_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def read_parquet(rel: str) -> pd.DataFrame:
    return pd.read_parquet(ROOT / rel)


def read_optional_parquet(rel: str) -> pd.DataFrame:
    path = ROOT / rel
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def first_by_mol(df: pd.DataFrame, property_name: str) -> dict[str, pd.Series]:
    sub = df[df["property_name"] == property_name]
    return {str(row.mol_id): row for row in sub.itertuples(index=False)}


def row_value(row: Any, *, numeric: bool = True) -> Any:
    if row is None:
        return None
    value = getattr(row, "value_num", None) if numeric else getattr(row, "value", None)
    if numeric and clean(value) is None:
        value = getattr(row, "value", None)
    return clean(value)


def cas_like(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if re.fullmatch(r"\d{2,7}-\d{2}-\d", text) else None


def build_molecules(molecule_master: pd.DataFrame, property_recommended: pd.DataFrame, seed_catalog: pd.DataFrame) -> list[dict[str, Any]]:
    seed = seed_catalog.set_index("seed_id", drop=False) if "seed_id" in seed_catalog.columns else pd.DataFrame()
    props = {name: first_by_mol(property_recommended, name) for name in [
        "boiling_point_c",
        "critical_temp_c",
        "critical_pressure_mpa",
        "gwp_100yr",
        "odp",
        "ashrae_safety",
        "toxicity_class",
    ]}

    def prop(mol_id: str, name: str, *, numeric: bool = True) -> Any:
        return row_value(props[name].get(mol_id), numeric=numeric)

    records: list[dict[str, Any]] = []
    for row in molecule_master.itertuples(index=False):
        mol_id = str(row.mol_id)
        seed_id = str(getattr(row, "seed_id", "") or "")
        seed_row = seed.loc[seed_id] if seed_id and not seed.empty and seed_id in seed.index else None
        tier = clean(getattr(seed_row, "coverage_tier", None)) if seed_row is not None else None
        if not tier:
            tier = "D" if str(getattr(row, "entity_scope", "")) == "candidate" else "C"
        r_number = clean(getattr(row, "r_number_primary", None)) or (clean(getattr(seed_row, "r_number", None)) if seed_row is not None else None)
        query_name = clean(getattr(seed_row, "query_name", None)) if seed_row is not None else None
        r_name = r_number or query_name or mol_id
        nbp_c = prop(mol_id, "boiling_point_c")
        tcrit_c = prop(mol_id, "critical_temp_c")
        pcrit_mpa = prop(mol_id, "critical_pressure_mpa")
        applications: list[str] = []
        scope = clean(getattr(row, "entity_scope", None))
        if scope:
            applications.append(str(scope))
        notes = clean(getattr(seed_row, "notes", None)) if seed_row is not None else None
        if notes:
            applications.append(str(notes))
        records.append({
            "mol_id": mol_id,
            "r_name": str(r_name),
            "family": clean(getattr(row, "family", None)) or "Other",
            "formula": clean(getattr(row, "formula", None)),
            "mw": clean(getattr(row, "molecular_weight", None) or getattr(row, "mol_weight", None)),
            "smiles": clean(getattr(row, "isomeric_smiles", None) or getattr(row, "canonical_smiles", None)),
            "inchikey": clean(getattr(row, "inchikey", None)),
            "cas": cas_like(query_name) or "—",
            "tier": str(tier),
            "model_inclusion": clean(getattr(row, "model_inclusion", None)) or "no",
            "status": clean(getattr(row, "status", None)) or "resolved",
            "odp": prop(mol_id, "odp"),
            "gwp": prop(mol_id, "gwp_100yr"),
            "ashrae": prop(mol_id, "ashrae_safety", numeric=False),
            "tox": prop(mol_id, "toxicity_class", numeric=False),
            "nbp": clean(nbp_c + 273.15) if isinstance(nbp_c, (int, float)) else None,
            "tcrit": clean(tcrit_c + 273.15) if isinstance(tcrit_c, (int, float)) else None,
            "pcrit": clean(pcrit_mpa * 1000) if isinstance(pcrit_mpa, (int, float)) else None,
            "applications": applications[:3],
        })

    tier_rank = {"A": 0, "B": 1, "C": 2, "D": 3}
    records.sort(key=lambda r: (0 if r.get("model_inclusion") == "yes" else 1, tier_rank.get(str(r.get("tier")), 9), str(r.get("r_name") or ""), str(r.get("mol_id") or "")))
    return records


def title_group(group: Any) -> str:
    text = str(group or "Other").strip()
    mapping = {
        "thermodynamic": "Thermodynamic",
        "environmental": "Environmental",
        "safety": "Safety",
        "molecular_descriptor": "Structural",
        "identifier": "Structural",
        "synthesis": "Structural",
        "quantum": "Structural",
        "cycle": "Cycle",
        "transport": "Thermodynamic",
    }
    return mapping.get(text, text[:1].upper() + text[1:] if text else "Other")


def build_properties(property_dictionary: pd.DataFrame, canonical: pd.DataFrame, strict: pd.DataFrame) -> list[dict[str, Any]]:
    strict_keys = set(strict.get("canonical_feature_key", pd.Series(dtype=str)).dropna().astype(str))
    records: dict[str, dict[str, Any]] = {}
    if not property_dictionary.empty:
        for row in property_dictionary.itertuples(index=False):
            key = str(getattr(row, "canonical_feature_key"))
            records[key] = {
                "key": key,
                "group": title_group(getattr(row, "canonical_property_group", None)),
                "name": clean(getattr(row, "display_name", None)) or clean(getattr(row, "canonical_property_name", None)) or key,
                "unit": clean(getattr(row, "standard_unit", None)) or "—",
                "strict": key in strict_keys,
            }
    for row in canonical[["canonical_feature_key", "canonical_property_group", "canonical_property_name", "unit"]].drop_duplicates().itertuples(index=False):
        key = str(row.canonical_feature_key)
        records.setdefault(key, {
            "key": key,
            "group": title_group(row.canonical_property_group),
            "name": clean(row.canonical_property_name) or key,
            "unit": clean(row.unit) or "—",
            "strict": key in strict_keys,
        })
    return sorted(records.values(), key=lambda r: (r["group"], r["key"]))


def build_canonical(canonical: pd.DataFrame, strict: pd.DataFrame) -> list[dict[str, Any]]:
    strict_pairs = set(zip(strict.get("mol_id", pd.Series(dtype=str)).astype(str), strict.get("canonical_feature_key", pd.Series(dtype=str)).astype(str)))
    fields = [
        "mol_id", "canonical_feature_key", "value", "unit", "selected_source_name",
        "source_priority_rank", "data_quality_score_100", "is_proxy_or_screening",
        "source_divergence_flag", "conflict_flag", "proxy_only_flag", "nonproxy_candidate_count",
        "source_count", "ml_use_status",
    ]
    out: list[dict[str, Any]] = []
    for row in canonical[fields].itertuples(index=False):
        mol_id = str(row.mol_id)
        key = str(row.canonical_feature_key)
        out.append({
            "mol_id": mol_id,
            "canonical_feature_key": key,
            "value": clean(row.value),
            "unit": clean(row.unit) or "—",
            "selected_source_name": clean(row.selected_source_name) or "",
            "source_priority_rank": clean(row.source_priority_rank),
            "data_quality_score_100": clean(row.data_quality_score_100),
            "is_proxy_or_screening": bool(clean(row.is_proxy_or_screening)),
            "source_divergence_flag": bool(clean(row.source_divergence_flag)),
            "conflict_flag": bool(clean(row.conflict_flag)),
            "proxy_only_flag": bool(clean(row.proxy_only_flag)),
            "nonproxy_candidate_count": clean(row.nonproxy_candidate_count) or 0,
            "source_count": clean(row.source_count) or 0,
            "in_strict": (mol_id, key) in strict_pairs,
            "ml_use_status": clean(row.ml_use_status) or "recommended_only",
        })
    return out


def build_observations(observations: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "observation_id", "mol_id", "property_name", "value", "value_num", "unit", "source_name",
        "source_type", "method", "temperature", "pressure", "phase", "quality_level", "qc_status",
        "canonical_feature_key",
    ]
    out: list[dict[str, Any]] = []
    for row in observations[cols].itertuples(index=False):
        value = clean(row.value_num)
        if value is None:
            value = clean(row.value)
        out.append({
            "observation_id": clean(row.observation_id),
            "mol_id": clean(row.mol_id),
            "property_name": clean(row.property_name),
            "value": value,
            "unit": clean(row.unit) or "—",
            "source": clean(row.source_name) or "",
            "source_type": clean(row.source_type) or "",
            "method": clean(row.method) or "",
            "temperature": clean(row.temperature),
            "pressure": clean(row.pressure),
            "phase": clean(row.phase),
            "quality": clean(row.quality_level) or "",
            "qc": clean(row.qc_status) or "",
            "canonical_feature_key": clean(row.canonical_feature_key),
        })
    return out


def build_regulatory(regulatory: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "mol_id": clean(r.mol_id),
            "jurisdiction": clean(r.jurisdiction),
            "end_use": clean(r.end_use),
            "acceptability": clean(r.acceptability or r.listing_status),
            "effective": clean(r.effective_date),
        }
        for r in regulatory.itertuples(index=False)
    ]


def build_mixtures(mixture_core: pd.DataFrame, mixture_composition: pd.DataFrame) -> list[dict[str, Any]]:
    comp_groups = {mid: group for mid, group in mixture_composition.groupby("mixture_id")}
    out: list[dict[str, Any]] = []
    for row in mixture_core.itertuples(index=False):
        group = comp_groups.get(row.mixture_id, pd.DataFrame())
        comps = []
        if not group.empty:
            for comp in group.itertuples(index=False):
                frac = clean(getattr(comp, "fraction_value", None))
                comps.append({"mol_id": clean(comp.component_mol_id), "pct": clean(frac * 100) if isinstance(frac, (int, float)) else None})
        out.append({
            "mixture_id": clean(row.mixture_id),
            "mixture_name": clean(row.mixture_name),
            "ashrae": clean(row.ashrae_blend_designation),
            "components": comps,
            "glide_k": None,
            "gwp": None,
            "ashrae_class": None,
            "app": clean(row.notes),
        })
    return out


def build_active_learning(active_learning: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for i, row in enumerate(active_learning.sort_values("priority_score", ascending=False).itertuples(index=False)):
        feature = clean(getattr(row, "recommended_next_action", None)) or clean(getattr(row, "acquisition_strategy", None)) or "next_measurement"
        try:
            payload = json.loads(row.payload_json or "{}")
            missing = payload.get("missing_properties") or []
            if missing:
                feature = missing[0]
        except (TypeError, json.JSONDecodeError):
            pass
        score = clean(getattr(row, "priority_score", None)) or 0
        priority = 1 if i < 250 or score >= 0.75 else 2 if i < 1000 or score >= 0.5 else 3
        out.append({
            "item_id": clean(row.queue_entry_id),
            "mol_id": clean(row.mol_id),
            "feature": feature,
            "uncertainty": clean(getattr(row, "uncertainty_score", None)),
            "priority": priority,
            "status": clean(row.status) or "queued",
        })
    return out


def build_sources(observations: pd.DataFrame) -> list[dict[str, Any]]:
    if observations.empty:
        return []
    grouped = (
        observations.groupby(["source_name", "source_type"], dropna=False)
        .size()
        .reset_index(name="obs")
        .sort_values("obs", ascending=False)
        .head(30)
    )
    return [
        {
            "name": clean(r.source_name) or "unknown",
            "type": clean(r.source_type) or "unknown",
            "rank": i + 1,
            "obs": int(r.obs),
            "note": "Synced from property_observation.parquet",
        }
        for i, r in enumerate(grouped.itertuples(index=False))
    ]


def table_count(validation: dict[str, Any], table: str, fallback: int) -> int:
    for item in validation.get("schema_checks", []):
        if item.get("table") == table:
            return int(item.get("row_count", fallback))
    return fallback


def build_stats(
    quality: dict[str, Any],
    validation: dict[str, Any],
    version: str,
    snapshot_date: str,
    molecule_master: pd.DataFrame,
    property_recommended: pd.DataFrame,
    property_canonical: pd.DataFrame,
    property_strict: pd.DataFrame,
    model_index: pd.DataFrame,
    regulatory: pd.DataFrame,
    pending_sources: pd.DataFrame,
    qc_issues: pd.DataFrame,
    mixture_core: pd.DataFrame,
    mixture_composition: pd.DataFrame,
    active_learning: pd.DataFrame,
    active_decisions: pd.DataFrame,
    cycle_case: pd.DataFrame,
    cycle_operating_point: pd.DataFrame,
) -> dict[str, Any]:
    split_counts = model_index["split"].value_counts(dropna=False).to_dict() if "split" in model_index.columns else {}
    tier_coverage = quality.get("tier_coverage", {})
    proxy_rows = int(property_strict.get("is_proxy_or_screening", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not property_strict.empty else 0
    proxy_rules = 0
    if "proxy_policy_id" in property_strict.columns:
        proxy_rules = int(property_strict["proxy_policy_id"].fillna("").astype(str).str.len().gt(0).groupby(property_strict["proxy_policy_id"]).any().sum())
    return {
        "version": version,
        "snapshot_date": snapshot_date,
        "inventory_rows": int(quality.get("seed_catalog_count", 0)),
        "refrigerants": int(quality.get("refrigerant_count", 0)),
        "candidates": int(quality.get("candidate_count", 0)),
        "resolved_molecules": int(len(molecule_master)),
        "model_dataset_index": int(len(model_index)),
        "tier_a": int(tier_coverage.get("A", {}).get("molecule_count", 0)),
        "tier_b": int(tier_coverage.get("B", {}).get("molecule_count", 0)),
        "tier_c": int(tier_coverage.get("C", {}).get("molecule_count", 0)),
        "tier_d": int(tier_coverage.get("D", {}).get("molecule_count", max(0, len(molecule_master) - sum(int(tier_coverage.get(k, {}).get("molecule_count", 0)) for k in ["A", "B", "C"])))),
        "property_observation": table_count(validation, "property_observation", int(quality.get("observation_count", 0))),
        "property_observation_canonical": table_count(validation, "property_observation_canonical", 0),
        "property_recommended": int(len(property_recommended)),
        "property_recommended_canonical": int(len(property_canonical)),
        "property_recommended_canonical_strict": int(len(property_strict)),
        "review_queue_open": table_count(validation, "property_recommended_canonical_review_queue", 0),
        "review_decisions_applied": int(len(active_decisions)),
        "proxy_rules_applied": int(proxy_rules),
        "mixtures": int(len(mixture_core)),
        "mixture_components": int(len(mixture_composition)),
        "canonical_keys": int(property_canonical["canonical_feature_key"].nunique()) if "canonical_feature_key" in property_canonical else 0,
        "strict_keys": int(property_strict["canonical_feature_key"].nunique()) if "canonical_feature_key" in property_strict else 0,
        "proxy_rows_promoted": proxy_rows,
        "train": int(split_counts.get("train", 0)),
        "validation": int(split_counts.get("validation", 0)),
        "test": int(split_counts.get("test", 0)),
        "regulatory": int(len(regulatory)),
        "pending_sources": int(len(pending_sources)),
        "qc_issue_count": int(len(qc_issues)),
        "cycle_cases": int(len(cycle_case)),
        "cycle_operating_points": int(len(cycle_operating_point)),
        "active_learning_queue": int(len(active_learning)),
    }


def build_db_script() -> tuple[str, str, dict[str, Any]]:
    version = (ROOT / "data" / "gold" / "VERSION").read_text(encoding="utf-8").strip()
    snapshot_date = datetime.fromtimestamp((ROOT / "data" / "gold" / "VERSION").stat().st_mtime).date().isoformat()
    quality = json.loads((ROOT / "data" / "gold" / "quality_report.json").read_text(encoding="utf-8"))
    validation = json.loads((ROOT / "data" / "gold" / "validation_report.json").read_text(encoding="utf-8"))

    molecule_master = read_parquet("data/gold/molecule_master.parquet")
    property_recommended = read_parquet("data/gold/property_recommended.parquet")
    property_canonical = read_parquet("data/gold/property_recommended_canonical.parquet")
    property_strict = read_parquet("data/gold/property_recommended_canonical_strict.parquet")
    property_dictionary = read_parquet("data/gold/property_dictionary.parquet")
    model_index = read_parquet("data/gold/model_dataset_index.parquet")
    observations = read_parquet("data/silver/property_observation.parquet")
    regulatory = read_parquet("data/silver/regulatory_status.parquet")
    mixture_core = read_parquet("data/silver/mixture_core.parquet")
    mixture_composition = read_parquet("data/silver/mixture_composition.parquet")
    seed_catalog = pd.read_csv(ROOT / "data" / "raw" / "manual" / "seed_catalog.csv")
    pending_sources = read_parquet("data/bronze/pending_sources.parquet")
    qc_issues = read_parquet("data/silver/qc_issues.parquet")
    active_learning = read_parquet("data/gold/active_learning_queue.parquet")
    active_decisions = read_parquet("data/gold/active_learning_decision_log.parquet")
    cycle_case = read_parquet("data/silver/cycle_case.parquet")
    cycle_operating_point = read_parquet("data/silver/cycle_operating_point.parquet")

    stats = build_stats(
        quality,
        validation,
        version,
        snapshot_date,
        molecule_master,
        property_recommended,
        property_canonical,
        property_strict,
        model_index,
        regulatory,
        pending_sources,
        qc_issues,
        mixture_core,
        mixture_composition,
        active_learning,
        active_decisions,
        cycle_case,
        cycle_operating_point,
    )
    db = {
        "mols": build_molecules(molecule_master, property_recommended, seed_catalog),
        "properties": build_properties(property_dictionary, property_canonical, property_strict),
        "regulatory": build_regulatory(regulatory),
        "canonical": build_canonical(property_canonical, property_strict),
        "observations": build_observations(observations),
        "mixtures": build_mixtures(mixture_core, mixture_composition),
        "reviewQueue": [],
        "activeLearning": build_active_learning(active_learning),
        "sources": build_sources(observations),
        "stats": stats,
    }
    script = (
        "window.DB = (function () {\n"
        "  // Synced from data/gold + data/silver by scripts/sync_frontend_data.py.\n"
        f"  const DATA = {js_json(db)};\n"
        "  return DATA;\n"
        "})();"
    )
    return script, version, stats


def replace_db_block(html: str, db_script: str) -> str:
    pattern = r"window\.DB = \(function \(\) \{.*?\n\}\)\(\);"
    html, count = re.subn(pattern, db_script, html, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError("failed to replace window.DB block")
    return html


def replace_structure_block(html: str) -> str:
    pattern = (
        r"// Offline (?:molecule depictions and conformers generated from mols\[\]\.smiles using RDKit|"
        r"depictions for the synced v1\.6 dataset are generated separately)\.\n"
        r"window\.STRUCTURE_DATA = .*?;\n</script>"
    )
    replacement = (
        "// Offline depictions for the synced v1.6 dataset are generated separately.\n"
        "window.STRUCTURE_DATA = {};\n</script>"
    )
    html, count = re.subn(pattern, replacement, html, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError("failed to replace window.STRUCTURE_DATA block")
    return html


def apply_static_text_updates(html: str, version: str, stats: dict[str, Any]) -> str:
    snapshot = stats["snapshot_date"]
    replacements = {
        "refrigerant · v1.5.0-draft": f"refrigerant · {version}",
        "制冷剂 · v1.5.0-draft": f"制冷剂 · {version}",
        "Demo snapshot · local mock data": "Dataset snapshot · synced data",
        "Prototype · local mock data": "Prototype · synced data",
        "演示快照 · 本地模拟数据": "数据快照 · 已同步数据",
        "shareable identifier · v1.5.0-draft": f"shareable identifier · {version}",
        "可共享标识符 · v1.5.0-draft": f"可共享标识符 · {version}",
        "v1.5.0-draft, snapshot 2026-04-24": f"{version}, snapshot {snapshot}",
        "{t(\"meta.snapshot\")} 2026-04-24<br/>": "{t(\"meta.snapshot\")} {window.DB.stats.snapshot_date}<br/>",
        "<span className=\"strong\">v1.5.0-draft</span>": f"<span className=\"strong\">{version}</span>",
        "fmtInt(5598), subset: fmtInt(120)": "fmtInt(window.DB.stats.resolved_molecules), subset: fmtInt(window.DB.stats.model_dataset_index)",
        'canonical_feature_key==="cycle.cop_standard"': 'canonical_feature_key==="cycle.cop"',
        'canonical_feature_key==="cycle.volumetric_capacity_kj_m3"': 'canonical_feature_key==="cycle.volumetric_cooling_capacity"',
        'getCanon(m.mol_id,"cycle.cop_standard")': 'getCanon(m.mol_id,"cycle.cop")',
        'getCanon(m.mol_id,"cycle.volumetric_capacity_kj_m3")': 'getCanon(m.mol_id,"cycle.volumetric_cooling_capacity")',
        'const selectedMol = route.molId || "MOL_R32";': 'const selectedMol = route.molId || window.DB.mols[0]?.mol_id || "MOL_R32";',
        'const sources = [\n    {name:"NIST WebBook (REFPROP 10.0)",': 'const sources = _DB.sources || [\n    {name:"NIST WebBook (REFPROP 10.0)",',
        '{t(s.noteKey)}</td>': '{s.note || (s.noteKey ? t(s.noteKey) : "")}</td>',
    }
    for old, new in replacements.items():
        html = html.replace(old, new)
    return html


def make_lan_html(root_html: str) -> str:
    lan = root_html
    lan = lan.replace(
        '<link rel="preconnect" href="https://fonts.googleapis.com"/>\n'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>\n'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Source+Serif+4:opsz,wght@8..60,400;8..60,500;8..60,600;8..60,700&display=swap" rel="stylesheet"/>\n',
        "<!-- Fonts use local system fallbacks for LAN/offline deployment. -->\n",
    )
    lan = lan.replace("https://unpkg.com/react@18.3.1/umd/react.production.min.js", "./vendor/react.production.min.js")
    lan = lan.replace("https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js", "./vendor/react-dom.production.min.js")
    lan = lan.replace("https://unpkg.com/@babel/standalone@7.29.0/babel.min.js", "./vendor/babel.min.js")
    return lan


def main() -> None:
    db_script, version, stats = build_db_script()
    html = ROOT_HTML.read_text(encoding="utf-8")
    html = replace_db_block(html, db_script)
    html = replace_structure_block(html)
    html = apply_static_text_updates(html, version, stats)
    ROOT_HTML.write_text(html, encoding="utf-8")
    LAN_HTML.write_text(make_lan_html(html), encoding="utf-8")
    print(json.dumps({"version": version, "stats": stats, "root_html_bytes": ROOT_HTML.stat().st_size, "lan_html_bytes": LAN_HTML.stat().st_size}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
