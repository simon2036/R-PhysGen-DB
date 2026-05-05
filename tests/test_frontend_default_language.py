from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_FILES = [
    ROOT / "R-PhysGen-DB.html",
    ROOT / "deploy" / "lan" / "index.html",
]


def test_frontend_defaults_to_english_when_no_saved_preference() -> None:
    for path in FRONTEND_FILES:
        text = path.read_text(encoding="utf-8")
        assert '<html lang="en">' in text
        assert 'React.createContext({ lang: "en"' in text
        assert 'localStorage.getItem("rpg-lang") || "en"' in text
        assert 'I18N[lang] || I18N.en' in text


def test_frontend_entries_include_shared_auth_shell_styles() -> None:
    for path in FRONTEND_FILES:
        text = path.read_text(encoding="utf-8")
        assert ".auth-shell" in text
        assert ".auth-card" in text
        assert ".auth-error" in text
        assert ".auth-submit" in text


def test_lan_entry_matches_root_except_expected_offline_paths() -> None:
    root_text = (ROOT / "R-PhysGen-DB.html").read_text(encoding="utf-8")
    lan_text = (ROOT / "deploy" / "lan" / "index.html").read_text(encoding="utf-8")

    normalized_root = root_text
    normalized_root = normalized_root.replace(
        '<link rel="preconnect" href="https://fonts.googleapis.com"/>\n'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>\n'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&family=Source+Serif+4:opsz,wght@8..60,400;8..60,500;8..60,600;8..60,700&display=swap" rel="stylesheet"/>\n',
        "<!-- Fonts use local system fallbacks for LAN/offline deployment. -->\n",
    )
    normalized_root = normalized_root.replace(
        "https://unpkg.com/react@18.3.1/umd/react.production.min.js",
        "./vendor/react.production.min.js",
    )
    normalized_root = normalized_root.replace(
        "https://unpkg.com/react-dom@18.3.1/umd/react-dom.production.min.js",
        "./vendor/react-dom.production.min.js",
    )
    normalized_root = normalized_root.replace(
        "https://unpkg.com/@babel/standalone@7.29.0/babel.min.js",
        "./vendor/babel.min.js",
    )

    assert normalized_root == lan_text


def _frontend_stats(text: str) -> dict[str, object]:
    match = re.search(r'"stats":(\{[^{}]+\})', text)
    assert match, "missing embedded frontend stats object"
    return json.loads(match.group(1))


def test_frontend_embedded_data_matches_latest_gold_version_and_counts() -> None:
    expected_version = (ROOT / "data" / "gold" / "VERSION").read_text(encoding="utf-8").strip()
    expected_counts = {
        "resolved_molecules": len(pd.read_parquet(ROOT / "data" / "gold" / "molecule_master.parquet")),
        "property_observation": len(pd.read_parquet(ROOT / "data" / "silver" / "property_observation.parquet")),
        "property_recommended": len(pd.read_parquet(ROOT / "data" / "gold" / "property_recommended.parquet")),
        "property_recommended_canonical": len(pd.read_parquet(ROOT / "data" / "gold" / "property_recommended_canonical.parquet")),
        "property_recommended_canonical_strict": len(pd.read_parquet(ROOT / "data" / "gold" / "property_recommended_canonical_strict.parquet")),
        "model_dataset_index": len(pd.read_parquet(ROOT / "data" / "gold" / "model_dataset_index.parquet")),
        "regulatory": len(pd.read_parquet(ROOT / "data" / "silver" / "regulatory_status.parquet")),
        "mixtures": len(pd.read_parquet(ROOT / "data" / "silver" / "mixture_core.parquet")),
    }

    for path in FRONTEND_FILES:
        text = path.read_text(encoding="utf-8")
        stats = _frontend_stats(text)
        assert stats["version"] == expected_version
        assert expected_version in text
        assert "local mock data" not in text
        for key, expected in expected_counts.items():
            assert int(stats[key]) == expected
