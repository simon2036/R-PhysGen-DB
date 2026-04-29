from __future__ import annotations

from pathlib import Path

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
