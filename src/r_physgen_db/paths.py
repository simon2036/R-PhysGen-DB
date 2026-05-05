"""Repository path constants with legacy-layout fallbacks."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"
DATA_LAKE_DIR = DATA_ROOT / "lake"
LEGACY_DATA_LAKE_DIR = DATA_ROOT
DATA_SOURCES_DIR = DATA_ROOT / "sources"
DATA_ARTIFACTS_LOCAL_DIR = DATA_ROOT / "artifacts" / "local"
SCHEMA_DIR = PROJECT_ROOT / "schemas"
DOCS_DIR = PROJECT_ROOT / "docs"
DATASET_MIGRATIONS_DIR = DOCS_DIR / "migrations" / "dataset"
LEGACY_DATASET_MIGRATIONS_DIR = DOCS_DIR / "dataset_migrations"
STATIC_FRONTEND_HTML = PROJECT_ROOT / "deploy" / "static" / "R-PhysGen-DB.html"
LEGACY_FRONTEND_HTML = PROJECT_ROOT / "R-PhysGen-DB.html"

_LAKE_LAYERS = ("raw", "bronze", "silver", "gold", "extensions")


def legacy_data_path(*parts: str) -> Path:
    """Return the pre-reorganization data path for a lake-relative path."""

    return LEGACY_DATA_LAKE_DIR.joinpath(*parts)


def _legacy_lake_exists() -> bool:
    return any((LEGACY_DATA_LAKE_DIR / layer).exists() for layer in _LAKE_LAYERS)


def _select_data_lake_dir() -> Path:
    if DATA_LAKE_DIR.exists() or not _legacy_lake_exists():
        return DATA_LAKE_DIR
    return LEGACY_DATA_LAKE_DIR


DATA_DIR = _select_data_lake_dir()
RAW_DATA_DIR = DATA_DIR / "raw"
BRONZE_DATA_DIR = DATA_DIR / "bronze"
SILVER_DATA_DIR = DATA_DIR / "silver"
GOLD_DATA_DIR = DATA_DIR / "gold"
EXTENSIONS_DATA_DIR = DATA_DIR / "extensions"


def lake_path(*parts: str) -> Path:
    """Return a path under the selected data-lake root."""

    return DATA_DIR.joinpath(*parts)


def data_index_dir() -> Path:
    """Return the preferred index directory, falling back to legacy data/index."""

    preferred = DATA_ROOT / "indexes"
    legacy = DATA_ROOT / "index"
    if preferred.exists() or not legacy.exists():
        return preferred
    return legacy


DATA_INDEX_DIR = data_index_dir()


def data_index_path(*parts: str) -> Path:
    """Return a path under the selected query-index directory."""

    return DATA_INDEX_DIR.joinpath(*parts)


def dataset_migrations_dir() -> Path:
    """Return the preferred dataset-migration docs directory with legacy fallback."""

    if DATASET_MIGRATIONS_DIR.exists() or not LEGACY_DATASET_MIGRATIONS_DIR.exists():
        return DATASET_MIGRATIONS_DIR
    return LEGACY_DATASET_MIGRATIONS_DIR


def frontend_html_path() -> Path:
    """Return the preferred static frontend entrypoint with legacy fallback."""

    if STATIC_FRONTEND_HTML.exists() or not LEGACY_FRONTEND_HTML.exists():
        return STATIC_FRONTEND_HTML
    return LEGACY_FRONTEND_HTML


def source_path(*parts: str) -> Path:
    """Return a categorized source-input path."""

    return DATA_SOURCES_DIR.joinpath(*parts)
