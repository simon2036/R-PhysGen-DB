from __future__ import annotations

import pytest
import requests

from r_physgen_db.pipeline import _load_or_fetch_json, _load_or_fetch_text, _load_or_fetch_text_payload


def test_text_refresh_uses_cached_snapshot_on_transient_ssl_error(tmp_path) -> None:
    snapshot_path = tmp_path / "snap_example.html"
    cached_html = "<html><body>cached snapshot</body></html>"
    snapshot_path.write_text(cached_html, encoding="utf-8")

    def fetcher() -> str:
        raise requests.exceptions.SSLError("temporary ssl failure")

    snapshot = _load_or_fetch_text(
        snapshot_path,
        True,
        fetcher,
        upstream_url="https://example.org/snap",
    )

    assert snapshot["source_status"] == "cached_fallback"
    assert snapshot["text"] == cached_html
    assert snapshot_path.read_text(encoding="utf-8") == cached_html
    assert (tmp_path / "snap_example.refresh_error.txt").exists()


def test_payload_refresh_uses_cached_snapshot_on_transient_connection_error(tmp_path) -> None:
    snapshot_path = tmp_path / "nist_example.html"
    cached_html = "<html><body>NIST cached snapshot</body></html>"
    snapshot_path.write_text(cached_html, encoding="utf-8")

    def fetcher() -> dict[str, str]:
        raise requests.exceptions.ConnectionError("connection reset")

    snapshot = _load_or_fetch_text_payload(
        snapshot_path,
        True,
        fetcher,
        fallback_url="https://webbook.nist.gov/example",
    )

    assert snapshot["status"] == "ok"
    assert snapshot["source_status"] == "cached_fallback"
    assert snapshot["url"] == "https://webbook.nist.gov/example"
    assert snapshot["html"] == cached_html
    assert snapshot_path.read_text(encoding="utf-8") == cached_html
    assert (tmp_path / "nist_example.refresh_error.txt").exists()


def test_text_refresh_does_not_hide_non_transient_http_errors(tmp_path) -> None:
    snapshot_path = tmp_path / "snap_example.html"
    snapshot_path.write_text("<html><body>cached snapshot</body></html>", encoding="utf-8")

    response = requests.Response()
    response.status_code = 404
    response.url = "https://example.org/missing"
    error = requests.HTTPError("404 Client Error", response=response)

    def fetcher() -> str:
        raise error

    with pytest.raises(requests.HTTPError):
        _load_or_fetch_text(
            snapshot_path,
            True,
            fetcher,
            upstream_url="https://example.org/missing",
        )

    assert not (tmp_path / "snap_example.refresh_error.txt").exists()


def test_json_refresh_uses_cached_snapshot_on_transient_proxy_error(tmp_path) -> None:
    snapshot_path = tmp_path / "pubchem_example.json"
    snapshot_path.write_text('{"hello": "world"}', encoding="utf-8")

    def fetcher() -> dict[str, str]:
        raise requests.exceptions.ProxyError("proxy reset")

    snapshot = _load_or_fetch_json(snapshot_path, True, fetcher)

    assert snapshot["source_status"] == "cached_fallback"
    assert snapshot["payload"] == {"hello": "world"}
    assert (tmp_path / "pubchem_example.refresh_error.txt").exists()
