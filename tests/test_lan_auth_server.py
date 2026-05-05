from __future__ import annotations

import http.client
import importlib.util
import json
import sys
import threading
import urllib.parse
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
LAN_DIR = ROOT / "deploy" / "lan"


def load_auth_module() -> ModuleType:
    module_path = LAN_DIR / "serve_auth.py"
    spec = importlib.util.spec_from_file_location("lan_auth_server", module_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_password_hash_config_verifies_correct_password_only() -> None:
    auth = load_auth_module()

    config = auth.build_auth_config(
        "internal",
        "correct horse battery staple",
        salt=bytes.fromhex("00" * 16),
        iterations=1_000,
    )

    assert config["username"] == "internal"
    assert config["iterations"] == 1_000
    assert "correct horse battery staple" not in json.dumps(config)
    assert auth.verify_password("correct horse battery staple", config)
    assert not auth.verify_password("wrong password", config)


def test_missing_auth_config_error_tells_operator_how_to_initialize(tmp_path: Path) -> None:
    auth = load_auth_module()

    with pytest.raises(auth.AuthConfigError, match=r"init_auth\.py"):
        auth.load_auth_config(tmp_path / ".auth.json")


class AuthServer:
    def __init__(self, auth: ModuleType, directory: Path, auth_config: dict[str, object]) -> None:
        handler = auth.make_handler(
            directory=directory,
            auth_config=auth_config,
            session_seconds=60,
        )
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def request(
        self,
        method: str,
        path: str,
        *,
        body: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], str]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request(method, path, body=body, headers=headers or {})
        response = conn.getresponse()
        data = response.read().decode("utf-8")
        response_headers = {key.lower(): value for key, value in response.getheaders()}
        conn.close()
        return response.status, response_headers, data

    def close(self) -> None:
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()


@pytest.fixture()
def auth_server(tmp_path: Path):
    auth = load_auth_module()
    (tmp_path / "index.html").write_text(
        "<!doctype html><html><body>DATABASE_PAGE_MARKER</body></html>",
        encoding="utf-8",
    )
    (tmp_path / "vendor").mkdir()
    (tmp_path / "vendor" / "bundle.js").write_text("VENDOR_MARKER", encoding="utf-8")
    auth_config = auth.build_auth_config(
        "admin",
        "s3cret",
        salt=bytes.fromhex("11" * 16),
        iterations=1_000,
    )
    server = AuthServer(auth, tmp_path, auth_config)
    try:
        yield server
    finally:
        server.close()


def form_body(**fields: str) -> str:
    return urllib.parse.urlencode(fields)


def test_unauthenticated_root_returns_centered_login_only(auth_server: AuthServer) -> None:
    status, headers, body = auth_server.request("GET", "/")

    assert status == 200
    assert headers["content-type"].startswith("text/html")
    assert '<form class="auth-form" method="post" action="/login">' in body
    assert "R-PhysGen-DB" in body
    assert "Register" not in body
    assert "DATABASE_PAGE_MARKER" not in body


def test_login_allows_database_access_and_logout_blocks_it_again(auth_server: AuthServer) -> None:
    bad_status, _, bad_body = auth_server.request(
        "POST",
        "/login",
        body=form_body(username="admin", password="wrong"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert bad_status == 401
    assert "Invalid username or password" in bad_body

    login_status, login_headers, _ = auth_server.request(
        "POST",
        "/login",
        body=form_body(username="admin", password="s3cret"),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert login_status == 303
    cookie = login_headers["set-cookie"]
    assert "rpg_session=" in cookie
    assert "HttpOnly" in cookie
    assert "Max-Age" not in cookie
    assert "Expires" not in cookie
    assert login_headers["location"] == "/"

    authed_headers = {"Cookie": cookie.split(";", 1)[0]}
    home_status, home_headers, home_body = auth_server.request("GET", "/", headers=authed_headers)
    assert home_status == 200
    assert home_headers["cache-control"] == "no-store"
    assert "DATABASE_PAGE_MARKER" in home_body
    assert '<form class="auth-form"' not in home_body

    vendor_status, _, vendor_body = auth_server.request(
        "GET",
        "/vendor/bundle.js",
        headers=authed_headers,
    )
    assert vendor_status == 200
    assert vendor_body == "VENDOR_MARKER"

    blocked_status, _, blocked_body = auth_server.request("GET", "/vendor/bundle.js")
    assert blocked_status == 401
    assert "Authentication required" in blocked_body

    logout_status, logout_headers, _ = auth_server.request(
        "POST",
        "/logout",
        headers=authed_headers,
    )
    assert logout_status == 303
    assert "Max-Age=0" in logout_headers["set-cookie"]

    relog_status, _, relog_body = auth_server.request("GET", "/", headers=authed_headers)
    assert relog_status == 200
    assert '<form class="auth-form" method="post" action="/login">' in relog_body
    assert "DATABASE_PAGE_MARKER" not in relog_body
