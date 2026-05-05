#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import html
import json
import secrets
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

DEFAULT_AUTH_PATH = Path(__file__).resolve().parent / ".auth.json"
DEFAULT_DIRECTORY = Path(__file__).resolve().parent
DEFAULT_HOST = "0.0.0.0"
DEFAULT_ITERATIONS = 260_000
DEFAULT_PORT = 8088
DEFAULT_SESSION_SECONDS = 12 * 60 * 60
SESSION_COOKIE = "rpg_session"
_HASH_NAME = "sha256"


class AuthConfigError(RuntimeError):
    """Raised when the local LAN authentication config is missing or invalid."""


@dataclass(frozen=True)
class AuthConfig:
    username: str
    salt: str
    iterations: int
    password_hash: str

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "AuthConfig":
        try:
            username = str(data["username"])
            salt = str(data["salt"])
            iterations = int(data["iterations"])
            password_hash = str(data["password_hash"])
        except (KeyError, TypeError, ValueError) as exc:
            raise AuthConfigError(
                "Invalid auth config. Expected username, salt, iterations, and password_hash fields."
            ) from exc
        if not username:
            raise AuthConfigError("Invalid auth config: username must be non-empty.")
        if iterations <= 0:
            raise AuthConfigError("Invalid auth config: iterations must be positive.")
        try:
            bytes.fromhex(salt)
            bytes.fromhex(password_hash)
        except ValueError as exc:
            raise AuthConfigError("Invalid auth config: salt and password_hash must be hex strings.") from exc
        return cls(
            username=username,
            salt=salt,
            iterations=iterations,
            password_hash=password_hash,
        )

    def as_dict(self) -> dict[str, str | int]:
        return {
            "username": self.username,
            "salt": self.salt,
            "iterations": self.iterations,
            "password_hash": self.password_hash,
        }


def _coerce_config(config: Mapping[str, Any] | AuthConfig) -> AuthConfig:
    if isinstance(config, AuthConfig):
        return config
    return AuthConfig.from_mapping(config)


def hash_password(password: str, *, salt: bytes, iterations: int) -> str:
    if iterations <= 0:
        raise ValueError("iterations must be positive")
    digest = hashlib.pbkdf2_hmac(
        _HASH_NAME,
        password.encode("utf-8"),
        salt,
        iterations,
    )
    return digest.hex()


def build_auth_config(
    username: str,
    password: str,
    *,
    salt: bytes | None = None,
    iterations: int = DEFAULT_ITERATIONS,
) -> dict[str, str | int]:
    username = username.strip()
    if not username:
        raise ValueError("username must be non-empty")
    if not password:
        raise ValueError("password must be non-empty")
    if iterations <= 0:
        raise ValueError("iterations must be positive")
    salt_bytes = salt if salt is not None else secrets.token_bytes(16)
    return {
        "username": username,
        "salt": salt_bytes.hex(),
        "iterations": iterations,
        "password_hash": hash_password(password, salt=salt_bytes, iterations=iterations),
    }


def verify_password(password: str, config: Mapping[str, Any] | AuthConfig) -> bool:
    try:
        parsed = _coerce_config(config)
        expected = parsed.password_hash
        actual = hash_password(
            password,
            salt=bytes.fromhex(parsed.salt),
            iterations=parsed.iterations,
        )
    except (AuthConfigError, ValueError):
        return False
    return hmac.compare_digest(actual, expected)


def load_auth_config(path: Path | str = DEFAULT_AUTH_PATH) -> dict[str, str | int]:
    config_path = Path(path)
    if not config_path.exists():
        raise AuthConfigError(
            f"Auth config not found at {config_path}. "
            "Run `python deploy/lan/init_auth.py` to create the local internal account."
        )
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise AuthConfigError(f"Invalid auth config JSON at {config_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise AuthConfigError(f"Invalid auth config at {config_path}: expected a JSON object.")
    return AuthConfig.from_mapping(data).as_dict()


class SessionStore:
    def __init__(self, session_seconds: int = DEFAULT_SESSION_SECONDS) -> None:
        if session_seconds <= 0:
            raise ValueError("session_seconds must be positive")
        self._session_seconds = session_seconds
        self._sessions: dict[str, tuple[str, float]] = {}
        self._lock = threading.Lock()

    def create(self, username: str) -> str:
        token = secrets.token_urlsafe(32)
        expires_at = time.time() + self._session_seconds
        with self._lock:
            self._purge_expired_locked()
            self._sessions[token] = (username, expires_at)
        return token

    def validate(self, token: str | None) -> bool:
        if not token:
            return False
        now = time.time()
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return False
            _, expires_at = session
            if expires_at <= now:
                self._sessions.pop(token, None)
                return False
            return True

    def clear(self, token: str | None) -> None:
        if not token:
            return
        with self._lock:
            self._sessions.pop(token, None)

    def _purge_expired_locked(self) -> None:
        now = time.time()
        expired = [token for token, (_, expires_at) in self._sessions.items() if expires_at <= now]
        for token in expired:
            self._sessions.pop(token, None)


LOGIN_PAGE_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>R-PhysGen-DB · Login</title>
<style>
:root {{
  --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  --bg: oklch(98.5% 0.005 80);
  --bg-raised: oklch(100% 0 0);
  --bg-sunken: oklch(96.5% 0.007 80);
  --border: oklch(91% 0.008 80);
  --ink: oklch(21% 0.012 240);
  --ink-muted: oklch(46% 0.013 240);
  --ink-subtle: oklch(62% 0.011 240);
  --accent: oklch(52% 0.12 210);
  --accent-ink: oklch(40% 0.12 210);
  --danger: oklch(55% 0.17 25);
  --danger-soft: oklch(95% 0.04 25);
  --focus: oklch(60% 0.18 250);
  --shadow-lg: 0 14px 44px oklch(20% 0.02 240 / 0.12), 0 4px 10px oklch(20% 0.02 240 / 0.06);
  --radius: 8px;
  --radius-lg: 12px;
  color-scheme: light;
}}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; min-height: 100%; }}
body {{
  font-family: var(--font-sans);
  background: radial-gradient(circle at top, oklch(94% 0.03 210), transparent 34rem), var(--bg);
  color: var(--ink);
  font-size: 14px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}}
.auth-shell {{
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 32px 18px;
}}
.auth-card {{
  width: min(100%, 420px);
  background: color-mix(in oklab, var(--bg-raised) 94%, var(--bg-sunken));
  border: 1px solid var(--border);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-lg);
  padding: 28px;
}}
.auth-brand {{ display: flex; align-items: center; gap: 12px; margin-bottom: 24px; }}
.auth-brand-mark {{
  width: 40px;
  height: 40px;
  display: grid;
  place-items: center;
  border-radius: 12px;
  background: var(--accent);
  color: white;
  font-weight: 800;
}}
.auth-brand-title {{ font-weight: 760; font-size: 18px; letter-spacing: -0.02em; }}
.auth-brand-subtitle {{ color: var(--ink-muted); font-size: 13px; }}
.auth-form {{ display: grid; gap: 16px; }}
.auth-field {{ display: grid; gap: 7px; color: var(--ink-muted); font-weight: 600; }}
.auth-input {{
  width: 100%;
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 11px 12px;
  background: var(--bg-raised);
  color: var(--ink);
  font: inherit;
}}
.auth-input:focus {{ outline: 2px solid color-mix(in oklab, var(--focus) 60%, transparent); outline-offset: 2px; }}
.auth-error {{
  border: 1px solid color-mix(in oklab, var(--danger) 28%, var(--danger-soft));
  border-radius: var(--radius);
  background: var(--danger-soft);
  color: var(--danger);
  padding: 10px 12px;
  font-weight: 650;
}}
.auth-submit {{
  border: 0;
  border-radius: var(--radius);
  background: var(--accent);
  color: white;
  padding: 11px 14px;
  font: inherit;
  font-weight: 750;
  cursor: pointer;
}}
.auth-submit:hover {{ filter: brightness(0.97); }}
.auth-meta {{ color: var(--ink-subtle); font-size: 12px; margin-top: 18px; text-align: center; }}
</style>
</head>
<body>
<div class="auth-shell">
  <main class="auth-card" aria-labelledby="auth-title">
    <div class="auth-brand">
      <div class="auth-brand-mark" aria-hidden="true">R</div>
      <div>
        <div class="auth-brand-title" id="auth-title">R-PhysGen-DB</div>
        <div class="auth-brand-subtitle">Internal database access</div>
      </div>
    </div>
    <form class="auth-form" method="post" action="/login">
      <label class="auth-field">Username
        <input class="auth-input" type="text" name="username" autocomplete="username" required autofocus/>
      </label>
      <label class="auth-field">Password
        <input class="auth-input" type="password" name="password" autocomplete="current-password" required/>
      </label>
      {error_html}
      <button class="auth-submit" type="submit">Log in</button>
    </form>
    <div class="auth-meta">Authorized internal users only.</div>
  </main>
</div>
</body>
</html>
"""


class LANAuthRequestHandler(SimpleHTTPRequestHandler):
    auth_config: Mapping[str, Any]
    sessions: SessionStore
    auth_directory: Path
    server_version = "RPhysGenLANAuth/1.0"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, directory=str(self.auth_directory), **kwargs)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - stdlib API name
        # Keep tests and LAN console output quiet except for explicit startup/errors.
        return

    def end_headers(self) -> None:
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html") or path.endswith(".html"):
            self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self) -> None:  # noqa: N802 - stdlib API name
        self._handle_get(head_only=False)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib API name
        self._handle_get(head_only=True)

    def do_POST(self) -> None:  # noqa: N802 - stdlib API name
        path = urllib.parse.urlparse(self.path).path
        if path == "/login":
            self._handle_login()
            return
        if path == "/logout":
            self._handle_logout()
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def _handle_get(self, *, head_only: bool) -> None:
        path = urllib.parse.urlparse(self.path).path
        if self._is_sensitive_path(path):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return
        if path in ("/", "/index.html"):
            if self._is_authenticated():
                self.path = "/index.html"
                return super().do_HEAD() if head_only else super().do_GET()
            self._send_login_page(head_only=head_only)
            return
        if path == "/login":
            self._send_login_page(head_only=head_only)
            return
        if not self._is_authenticated():
            self._send_text(
                HTTPStatus.UNAUTHORIZED,
                "Authentication required",
                content_type="text/plain; charset=utf-8",
                head_only=head_only,
            )
            return
        return super().do_HEAD() if head_only else super().do_GET()

    def _handle_login(self) -> None:
        fields = self._read_form_fields()
        username = fields.get("username", "")
        password = fields.get("password", "")
        config = _coerce_config(self.auth_config)
        if username == config.username and verify_password(password, config):
            token = self.sessions.create(config.username)
            self._redirect("/", cookie=self._session_cookie(token))
            return
        self._send_login_page(
            status=HTTPStatus.UNAUTHORIZED,
            error="Invalid username or password",
        )

    def _handle_logout(self) -> None:
        self.sessions.clear(self._session_token())
        self._redirect("/", cookie=self._clear_session_cookie())

    def _read_form_fields(self) -> dict[str, str]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length > 64 * 1024:
            self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Login form is too large")
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        parsed = urllib.parse.parse_qs(raw, keep_blank_values=True)
        return {key: values[0] if values else "" for key, values in parsed.items()}

    def _is_authenticated(self) -> bool:
        return self.sessions.validate(self._session_token())

    def _session_token(self) -> str | None:
        cookie_header = self.headers.get("Cookie", "")
        for part in cookie_header.split(";"):
            name, sep, value = part.strip().partition("=")
            if sep and name == SESSION_COOKIE:
                return value
        return None

    def _send_login_page(
        self,
        *,
        status: HTTPStatus = HTTPStatus.OK,
        error: str | None = None,
        head_only: bool = False,
    ) -> None:
        error_html = ""
        if error:
            error_html = f'<div class="auth-error" role="alert">{html.escape(error)}</div>'
        body = LOGIN_PAGE_TEMPLATE.format(error_html=error_html).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_text(
        self,
        status: HTTPStatus,
        body: str,
        *,
        content_type: str,
        head_only: bool = False,
    ) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if not head_only:
            self.wfile.write(data)

    def _redirect(self, location: str, *, cookie: str) -> None:
        body = b""
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", location)
        self.send_header("Set-Cookie", cookie)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _session_cookie(self, token: str) -> str:
        return f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax"

    @staticmethod
    def _clear_session_cookie() -> str:
        return f"{SESSION_COOKIE}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"

    @staticmethod
    def _is_sensitive_path(path: str) -> bool:
        decoded_path = urllib.parse.unquote(path)
        parts = [part for part in PurePosixPath(decoded_path).parts if part not in ("/", "")]
        return any(part.startswith(".") for part in parts) or any(
            part == "__pycache__" or part.endswith(".py") for part in parts
        )


def make_handler(
    *,
    directory: Path | str = DEFAULT_DIRECTORY,
    auth_config: Mapping[str, Any] | AuthConfig,
    session_seconds: int = DEFAULT_SESSION_SECONDS,
) -> type[LANAuthRequestHandler]:
    parsed_config = _coerce_config(auth_config).as_dict()
    auth_directory = Path(directory).resolve()
    sessions = SessionStore(session_seconds)

    class ConfiguredLANAuthRequestHandler(LANAuthRequestHandler):
        pass

    ConfiguredLANAuthRequestHandler.auth_config = parsed_config
    ConfiguredLANAuthRequestHandler.auth_directory = auth_directory
    ConfiguredLANAuthRequestHandler.sessions = sessions
    return ConfiguredLANAuthRequestHandler


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve R-PhysGen-DB on a LAN behind local password auth.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"bind host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"bind port (default: {DEFAULT_PORT})")
    parser.add_argument(
        "--auth-file",
        type=Path,
        default=DEFAULT_AUTH_PATH,
        help=f"auth config JSON path (default: {DEFAULT_AUTH_PATH})",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        default=DEFAULT_DIRECTORY,
        help=f"static file directory (default: {DEFAULT_DIRECTORY})",
    )
    parser.add_argument(
        "--session-seconds",
        type=int,
        default=DEFAULT_SESSION_SECONDS,
        help="session lifetime in seconds (default: 43200)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        config = load_auth_config(args.auth_file)
    except AuthConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    handler = make_handler(
        directory=args.directory,
        auth_config=config,
        session_seconds=args.session_seconds,
    )
    with ThreadingHTTPServer((args.host, args.port), handler) as httpd:
        print(
            f"Serving authenticated R-PhysGen-DB at http://{args.host}:{args.port}/ "
            f"from {Path(args.directory).resolve()}"
        )
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
