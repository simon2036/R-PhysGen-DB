#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import json
import os
import stat
import sys
from pathlib import Path

from serve_auth import DEFAULT_AUTH_PATH, DEFAULT_ITERATIONS, build_auth_config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create the local private LAN auth config for R-PhysGen-DB.")
    parser.add_argument("--username", help="internal username; prompts when omitted")
    parser.add_argument(
        "--auth-file",
        type=Path,
        default=DEFAULT_AUTH_PATH,
        help=f"output auth config path (default: {DEFAULT_AUTH_PATH})",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=DEFAULT_ITERATIONS,
        help=f"PBKDF2 iterations (default: {DEFAULT_ITERATIONS})",
    )
    parser.add_argument("--force", action="store_true", help="replace an existing auth config")
    return parser.parse_args(argv)


def prompt_username(existing: str | None) -> str:
    username = (existing or input("Username: ")).strip()
    if not username:
        raise ValueError("username must be non-empty")
    return username


def prompt_password() -> str:
    password = getpass.getpass("Password: ")
    confirm = getpass.getpass("Confirm password: ")
    if password != confirm:
        raise ValueError("passwords did not match")
    if not password:
        raise ValueError("password must be non-empty")
    return password


def write_auth_config(path: Path, config: dict[str, str | int], *, force: bool = False) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists; pass --force to replace it")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        # Best-effort on filesystems that do not support POSIX mode changes.
        pass
    os.replace(tmp_path, path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        username = prompt_username(args.username)
        password = prompt_password()
        config = build_auth_config(username, password, iterations=args.iterations)
        write_auth_config(args.auth_file, config, force=args.force)
    except (FileExistsError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    print(f"Wrote private auth config: {args.auth_file}")
    print("Start the LAN service with: python deploy/lan/serve_auth.py --host 0.0.0.0 --port 8088")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
