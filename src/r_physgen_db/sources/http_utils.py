"""Shared HTTP helpers for resilient public snapshot acquisition."""

from __future__ import annotations

from collections.abc import Iterable

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError,
    ContentDecodingError,
    HTTPError,
    InvalidURL,
    ProxyError,
    RequestException,
    SSLError,
    Timeout,
    TooManyRedirects,
)
from urllib3.util.retry import Retry

TRANSIENT_HTTP_STATUS_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})
DEFAULT_ALLOWED_METHODS = frozenset({"GET"})


def build_retry_session(
    *,
    user_agent: str = "R-PhysGen-DB/2.0",
    total_retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: Iterable[int] = TRANSIENT_HTTP_STATUS_CODES,
) -> Session:
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        status=total_retries,
        allowed_methods=DEFAULT_ALLOWED_METHODS,
        backoff_factor=backoff_factor,
        status_forcelist=frozenset(status_forcelist),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session


def is_transient_request_exception(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        status_code = exc.response.status_code if exc.response is not None else None
        return status_code in TRANSIENT_HTTP_STATUS_CODES

    if isinstance(exc, (InvalidURL, TooManyRedirects)):
        return False

    return isinstance(
        exc,
        (
            Timeout,
            ConnectionError,
            SSLError,
            ProxyError,
            ChunkedEncodingError,
            ContentDecodingError,
            RequestException,
        ),
    )
