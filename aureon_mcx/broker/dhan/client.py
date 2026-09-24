"""Thin DhanHQ v2 HTTP client: auth headers, client-side rate limit, bounded retry.

Tokens are never logged. Errors carry endpoint/status/context only.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any, Callable

import httpx

from aureon_mcx.logging_setup import kv

from .errors import DhanApiError, DhanCredentialsError

log = logging.getLogger("aureon.dhan.http")

DHAN_API_BASE = "https://api.dhan.co/v2"


class RateLimiter:
    """Sliding-window limiter: at most `per_second` calls/s and `per_minute` calls/min."""

    def __init__(self, per_second: int = 4, per_minute: int = 90, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.per_second = per_second
        self.per_minute = per_minute
        self._sleep = sleep
        self._clock = clock
        self._sec: deque[float] = deque()
        self._min: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = self._clock()
                while self._sec and now - self._sec[0] >= 1.0:
                    self._sec.popleft()
                while self._min and now - self._min[0] >= 60.0:
                    self._min.popleft()
                if len(self._sec) < self.per_second and len(self._min) < self.per_minute:
                    self._sec.append(now)
                    self._min.append(now)
                    return
                wait = 0.05
                if self._sec and len(self._sec) >= self.per_second:
                    wait = max(wait, 1.0 - (now - self._sec[0]))
                if self._min and len(self._min) >= self.per_minute:
                    wait = max(wait, 60.0 - (now - self._min[0]))
            self._sleep(wait)


class DhanHttpClient:
    RETRY_STATUSES = {429, 500, 502, 503, 504}

    def __init__(self, client_id: str | None, access_token: str | None, base_url: str = DHAN_API_BASE,
                 timeout: float = 20.0, max_retries: int = 4, backoff_base: float = 0.5,
                 limiter: RateLimiter | None = None, transport: httpx.BaseTransport | None = None,
                 sleep: Callable[[float], None] = time.sleep):
        self._client_id = (client_id or "").strip()
        self._token = (access_token or "").strip()
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.limiter = limiter or RateLimiter()
        self._sleep = sleep
        self._http = httpx.Client(timeout=timeout, transport=transport, headers={"Accept": "application/json"})

    # -- credentials -------------------------------------------------------
    def require_credentials(self) -> None:
        if not self._client_id or not self._token:
            raise DhanCredentialsError("DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN missing from environment")

    def _auth_headers(self) -> dict[str, str]:
        self.require_credentials()
        return {"access-token": self._token, "client-id": self._client_id, "Content-Type": "application/json"}

    # -- requests ----------------------------------------------------------
    def post_json(self, path: str, body: dict[str, Any], context: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        return self._request("POST", url, json=body, headers=self._auth_headers(), context=context)

    def get_json(self, path: str, context: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        return self._request("GET", url, headers=self._auth_headers(), context=context)

    def get_text(self, url: str, context: dict[str, Any] | None = None) -> str:
        resp = self._request("GET", url, headers={}, context=context, raw=True)
        return resp.text

    def _request(self, method: str, url: str, headers: dict[str, str], json: Any = None,
                 context: dict[str, Any] | None = None, raw: bool = False) -> Any:
        endpoint = url.replace(self.base_url, "") or url
        ctx = dict(context or {})
        attempt = 0
        while True:
            attempt += 1
            self.limiter.acquire()
            try:
                resp = self._http.request(method, url, headers=headers, json=json)
            except httpx.TransportError as exc:
                if attempt > self.max_retries:
                    raise DhanApiError(endpoint, None, f"transport error after {attempt - 1} retries: {type(exc).__name__}", ctx) from exc
                delay = self.backoff_base * (2 ** (attempt - 1))
                log.warning("dhan_transport_retry %s", kv(endpoint=endpoint, attempt=attempt, delay=delay, error=type(exc).__name__, **ctx))
                self._sleep(delay)
                continue
            if resp.status_code in self.RETRY_STATUSES and attempt <= self.max_retries:
                delay = self.backoff_base * (2 ** (attempt - 1))
                retry_after = resp.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = max(delay, float(retry_after))
                log.warning("dhan_http_retry %s", kv(endpoint=endpoint, status=resp.status_code, attempt=attempt, delay=delay, **ctx))
                self._sleep(delay)
                continue
            if resp.status_code >= 400:
                detail = resp.text[:300].replace("\n", " ")
                log.error("dhan_http_error %s", kv(endpoint=endpoint, status=resp.status_code, **ctx))
                raise DhanApiError(endpoint, resp.status_code, detail, ctx)
            if raw:
                return resp
            try:
                return resp.json()
            except ValueError as exc:
                raise DhanApiError(endpoint, resp.status_code, "invalid JSON body", ctx) from exc

    def close(self) -> None:
        self._http.close()
