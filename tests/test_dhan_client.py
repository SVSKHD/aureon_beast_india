from __future__ import annotations

import logging

import httpx
import pytest

from aureon_mcx.broker.dhan.client import DhanHttpClient, RateLimiter
from aureon_mcx.broker.dhan.errors import DhanApiError, DhanCredentialsError
from aureon_mcx.logging_setup import RedactingFilter, configure_logging, kv


def test_rate_limiter_blocks_after_burst():
    clock = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        clock[0] += s

    rl = RateLimiter(per_second=2, per_minute=100, sleep=sleep, clock=lambda: clock[0])
    rl.acquire(); rl.acquire()
    rl.acquire()  # third call within the same second must wait
    assert slept and sum(slept) >= 1.0 - 1e-9


def test_retry_then_success_and_no_token_in_error(caplog):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        assert request.headers["access-token"] == "tok-SECRET-123"
        if calls["n"] < 3:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json={"ok": True})

    slept = []
    c = DhanHttpClient("cid", "tok-SECRET-123", transport=httpx.MockTransport(handler), sleep=slept.append,
                       limiter=RateLimiter(per_second=100, per_minute=1000, sleep=lambda s: None))
    assert c.post_json("/charts/intraday", {"a": 1}) == {"ok": True}
    assert calls["n"] == 3 and slept == [0.5, 1.0]

    def bad(request):
        return httpx.Response(400, text="bad request")

    c2 = DhanHttpClient("cid", "tok-SECRET-123", transport=httpx.MockTransport(bad), sleep=slept.append,
                        limiter=RateLimiter(per_second=100, per_minute=1000, sleep=lambda s: None))
    with pytest.raises(DhanApiError) as exc:
        c2.post_json("/charts/intraday", {"securityId": "1"}, context={"security_id": "1"})
    assert exc.value.status == 400 and "security_id=1" in str(exc.value)
    assert "tok-SECRET-123" not in str(exc.value)


def test_missing_credentials_fail_closed():
    c = DhanHttpClient(None, None)
    with pytest.raises(DhanCredentialsError):
        c.get_json("/profile")


def test_redacting_filter_hides_tokens(caplog):
    configure_logging("INFO", secrets=["tok-SECRET-123"])
    logger = logging.getLogger("aureon.test")
    caplog.set_level(logging.INFO)
    logger.info("token is tok-SECRET-123 and jwt eyJabcdefghij.eyJabcdefghij.abcdefghijkl")
    logger.info("headers %s", {"access-token": "another-secret"})
    for rec in caplog.records:
        assert "tok-SECRET-123" not in RedactingFilter.redact(rec.getMessage())
    assert RedactingFilter.redact("access-token: abc123xyz") == "access-token: [REDACTED]"
    assert "eyJabcdefghij" not in RedactingFilter.redact("eyJabcdefghij.eyJabcdefghij.abcdefghijkl")
    assert kv(a=1, b="two words", c=None) == 'a=1 b="two words"'
