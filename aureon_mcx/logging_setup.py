"""Structured key=value logging with credential redaction.

The RedactingFilter rewrites any log record that contains a registered secret
(or a JWT-looking token) before it reaches a handler. Credentials therefore
cannot leak through logging even if some code path formats them by mistake.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Iterable

_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}")
_HEADER_RE = re.compile(r"(?i)(access[-_ ]?token|client[-_ ]?id|authorization)(['\"]?\s*[:=]\s*['\"]?)([^'\",\s}]+)")

REDACTED = "[REDACTED]"


class RedactingFilter(logging.Filter):
    _secrets: set[str] = set()

    @classmethod
    def register(cls, secrets: Iterable[str | None]) -> None:
        for s in secrets:
            if s and len(s) >= 4:
                cls._secrets.add(s)

    @classmethod
    def clear(cls) -> None:
        cls._secrets.clear()

    @classmethod
    def redact(cls, text: str) -> str:
        for s in cls._secrets:
            if s in text:
                text = text.replace(s, REDACTED)
        text = _JWT_RE.sub(REDACTED, text)
        text = _HEADER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{REDACTED}", text)
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # pragma: no cover - defensive
            return True
        redacted = self.redact(msg)
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        return True


def kv(**fields: Any) -> str:
    """Render fields as `key=value` pairs (values with spaces are quoted)."""
    parts = []
    for k, v in fields.items():
        if v is None:
            continue
        s = str(v)
        if " " in s or "=" in s:
            s = f'"{s}"'
        parts.append(f"{k}={s}")
    return " ".join(parts)


def configure_logging(level: str = "INFO", secrets: Iterable[str | None] = ()) -> None:
    RedactingFilter.register(secrets)
    root = logging.getLogger()
    root.setLevel(level.upper())
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s level=%(levelname)s logger=%(name)s %(message)s"))
        root.addHandler(handler)
    for h in root.handlers:
        if not any(isinstance(f, RedactingFilter) for f in h.filters):
            h.addFilter(RedactingFilter())
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("discord").setLevel(logging.WARNING)
