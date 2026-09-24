from __future__ import annotations


class DhanError(RuntimeError):
    """Base class for broker-layer failures."""


class DhanApiError(DhanError):
    def __init__(self, endpoint: str, status: int | None, detail: str = "", context: dict | None = None):
        self.endpoint = endpoint
        self.status = status
        self.detail = detail
        self.context = context or {}
        ctx = " ".join(f"{k}={v}" for k, v in self.context.items())
        super().__init__(f"dhan api failure endpoint={endpoint} status={status} {ctx} detail={detail}".strip())


class DhanCredentialsError(DhanError):
    pass


class SymbolResolutionError(DhanError):
    """Resolution is ambiguous or impossible. The observer for that symbol must not start."""

    def __init__(self, logical_symbol: str, reason: str, candidates: list | None = None):
        self.logical_symbol = logical_symbol
        self.reason = reason
        self.candidates = candidates or []
        super().__init__(f"symbol resolution failed logical={logical_symbol} reason={reason} candidates={len(self.candidates)}")
