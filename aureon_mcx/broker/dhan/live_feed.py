"""DhanHQ v2 live market feed (WebSocket) provider.

* subscribes only to the resolved security ids
* bounded reconnect backoff, ping handling, re-subscription after reconnect
* rollover: `replace_subscription(old, new)` unsubscribes old / subscribes new
* emits Tick objects only; candle building happens in market.candle_builder

Binary packet layout (little-endian) per Dhan v2 docs:
  header  : <B H B I>  response_code, message_length, exchange_segment, security_id
  ticker  : <f I>      ltp, ltt                          (code 2)
  quote   : <f h I f I I I f f f f>                      (code 4)
  oi      : <I>        open_interest                     (code 5)
  prev cls: <f I>      prev_close, prev_oi               (code 6)
  full    : quote + oi + ... (code 8) - parsed for ltp/ltt/volume/oi
  disconn : <h>        reason code                       (code 50)
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol

from aureon_mcx.logging_setup import kv
from aureon_mcx.market.candle_builder import Tick
from aureon_mcx.market.timeutil import from_epoch

log = logging.getLogger("aureon.dhan.live")

DHAN_FEED_URL = "wss://api-feed.dhan.co"

EXCHANGE_SEGMENT_CODES = {"IDX_I": 0, "NSE_EQ": 1, "NSE_FNO": 2, "NSE_CURRENCY": 3, "BSE_EQ": 4, "MCX_COMM": 5, "BSE_CURRENCY": 7, "BSE_FNO": 8}

REQ_SUBSCRIBE = {"ticker": 15, "quote": 17, "full": 21}
REQ_UNSUBSCRIBE = {"ticker": 16, "quote": 18, "full": 22}
REQ_DISCONNECT = 12
MAX_INSTRUMENTS_PER_REQUEST = 100

CODE_TICKER, CODE_QUOTE, CODE_OI, CODE_PREV_CLOSE, CODE_MARKET_STATUS, CODE_FULL, CODE_DISCONNECT = 2, 4, 5, 6, 7, 8, 50


@dataclass(frozen=True)
class FeedPacket:
    code: int
    security_id: str
    exchange_segment: int
    ltp: float | None = None
    ltt: int | None = None
    last_qty: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    disconnect_reason: int | None = None


def parse_packet(data: bytes) -> FeedPacket | None:
    if len(data) < 8:
        return None
    code, _length, segment, sec_id = struct.unpack_from("<BHBI", data, 0)
    body = data[8:]
    sid = str(sec_id)
    if code == CODE_TICKER and len(body) >= 8:
        ltp, ltt = struct.unpack_from("<fI", body, 0)
        return FeedPacket(code, sid, segment, ltp=ltp, ltt=ltt)
    if code in (CODE_QUOTE, CODE_FULL) and len(body) >= 42:
        ltp, ltq, ltt, _atp, volume, _tsq, _tbq, _o, _c, _h, _l = struct.unpack_from("<fhIfIIIffff", body, 0)
        oi = None
        if code == CODE_FULL and len(body) >= 46:
            (oi,) = struct.unpack_from("<I", body, 42)
        return FeedPacket(code, sid, segment, ltp=ltp, ltt=ltt, last_qty=float(ltq), volume=float(volume), open_interest=float(oi) if oi else None)
    if code == CODE_OI and len(body) >= 4:
        (oi,) = struct.unpack_from("<I", body, 0)
        return FeedPacket(code, sid, segment, open_interest=float(oi))
    if code == CODE_PREV_CLOSE and len(body) >= 8:
        return FeedPacket(code, sid, segment)
    if code == CODE_DISCONNECT and len(body) >= 2:
        (reason,) = struct.unpack_from("<h", body, 0)
        return FeedPacket(code, sid, segment, disconnect_reason=reason)
    return FeedPacket(code, sid, segment)


def packet_to_tick(p: FeedPacket, oi_cache: dict[str, float]) -> Tick | None:
    if p.open_interest is not None:
        oi_cache[p.security_id] = p.open_interest
    if p.ltp is None or p.ltt is None or p.ltp <= 0:
        return None
    return Tick(security_id=p.security_id, price=float(p.ltp), ts=from_epoch(p.ltt), last_qty=p.last_qty,
                day_volume=p.volume, open_interest=oi_cache.get(p.security_id))


def build_subscribe_message(security_ids: list[str], exchange_segment: str, mode: str = "quote", unsubscribe: bool = False) -> list[dict]:
    code = (REQ_UNSUBSCRIBE if unsubscribe else REQ_SUBSCRIBE)[mode]
    msgs = []
    for i in range(0, len(security_ids), MAX_INSTRUMENTS_PER_REQUEST):
        chunk = security_ids[i: i + MAX_INSTRUMENTS_PER_REQUEST]
        msgs.append({"RequestCode": code, "InstrumentCount": len(chunk),
                     "InstrumentList": [{"ExchangeSegment": exchange_segment, "SecurityId": str(s)} for s in chunk]})
    return msgs


class LiveFeedProvider(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...
    async def subscribe(self, security_ids: list[str]) -> None: ...
    async def unsubscribe(self, security_ids: list[str]) -> None: ...


class DhanLiveFeedProvider:
    def __init__(self, client_id: str, access_token: str, exchange_segment: str, on_tick: Callable[[Tick], None],
                 on_status: Callable[[str, dict], None] | None = None, mode: str = "quote", url: str = DHAN_FEED_URL,
                 backoff: tuple[float, ...] = (1, 2, 4, 8, 16, 30), ping_interval: float = 20.0,
                 connector: Callable[[str], Awaitable] | None = None):
        self._client_id = client_id
        self._token = access_token
        self.exchange_segment = exchange_segment
        self.on_tick = on_tick
        self.on_status = on_status or (lambda *_: None)
        self.mode = mode
        self.url = url
        self.backoff = backoff
        self.ping_interval = ping_interval
        self._connector = connector
        self._ws = None
        self._subscribed: set[str] = set()
        self._oi_cache: dict[str, float] = {}
        self.connected = False
        self.reconnects = 0
        self.packets = 0

    # -- connection --------------------------------------------------------
    def _feed_url(self) -> str:
        return f"{self.url}?version=2&token={self._token}&clientId={self._client_id}&authType=2"

    async def _connect(self):
        if self._connector is not None:
            return await self._connector(self._feed_url())
        import websockets

        return await websockets.connect(self._feed_url(), ping_interval=self.ping_interval, ping_timeout=self.ping_interval, max_queue=4096)

    async def _send(self, msg: dict) -> None:
        if self._ws is None:
            return
        await self._ws.send(json.dumps(msg))

    async def subscribe(self, security_ids: list[str]) -> None:
        ids = [str(s) for s in security_ids]
        self._subscribed.update(ids)
        if self._ws is not None:
            for m in build_subscribe_message(ids, self.exchange_segment, self.mode):
                await self._send(m)
            log.info("feed_subscribe %s", kv(segment=self.exchange_segment, ids=",".join(ids), mode=self.mode))

    async def unsubscribe(self, security_ids: list[str]) -> None:
        ids = [str(s) for s in security_ids]
        for s in ids:
            self._subscribed.discard(s)
        if self._ws is not None:
            for m in build_subscribe_message(ids, self.exchange_segment, self.mode, unsubscribe=True):
                await self._send(m)
            log.info("feed_unsubscribe %s", kv(ids=",".join(ids)))

    async def replace_subscription(self, old_id: str | None, new_id: str) -> None:
        if old_id and old_id != new_id:
            await self.unsubscribe([old_id])
        await self.subscribe([new_id])

    async def _resubscribe(self) -> None:
        if self._subscribed:
            for m in build_subscribe_message(sorted(self._subscribed), self.exchange_segment, self.mode):
                await self._send(m)
            log.info("feed_resubscribed %s", kv(ids=",".join(sorted(self._subscribed))))

    # -- main loop ---------------------------------------------------------
    async def run(self, stop: asyncio.Event) -> None:
        attempt = 0
        while not stop.is_set():
            try:
                self._ws = await self._connect()
                self.connected = True
                attempt = 0
                self.on_status("connected", {"reconnects": self.reconnects})
                log.info("feed_connected %s", kv(reconnects=self.reconnects))
                await self._resubscribe()
                await self._recv_loop(stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - any transport failure triggers bounded backoff
                log.warning("feed_error %s", kv(error=type(exc).__name__, detail=str(exc)[:120]))
            finally:
                self.connected = False
                if self._ws is not None:
                    try:
                        await self._ws.close()
                    except Exception:  # noqa: BLE001
                        pass
                    self._ws = None
            if stop.is_set():
                break
            delay = self.backoff[min(attempt, len(self.backoff) - 1)]
            attempt += 1
            self.reconnects += 1
            self.on_status("reconnecting", {"delay": delay, "attempt": attempt})
            log.warning("feed_reconnect %s", kv(delay=delay, attempt=attempt))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
        self.on_status("stopped", {})

    async def _recv_loop(self, stop: asyncio.Event) -> None:
        assert self._ws is not None
        while not stop.is_set():
            data = await self._ws.recv()
            if isinstance(data, str):
                log.debug("feed_text %s", kv(text=data[:200]))
                continue
            self.packets += 1
            self.handle_binary(data)

    def handle_binary(self, data: bytes) -> Tick | None:
        p = parse_packet(data)
        if p is None:
            return None
        if p.code == CODE_DISCONNECT:
            log.warning("feed_disconnect_packet %s", kv(reason=p.disconnect_reason))
            raise ConnectionError(f"feed disconnect reason={p.disconnect_reason}")
        if p.security_id not in self._subscribed and self._subscribed:
            return None
        tick = packet_to_tick(p, self._oi_cache)
        if tick is not None:
            self.on_tick(tick)
        return tick

    async def disconnect(self) -> None:
        if self._ws is not None:
            try:
                await self._send({"RequestCode": REQ_DISCONNECT})
            except Exception:  # noqa: BLE001
                pass
