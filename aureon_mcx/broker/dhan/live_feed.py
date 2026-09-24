"""DhanHQ v2 live market feed (WebSocket) provider.

* subscribes only to the resolved security ids
* bounded reconnect backoff, ping handling, re-subscription after reconnect
* rollover: `replace_subscription(old, new)` unsubscribes old / subscribes new
* emits Tick objects only; candle building happens in market.candle_builder

Binary packet layouts (little-endian) per the DhanHQ v2 "Live Market Feed" spec.
Absolute byte offsets, header included:

  header (8)      : 0 response code B | 1 message length H | 3 exchange segment B | 4 security id I
  ticker  (16)    : 8 LTP f | 12 LTT I
  quote   (50)    : 8 LTP f | 12 LTQ h | 14 LTT I | 18 ATP f | 22 volume I | 26 total sell qty I |
                    30 total buy qty I | 34 open f | 38 close f | 42 high f | 46 low f
  oi      (12)    : 8 OI I
  prev cl (16)    : 8 prev close f | 12 prev OI I
  status  (8)     : header only
  full    (162)   : 8 LTP f | 12 LTQ h | 14 LTT I | 18 ATP f | 22 volume I | 26 total sell qty I |
                    30 total buy qty I | 34 OI I | 38 OI day high I | 42 OI day low I | 46 open f |
                    50 close f | 54 high f | 58 low f | 62.. 5 depth levels x 20 bytes
                    (bid qty I, ask qty I, bid orders h, ask orders h, bid price f, ask price f)
  disconn (10)    : 8 reason h

The Quote and Full layouts share only their first 34 bytes; Full inserts the three OI
fields BEFORE the day OHLC, so the two are parsed with separate structs.
"""
from __future__ import annotations

import asyncio
import json
import logging
import struct
from dataclasses import dataclass, field
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

HEADER = struct.Struct("<BHBI")                     # 8 bytes
TICKER = struct.Struct("<fI")                       # 8 bytes  -> 16 total
QUOTE = struct.Struct("<fhIfIIIffff")               # 42 bytes -> 50 total
OI = struct.Struct("<I")                            # 4 bytes  -> 12 total
PREV_CLOSE = struct.Struct("<fI")                   # 8 bytes  -> 16 total
FULL_HEAD = struct.Struct("<fhIfIIIIIIffff")        # 54 bytes -> 62, then depth
DEPTH_LEVEL = struct.Struct("<IIhhff")              # 20 bytes x 5 -> 162 total
DISCONNECT = struct.Struct("<h")                    # 2 bytes  -> 10 total

PACKET_SIZES = {CODE_TICKER: 16, CODE_QUOTE: 50, CODE_OI: 12, CODE_PREV_CLOSE: 16, CODE_MARKET_STATUS: 8,
                CODE_FULL: 8 + FULL_HEAD.size + 5 * DEPTH_LEVEL.size, CODE_DISCONNECT: 10}


@dataclass(frozen=True)
class DepthLevel:
    bid_qty: int
    ask_qty: int
    bid_orders: int
    ask_orders: int
    bid_price: float
    ask_price: float


@dataclass(frozen=True)
class FeedPacket:
    code: int
    security_id: str
    exchange_segment: int
    ltp: float | None = None
    ltt: int | None = None
    last_qty: float | None = None
    atp: float | None = None
    volume: float | None = None
    total_sell_qty: int | None = None
    total_buy_qty: int | None = None
    day_open: float | None = None
    day_close: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    open_interest: float | None = None
    oi_day_high: float | None = None
    oi_day_low: float | None = None
    prev_close: float | None = None
    prev_open_interest: float | None = None
    depth: tuple[DepthLevel, ...] = field(default_factory=tuple)
    disconnect_reason: int | None = None


def parse_packet(data: bytes) -> FeedPacket | None:
    """Parse one binary feed packet. Returns None for truncated / unknown packets."""
    if len(data) < HEADER.size:
        return None
    code, declared, segment, sec_id = HEADER.unpack_from(data, 0)
    sid = str(sec_id)
    required = PACKET_SIZES.get(code)
    if required is not None and declared != required:
        # The header's message length must agree with the layout the response code implies.
        # A mismatch means a layout change (or corruption): decoding the wrong offsets would
        # produce plausible-looking but wrong prices, so the packet is rejected instead.
        log.warning("feed_packet_length_mismatch %s", kv(code=code, security_id=sid, declared=declared, required=required, length=len(data)))
        return None
    if declared < HEADER.size or len(data) < declared:
        log.warning("feed_packet_truncated %s", kv(code=code, security_id=sid, length=len(data), declared=declared))
        return None
    if code == CODE_TICKER:
        ltp, ltt = TICKER.unpack_from(data, 8)
        return FeedPacket(code, sid, segment, ltp=ltp, ltt=ltt)
    if code == CODE_QUOTE:
        ltp, ltq, ltt, atp, volume, tsq, tbq, o, c, h, l = QUOTE.unpack_from(data, 8)
        return FeedPacket(code, sid, segment, ltp=ltp, ltt=ltt, last_qty=float(ltq), atp=atp, volume=float(volume), total_sell_qty=tsq,
                          total_buy_qty=tbq, day_open=o, day_close=c, day_high=h, day_low=l)
    if code == CODE_FULL:
        ltp, ltq, ltt, atp, volume, tsq, tbq, oi, oi_hi, oi_lo, o, c, h, l = FULL_HEAD.unpack_from(data, 8)
        depth = tuple(DepthLevel(*DEPTH_LEVEL.unpack_from(data, 8 + FULL_HEAD.size + i * DEPTH_LEVEL.size)) for i in range(5))
        return FeedPacket(code, sid, segment, ltp=ltp, ltt=ltt, last_qty=float(ltq), atp=atp, volume=float(volume), total_sell_qty=tsq,
                          total_buy_qty=tbq, day_open=o, day_close=c, day_high=h, day_low=l, open_interest=float(oi) if oi else None,
                          oi_day_high=float(oi_hi), oi_day_low=float(oi_lo), depth=depth)
    if code == CODE_OI:
        (oi,) = OI.unpack_from(data, 8)
        return FeedPacket(code, sid, segment, open_interest=float(oi))
    if code == CODE_PREV_CLOSE:
        prev_close, prev_oi = PREV_CLOSE.unpack_from(data, 8)
        return FeedPacket(code, sid, segment, prev_close=prev_close, prev_open_interest=float(prev_oi))
    if code == CODE_DISCONNECT:
        (reason,) = DISCONNECT.unpack_from(data, 8)
        return FeedPacket(code, sid, segment, disconnect_reason=reason)
    if code == CODE_MARKET_STATUS:
        return FeedPacket(code, sid, segment)
    log.debug("feed_packet_unknown %s", kv(code=code, security_id=sid, length=len(data)))
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
