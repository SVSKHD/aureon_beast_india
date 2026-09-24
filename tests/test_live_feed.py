from __future__ import annotations

import asyncio
import json
import struct

import pytest

from aureon_mcx.broker.dhan.live_feed import (
    CODE_DISCONNECT, CODE_FULL, CODE_MARKET_STATUS, CODE_OI, CODE_PREV_CLOSE, CODE_QUOTE, CODE_TICKER, DhanLiveFeedProvider,
    build_subscribe_message, packet_to_tick, parse_packet,
)
from aureon_mcx.market.timeutil import from_epoch


def _header(code, length, segment, sec_id):
    return struct.pack("<BHBI", code, length, segment, sec_id)


def ticker_packet(sec_id, ltp, ltt):
    return _header(CODE_TICKER, 16, 5, sec_id) + struct.pack("<fI", ltp, ltt)


def quote_packet(sec_id, ltp, ltq, ltt, atp, volume, tsq, tbq, o, c, h, l):
    # official quote layout: 8 LTP | 12 LTQ | 14 LTT | 18 ATP | 22 vol | 26 sell | 30 buy | 34 O | 38 C | 42 H | 46 L  (50 bytes)
    return _header(CODE_QUOTE, 50, 5, sec_id) + struct.pack("<fhIfIIIffff", ltp, ltq, ltt, atp, volume, tsq, tbq, o, c, h, l)


def full_packet(sec_id, ltp, ltq, ltt, atp, volume, tsq, tbq, oi, oi_hi, oi_lo, o, c, h, l, depth):
    # official full layout: 8 LTP | 12 LTQ | 14 LTT | 18 ATP | 22 vol | 26 sell | 30 buy | 34 OI | 38 OI hi | 42 OI lo |
    # 46 O | 50 C | 54 H | 58 L | 62.. 5 x (bid qty I, ask qty I, bid orders h, ask orders h, bid px f, ask px f)   (162 bytes)
    body = struct.pack("<fhIfIIIIIIffff", ltp, ltq, ltt, atp, volume, tsq, tbq, oi, oi_hi, oi_lo, o, c, h, l)
    for bq, aq, bo, ao, bp, ap in depth:
        body += struct.pack("<IIhhff", bq, aq, bo, ao, bp, ap)
    assert len(body) == 154
    return _header(CODE_FULL, 162, 5, sec_id) + body


def test_parse_ticker_quote_full_oi_prev_close_disconnect_with_official_offsets():
    p = parse_packet(ticker_packet(428291, 70125.0, 1789962600))
    assert p.code == CODE_TICKER and p.security_id == "428291" and p.exchange_segment == 5 and p.ltp == 70125.0 and p.ltt == 1789962600
    tick = packet_to_tick(p, {})
    assert tick.price == 70125.0 and tick.ts == from_epoch(1789962600) and tick.last_qty is None

    q = parse_packet(quote_packet(429001, 84000.0, 3, 1789962605, 83990.0, 1200, 50, 60, 83900.0, 83800.0, 84100.0, 83700.0))
    assert (q.ltp, q.last_qty, q.ltt, q.atp, q.volume, q.total_sell_qty, q.total_buy_qty) == (84000.0, 3, 1789962605, 83990.0, 1200, 50, 60)
    assert (q.day_open, q.day_close, q.day_high, q.day_low) == (83900.0, 83800.0, 84100.0, 83700.0)
    assert q.open_interest is None

    depth = [(10, 11, 1, 2, 83999.0, 84001.0), (20, 21, 3, 4, 83998.0, 84002.0), (30, 31, 5, 6, 83997.0, 84003.0),
             (40, 41, 7, 8, 83996.0, 84004.0), (50, 51, 9, 10, 83995.0, 84005.0)]
    raw = full_packet(429001, 84000.0, 3, 1789962605, 83990.0, 1200, 50, 60, 15055, 15100, 15000, 83900.0, 83800.0, 84100.0, 83700.0, depth)
    assert len(raw) == 162
    # sanity-check the documented absolute offsets on the raw bytes
    assert struct.unpack_from("<I", raw, 34)[0] == 15055 and struct.unpack_from("<f", raw, 46)[0] == 83900.0
    f = parse_packet(raw)
    assert f.code == CODE_FULL and f.ltp == 84000.0 and f.last_qty == 3 and f.ltt == 1789962605 and f.atp == 83990.0
    assert f.volume == 1200 and f.total_sell_qty == 50 and f.total_buy_qty == 60
    assert f.open_interest == 15055 and f.oi_day_high == 15100 and f.oi_day_low == 15000
    assert (f.day_open, f.day_close, f.day_high, f.day_low) == (83900.0, 83800.0, 84100.0, 83700.0)
    assert len(f.depth) == 5 and f.depth[0].bid_qty == 10 and f.depth[4].ask_price == 84005.0 and f.depth[2].bid_orders == 5
    t = packet_to_tick(f, {})
    assert t.open_interest == 15055 and t.day_volume == 1200 and t.last_qty == 3

    oi = parse_packet(_header(CODE_OI, 12, 5, 429001) + struct.pack("<I", 15060))
    assert oi.open_interest == 15060
    cache = {}
    assert packet_to_tick(oi, cache) is None and cache["429001"] == 15060
    pc = parse_packet(_header(CODE_PREV_CLOSE, 16, 5, 429001) + struct.pack("<fI", 83850.0, 14900))
    assert pc.prev_close == 83850.0 and pc.prev_open_interest == 14900
    d = parse_packet(_header(CODE_DISCONNECT, 10, 5, 1) + struct.pack("<h", 805))
    assert d.disconnect_reason == 805
    assert parse_packet(_header(CODE_MARKET_STATUS, 8, 5, 1)).code == CODE_MARKET_STATUS


def test_truncated_packets_are_rejected():
    assert parse_packet(b"\x00") is None
    assert parse_packet(ticker_packet(1, 1.0, 1)[:12]) is None
    full = full_packet(1, 1.0, 1, 1, 1.0, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1.0, [(0, 0, 0, 0, 0.0, 0.0)] * 5)
    assert parse_packet(full[:100]) is None
    # a quote-sized buffer must never be parsed with the full layout
    assert parse_packet(_header(CODE_FULL, 162, 5, 1) + b"\x00" * 42) is None


def test_subscribe_messages_chunk_and_codes():
    msgs = build_subscribe_message(["428291", "429001"], "MCX_COMM", "quote")
    assert msgs == [{"RequestCode": 17, "InstrumentCount": 2, "InstrumentList": [
        {"ExchangeSegment": "MCX_COMM", "SecurityId": "428291"}, {"ExchangeSegment": "MCX_COMM", "SecurityId": "429001"}]}]
    assert build_subscribe_message(["1"], "MCX_COMM", "ticker", unsubscribe=True)[0]["RequestCode"] == 16
    many = build_subscribe_message([str(i) for i in range(250)], "MCX_COMM")
    assert [m["InstrumentCount"] for m in many] == [100, 100, 50]


class FakeWs:
    def __init__(self, frames, fail_after=None):
        self.frames = list(frames)
        self.sent = []
        self.closed = False
        self.fail_after = fail_after

    async def send(self, msg):
        self.sent.append(json.loads(msg))

    async def recv(self):
        if self.frames:
            return self.frames.pop(0)
        raise ConnectionError("socket closed")

    async def close(self):
        self.closed = True


def test_reconnect_resubscribes_and_dispatches_ticks():
    ticks = []
    sockets = []
    frames1 = [ticker_packet(428291, 70125.0, 1789962600)]
    frames2 = [ticker_packet(428291, 70130.0, 1789962660), ticker_packet(999999, 1.0, 1789962660)]  # unsubscribed id ignored

    async def connector(url):
        assert "token=tok" in url and "clientId=cid" in url
        ws = FakeWs([frames1, frames2, []][min(len(sockets), 2)])
        sockets.append(ws)
        return ws

    statuses = []
    feed = DhanLiveFeedProvider("cid", "tok", "MCX_COMM", ticks.append, lambda s, d: statuses.append(s), backoff=(0.01, 0.01),
                                connector=connector)

    async def main():
        stop = asyncio.Event()
        await feed.subscribe(["428291"])
        task = asyncio.create_task(feed.run(stop))
        for _ in range(200):
            await asyncio.sleep(0.005)
            if len(sockets) >= 3:
                break
        stop.set()
        await task

    asyncio.run(main())
    assert [t.price for t in ticks] == [70125.0, 70130.0]
    assert sockets[0].sent[0]["RequestCode"] == 17 and sockets[1].sent[0]["InstrumentList"][0]["SecurityId"] == "428291"
    assert feed.reconnects >= 2 and "reconnecting" in statuses and statuses[-1] == "stopped"
    assert all(s.closed for s in sockets)


def test_replace_subscription_on_rollover():
    sent = []

    class Ws:
        async def send(self, m):
            sent.append(json.loads(m))

    feed = DhanLiveFeedProvider("cid", "tok", "MCX_COMM", lambda t: None)
    feed._ws = Ws()
    asyncio.run(feed.replace_subscription("428291", "431102"))
    assert [m["RequestCode"] for m in sent] == [18, 17]
    assert sent[0]["InstrumentList"][0]["SecurityId"] == "428291" and sent[1]["InstrumentList"][0]["SecurityId"] == "431102"
    assert feed._subscribed == {"431102"}


def test_header_declared_length_must_match_response_type():
    # each known response code implies one layout; a header that declares another length is rejected
    assert parse_packet(_header(CODE_TICKER, 50, 5, 1) + struct.pack("<fI", 1.0, 1) + b"\x00" * 34) is None
    assert parse_packet(_header(CODE_QUOTE, 16, 5, 1) + struct.pack("<fI", 1.0, 1)) is None
    full = full_packet(1, 1.0, 1, 1, 1.0, 1, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0, 1.0, [(0, 0, 0, 0, 0.0, 0.0)] * 5)
    assert parse_packet(_header(CODE_FULL, 50, 5, 1) + full[8:]) is None
    assert parse_packet(_header(CODE_OI, 16, 5, 1) + struct.pack("<fI", 1.0, 1)) is None  # OI packet is 12, not 16
    assert parse_packet(_header(CODE_MARKET_STATUS, 16, 5, 1) + b"\x00" * 8) is None
    # the declared length must also fit inside the buffer, and never be shorter than the header
    assert parse_packet(_header(CODE_TICKER, 16, 5, 1) + struct.pack("<f", 1.0)) is None
    assert parse_packet(_header(99, 4, 5, 1)) is None
    assert parse_packet(_header(99, 40, 5, 1) + b"\x00" * 8) is None
    # well-formed packets still parse, and trailing bytes after a correct declared length are tolerated
    assert parse_packet(ticker_packet(428291, 70125.0, 1789962600)).ltp == pytest.approx(70125.0)
    assert parse_packet(ticker_packet(428291, 70125.0, 1789962600) + b"\x00" * 4).ltt == 1789962600
    unknown = parse_packet(_header(99, 12, 5, 7) + b"\x00" * 4)
    assert unknown is not None and unknown.code == 99 and unknown.security_id == "7"
