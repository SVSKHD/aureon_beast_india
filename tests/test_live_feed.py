from __future__ import annotations

import asyncio
import json
import struct

import pytest

from aureon_mcx.broker.dhan.live_feed import (
    CODE_DISCONNECT, CODE_FULL, CODE_QUOTE, CODE_TICKER, DhanLiveFeedProvider, build_subscribe_message, packet_to_tick, parse_packet,
)
from aureon_mcx.market.timeutil import from_epoch


def _header(code, length, segment, sec_id):
    return struct.pack("<BHBI", code, length, segment, sec_id)


def test_parse_ticker_and_quote_packets():
    data = _header(CODE_TICKER, 16, 5, 428291) + struct.pack("<fI", 70125.0, 1789962600)
    p = parse_packet(data)
    assert p.code == CODE_TICKER and p.security_id == "428291" and p.exchange_segment == 5 and p.ltp == 70125.0 and p.ltt == 1789962600
    tick = packet_to_tick(p, {})
    assert tick.price == 70125.0 and tick.ts == from_epoch(1789962600) and tick.last_qty is None
    quote = _header(CODE_QUOTE, 50, 5, 429001) + struct.pack("<fhIfIIIffff", 84000.0, 3, 1789962605, 83990.0, 1200, 50, 60, 83900.0, 83800.0, 84100.0, 83700.0)
    q = parse_packet(quote)
    assert q.ltp == 84000.0 and q.last_qty == 3 and q.volume == 1200 and q.security_id == "429001"
    full = _header(CODE_FULL, 60, 5, 429001) + struct.pack("<fhIfIIIffffI", 84000.0, 3, 1789962605, 83990.0, 1200, 50, 60, 83900.0, 83800.0, 84100.0, 83700.0, 15055)
    f = parse_packet(full)
    assert f.open_interest == 15055
    t = packet_to_tick(f, {})
    assert t.open_interest == 15055 and t.day_volume == 1200
    assert parse_packet(b"\x00") is None
    d = parse_packet(_header(CODE_DISCONNECT, 10, 5, 1) + struct.pack("<h", 805))
    assert d.disconnect_reason == 805


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
    frames1 = [_header(CODE_TICKER, 16, 5, 428291) + struct.pack("<fI", 70125.0, 1789962600)]
    frames2 = [_header(CODE_TICKER, 16, 5, 428291) + struct.pack("<fI", 70130.0, 1789962660),
               _header(CODE_TICKER, 16, 5, 999999) + struct.pack("<fI", 1.0, 1789962660)]  # unsubscribed id ignored

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
