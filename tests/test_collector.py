import asyncio

import pytest

from polycollect import gamma
from polycollect.collector import Collector
from polycollect.gamma import Token

YES = Token("1", "Yes", "will-it-rain", "0xm")
NO = Token("2", "No", "will-it-rain", "0xm")


class FakeWriter:
    def __init__(self):
        self.rows = []

    def append(self, row):
        self.rows.append(row)


class FakeStream:
    def __init__(self):
        self.asset_ids = set()

    async def subscribe(self, ids):
        self.asset_ids.update(ids)

    async def unsubscribe(self, ids):
        self.asset_ids.difference_update(ids)


def make_collector(tmp_path, tokens=(YES, NO)):
    c = Collector(["will-it-rain"], str(tmp_path))
    c.writer, c.stream = FakeWriter(), FakeStream()
    c.tokens = {t.asset_id: t for t in tokens}
    return c


def test_book_row_carries_best_prices_and_the_snapshot_as_sent(tmp_path):
    c = make_collector(tmp_path)
    bids = [{"price": "0.001", "size": "5"}, {"price": "0.48", "size": "10"}]  # worst first, as Polymarket sends
    asks = [{"price": "0.999", "size": "1"}, {"price": "0.52", "size": "7"}]
    c._on_event({"event_type": "book", "asset_id": "1", "market": "0xm", "timestamp": "1700000000123",
                 "hash": "h", "tick_size": "0.01", "bids": bids, "asks": asks}, 42)
    [row] = c.writer.rows
    assert row["recv_time"] == 42 and row["timestamp"] == 1700000000123
    assert (row["slug"], row["outcome"], row["asset_id"]) == ("will-it-rain", "Yes", "1")
    assert (row["best_bid"], row["best_ask"], row["tick_size"]) == ("0.48", "0.52", "0.01")
    assert row["bids_json"] == '[{"price": "0.001", "size": "5"}, {"price": "0.48", "size": "10"}]'


def test_price_change_becomes_one_row_per_level(tmp_path):
    c = make_collector(tmp_path)
    c._on_event({"event_type": "price_change", "market": "0xm", "timestamp": "1000", "price_changes": [
        {"asset_id": "1", "price": "0.49", "size": "20", "side": "BUY", "best_bid": "0.49", "best_ask": "0.52"},
        {"asset_id": "2", "price": "0.51", "size": "0", "side": "SELL", "best_bid": "0.47", "best_ask": "0.53"},
        {"asset_id": "unknown", "price": "0.1", "size": "1", "side": "BUY"},
    ]}, 7)
    rows = c.writer.rows
    assert [(r["outcome"], r["side"], r["price"], r["size"], r["best_bid"]) for r in rows] == [
        ("Yes", "BUY", "0.49", "20", "0.49"), ("No", "SELL", "0.51", "0", "0.47"),
    ]
    assert all(r["timestamp"] == 1000 and r["recv_time"] == 7 for r in rows)


def test_trades_tick_sizes_and_reconnect_markers(tmp_path):
    c = make_collector(tmp_path)
    c._on_event({"event_type": "last_trade_price", "asset_id": "1", "price": "0.5", "size": "3", "side": "BUY",
                 "timestamp": "5"}, 1)
    c._on_event({"event_type": "tick_size_change", "asset_id": "1", "new_tick_size": "0.001", "timestamp": "6"}, 2)
    c._on_event({"event_type": "something_new", "asset_id": "1"}, 3)
    c._on_connect(4)
    assert [(r["event_type"], r.get("price"), r.get("tick_size")) for r in c.writer.rows] == [
        ("last_trade_price", "0.5", None), ("tick_size_change", None, "0.001"), ("connected", None, None),
    ]
    assert dict(c.counts) == {"last_trade_price": 1, "tick_size_change": 1, "connected": 1}


def test_sync_adds_new_markets_and_drops_closed_ones(tmp_path, monkeypatch):
    listed = {"will-it-rain": [YES, NO]}
    monkeypatch.setattr(gamma, "resolve", lambda slug: listed[slug])
    c = make_collector(tmp_path, tokens=())

    asyncio.run(c.sync(strict=True))
    assert c.stream.asset_ids == {"1", "2"} and set(c.tokens) == {"1", "2"}

    listed["will-it-rain"] = [Token("3", "Yes", "will-it-snow", "0xs"), Token("4", "No", "will-it-snow", "0xs")]
    asyncio.run(c.sync())
    assert c.stream.asset_ids == {"3", "4"} and set(c.tokens) == {"3", "4"}


def test_sync_keeps_markets_through_a_gamma_outage_but_fails_fast_at_startup(tmp_path, monkeypatch):
    c = make_collector(tmp_path, tokens=())
    monkeypatch.setattr(gamma, "resolve", lambda slug: [YES, NO])
    asyncio.run(c.sync(strict=True))

    def down(slug):
        raise gamma.GammaError("down")

    monkeypatch.setattr(gamma, "resolve", down)
    asyncio.run(c.sync())
    assert set(c.tokens) == {"1", "2"}
    with pytest.raises(gamma.GammaError):
        asyncio.run(c.sync(strict=True))
