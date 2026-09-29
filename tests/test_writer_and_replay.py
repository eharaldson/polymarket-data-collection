import json

import pyarrow.parquet as pq

from polycollect.collector import SCHEMA
from polycollect.replay import read_events, replay
from polycollect.writer import DailyParquetWriter

DAY = "2026-01-15"


def make_writer(base, day=DAY):
    w = DailyParquetWriter(str(base), "events.parquet", SCHEMA, flush_threshold=3)
    w._today_utc = lambda: day
    return w


def trade(i):
    return {"recv_time": i, "event_type": "last_trade_price", "asset_id": "1", "price": "0.5", "size": str(i)}


def test_seal_writes_parts_and_a_restart_continues_the_sequence(tmp_path):
    for run in range(2):
        w = make_writer(tmp_path)
        for part in range(2):
            for i in range(5):
                w.append(trade(run * 100 + part * 10 + i))
            w.seal()
    day = tmp_path / DAY
    assert sorted(f.name for f in day.iterdir()) == [
        "events.parquet", "events.part1.parquet", "events.part2.parquet", "events.part3.parquet",
    ]
    assert read_events(tmp_path).column("recv_time").to_pylist() == [
        0, 1, 2, 3, 4, 10, 11, 12, 13, 14, 100, 101, 102, 103, 104, 110, 111, 112, 113, 114,
    ]


def test_writer_rotates_at_utc_midnight(tmp_path):
    w = make_writer(tmp_path, day="2026-01-15")
    for i in range(4):
        w.append(trade(i))
    w._today_utc = lambda: "2026-01-16"
    w.append(trade(99))
    w.seal()
    assert pq.read_metadata(tmp_path / "2026-01-15/events.parquet").num_rows == 4
    assert pq.read_metadata(tmp_path / "2026-01-16/events.parquet").num_rows == 1


def test_read_events_orders_parts_numerically(tmp_path):
    w = make_writer(tmp_path)
    for i in range(12):  # events.parquet, part1 ... part11
        w.append(trade(i))
        w.seal()
    assert read_events(tmp_path / DAY).column("recv_time").to_pylist() == list(range(12))


def book(asset_id, bids, asks):
    return {"event_type": "book", "asset_id": asset_id, "bids_json": json.dumps(bids), "asks_json": json.dumps(asks)}


def change(asset_id, side, price, size):
    return {"event_type": "price_change", "asset_id": asset_id, "side": side, "price": price, "size": size}


def test_replay_rebuilds_the_book_from_snapshots_and_deltas():
    events = [
        change("1", "BUY", "0.40", "9"),  # before any snapshot: skipped
        book("1", [{"price": "0.48", "size": "10"}], [{"price": "0.52", "size": "7"}]),
        change("1", "BUY", "0.49", "3"),
        change("1", "SELL", "0.52", "0"),
        {"event_type": "last_trade_price", "asset_id": "1", "price": "0.49", "size": "1"},
    ]
    states = [(e["event_type"], b.best_bid(), b.best_ask()) for e, b in replay(events)]
    assert states == [
        ("book", (0.48, 10.0), (0.52, 7.0)),
        ("price_change", (0.49, 3.0), (0.52, 7.0)),
        ("price_change", (0.49, 3.0), None),
    ]


def test_replay_discards_books_at_reconnect_until_the_next_snapshot():
    events = [
        book("1", [{"price": "0.48", "size": "10"}], []),
        {"event_type": "connected", "asset_id": None},
        change("1", "BUY", "0.49", "3"),  # book unknown after the gap: skipped
        book("1", [{"price": "0.47", "size": "1"}], []),
        book("2", [{"price": "0.10", "size": "1"}], []),
    ]
    assert [b.levels()[0] for _, b in replay(events)] == [[(0.48, 10.0)], [(0.47, 1.0)], [(0.10, 1.0)]]
    assert [e["asset_id"] for e, _ in replay(events, asset_ids=["2"])] == ["2"]


def test_replay_reads_a_table_written_by_the_writer(tmp_path):
    w = make_writer(tmp_path)
    w.append(book("1", [{"price": "0.30", "size": "2"}], [{"price": "0.35", "size": "4"}]))
    w.append(change("1", "BUY", "0.31", "5"))
    w.seal()
    [(_, final)] = list(replay(read_events(tmp_path)))[-1:]
    assert final.levels() == ([(0.31, 5.0), (0.30, 2.0)], [(0.35, 4.0)])
