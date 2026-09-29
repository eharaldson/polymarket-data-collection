import json
import runpy
from pathlib import Path

# Examples are standalone scripts, not part of the installed package.
verify = runpy.run_path(str(Path(__file__).resolve().parents[1] / "examples" / "verify_replay.py"))["verify"]


def snapshot(timestamp=1):
    return {
        "event_type": "book", "asset_id": "token", "timestamp": timestamp,
        "bids_json": json.dumps([{"price": "0.4", "size": "10"}]),
        "asks_json": json.dumps([{"price": "0.6", "size": "10"}]),
    }


def change(timestamp=2, **fields):
    return {
        "event_type": "price_change", "asset_id": "token", "timestamp": timestamp,
        "side": "BUY", "price": "0.5", "size": "10", "best_bid": "0.5", "best_ask": "0.6",
        **fields,
    }


def test_check_waits_for_last_state_at_each_token_timestamp():
    events = [
        snapshot(),
        change(best_ask="0.55"),  # Reported ask reflects the next delta as well.
        change(side="SELL", price="0.55", best_ask="0.55"),
        change(timestamp=3, price="0.52", best_bid="0.52", best_ask="0.55"),
    ]
    # A later delta must not mutate the stored comparison for timestamp 2.
    assert verify(events) == (2, 0)


def test_check_separates_sessions_and_skips_deltas_without_a_snapshot():
    events = [
        snapshot(), change(best_bid="0.9"),
        {"event_type": "connected"},
        change(),  # No new snapshot yet; this delta cannot be replayed.
        snapshot(), change(),  # Same token and timestamp, different session.
    ]
    assert verify(events) == (2, 1)


def test_check_excludes_snapshots_missing_quotes_and_missing_timestamps():
    assert verify([snapshot(), change(best_ask=None), change(timestamp=None)]) == (0, 0)


def test_check_handles_empty_sides_using_documented_sentinels():
    events = [
        snapshot(),
        change(price="0.4", size="0", best_bid="0"),
        change(timestamp=3, side="SELL", price="0.6", size="0", best_bid="0", best_ask="1"),
    ]
    assert verify(events) == (2, 0)
