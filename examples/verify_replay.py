"""Compare replayed quotes with reported quotes at sampled timestamp boundaries.

This checks consistency, not complete capture or full-depth correctness. An
exchange timestamp is not an update ID. For each connection session, token
and timestamp, retain the last replayed book state; compare only when that
final row is a price change with both quote fields.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone

from polycollect.replay import read_events, replay


def verify(events):
    session = 0

    def rows():
        nonlocal session
        for event in events:
            if event["event_type"] == "connected":
                session += 1
            yield event

    last = {}
    for event, book in replay(rows()):
        if event["timestamp"] is None:
            continue
        # Copy prices now: replay mutates each book as subsequent rows arrive.
        bid, ask = book.best_bid(), book.best_ask()
        last[session, event["asset_id"], event["timestamp"]] = (
            event, bid[0] if bid else 0.0, ask[0] if ask else 1.0,
        )

    checked = mismatches = 0
    for event, bid, ask in last.values():
        if event["event_type"] != "price_change" or event["best_bid"] is None or event["best_ask"] is None:
            continue
        checked += 1
        # This check treats 0 / 1 as the reported empty bid / ask sentinels.
        mismatches += (bid, ask) != (float(event["best_bid"]), float(event["best_ask"]))
    return checked, mismatches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir")
    args = parser.parse_args()
    events = read_events(args.data_dir).to_pylist()
    print(f"Rows: {len(events):,}")
    for kind, count in sorted(Counter(e["event_type"] for e in events).items()):
        print(f"  {kind}: {count:,}")
    times = [e["recv_time"] for e in events if e["recv_time"] is not None]
    if times:
        start, end = (datetime.fromtimestamp(t / 1000, timezone.utc).isoformat() for t in (min(times), max(times)))
        print(f"Receipt window (UTC): {start} to {end}")
    checked, mismatches = verify(events)
    print(f"Quote checks: {checked:,}; matches: {checked - mismatches:,}; mismatches: {mismatches:,}")
    raise SystemExit(1 if mismatches or not checked else 0)


if __name__ == "__main__":
    main()
