"""Read recorded events and rebuild each token's full order book from them."""

import json
import re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq

Level = Tuple[float, float]  # (price, size)


def _file_order(path: Path):
    part = re.search(r"\.part(\d+)\.parquet$", path.name)
    return path.parent.name, int(part.group(1)) if part else 0


def read_events(path: Union[str, Path]) -> pa.Table:
    """Every sealed events file under ``path`` (a data directory or one day's
    directory) as one table, in the order the events arrived."""
    files = sorted(
        (f for f in Path(path).rglob("events*.parquet") if re.fullmatch(r"events(\.part\d+)?\.parquet", f.name)),
        key=_file_order,
    )
    if not files:
        raise FileNotFoundError(f"No events*.parquet files under {path}")
    return pa.concat_tables(pq.read_table(f) for f in files)


class OrderBook:
    """One token's book, rebuilt from its ``book`` and ``price_change`` events."""

    def __init__(self, asset_id: str):
        self.asset_id = asset_id
        self.bids: Dict[float, float] = {}  # price -> size
        self.asks: Dict[float, float] = {}
        self.ready = False  # True once a full snapshot has been applied

    def apply(self, event: dict) -> bool:
        """Apply one event row. Returns True if it changed the book."""
        kind = event["event_type"]
        if kind == "book":
            self.bids = {float(lv["price"]): float(lv["size"]) for lv in json.loads(event["bids_json"])}
            self.asks = {float(lv["price"]): float(lv["size"]) for lv in json.loads(event["asks_json"])}
            self.ready = True
            return True
        if kind == "price_change" and self.ready:
            side = self.bids if event["side"] == "BUY" else self.asks
            price, size = float(event["price"]), float(event["size"])
            if size > 0:
                side[price] = size
            else:
                side.pop(price, None)
            return True
        return False

    def best_bid(self) -> Optional[Level]:
        return (max(self.bids), self.bids[max(self.bids)]) if self.bids else None

    def best_ask(self) -> Optional[Level]:
        return (min(self.asks), self.asks[min(self.asks)]) if self.asks else None

    def levels(self, depth: Optional[int] = None) -> Tuple[List[Level], List[Level]]:
        """(bids, asks), best first, optionally the top ``depth`` of each."""
        return sorted(self.bids.items(), reverse=True)[:depth], sorted(self.asks.items())[:depth]


def replay(
    events: Union[pa.Table, Iterable[dict]], asset_ids: Optional[Iterable[str]] = None,
) -> Iterator[Tuple[dict, OrderBook]]:
    """Yield ``(event, book)`` after every event that changed a book.

    ``events`` is a table from :func:`read_events`, or any iterable of row
    dicts in arrival order. Pass ``asset_ids`` to rebuild only those tokens.
    All books are discarded at each ``connected`` row: after a reconnect a
    book is unknown until its next snapshot, and deltas before it are skipped.
    """
    if isinstance(events, pa.Table):
        events = (row for batch in events.to_batches() for row in batch.to_pylist())
    wanted = set(asset_ids) if asset_ids is not None else None
    books: Dict[str, OrderBook] = {}
    for event in events:
        if event["event_type"] == "connected":
            books.clear()
            continue
        asset_id = event["asset_id"]
        if wanted is not None and asset_id not in wanted:
            continue
        book = books.get(asset_id)
        if book is None:
            book = books[asset_id] = OrderBook(asset_id)
        if book.apply(event):
            yield event, book
