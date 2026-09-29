"""Record every Polymarket order book event for a set of slugs to daily Parquet files."""

import asyncio
import json
import logging
import signal
from collections import Counter
from typing import Dict, List, Optional

import pyarrow as pa

from . import gamma
from .gamma import Token
from .stream import MarketStream
from .writer import DailyParquetWriter

logger = logging.getLogger(__name__)

# One row per event. A price_change message becomes one row per level it changes.
SCHEMA = pa.schema([
    ("recv_time", pa.int64()),    # local receipt time, ms epoch
    ("timestamp", pa.int64()),    # Polymarket's timestamp, ms epoch
    ("event_type", pa.string()),  # book | price_change | last_trade_price | tick_size_change | connected
    ("slug", pa.string()),        # market slug
    ("outcome", pa.string()),     # e.g. Yes / No
    ("asset_id", pa.string()),    # CLOB token ID; every outcome has its own book
    ("market", pa.string()),      # condition ID
    ("side", pa.string()),        # BUY / SELL
    ("price", pa.string()),
    ("size", pa.string()),        # price_change: new total size at that level (0 = level removed)
    ("best_bid", pa.string()),    # top of book after this event (book and price_change)
    ("best_ask", pa.string()),
    ("bids_json", pa.string()),   # book: the full snapshot, as sent
    ("asks_json", pa.string()),
    ("tick_size", pa.string()),   # book: current tick size; tick_size_change: the new one
    ("hash", pa.string()),
])

SEAL_INTERVAL = 300.0  # seconds between seals: the most a hard crash can lose


def _ms(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str(value) -> Optional[str]:
    return None if value is None else str(value)


def _best(levels: list, pick) -> Optional[str]:
    """Price string of the best level; ``pick`` is max for bids, min for asks."""
    prices = []
    for level in levels:
        try:
            prices.append((float(level["price"]), level["price"]))
        except (KeyError, TypeError, ValueError):
            continue
    return _str(pick(prices)[1]) if prices else None


class Collector:
    """Resolves slugs to tokens, keeps them subscribed and writes every event."""

    def __init__(self, slugs: List[str], data_dir: str, refresh: float = 300.0):
        self.slugs = slugs
        self.refresh = refresh
        self.tokens: Dict[str, Token] = {}           # asset_id -> Token, everything subscribed
        self._resolved: Dict[str, List[Token]] = {}  # input slug -> tokens at its last resolve
        self.stream = MarketStream(self._on_event, self._on_connect)
        self.writer = DailyParquetWriter(data_dir, "events.parquet", SCHEMA)
        self.counts: Counter = Counter()

    # ------------------------------------------------------------------
    # Subscriptions
    # ------------------------------------------------------------------

    async def sync(self, strict: bool = False):
        """Resolve every slug and bring the subscription in line with its open markets.

        With ``strict`` a slug that fails to resolve raises; otherwise its
        previous markets are kept, so a Gamma outage doesn't drop anything.
        """
        for slug in self.slugs:
            had_markets = bool(self._resolved.get(slug))
            try:
                self._resolved[slug] = await asyncio.to_thread(gamma.resolve, slug)
            except gamma.GammaError as e:
                if strict:
                    raise
                logger.warning(f"{e}; keeping previous markets for {slug!r}")
            if not self._resolved.get(slug) and (strict or had_markets):
                logger.warning(f"No open markets for {slug!r}")

        wanted = {t.asset_id: t for tokens in self._resolved.values() for t in tokens}
        added = [t for aid, t in wanted.items() if aid not in self.tokens]
        removed = [self.tokens[aid] for aid in self.tokens if aid not in wanted]

        # Map before subscribing so the first snapshots aren't dropped
        self.tokens.update(wanted)
        await self.stream.subscribe(t.asset_id for t in added)
        await self.stream.unsubscribe(t.asset_id for t in removed)
        for t in removed:
            del self.tokens[t.asset_id]

        for slug in dict.fromkeys(t.slug for t in added):
            outcomes = [t.outcome for t in added if t.slug == slug]
            logger.info(f"Recording {slug} ({' / '.join(outcomes)})")
        for slug in dict.fromkeys(t.slug for t in removed):
            logger.info(f"Stopped recording {slug} (closed)")

    # ------------------------------------------------------------------
    # Events -> rows
    # ------------------------------------------------------------------

    def _write(self, event_type: str, asset_id: str, recv_ms: int, timestamp, **fields):
        token = self.tokens.get(asset_id)
        if token is None:
            return  # not (or no longer) subscribed
        self.writer.append({
            "recv_time": recv_ms,
            "timestamp": _ms(timestamp),
            "event_type": event_type,
            "slug": token.slug,
            "outcome": token.outcome,
            "asset_id": asset_id,
            "market": token.market,
            **fields,
        })
        self.counts[event_type] += 1

    def _on_event(self, event: dict, recv_ms: int):
        kind = event.get("event_type")
        if kind == "book":
            bids = event.get("bids", event.get("buys")) or []
            asks = event.get("asks", event.get("sells")) or []
            self._write(
                kind, event.get("asset_id"), recv_ms, event.get("timestamp"),
                best_bid=_best(bids, max), best_ask=_best(asks, min),
                bids_json=json.dumps(bids), asks_json=json.dumps(asks),
                tick_size=_str(event.get("tick_size")), hash=_str(event.get("hash")),
            )
        elif kind == "price_change":
            changes = event.get("price_changes") or ([event] if event.get("asset_id") else [])
            for c in changes:
                self._write(
                    kind, c.get("asset_id"), recv_ms, c.get("timestamp", event.get("timestamp")),
                    side=_str(c.get("side")), price=_str(c.get("price")), size=_str(c.get("size")),
                    best_bid=_str(c.get("best_bid")), best_ask=_str(c.get("best_ask")), hash=_str(c.get("hash")),
                )
        elif kind == "last_trade_price":
            self._write(
                kind, event.get("asset_id"), recv_ms, event.get("timestamp"),
                side=_str(event.get("side")), price=_str(event.get("price")), size=_str(event.get("size")),
            )
        elif kind == "tick_size_change":
            self._write(
                kind, event.get("asset_id"), recv_ms, event.get("timestamp"),
                tick_size=_str(event.get("new_tick_size")),
            )

    def _on_connect(self, recv_ms: int):
        # Marks a possible gap: books are unknown until each token's next snapshot
        self.writer.append({"recv_time": recv_ms, "event_type": "connected"})
        self.counts["connected"] += 1

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _every(self, seconds: float, action):
        while True:
            await asyncio.sleep(seconds)
            try:
                result = action()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"{getattr(action, '__name__', action)} failed: {e}")

    def _heartbeat(self):
        logger.info(f"{len(self.tokens)} tokens | events so far: {dict(self.counts)}")

    async def run(self):
        """Record until SIGINT/SIGTERM. Raises GammaError if a slug can't be resolved at startup."""
        await self.sync(strict=True)
        if not self.tokens:
            raise gamma.GammaError(f"No open markets found for {', '.join(self.slugs)}")

        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass  # Windows: Ctrl-C cancels run() instead, which still seals below

        tasks = [
            asyncio.create_task(self.stream.run()),
            asyncio.create_task(self._every(SEAL_INTERVAL, self.writer.seal)),
            asyncio.create_task(self._every(60, self._heartbeat)),
        ]
        if self.refresh:
            tasks.append(asyncio.create_task(self._every(self.refresh, self.sync)))
        stopper = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait([stopper, *tasks], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stopper and task.exception():
                    raise task.exception()
        finally:
            for task in (stopper, *tasks):
                task.cancel()
            await asyncio.gather(stopper, *tasks, return_exceptions=True)
            await self.stream.close()
            self.writer.seal()
            logger.info(f"Stopped. Events recorded: {dict(self.counts)}")
