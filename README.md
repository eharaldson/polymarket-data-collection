# polymarket-data-collection

Record every order book event for any Polymarket market or event. Give it a
slug or a polymarket.com URL, and it writes tick-by-tick data from the CLOB
WebSocket to daily Parquet files.

- **Lossless.** Every snapshot, every level change, every trade and tick size
  change is recorded exactly as Polymarket sent it, with a local receipt time.
- **Plug and play.** Paste an event URL and it records every open market in
  the event, picks up markets added later, and drops ones that close.
- **Easy to use.** Every row carries the best bid and ask, so a quote series is
  one filter, and `replay()` rebuilds the full-depth book at any point.

---

## Quickstart

```bash
git clone https://github.com/eharaldson/polymarket-data-collection.git
cd polymarket-data-collection
python -m venv .venv && source .venv/bin/activate
pip install -e .

polycollect https://polymarket.com/event/<event-slug>
```

Stop it with Ctrl-C (it writes out everything before exiting). To check what a
slug resolves to before recording:

```bash
polycollect <slug-or-url> --list
```

### What to pass

| Input | Records |
|---|---|
| `https://polymarket.com/event/<event>` | Every open market in the event, re-checked every `--refresh` seconds |
| `https://polymarket.com/event/<event>/<market>` | Just that market |
| `<slug>` | The event with that slug, or else the market with that slug |
| `event:<slug>` / `market:<slug>` | Only that kind (an event's main market can share the event's slug) |

Pass several to record them all into one dataset: `polycollect slug-a slug-b`.

### Options

```
polycollect SLUG_OR_URL [SLUG_OR_URL ...]
            [--data-dir DIR]      # default: $DATA_DIR or ./data
            [--refresh SECONDS]   # re-resolve slugs; 0 = never (default 300)
            [--list]              # print resolved markets and token IDs, then exit
```

---

## Output

```
{data-dir}/{YYYY-MM-DD}/events.parquet
                        events.part1.parquet
                        events.part2.parquet ...
```

Days are UTC. Every 5 minutes the current file is sealed and a new part begins,
and a file still being written ends in `.tmp`. A hard crash loses at most the
last 5 minutes. Ctrl-C or SIGTERM loses nothing, and a restart continues the
day's part sequence. Give each running process its own `--data-dir`.

Each row is one event, in arrival order:

| `event_type` | What it is | Filled columns |
|---|---|---|
| `book` | Full snapshot of one token's book, sent on subscribe and after trades | `bids_json`, `asks_json`, `best_bid`, `best_ask`, `tick_size`, `hash` |
| `price_change` | One price level changed. `size` is the new total at that price; `0` means the level is gone | `side` (`BUY` = bid), `price`, `size`, `best_bid`, `best_ask`, `hash` |
| `last_trade_price` | A trade | `side`, `price`, `size` |
| `tick_size_change` | The tick size changed (it drops to 0.001 near 0 and 1) | `tick_size` |
| `connected` | The collector (re)connected. Books are unknown until their next snapshot | only `recv_time` |

Every event row also has `recv_time` (local receipt time, ms), `timestamp`
(Polymarket's time, ms), `slug`, `outcome` (e.g. `Yes`), `asset_id` (the token
ID) and `market` (the condition ID). Prices and sizes are strings exactly as
sent. The schema is in [`polycollect/collector.py`](polycollect/collector.py).

---

## Reading the data

Top of book over time (this example uses pandas, which isn't a dependency):

```python
from polycollect.replay import read_events

df = read_events("data").to_pandas()            # or one day: "data/2026-09-29"
quotes = df[df.event_type.isin(["book", "price_change"])]
yes = quotes[(quotes.slug == "<market-slug>") & (quotes.outcome == "Yes")]
mid = (yes.best_bid.astype(float) + yes.best_ask.astype(float)) / 2

trades = df[df.event_type == "last_trade_price"]
```

Full depth, rebuilt event by event:

```python
from polycollect.replay import read_events, replay

for event, book in replay(read_events("data")):
    bids, asks = book.levels(depth=5)           # [(price, size), ...] best first
```

`replay()` applies snapshots and deltas per token, skips deltas it can't place
(before the first snapshot, or after a reconnect until the next one), and takes
`asset_ids=[...]` to rebuild only some tokens.

## Things to know

1. **Two clocks.** `recv_time` is when you received an event, which is when you
   could have acted on it. `timestamp` is Polymarket's. Replay and backtest on
   `recv_time`, and keep the host clock synced with NTP, since the two aren't
   on the same time base.
2. **One update can span several rows.** Changes with the same `asset_id` and
   `timestamp` are one update, sometimes split across two messages. The
   `best_bid`/`best_ask` on a `price_change` row describe the book after the
   whole update, so read them from the last row of the group.
3. **Gaps are real.** Polymarket drops slow or idle connections now and then,
   and the collector reconnects on its own. A `connected` row marks each
   (re)connect. Don't carry a book across it; the next `book` snapshot is the
   first reliable state.
4. **Each outcome is its own book.** `Yes` and `No` have separate tokens and
   spreads, so `P(Yes) ≈ 1 − P(No)` only roughly holds. Use the side you'd trade.
5. **Prices are strings on purpose.** They're stored exactly as sent. Convert
   them to numbers yourself, once, deliberately.

## Running it 24/7

```bash
docker build -t polycollect .
docker run -d --restart unless-stopped -v "$PWD/data:/data" polycollect <slug-or-url>
```

Or run `polycollect` under any process manager that restarts it if it exits.

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
```

## License

MIT. Built by [Erik Haraldson](https://github.com/eharaldson), co-founder of
[EvenFold](https://www.evenfold.ai/), where we build AI systems for markets.

Nothing here is investment advice. Check Polymarket's terms of service for your
jurisdiction before you run it.
