"""Chart recorded quotes and trades without joining quotes across reconnects.

    pip install pandas matplotlib
    python examples/plot_quotes.py data docs/example.png --title "White Sox vs. Astros"
"""

import argparse
from datetime import timezone

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from polycollect.replay import read_events


def plot_quotes(events, title=None):
    df = events.to_pandas()
    # Assign sessions before removing markers. Preserve distinct changes even
    # when they share an exchange timestamp.
    df["session"] = df.event_type.eq("connected").cumsum()
    df = df[df.event_type != "connected"].copy()
    if df.empty:
        raise ValueError("The recording contains no market events")
    df["time"] = pd.to_datetime(df.recv_time, unit="ms", utc=True)
    for col in ["price", "best_bid", "best_ask"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Prefer Yes; otherwise show the first outcome alphabetically for each market.
    tokens = df.drop_duplicates("asset_id").sort_values(["market", "outcome"])
    tokens = tokens.sort_values("outcome", key=lambda s: s != "Yes", kind="stable").drop_duplicates("market")
    fig, axes = plt.subplots(len(tokens), 1, figsize=(11, 3.2 * len(tokens)), sharex=True, squeeze=False)
    for ax, token in zip(axes[:, 0], tokens.itertuples()):
        rows = df[df.asset_id == token.asset_id]
        for i, (_, session) in enumerate(rows.groupby("session", sort=False)):
            # Wait for this token's first snapshot after each reconnect.
            ready = session.event_type.eq("book").cummax()
            quotes = session[ready & session.event_type.isin(["book", "price_change"])]
            ax.fill_between(quotes.time, quotes.best_bid, quotes.best_ask, step="post", color="C0", alpha=0.12)
            ax.step(quotes.time, quotes.best_bid, where="post", color="C2", lw=1,
                    label="Best bid" if i == 0 else None)
            ax.step(quotes.time, quotes.best_ask, where="post", color="C3", lw=1,
                    label="Best ask" if i == 0 else None)
        trades = rows[rows.event_type == "last_trade_price"]
        ax.scatter(trades.time, trades.price, s=8, color="k", zorder=3, label="Trade")

        first, last = rows.time.min(), rows.time.max()
        dates = first.strftime("%d %B %Y")
        if first.date() != last.date():
            dates += " – " + last.strftime("%d %B %Y")
        ax.set_title(f"{title or token.slug} · {token.outcome} outcome · {dates}", fontsize=10, loc="left")
        ax.set_ylabel("Price")
        ax.grid(alpha=0.25)

    axes[0, 0].legend(loc="best", fontsize=8, ncol=3)
    locator = mdates.AutoDateLocator(tz=timezone.utc)
    axes[-1, 0].xaxis.set_major_locator(locator)
    axes[-1, 0].xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator, tz=timezone.utc))
    axes[-1, 0].set_xlabel("Time (UTC)")
    fig.tight_layout()
    return fig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir")
    parser.add_argument("output")
    parser.add_argument("--title", help="Market display name (best used with a single-market recording)")
    args = parser.parse_args()
    fig = plot_quotes(read_events(args.data_dir), title=args.title)
    fig.savefig(args.output, dpi=120)
    plt.close(fig)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
