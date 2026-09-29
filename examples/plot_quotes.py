"""Chart the best bid, best ask and trades of each market in a recording.

    pip install pandas matplotlib
    python examples/plot_quotes.py data quotes.png
"""

import re
import sys

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from polycollect.replay import read_events

data_dir, out = sys.argv[1], sys.argv[2]
df = read_events(data_dir).to_pandas()
df = df[df.event_type != "connected"]
df["time"] = pd.to_datetime(df.recv_time, unit="ms")
for col in ["price", "best_bid", "best_ask"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# One token per market: its "Yes" token if it has one, else the first outcome alphabetically.
tokens = df.drop_duplicates("asset_id").sort_values(["market", "outcome"])
tokens = tokens.sort_values("outcome", key=lambda s: s != "Yes", kind="stable").drop_duplicates("market")

fig, axes = plt.subplots(len(tokens), 1, figsize=(10, 2.6 * len(tokens)), sharex=True, squeeze=False)
for ax, token in zip(axes[:, 0], tokens.itertuples()):
    rows = df[df.asset_id == token.asset_id]
    # Rows sharing a timestamp are one update; its last row has the final quote.
    quotes = rows[rows.event_type.isin(["book", "price_change"])].drop_duplicates("timestamp", keep="last")
    trades = rows[rows.event_type == "last_trade_price"]

    ax.fill_between(quotes.time, quotes.best_bid, quotes.best_ask, step="post", color="C0", alpha=0.15)
    ax.step(quotes.time, quotes.best_bid, where="post", color="C2", lw=1, label="best bid")
    ax.step(quotes.time, quotes.best_ask, where="post", color="C3", lw=1, label="best ask")
    ax.scatter(trades.time, trades.price, s=8, color="k", zorder=3, label="trade")

    title = re.sub(r"-\d{8,}$", "", token.slug)  # drop Gamma's numeric suffix
    ax.set_title(f"{title}: {token.outcome}", fontsize=9, loc="left")
    ax.grid(alpha=0.3)

axes[0, 0].legend(loc="best", fontsize=8, ncol=3)
axes[-1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
axes[-1, 0].set_xlabel("recv_time (UTC)")
fig.tight_layout()
fig.savefig(out, dpi=120)
print(f"Wrote {out}")
