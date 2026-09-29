"""Command line entry point: polycollect SLUG_OR_URL [...]"""

import argparse
import asyncio
import logging
import os
import sys

from . import gamma
from .collector import Collector


def main():
    parser = argparse.ArgumentParser(
        prog="polycollect",
        description="Record tick-by-tick Polymarket order book data to daily Parquet files.",
    )
    parser.add_argument(
        "slugs", nargs="+", metavar="SLUG_OR_URL",
        help="Polymarket event or market slug, or a polymarket.com URL. An event records all of its open markets.",
    )
    parser.add_argument(
        "--data-dir", default=os.environ.get("DATA_DIR", "./data"),
        help="Where Parquet files are written (default: $DATA_DIR or ./data)",
    )
    parser.add_argument(
        "--refresh", type=float, default=300,
        help="Re-resolve slugs every N seconds to pick up new markets and drop closed ones (0 = never, default 300)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Print the markets and tokens each slug resolves to, then exit",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # it logs every request at INFO

    try:
        slugs = [gamma.slug_from_input(s) for s in args.slugs]
    except ValueError as e:
        sys.exit(f"error: {e}")

    try:
        if args.list:
            for slug in slugs:
                print(slug)
                tokens = gamma.resolve(slug)
                for t in tokens:
                    print(f"  {t.slug}  {t.outcome:<10} {t.asset_id}")
                if not tokens:
                    print("  (no open markets)")
            return
        asyncio.run(Collector(slugs, args.data_dir, args.refresh).run())
    except gamma.GammaError as e:
        sys.exit(f"error: {e}")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
