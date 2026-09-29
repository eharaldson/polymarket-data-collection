"""Resolve Polymarket slugs and URLs to CLOB token IDs via the Gamma API."""

import json
from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import urlparse

import httpx

GAMMA_URL = "https://gamma-api.polymarket.com"


class GammaError(RuntimeError):
    pass


@dataclass(frozen=True)
class Token:
    """One outcome of a market. Every token has its own order book."""
    asset_id: str  # CLOB token ID
    outcome: str   # e.g. "Yes", "No"
    slug: str      # market slug
    market: str    # condition ID


def slug_from_input(text: str) -> str:
    """Turn a slug or polymarket.com URL into what :func:`resolve` takes.

    ``polymarket.com/event/<event>`` becomes ``event:<event>`` and
    ``polymarket.com/event/<event>/<market>`` becomes ``market:<market>``, since an
    event's main market can share the event's slug. Bare slugs (optionally
    prefixed ``event:`` or ``market:``) pass through unchanged.
    """
    text = text.strip()
    if "/" not in text:
        return text
    parts = [p for p in urlparse(text if "://" in text else f"https://{text}").path.split("/") if p]
    if len(parts) == 2 and parts[0] == "event":
        return f"event:{parts[1]}"
    if (len(parts) == 3 and parts[0] == "event") or (len(parts) == 2 and parts[0] == "market"):
        return f"market:{parts[-1]}"
    raise ValueError(f"Not a Polymarket event or market URL: {text!r}")


def _json_list(value: Any) -> List[str]:
    """Gamma returns some list fields as JSON-encoded strings, e.g. '["123", "456"]'."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return [str(v) for v in value] if isinstance(value, list) else []


def _get(path: str, params: dict) -> Any:
    try:
        resp = httpx.get(f"{GAMMA_URL}{path}", params=params, timeout=10.0)
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, ValueError) as e:
        raise GammaError(f"Gamma request {path} {params} failed: {e}") from e


def _tokens(market: dict) -> List[Token]:
    """Tokens of one market, or none if it's closed or has no order book."""
    if market.get("closed"):
        return []
    outcomes = _json_list(market.get("outcomes"))
    return [
        Token(
            asset_id=asset_id,
            outcome=outcomes[i] if i < len(outcomes) else "",
            slug=str(market.get("slug", "")),
            market=str(market.get("conditionId", "")),
        )
        for i, asset_id in enumerate(_json_list(market.get("clobTokenIds")))
    ]


def _first(path: str, slug: str) -> Optional[dict]:
    found = _get(path, {"slug": slug})
    return found[0] if isinstance(found, list) and found and isinstance(found[0], dict) else None


def resolve(spec: str) -> List[Token]:
    """Tokens of every open market behind a slug: each market of an event, or one market.

    ``event:<slug>`` and ``market:<slug>`` look up only that kind; a bare slug
    is tried as an event first, then as a market. Returns an empty list if
    everything behind it has closed, and raises GammaError if it doesn't exist.
    """
    kind, _, slug = spec.rpartition(":")
    if kind not in ("", "event", "market"):
        raise GammaError(f"Unknown prefix {kind!r} in {spec!r}: use event: or market:")
    if kind in ("", "event"):
        event = _first("/events", slug)
        if event:
            return [t for m in event.get("markets") or [] if isinstance(m, dict) for t in _tokens(m)]
    if kind in ("", "market"):
        market = _first("/markets", slug)
        if market:
            return _tokens(market)
    raise GammaError(f"No Polymarket {kind or 'event or market'} with slug {slug!r}")
