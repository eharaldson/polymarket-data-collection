"""Polymarket CLOB market channel: subscribe to tokens and read their events, reconnecting as needed."""

import asyncio
import json
import logging
import time
from typing import Callable, Iterable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL = 10.0  # Polymarket expects a text "PING" roughly every 10s
MAX_RECONNECT_DELAY = 60.0


class MarketStream:
    """
    Keeps one connection to the market channel open from ``run()`` until ``close()``.

    ``asset_ids`` is the desired subscription. It's sent in full on every
    (re)connect, and changes made while connected go out as subscribe and
    unsubscribe operations. Every event is passed to ``on_event(event, recv_ms)``
    with the local receipt time in ms, and ``on_connect(recv_ms)`` fires after
    each (re)connect, since books are unknown across a gap until fresh
    snapshots arrive.
    """

    def __init__(
        self,
        on_event: Callable[[dict, int], None],
        on_connect: Optional[Callable[[int], None]] = None,
        url: str = WS_URL,
    ):
        self.url = url
        self.on_event = on_event
        self.on_connect = on_connect
        self.asset_ids: set = set()
        self.websocket = None
        self.running = False
        self._reconnect_delay = 0.0  # no wait before the first attempt
        self._last_connect = 0.0

    async def subscribe(self, asset_ids: Iterable[str]):
        new = sorted(set(asset_ids) - self.asset_ids)
        if new:
            self.asset_ids.update(new)
            await self._send(new, "subscribe")

    async def unsubscribe(self, asset_ids: Iterable[str]):
        gone = sorted(set(asset_ids) & self.asset_ids)
        if gone:
            self.asset_ids.difference_update(gone)
            await self._send(gone, "unsubscribe")

    async def _send(self, asset_ids, operation: str):
        if self.websocket is None:
            return  # sent with the full subscription on (re)connect
        try:
            await self.websocket.send(json.dumps({"assets_ids": asset_ids, "operation": operation}))
        except ConnectionClosed:
            pass  # the read loop reconnects and resubscribes

    async def _open(self):
        ws = await websockets.connect(self.url, additional_headers={"User-Agent": "polycollect"})
        sent = set(self.asset_ids)
        await ws.send(json.dumps({"assets_ids": sorted(sent), "type": "market"}))
        self.websocket = ws
        # Catch up on changes made while the initial subscription was in flight
        added, removed = self.asset_ids - sent, sent - self.asset_ids
        if added:
            await self._send(sorted(added), "subscribe")
        if removed:
            await self._send(sorted(removed), "unsubscribe")

    async def _ping(self):
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                await self.websocket.send("PING")
        except Exception:
            pass  # the read loop sees the broken connection

    def _dispatch(self, message, recv_ms: int):
        if not isinstance(message, str) or not message.startswith(("{", "[")):
            return  # "PONG" and other plain-text replies
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.debug(f"Non-JSON message: {message[:200]!r}")
            return
        for event in data if isinstance(data, list) else [data]:
            if isinstance(event, dict):
                try:
                    self.on_event(event, recv_ms)
                except Exception as e:
                    logger.error(f"Failed to handle {event.get('event_type')} event: {e}")

    async def run(self):
        self.running = True
        while self.running:
            if self._reconnect_delay:
                logger.info(f"Reconnecting in {self._reconnect_delay:.0f}s")
                await asyncio.sleep(self._reconnect_delay)
            try:
                await self._open()
            except Exception as e:
                logger.error(f"Connect failed: {e}")
                self._reconnect_delay = min(max(1.0, self._reconnect_delay * 2), MAX_RECONNECT_DELAY)
                continue
            # A connection that lasted 30s+ starts the backoff over; a flapping one keeps growing it
            now = time.time()
            self._reconnect_delay = 1.0 if now - self._last_connect > 30 else min(
                self._reconnect_delay * 2, MAX_RECONNECT_DELAY)
            self._last_connect = now
            logger.info(f"Connected, subscribed to {len(self.asset_ids)} tokens")
            if self.on_connect:
                self.on_connect(int(now * 1000))

            ping = asyncio.create_task(self._ping())
            try:
                async for message in self.websocket:
                    self._dispatch(message, int(time.time() * 1000))
            except ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}")
            except Exception as e:
                logger.error(f"Stream error: {e}")
            finally:
                ping.cancel()
                await self._drop()

    async def _drop(self):
        if self.websocket is not None:
            ws, self.websocket = self.websocket, None
            try:
                await asyncio.wait_for(ws.close(), timeout=1.0)
            except Exception:
                pass

    async def close(self):
        self.running = False
        await self._drop()
