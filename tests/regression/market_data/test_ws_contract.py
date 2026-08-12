"""WS proxy contracts. Auth here is Supabase-JWT-only (no service-token path),
so live-frame tests need REGRESSION_WS_TOKEN; the rest run tokenless."""

import asyncio
import json
import os

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from .conftest import BASE_URL, US_STOCK

pytestmark = pytest.mark.regression

WS_BASE = BASE_URL.replace("http://", "ws://").replace("https://", "wss://")
WS_TOKEN = os.getenv("REGRESSION_WS_TOKEN", "")


def test_ws_status_probe_is_public():
    r = httpx.get(f"{BASE_URL}/ws/v1/market-data/status", timeout=10)
    assert r.status_code == 200
    assert r.json() == {"enabled": True}


def test_ws_rejects_unauthenticated():
    async def _connect():
        async with websockets.connect(f"{WS_BASE}/ws/v1/market-data/aggregates/stock") as ws:
            await ws.recv()

    # Server closes before accept — handshake 403 or immediate 1008 close
    with pytest.raises((InvalidStatus, ConnectionClosed)):
        asyncio.run(_connect())


def test_ws_rejects_invalid_market():
    async def _connect():
        url = f"{WS_BASE}/ws/v1/market-data/aggregates/bogus"
        if WS_TOKEN:
            url += f"?token={WS_TOKEN}"
        async with websockets.connect(url) as ws:
            await ws.recv()

    with pytest.raises((InvalidStatus, ConnectionClosed)):
        asyncio.run(_connect())


@pytest.mark.skipif(not WS_TOKEN, reason="REGRESSION_WS_TOKEN not set (WS auth accepts only Supabase JWTs)")
def test_ws_subscribe_ping_and_frames(http):
    """Subscribe + ping→pong is deterministic; raw upstream frames are only
    asserted while the market is open (frames stop when it closes)."""
    market_open = http.get("/market-status").json()["market"] == "open"

    async def _run() -> tuple[dict, list[str]]:
        url = f"{WS_BASE}/ws/v1/market-data/aggregates/stock?token={WS_TOKEN}"
        frames: list[str] = []
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"action": "subscribe", "symbols": [US_STOCK]}))
            await ws.send(json.dumps({"action": "ping"}))
            pong = None
            deadline = asyncio.get_event_loop().time() + (30 if market_open else 5)
            while asyncio.get_event_loop().time() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=deadline - asyncio.get_event_loop().time())
                except asyncio.TimeoutError:
                    break
                parsed = json.loads(msg)
                if isinstance(parsed, dict) and parsed.get("type") == "pong":
                    pong = parsed
                    if not market_open:
                        break
                else:
                    frames.append(msg)
                    if market_open and pong:
                        break
            return pong, frames

    pong, frames = asyncio.run(_run())
    assert pong == {"type": "pong"}, "ping must round-trip a pong"
    if market_open:
        assert frames, "market open but no upstream frames within 30s"
        # Today's wire format: raw upstream JSON passthrough (list or dict of
        # aggregate events). Phase 1 keeps this default; ?format=cmdp is additive.
        first = json.loads(frames[0])
        assert isinstance(first, (list, dict))
