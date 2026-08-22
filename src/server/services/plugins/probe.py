"""Streamable-HTTP upgrade probe for held-back legacy sse entries.

A plugin's ``sse`` entries are not installed as-is (the sandbox client
refuses the legacy transport); instead the endpoint is probed for streamable
HTTP and, on the user's consent, installed as ``transport="http"``. The
probe reuses the discovery posture wholesale — pinned session, no redirects,
bounded body — and counts a 401/403 as SUCCESS: an auth challenge proves a
streamable-HTTP listener; credentials are the connector flow's job, not the
probe's.
"""

import asyncio
import logging
from dataclasses import dataclass

from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

from src.server.services.mcp_oauth.http import pinned_discovery_client
from src.server.utils.egress_guard import pin_public_url

logger = logging.getLogger(__name__)

PROBE_TIMEOUT_S = 10
# One package decides how many endpoints get probed, and mcp.json puts no
# ceiling on its entry count. Concurrency alone bounds the sockets but not the
# clock — 10,000 stalling entries eight at a time is hours of held request. The
# two caps together bound the phase at
# ceil(MAX_PROBED_ENTRIES / MAX_CONCURRENT_PROBES) * PROBE_TIMEOUT_S, which is
# why no separate overall deadline is needed.
MAX_CONCURRENT_PROBES = 8
MAX_PROBED_ENTRIES = 32


@dataclass(frozen=True)
class ProbeResult:
    key: str  # the mcp.json entry key
    url: str
    ok: bool
    detail: str = ""


def _is_auth_challenge(error: Exception) -> bool:
    """A 401/403 on the MCP endpoint: the transport exists, it just wants
    credentials. The SDK surfaces it as an exception whose text carries the
    status — a string check is the stable cross-version signal."""
    text = str(error)
    return "401" in text or "403" in text


async def probe_streamable_http(key: str, url: str) -> ProbeResult:
    """Probe one endpoint for streamable-HTTP support."""
    try:
        target = await pin_public_url(url)
    except Exception as e:
        return ProbeResult(key=key, url=url, ok=False, detail=f"blocked url: {e}")
    try:
        async with asyncio.timeout(PROBE_TIMEOUT_S):
            async with pinned_discovery_client(target) as http_client:
                transport = streamable_http_client(url, http_client=http_client)
                async with Client(transport) as client:
                    await client.list_tools(cache_mode="refresh")
        return ProbeResult(key=key, url=url, ok=True)
    except Exception as e:  # noqa: BLE001 — every failure class is a verdict
        if _is_auth_challenge(e):
            return ProbeResult(
                key=key, url=url, ok=True,
                detail="endpoint requires authentication (streamable HTTP "
                "confirmed)",
            )
        logger.info("[plugins] sse upgrade probe failed for %s: %s", url, e)
        return ProbeResult(key=key, url=url, ok=False, detail=str(e))


async def probe_all(entries: list[tuple[str, str]]) -> list[ProbeResult]:
    """Probe (key, url) pairs, bounded in both concurrency and count.

    Entries past the cap come back not-ok with the reason rather than being
    dropped, so the caller still reports them — as ordinary un-upgradable sse
    entries, which is what an unprobed one is.
    """
    gate = asyncio.Semaphore(MAX_CONCURRENT_PROBES)

    async def one(key: str, url: str) -> ProbeResult:
        async with gate:
            return await probe_streamable_http(key, url)

    probed = await asyncio.gather(
        *(one(key, url) for key, url in entries[:MAX_PROBED_ENTRIES])
    )
    return list(probed) + [
        ProbeResult(
            key=key, url=url, ok=False,
            detail=(
                "not probed: the package declares more than "
                f"{MAX_PROBED_ENTRIES} legacy sse entries"
            ),
        )
        for key, url in entries[MAX_PROBED_ENTRIES:]
    ]
