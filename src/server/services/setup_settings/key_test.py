"""One small real call per vendor, to say whether a key works before it is saved.

Keys travel in headers only: httpx logs request URLs and its errors repeat them.
Nothing here logs a value or returns a vendor's own response text.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_LIMIT_SECONDS = 10.0


def _firecrawl_base() -> str:
    return os.getenv("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev")


def _sandbox():
    from src.server.app import setup

    return setup.agent_config.sandbox if setup.agent_config else None


def _daytona_base() -> str:
    sandbox = _sandbox()
    return sandbox.daytona.base_url if sandbox else "https://app.daytona.io/api"


def _e2b_base() -> str:
    sandbox = _sandbox()
    e2b = sandbox.e2b if sandbox else None
    return (e2b.api_url if e2b else "") or (
        f"https://api.{e2b.domain}" if e2b and e2b.domain else "https://api.e2b.app"
    )


@dataclass(frozen=True)
class _Probe:
    method: str
    base: str | Callable[[], str]
    path: str
    header: str
    bearer: bool = False
    json: dict[str, Any] | None = None
    params: dict[str, Any] = field(default_factory=dict)

    @property
    def url(self) -> str:
        base = self.base() if callable(self.base) else self.base
        return f"{base.rstrip('/')}{self.path}"


_QUERY = "market news"

_PROBES: dict[str, _Probe] = {
    "FMP_API_KEY": _Probe(
        "GET", "https://financialmodelingprep.com/stable", "/profile", "apikey",
        params={"symbol": "AAPL"},
    ),
    "TAVILY_API_KEY": _Probe(
        "POST", "https://api.tavily.com", "/search", "Authorization", bearer=True,
        json={"query": _QUERY, "max_results": 1},
    ),
    "SERPER_API_KEY": _Probe(
        "POST", "https://google.serper.dev", "/search", "X-API-KEY",
        json={"q": _QUERY, "num": 1},
    ),
    "BOCHA_API_KEY": _Probe(
        "POST", "https://api.bochaai.com", "/v1/web-search", "Authorization", bearer=True,
        json={"query": _QUERY, "count": 1},
    ),
    "EXA_API_KEY": _Probe(
        "POST", "https://api.exa.ai", "/search", "x-api-key",
        json={"query": _QUERY, "numResults": 1},
    ),
    "PARALLEL_API_KEY": _Probe(
        "POST", "https://api.parallel.ai", "/v1/search", "x-api-key",
        json={"search_queries": [_QUERY], "advanced_settings": {"max_results": 1}},
    ),
    "FIRECRAWL_API_KEY": _Probe(
        "POST", _firecrawl_base, "/v2/map", "Authorization", bearer=True,
        json={"url": "https://example.com", "limit": 1},
    ),
    "DAYTONA_API_KEY": _Probe(
        "GET", _daytona_base, "/sandbox", "Authorization", bearer=True,
        params={"limit": 1},
    ),
    "E2B_API_KEY": _Probe(
        "GET", _e2b_base, "/sandboxes", "X-API-Key", params={"limit": 1},
    ),
}


def _verdict(status: int) -> dict[str, Any]:
    if status < 300:
        return {"ok": True}
    if status in (401, 403):
        return {"ok": False, "error": "The key was refused"}
    if status in (402, 432, 433):
        return {"ok": False, "error": "The key's plan has run out or does not include this"}
    if status == 429:
        return {"ok": False, "error": "Rate limited, so the key could not be checked yet"}
    return {"ok": False, "error": f"The vendor answered with status {status}"}


async def try_key(name: str, value: str) -> dict[str, Any]:
    probe = _PROBES.get(name)
    if probe is None:
        return {"ok": False, "error": "This key has no test"}
    value = value.strip()
    if not value.isprintable():
        return {"ok": False, "error": "This is not a usable key"}
    auth = f"Bearer {value}" if probe.bearer else value
    try:
        async with asyncio.timeout(_LIMIT_SECONDS):
            async with httpx.AsyncClient(timeout=_LIMIT_SECONDS) as client:
                response = await client.request(
                    probe.method, probe.url, params=probe.params or None,
                    json=probe.json, headers={probe.header: auth},
                )
    except (TimeoutError, httpx.TimeoutException):
        return {"ok": False, "error": "No answer within 10 seconds"}
    except (httpx.HTTPError, httpx.InvalidURL, ValueError) as exc:
        # The type alone: a value no header can hold is a ValueError that quotes it.
        logger.info("[SETUP] key test for %s failed: %s", name, type(exc).__name__)
        return {"ok": False, "error": "The vendor could not be reached"}
    logger.info("[SETUP] key test for %s answered %s", name, response.status_code)
    return _verdict(response.status_code)
