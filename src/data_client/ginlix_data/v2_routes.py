"""ginlix-data's protocol (v2) routes, shared by the host and sandbox clients.

A transport supplies ``_v2_get`` (base URL, auth, token refresh); the paths,
params and response contracts live here once, so ``GinlixDataClient`` and
``GinlixMCPClient`` expose the same methods and a source written against one
runs over the other. A 404 is the route's "no vendor has this" and comes back
as the empty value; every other non-2xx raises ``httpx.HTTPStatusError``, and
a 2xx that is not JSON raises :class:`NonJsonResponse`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from urllib.parse import quote

import httpx

logger = logging.getLogger(__name__)

# ginlix-data refuses a /quotes or /instruments request naming more keys than this.
MAX_KEYS_PER_REQUEST = 500
# The widest bars window (epoch to 2100-01-01 UTC, in ms) and fundamentals
# limit ginlix-data accepts. Past them a request is a 422 that sends the chain
# to another vendor on another adjustment basis, yet no bar lies outside the
# window and no listing has more rows, so a request is clamped to them instead.
MAX_BAR_MS = 4_102_444_800_000
MAX_FUNDAMENTAL_ROWS = 500


def _clamp_ms(value: int | None) -> int | None:
    return None if value is None else min(max(value, 0), MAX_BAR_MS)


class NonJsonResponse(httpx.DecodingError):
    """A 2xx from ginlix-data whose body is not JSON.

    An ``httpx.HTTPError``, so every soft-miss handler that already absorbs a
    transport failure absorbs this too instead of tracing a JSONDecodeError.
    """


def path_segment(value: object) -> str:
    """*value* escaped into exactly one URL path segment.

    Symbols arrive from users: left raw, ``?`` or ``#`` cuts the path short
    (``AAPL?.SH`` would fetch ``AAPL``) and ``/`` or a dot segment walks out of
    the route. httpx resolves a literal ``.``/``..`` even when it is the whole
    segment, so those dots are escaped as well.
    """
    segment = quote(str(value), safe="")
    return segment.replace(".", "%2E") if segment in (".", "..") else segment


class GinlixDataV2Routes:
    async def _v2_get(
        self, path: str, params: dict[str, Any] | None = None, *, user_id: str | None = None
    ) -> httpx.Response:
        raise NotImplementedError

    async def _v2_json(
        self, path: str, params: dict[str, Any] | None = None, *, user_id: str | None = None
    ) -> Any:
        """The route's JSON body, or ``None`` on a 404."""
        resp = await self._v2_get(path, params, user_id=user_id)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        content_type = resp.headers.get("content-type") or "none"
        if "json" not in content_type.lower():
            logger.error(
                "ginlix_data.v2.non_json_response | path=%s content_type=%s", path, content_type
            )
            raise NonJsonResponse(
                f"ginlix-data returned a non-JSON response ({content_type})", request=resp.request
            )
        return resp.json()

    async def get_bars_v2(
        self,
        instrument_key: str,
        schema: str,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        limit: int | None = None,
        user_id: str | None = None,
    ) -> dict[str, Any] | None:
        """A protocol Series as wire JSON, or ``None`` when no vendor has the series."""
        params: dict[str, Any] = {"schema": schema}
        if start_ms is not None:
            params["start"] = _clamp_ms(start_ms)
        if end_ms is not None:
            params["end"] = _clamp_ms(end_ms)
        if limit is not None:
            params["limit"] = limit
        return await self._v2_json(
            f"/api/v2/data/bars/{path_segment(instrument_key)}", params, user_id=user_id
        )

    async def _v2_keyed(
        self, path: str, field: str, keys: list[str], *, user_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """*field* of a keyed batch route, in as many requests as its key cap needs.

        The cap counts every key sent, so blanks and repeats go first. So does a
        key holding a comma: upstream it splits into several, naming a listing
        nobody asked for and pushing its chunk past the cap. Chunks run
        concurrently and merge into the one mapping a single request would
        return; a chunk that fails fails the call, as that request would.
        """
        wanted = list(dict.fromkeys(
            k for k in (k.strip() for k in keys) if k and "," not in k
        ))
        chunks = [
            wanted[i:i + MAX_KEYS_PER_REQUEST]
            for i in range(0, len(wanted), MAX_KEYS_PER_REQUEST)
        ]
        bodies = await asyncio.gather(
            *(self._v2_json(path, {"keys": ",".join(c)}, user_id=user_id) for c in chunks),
            return_exceptions=True,
        )
        out: dict[str, dict[str, Any]] = {}
        for body in bodies:
            if isinstance(body, BaseException):
                raise body
            out.update((body or {}).get(field) or {})
        return out

    async def get_quotes_v2(
        self, keys: list[str], *, user_id: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """Protocol quotes keyed by ``instrument_key`` (any spelling may be requested)."""
        return await self._v2_keyed("/api/v2/data/quotes", "quotes", keys, user_id=user_id)

    async def get_instruments_v2(self, keys: list[str]) -> dict[str, dict[str, Any]]:
        """Directory entries keyed by ``instrument_key``; unknown keys are absent."""
        return await self._v2_keyed("/api/v2/data/instruments", "instruments", keys)

    async def get_fundamentals_v2(
        self,
        instrument_key: str,
        kind: str,
        *,
        period: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Rows of one fundamentals kind, newest first; [] when no vendor has the name."""
        params: dict[str, Any] = {"kind": kind}
        if period is not None:
            params["period"] = period
        if limit is not None:
            params["limit"] = min(limit, MAX_FUNDAMENTAL_ROWS)
        body = await self._v2_json(
            f"/api/v2/data/fundamentals/{path_segment(instrument_key)}", params
        )
        return (body or {}).get("rows") or []

    async def search_v2(self, query: str, *, market: str, limit: int = 10) -> list[dict[str, Any]]:
        body = await self._v2_json(
            "/api/v2/data/search", {"q": query, "market": market, "limit": min(limit, 50)}
        )
        return (body or {}).get("results") or []

    async def get_news_v2(self, *, market: str, limit: int = 20) -> list[dict[str, Any]]:
        body = await self._v2_json("/api/v2/data/news", {"market": market, "limit": limit})
        return (body or {}).get("results") or []

    async def screen_v2(self, *, market: str, **filters: Any) -> list[dict[str, Any]]:
        params = {"market": market, **{k: v for k, v in filters.items() if v is not None}}
        body = await self._v2_json("/api/v2/data/screen", params)
        return (body or {}).get("results") or []

    async def get_options_chain_v2(self, underlying: str, **filters: Any) -> list[dict[str, Any]]:
        params = {"underlying": underlying, **{k: v for k, v in filters.items() if v is not None}}
        body = await self._v2_json("/api/v2/data/options/chain", params)
        return (body or {}).get("results") or []

    async def get_options_bars_v2(
        self, option_ticker: str, *, market: str, from_date: str | None, to_date: str | None
    ) -> list[dict[str, Any]]:
        params = {"market": market, "from": from_date, "to": to_date}
        body = await self._v2_json(
            f"/api/v2/data/options/bars/{path_segment(option_ticker)}",
            {k: v for k, v in params.items() if v},
        )
        return (body or {}).get("bars") or []
