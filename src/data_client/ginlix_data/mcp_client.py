"""Ginlix-data client for MCP servers running in Daytona sandboxes.

Uses file-based OAuth tokens for authentication, with auto-refresh on 401.
This is the sandbox counterpart to :class:`GinlixDataClient` (which uses
service tokens on the host).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import httpx

from ..market_data_provider import is_us_symbol
from ..normalize import normalize_bars
from .pagination import paginate_cursor

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Interval mapping (shared with MCP server for validation)
# ---------------------------------------------------------------------------

GINLIX_INTERVAL_MAP: dict[str, str] = {
    "1min": "1/minute",
    "5min": "5/minute",
    "15min": "15/minute",
    "30min": "30/minute",
    "1hour": "1/hour",
    "4hour": "4/hour",
    "1day": "1/day",
    "1week": "1/week",
    "1month": "1/month",
}

DAILY_INTERVALS: set[str] = {"daily", "1day"}

# ---------------------------------------------------------------------------
# Date/time utilities
# ---------------------------------------------------------------------------


def split_date_time(value: str | None) -> tuple[str | None, str | None]:
    """Split ``'YYYY-MM-DD HH:MM'`` into ``(date, time)``.

    Returns ``(date_part, time_part)``.  *time_part* is ``None`` for
    date-only values or midnight (``00:00``).
    """
    if not value:
        return None, None
    normalized = value.replace("T", " ")
    if " " in normalized:
        parts = normalized.split(" ", 1)
        time_part = parts[1].strip()
        if time_part.replace(":", "").replace("0", "") == "":
            return parts[0], None
        return parts[0], time_part
    return value, None


def filter_bars_by_time(
    bars: list[dict],
    start_time: str | None,
    end_time: str | None,
) -> list[dict]:
    """Filter normalized bars by time-of-day.  No-op if both times are None."""
    if not start_time and not end_time:
        return bars
    filtered = []
    for bar in bars:
        bar_date = bar.get("date", "")
        bar_time = ""
        if "T" in bar_date:
            bar_time = bar_date.split("T", 1)[1][:5]
        elif " " in bar_date:
            bar_time = bar_date.split(" ", 1)[1][:5]
        if not bar_time:
            filtered.append(bar)
            continue
        if start_time and bar_time < start_time[:5]:
            continue
        if end_time and bar_time > end_time[:5]:
            continue
        filtered.append(bar)
    return filtered


# ---------------------------------------------------------------------------
# Token file helpers
# ---------------------------------------------------------------------------

# Token file path. The host injects GINLIX_TOKEN_FILE with the exact path it
# uploaded to (_token_file_path = <work_dir>/_internal/.mcp_tokens.json), so the
# reader never has to guess. $HOME is NOT reliable here: Daytona sandboxes run as
# root ($HOME=/root) while the working dir is /home/workspace, so keying off $HOME
# read from the wrong place and the client always reported "not configured".
TOKEN_FILE = Path(
    os.environ.get("GINLIX_TOKEN_FILE", "/home/workspace/_internal/.mcp_tokens.json")
)

# Declared User-Agent for every sandbox→ginlix-data request (data + token
# refresh). Set explicitly so the traffic is self-identifying at the edge and
# does not depend on httpx's default UA — a generic client UA can be swept up by
# upstream bot mitigation, which would silently break the data path.
_USER_AGENT = "langalpha-sandbox/1.0"


def _load_tokens() -> dict:
    try:
        return json.loads(TOKEN_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_tokens(tokens: dict) -> None:
    TOKEN_FILE.write_text(json.dumps(tokens))


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_MAX_PAGES = 10  # Up to 500k bars (10 × 50k)


class GinlixMCPClient:
    """Sandbox-side ginlix-data client with OAuth token-file auth.

    Lazily initializes on first use.  Re-reads the token file until
    initialization succeeds, so tokens uploaded after MCP server start
    are picked up.
    """

    def __init__(self) -> None:
        self._http: httpx.AsyncClient | None = None

    # -- lifecycle -----------------------------------------------------------

    async def ensure(self) -> bool:
        """Ensure the HTTP client is initialized.  Returns availability."""
        if self._http is not None:
            return True

        tokens = _load_tokens()
        ginlix_url = tokens.get("ginlix_data_url") or os.getenv("GINLIX_DATA_URL", "")
        access_token = tokens.get("access_token", "")

        if ginlix_url and access_token:
            self._http = httpx.AsyncClient(
                base_url=ginlix_url.rstrip("/"),
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "User-Agent": _USER_AGENT,
                },
                timeout=30.0,
            )
            return True
        return False

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None

    # -- HTTP ----------------------------------------------------------------

    @staticmethod
    def _error_dict(action: str, e: Exception) -> dict:
        """Sanitized error dict for tool output; never embeds raw exception text.

        HTTP errors surface the status and response detail (parseable by the
        MCP servers' status→code mapper); anything else only the exception
        type — stringified httpx errors embed the request URL.
        """
        if isinstance(e, httpx.HTTPStatusError):
            try:
                detail = e.response.json().get("detail", e.response.text)
            except Exception:  # noqa: BLE001
                detail = e.response.text
            detail = str(detail)[:300]  # length-cap: never relay a full upstream body
            return {"error": f"ginlix-data error ({e.response.status_code}): {detail}"}
        return {"error": f"{action} failed ({type(e).__name__})"}

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """Make a ginlix-data request with auto-refresh on 401."""
        if not await self.ensure():
            raise RuntimeError("ginlix-data client not initialized")
        assert self._http is not None
        resp = await self._http.request(method, url, **kwargs)
        if resp.status_code == 401:
            new_token = await self._refresh_access_token()
            if new_token:
                self._http.headers["Authorization"] = f"Bearer {new_token}"
                resp = await self._http.request(method, url, **kwargs)
        return resp

    async def _refresh_access_token(self) -> str | None:
        """Refresh access token via OAuth2.  Persists new tokens to file."""
        tokens = _load_tokens()
        auth_url = tokens.get("auth_service_url", "")
        refresh_token = tokens.get("refresh_token", "")
        client_id = tokens.get("client_id", "")
        if not all([auth_url, refresh_token, client_id]):
            return None
        try:
            async with httpx.AsyncClient(
                headers={"User-Agent": _USER_AGENT}
            ) as client:
                resp = await client.post(
                    f"{auth_url}/oauth/token",
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id": client_id,
                    },
                    timeout=10,
                )
                if resp.status_code != 200:
                    # A non-200 is not an exception, so without this a misrouted
                    # or rejected refresh fails completely silently.
                    logger.warning(
                        "Token refresh rejected: HTTP %s %s",
                        resp.status_code,
                        resp.text[:200],
                    )
                    return None
                data = resp.json()
        except Exception as exc:
            logger.warning("Token refresh failed: %s", exc)
            return None

        tokens["access_token"] = data["access_token"]
        if data.get("refresh_token"):
            tokens["refresh_token"] = data["refresh_token"]
        try:
            _save_tokens(tokens)
        except Exception as exc:  # noqa: BLE001
            # The token is valid regardless; a failed write only costs the next
            # process one more refresh, so never discard it over this.
            logger.warning("Token file write failed: %s", exc)
        return data["access_token"]

    # -- shared helpers ------------------------------------------------------

    async def _fetch_paginated_bars(
        self, url: str, params: dict[str, Any],
    ) -> list[dict]:
        """Cursor-based pagination loop for aggregate bar endpoints."""
        all_bars: list[dict] = []
        multiplier = params["multiplier"]
        timespan = params["timespan"]
        for _page in range(_MAX_PAGES):
            resp = await self.request("GET", url, params=params)
            resp.raise_for_status()
            body = resp.json()
            all_bars.extend(body.get("results", []))
            next_cursor = body.get("next_cursor")
            if not next_cursor:
                break
            params = {
                "multiplier": multiplier,
                "timespan": timespan,
                "limit": 50000,
                "cursor": next_cursor,
            }
        return all_bars

    # -- data fetching -------------------------------------------------------

    async def fetch_stock_data(
        self,
        symbol: str,
        interval: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict] | dict | None:
        """Fetch stock OHLCV from ginlix-data with auto-pagination.

        Returns:
            ``list[dict]``: Normalized OHLCV bars on success.
            ``dict``: Error dict on HTTP/validation error.
            ``None``: Not available (non-US symbol, no client, unsupported
            interval, network error) — caller should fall back.
        """
        if not is_us_symbol(symbol):
            return None

        if not await self.ensure():
            return None

        interval_lower = interval.lower()
        ginlix_interval = GINLIX_INTERVAL_MAP.get(interval_lower)
        if interval_lower in DAILY_INTERVALS:
            ginlix_interval = "1/day"
        if not ginlix_interval:
            return None

        _, start_time = split_date_time(start_date)
        _, end_time = split_date_time(end_date)
        from_day, _ = split_date_time(start_date)
        to_day, _ = split_date_time(end_date)

        if not from_day or not to_day:
            return None

        intraday = interval_lower not in DAILY_INTERVALS
        multiplier, timespan = ginlix_interval.split("/")
        params: dict[str, Any] = {
            "multiplier": multiplier,
            "timespan": timespan,
            "limit": 50000,
            "from": from_day,
            "to": to_day,
        }

        try:
            all_bars = await self._fetch_paginated_bars(
                f"/api/v1/data/aggregates/stock/{symbol}", params,
            )
            normalized = normalize_bars(all_bars, symbol, intraday=intraday)
            if intraday:
                normalized = filter_bars_by_time(normalized, start_time, end_time)
            return normalized
        except httpx.HTTPStatusError as e:
            try:
                detail = e.response.json().get("detail", e.response.text)
            except Exception:
                detail = e.response.text
            status = e.response.status_code
            if 400 <= status < 500 and status != 429:
                return {"error": f"ginlix-data error ({status}): {detail}"}
            logger.warning("ginlix-data %s for %s: %s", status, symbol, detail)
            return None
        except Exception:
            logger.debug("ginlix-data fetch failed for %s", symbol, exc_info=True)
            return None

    async def fetch_options_chain(
        self,
        underlying_ticker: str,
        contract_type: str | None = None,
        expiration_date_gte: str | None = None,
        expiration_date_lte: str | None = None,
        strike_price_gte: float | None = None,
        strike_price_lte: float | None = None,
        limit: int = 50,
    ) -> dict:
        """Fetch options contracts with auto-pagination.

        Returns a dict with ``results`` list of contracts, or an error dict.
        """
        if not await self.ensure():
            return {"error": "Options data requires ginlix-data (not configured)."}

        page_size = min(limit, 1000)
        params: dict[str, Any] = {
            "underlying_ticker": underlying_ticker,
            "limit": page_size,
        }
        if contract_type:
            params["contract_type"] = contract_type
        if expiration_date_gte:
            params["expiration_date.gte"] = expiration_date_gte
        if expiration_date_lte:
            params["expiration_date.lte"] = expiration_date_lte
        if strike_price_gte is not None:
            params["strike_price.gte"] = strike_price_gte
        if strike_price_lte is not None:
            params["strike_price.lte"] = strike_price_lte

        try:
            async def _fetch_page(p: dict) -> dict:
                resp = await self.request(
                    "GET", "/api/v1/data/options/contracts", params=p,
                )
                resp.raise_for_status()
                return resp.json()

            results = await paginate_cursor(_fetch_page, params, limit=limit)
            return {"results": results}
        except Exception as e:  # noqa: BLE001
            return self._error_dict("Options chain fetch", e)

    async def fetch_options_prices(
        self,
        options_ticker: str,
        from_date: str | None = None,
        to_date: str | None = None,
        interval: str = "1day",
    ) -> list[dict] | dict:
        """Fetch OHLCV bars for an options contract.

        Returns normalized OHLCV bars on success, or an error dict.
        """
        if not await self.ensure():
            return {"error": "Options data requires ginlix-data (not configured)."}

        interval_lower = interval.lower()
        ginlix_interval = GINLIX_INTERVAL_MAP.get(interval_lower)
        if interval_lower in DAILY_INTERVALS:
            ginlix_interval = "1/day"
        if not ginlix_interval:
            return {"error": f"Unsupported interval: {interval}"}

        multiplier, timespan = ginlix_interval.split("/")
        params: dict[str, Any] = {
            "multiplier": multiplier,
            "timespan": timespan,
            "limit": 50000,
        }
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date

        try:
            all_bars = await self._fetch_paginated_bars(
                f"/api/v1/data/aggregates/option/{options_ticker}", params,
            )
            intraday = interval_lower not in DAILY_INTERVALS
            normalized = normalize_bars(all_bars, options_ticker, intraday=intraday)
            return normalized
        except Exception as e:  # noqa: BLE001
            return self._error_dict("Options prices fetch", e)

    async def fetch_options_snapshot(
        self,
        options_tickers: str | list[str],
    ) -> dict:
        """Fetch real-time snapshots for options contracts.

        Accepts a single ticker string or a list of tickers.
        Returns a dict with ``data`` list of snapshot results, or an error dict.
        """
        if not await self.ensure():
            return {"error": "Options data requires ginlix-data (not configured)."}

        if isinstance(options_tickers, str):
            options_tickers = [options_tickers]

        try:
            resp = await self.request(
                "GET",
                "/api/v1/data/snapshots/options",
                params={"symbols": ",".join(options_tickers)},
            )
            resp.raise_for_status()
            body = resp.json()
            results = [
                r for r in body.get("results", [])
                if r.get("error") != "NOT_FOUND"
            ]
            return {"count": len(results), "data": results, "source": "ginlix-data"}
        except Exception as e:  # noqa: BLE001
            return self._error_dict("Options snapshot fetch", e)

    async def fetch_short_data(
        self,
        symbol: str,
        data_type: str = "both",
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 20,
    ) -> dict:
        """Fetch short interest and/or short volume data."""
        if not await self.ensure():
            return {"error": "Short data requires ginlix-data (not configured)."}

        result: dict[str, Any] = {"symbol": symbol, "source": "ginlix-data"}

        if data_type in ("short_interest", "both"):
            params: dict[str, Any] = {
                "ticker": symbol,
                "limit": limit,
                "sort": "settlement_date.desc",
            }
            if from_date:
                params["settlement_date.gte"] = from_date
            if to_date:
                params["settlement_date.lte"] = to_date
            try:
                resp = await self.request(
                    "GET", "/api/v1/data/stocks/short-interest", params=params,
                )
                resp.raise_for_status()
                result["short_interest"] = resp.json().get("results", [])
            except Exception as e:  # noqa: BLE001
                result["short_interest_error"] = self._error_dict("Short interest fetch", e)["error"]

        if data_type in ("short_volume", "both"):
            params = {
                "ticker": symbol,
                "limit": limit,
                "sort": "date.desc",
            }
            if from_date:
                params["date.gte"] = from_date
            if to_date:
                params["date.lte"] = to_date
            try:
                resp = await self.request(
                    "GET", "/api/v1/data/stocks/short-volume", params=params,
                )
                resp.raise_for_status()
                result["short_volume"] = resp.json().get("results", [])
            except Exception as e:  # noqa: BLE001
                result["short_volume_error"] = self._error_dict("Short volume fetch", e)["error"]

        return result
