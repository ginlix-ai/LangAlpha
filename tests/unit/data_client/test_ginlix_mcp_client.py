"""Error-sanitization guard for the sandbox-side ginlix-data client.

``GinlixMCPClient.fetch_short_data`` runs inside a Daytona sandbox and returns
its errors straight into agent-visible tool output. A stringified httpx error
embeds the request URL (internal host + query params), so the client routes
every non-HTTP failure through ``_error_dict``, which surfaces only the action
label plus the exception type — never the URL.
"""

from unittest.mock import AsyncMock

import httpx
import pytest

from src.data_client.ginlix_data.mcp_client import GinlixMCPClient

# Made-up internal endpoint — a stand-in whose host/path must never surface.
_LEAKY_URL = "https://data.internal.example:8005/api/v1/data/stocks/short-interest?ticker=AAPL"


def _no_url_leaked(msg: str) -> None:
    assert "https://" not in msg
    assert "data.internal.example" not in msg
    assert "/api/v1/data/" not in msg


class TestFetchShortDataErrorSanitization:
    @pytest.mark.asyncio
    async def test_both_connect_errors_omit_url(self):
        client = GinlixMCPClient()
        client.ensure = AsyncMock(return_value=True)
        client.request = AsyncMock(
            side_effect=httpx.ConnectError(f"connection refused to {_LEAKY_URL}")
        )

        result = await client.fetch_short_data("AAPL", data_type="both")

        assert result["symbol"] == "AAPL"
        assert result["source"] == "ginlix-data"
        # Only the action + exception type, no URL.
        assert result["short_interest_error"] == "Short interest fetch failed (ConnectError)"
        assert result["short_volume_error"] == "Short volume fetch failed (ConnectError)"
        _no_url_leaked(result["short_interest_error"])
        _no_url_leaked(result["short_volume_error"])
        # Failed fetches never populate the success keys.
        assert "short_interest" not in result
        assert "short_volume" not in result

    @pytest.mark.asyncio
    async def test_single_short_volume_connect_error_omits_url(self):
        client = GinlixMCPClient()
        client.ensure = AsyncMock(return_value=True)
        client.request = AsyncMock(
            side_effect=httpx.ConnectError(f"connection refused to {_LEAKY_URL}")
        )

        result = await client.fetch_short_data("AAPL", data_type="short_volume")

        assert result["short_volume_error"] == "Short volume fetch failed (ConnectError)"
        _no_url_leaked(result["short_volume_error"])
        # short_interest branch was never entered for this data_type.
        assert "short_interest_error" not in result
