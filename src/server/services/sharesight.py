"""
Sharesight API client.

OAuth2 Client Credentials authentication with in-memory token caching.
Fetches portfolio holdings and maps them to LangAlpha's PortfolioHolding schema.
"""

import logging
import os
import time
from decimal import Decimal
from uuid import uuid5, NAMESPACE_URL

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.sharesight.com"
_TOKEN_URL = f"{_BASE_URL}/oauth2/token"

# Map Sharesight instrument types to LangAlpha InstrumentType enum values
_INSTRUMENT_TYPE_MAP = {
    "CommonStock": "stock",
    "ExchangeTradedFund": "etf",
    "MutualFund": "etf",
    "FixedInterest": "stock",
    "CryptoCurrency": "crypto",
    "Commodity": "commodity",
    "Currency": "currency",
    "Index": "index",
}


class SharesightClient:
    """Async Sharesight API client with OAuth2 token management."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        portfolio_name: str | None = None,
    ):
        self._explicit_client_id = client_id
        self._explicit_client_secret = client_secret
        self._explicit_portfolio_name = portfolio_name
        self._access_token: str | None = None
        self._token_expires_at: float = 0
        self._http: httpx.AsyncClient | None = None

    @property
    def _client_id(self) -> str:
        return self._explicit_client_id or os.getenv("SHARESIGHT_CLIENT_ID", "")

    @property
    def _client_secret(self) -> str:
        return self._explicit_client_secret or os.getenv("SHARESIGHT_CLIENT_SECRET", "")

    @property
    def _portfolio_name(self) -> str:
        return self._explicit_portfolio_name or os.getenv("SHARESIGHT_PORTFOLIO_NAME", "hunterbray")

    @property
    def is_configured(self) -> bool:
        return bool(self._client_id and self._client_secret)

    async def _get_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=30.0)
        return self._http

    async def _ensure_token(self) -> None:
        # Refresh if within 5 minutes of expiry
        if self._access_token and time.time() < self._token_expires_at - 300:
            return

        http = await self._get_http()
        response = await http.post(
            _TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )

        if response.status_code != 200:
            raise RuntimeError(
                f"Sharesight authentication failed (HTTP {response.status_code}): "
                f"{response.text}"
            )

        data = response.json()
        self._access_token = data["access_token"]
        self._token_expires_at = time.time() + data.get("expires_in", 1800)
        logger.info("Sharesight token acquired, expires in %ds", data.get("expires_in", 1800))

    async def _api_get(self, path: str) -> dict:
        await self._ensure_token()
        http = await self._get_http()
        response = await http.get(
            f"{_BASE_URL}{path}",
            headers={"Authorization": f"Bearer {self._access_token}"},
        )
        response.raise_for_status()
        return response.json()

    async def get_portfolios(self) -> list[dict]:
        data = await self._api_get("/api/v2/portfolios.json")
        return data.get("portfolios", [])

    async def get_holdings(self, portfolio_id: int) -> list[dict]:
        data = await self._api_get(f"/api/v2/portfolios/{portfolio_id}/valuation.json")
        return data.get("holdings", [])

    async def get_trades(self, portfolio_id: int) -> list[dict]:
        data = await self._api_get(f"/api/v2/portfolios/{portfolio_id}/trades.json")
        return data.get("trades", [])

    async def get_portfolio_holdings(self) -> list[dict]:
        """Fetch holdings from the configured portfolio and map to LangAlpha schema."""
        portfolios = await self.get_portfolios()

        portfolio = next(
            (p for p in portfolios if p["name"].lower() == self._portfolio_name.lower()),
            None,
        )
        if not portfolio:
            raise ValueError(
                f"Portfolio '{self._portfolio_name}' not found in Sharesight. "
                f"Available: {[p['name'] for p in portfolios]}"
            )

        pid = portfolio["id"]
        raw_holdings = await self.get_holdings(pid)

        # Fetch trades to calculate average cost per holding
        try:
            trades = await self.get_trades(pid)
            cost_by_holding = self._calc_avg_costs(trades)
        except Exception:
            logger.warning("Failed to fetch trades for cost basis, continuing without")
            cost_by_holding = {}

        return [self._map_holding(h, cost_by_holding) for h in raw_holdings]

    @staticmethod
    def _calc_avg_costs(trades: list[dict]) -> dict[int, Decimal]:
        """Calculate weighted average cost per holding from trade history."""
        from collections import defaultdict
        totals: dict[int, dict] = defaultdict(lambda: {"qty": Decimal(0), "cost": Decimal(0)})
        for t in trades:
            hid = t.get("holding_id")
            if not hid:
                continue
            qty = Decimal(str(t.get("quantity", 0) or 0))
            price = Decimal(str(t.get("price", 0) or 0))
            totals[hid]["qty"] += qty
            totals[hid]["cost"] += qty * price
        return {
            hid: d["cost"] / d["qty"] if d["qty"] else Decimal(0)
            for hid, d in totals.items()
        }

    def _map_holding(self, holding: dict, cost_by_holding: dict[int, Decimal] | None = None) -> dict:
        quantity = Decimal(str(holding.get("quantity", 0)))
        value = Decimal(str(holding.get("value", 0)))
        sharesight_price = float(value / quantity) if quantity else 0.0

        holding_id = holding.get("id", 0)
        average_cost = (cost_by_holding or {}).get(holding_id)
        stable_uuid = uuid5(NAMESPACE_URL, f"sharesight:{holding_id}")

        return {
            "user_portfolio_id": stable_uuid,
            "user_id": "sharesight",
            "symbol": holding.get("symbol", ""),
            "instrument_type": "stock",
            "quantity": quantity,
            "average_cost": average_cost,
            "exchange": holding.get("market"),
            "currency": "USD",
            "account_name": self._portfolio_name,
            "notes": None,
            "metadata": {"sharesight_price": sharesight_price, "sharesight_value": float(value)},
            "name": holding.get("name"),
            "first_purchased_at": None,
            "created_at": "2000-01-01T00:00:00Z",
            "updated_at": "2000-01-01T00:00:00Z",
        }

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None


# Module-level singleton — import and use directly
sharesight_client = SharesightClient()
