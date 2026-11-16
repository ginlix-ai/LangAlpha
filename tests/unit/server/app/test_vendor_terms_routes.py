"""The app's routes for the vendor terms the user is asked to accept."""

from __future__ import annotations

import dataclasses
import time
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.server.app import vendor_terms as terms_app
from src.server.services import vendor_terms
from src.server.services.egress import AccountUnavailable, RelayError, RelayRejection
from src.server.services.egress.platform_accounts import PLATFORM_ACCOUNTS
from tests.conftest import create_test_app
from tests.unit.server.dependencies.reach_world import act_as_computer
from tests.unit.server.vendor_terms_fakes import FakeRedis, fake_cache

USER = "test-user-123"
TERMS_URL = "https://www.firecrawl.dev/terms-of-service"
PENDING_KEY = f"vendorterms:{{{USER}}}:firecrawl"


@pytest.fixture
def world(monkeypatch):
    redis = FakeRedis()
    published: list[dict] = []
    provision = AsyncMock()

    async def publish(user_id, event):
        published.append(event)

    account = dataclasses.replace(PLATFORM_ACCOUNTS["firecrawl_account"], provision=provision)
    monkeypatch.setitem(PLATFORM_ACCOUNTS, "firecrawl_account", account)
    monkeypatch.setattr(vendor_terms, "get_cache_client", fake_cache(redis))
    monkeypatch.setattr(vendor_terms, "publish_user_event", publish)
    redis.values[PENDING_KEY] = int(time.time() * 1000)
    app = create_test_app(terms_app.router)
    return app, redis, provision, published


async def _client(app) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_the_app_lists_dismisses_and_accepts_terms(world):
    app, redis, provision, published = world
    async with await _client(app) as c:
        pending = (await c.get("/api/v1/vendor-terms/pending")).json()["pending"]
        assert [{k: v for k, v in p.items() if k != "requested_at"} for p in pending] == [
            {"vendor": "firecrawl", "name": "Firecrawl", "terms_url": TERMS_URL}
        ]
        datetime.fromisoformat(pending[0]["requested_at"])

        dismissed = await c.post("/api/v1/vendor-terms/firecrawl/dismiss")
        assert dismissed.status_code == 204
        assert (await c.get("/api/v1/vendor-terms/pending")).json() == {"pending": []}

        redis.values[PENDING_KEY] = int(time.time() * 1000)
        accepted = await c.post("/api/v1/vendor-terms/firecrawl/accept")
        assert accepted.status_code == 200
        assert accepted.json() == {"vendor": "firecrawl"}
        provision.assert_awaited_once_with(USER)
        assert (await c.get("/api/v1/vendor-terms/pending")).json() == {"pending": []}

        # Nothing lists what was accepted: the account is the record.
        assert (await c.get("/api/v1/vendor-terms")).status_code in (404, 405)
        for verb in ("accept", "dismiss"):
            assert (await c.post(f"/api/v1/vendor-terms/nosuch/{verb}")).status_code == 404

    assert published == [{"type": "vendor_terms", "vendors": ["firecrawl"]}] * 2


@pytest.mark.parametrize(
    ("refusal", "status", "detail"),
    [
        (
            AccountUnavailable("rate_limited", "Firecrawl rate limit", retryable=True),
            503,
            "Your Firecrawl account could not be set up",
        ),
        (
            AccountUnavailable("cap_reached", "Firecrawl refused: cap"),
            409,
            "Your Firecrawl account could not be set up",
        ),
        (
            RelayRejection(402, RelayError.CREDITS_EXHAUSTED, "the user has no credits left"),
            402,
            "the user has no credits left",
        ),
    ],
)
@pytest.mark.asyncio
async def test_an_accept_that_cannot_create_the_account_leaves_it_pending(
    world, refusal, status, detail
):
    app, redis, provision, published = world
    provision.side_effect = refusal
    async with await _client(app) as c:
        refused = await c.post("/api/v1/vendor-terms/firecrawl/accept")
        assert refused.status_code == status
        assert refused.json()["detail"] == detail
        pending = (await c.get("/api/v1/vendor-terms/pending")).json()["pending"]
    assert [p["vendor"] for p in pending] == ["firecrawl"]
    assert published == []


@pytest.mark.asyncio
async def test_a_computer_token_accepts_nothing(world):
    """The agent's own token accepting a vendor's terms would defeat asking."""
    app, _, provision, _ = world
    act_as_computer(app)
    async with await _client(app) as c:
        for method, path in (
            ("GET", "/api/v1/vendor-terms/pending"),
            ("POST", "/api/v1/vendor-terms/firecrawl/accept"),
            ("POST", "/api/v1/vendor-terms/firecrawl/dismiss"),
        ):
            assert (await c.request(method, path)).status_code == 403
    provision.assert_not_awaited()
