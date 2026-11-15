"""Firecrawl's terms stand before a user's account is created.

Firecrawl for Platforms requires the user be put Firecrawl's Terms of Service
before ``POST /partner/v1/accounts``; an account used to be created on the
user's first relay call with nothing asked. Now the relay only asks, and the
user's accept creates the account, which is the only record kept.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, patch

import pytest

from src.server.services import firecrawl_accounts, vendor_terms
from src.server.services.egress import AccountUnavailable, RelayError, RelayRejection
from tests.unit.server.vendor_terms_fakes import FakeRedis, fake_cache

USER = "user-terms"


@contextmanager
def _firecrawl(partner: AsyncMock, published: list[dict]):
    """Per-user accounts on, with an in-memory account table and pending set."""
    redis = FakeRedis()
    stored: dict[str, str] = {}

    async def get_account_key(user_id):
        return stored.get(user_id)

    async def save_account(user_id, *, api_key, **_):
        stored[user_id] = api_key
        return api_key

    async def publish(user_id, event):
        published.append(event)

    with (
        patch.object(firecrawl_accounts.env, "FIRECRAWL_PLATFORM_KEY", "pk"),
        patch.object(firecrawl_accounts.env, "FIRECRAWL_ACCOUNT_EMAIL_DOMAIN", "example.com"),
        patch.object(firecrawl_accounts.db, "get_account_key", get_account_key),
        patch.object(firecrawl_accounts.db, "save_account", save_account),
        patch.object(firecrawl_accounts, "_remembered_refusal", AsyncMock(return_value=None)),
        patch.object(firecrawl_accounts, "_remember_refusal", AsyncMock()),
        patch.object(firecrawl_accounts, "_require_credits", AsyncMock()),
        patch.object(firecrawl_accounts, "_partner_post", partner),
        patch.object(vendor_terms, "get_cache_client", fake_cache(redis)),
        patch.object(vendor_terms, "publish_user_event", publish),
    ):
        yield stored


def _server():
    from src.server.services.egress import credentials
    from src.server.services.egress.platform_accounts import PLATFORM_ACCOUNTS

    return credentials.PlatformServer(
        config=AsyncMock(), account=PLATFORM_ACCOUNTS["firecrawl_account"]
    )


@pytest.mark.asyncio
async def test_the_relay_asks_and_the_accept_creates_the_account():
    from src.server.services.egress import credentials

    published: list[dict] = []
    partner = AsyncMock(return_value={"apiKey": "fc-new"})
    with _firecrawl(partner, published) as stored:
        server = _server()
        for _ in range(2):
            with pytest.raises(RelayRejection) as refused:
                await credentials._platform_credential(USER, server)
            assert refused.value.status == 403
            assert refused.value.code is RelayError.TERMS_REQUIRED
            assert refused.value.detail == "the user has not accepted Firecrawl's Terms of Service"
        partner.assert_not_awaited()
        firecrawl_accounts._remember_refusal.assert_not_awaited()
        # Asked once, however often the agent retries, and still the first ask.
        assert published == [{"type": "vendor_terms", "vendors": ["firecrawl"]}]
        first = await vendor_terms.pending(USER)
        assert [p["vendor"] for p in first] == ["firecrawl"]
        with pytest.raises(RelayRejection):
            await credentials._platform_credential(USER, server)
        assert await vendor_terms.pending(USER) == first

        assert await vendor_terms.accept(USER, "firecrawl") == {"vendor": "firecrawl"}
        partner.assert_awaited_once()
        assert stored == {USER: "fc-new"}
        assert await vendor_terms.pending(USER) == []
        assert published[-1] == {"type": "vendor_terms", "vendors": ["firecrawl"]}

        # A second accept (another tab) finds the account and asks no one.
        assert await vendor_terms.accept(USER, "firecrawl") == {"vendor": "firecrawl"}
        partner.assert_awaited_once()
        assert (await credentials._platform_credential(USER, server)).headers == {
            "authorization": "Bearer fc-new"
        }


@pytest.mark.asyncio
async def test_a_failed_accept_leaves_the_ask_pending():
    published: list[dict] = []
    partner = AsyncMock(side_effect=AccountUnavailable("cap_reached", "Firecrawl refused"))
    with _firecrawl(partner, published) as stored:
        with pytest.raises(AccountUnavailable):
            await firecrawl_accounts.firecrawl_key_for(USER)
        with pytest.raises(AccountUnavailable) as refused:
            await vendor_terms.accept(USER, "firecrawl")
        assert refused.value.code == "cap_reached"
        firecrawl_accounts._remember_refusal.assert_awaited_once()
        assert stored == {}
        assert [p["vendor"] for p in await vendor_terms.pending(USER)] == ["firecrawl"]
        assert published == [{"type": "vendor_terms", "vendors": ["firecrawl"]}]


@pytest.mark.asyncio
async def test_a_stored_key_and_a_shared_key_ask_nothing():
    ask = AsyncMock(side_effect=AccountUnavailable("terms_required", "no"))
    partner = AsyncMock()
    with (
        patch.object(vendor_terms, "ask", ask),
        patch.object(firecrawl_accounts, "_partner_post", partner),
    ):
        with (
            patch.object(firecrawl_accounts.env, "FIRECRAWL_PLATFORM_KEY", "pk"),
            patch.object(firecrawl_accounts.db, "get_account_key", AsyncMock(return_value="fc-old")),
        ):
            assert await firecrawl_accounts.firecrawl_key_for(USER) == "fc-old"
            await firecrawl_accounts.provision_firecrawl_account(USER)
        with (
            patch.object(firecrawl_accounts.env, "FIRECRAWL_PLATFORM_KEY", ""),
            patch.dict("os.environ", {"FIRECRAWL_API_KEY": "fc-shared"}),
        ):
            assert await firecrawl_accounts.firecrawl_key_for(USER) == "fc-shared"
            await firecrawl_accounts.provision_firecrawl_account(USER)
    ask.assert_not_awaited()
    partner.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_accept_during_the_admission_is_not_asked_again():
    """A relay call that read no account, then waited on its admission while
    the user accepted, put the cleared ask back and the dialog reopened."""
    published: list[dict] = []
    with _firecrawl(AsyncMock(), published) as stored:
        with pytest.raises(AccountUnavailable):
            await firecrawl_accounts.firecrawl_key_for(USER)

        async def accept_meanwhile(user_id):
            # What an accept does: the account, then the ask cleared.
            stored[USER] = "fc-new"
            await vendor_terms._clear(USER, "firecrawl")

        with patch.object(firecrawl_accounts, "_require_credits", accept_meanwhile):
            assert await firecrawl_accounts.firecrawl_key_for(USER) == "fc-new"
        assert stored == {USER: "fc-new"}
        assert await vendor_terms.pending(USER) == []


@pytest.mark.asyncio
async def test_an_accept_between_the_last_read_and_the_ask_leaves_no_ask():
    """An accept that cleared the ask after the relay's last read of the
    account, and before its ask was written, had the ask put back over an
    account that existed, and every open app asked again for up to an hour."""
    from tests.unit.server.vendor_terms_fakes import _Pipe

    published: list[dict] = []
    with _firecrawl(AsyncMock(), published) as stored:
        execute = _Pipe.execute

        async def accept_then_execute(pipe):
            # What an accept does, landing just before the ask is written:
            # the account, then the ask cleared.
            stored[USER] = "fc-new"
            await vendor_terms._clear(USER, "firecrawl")
            return await execute(pipe)

        with patch.object(_Pipe, "execute", accept_then_execute):
            assert await firecrawl_accounts.firecrawl_key_for(USER) == "fc-new"
        assert await vendor_terms.pending(USER) == []
        assert published == []


@pytest.mark.asyncio
@pytest.mark.parametrize(("spendable", "refused"), [(3.0, True), (4.0, False), (None, False)])
async def test_an_account_needs_a_balance_that_covers_the_cheapest_call(spendable, refused):
    """Any positive balance passed, so a user short of one call's price had
    an account created and the very call that asked for it refused."""
    with (
        patch(
            "src.server.services.egress.metering.spendable_for_paid_calls",
            AsyncMock(return_value=spendable),
        ),
        patch("src.server.services.metered_charges.user_credits", lambda meter, units: 4.0),
    ):
        if refused:
            with pytest.raises(RelayRejection) as raised:
                await firecrawl_accounts._require_credits("u1")
            assert raised.value.code == RelayError.COST_EXCEEDS_BALANCE
        else:
            await firecrawl_accounts._require_credits("u1")


def test_no_two_user_ids_share_an_alias():
    """An id shaped like another id's digest was passed through as it, so the
    two users got one address, and Firecrawl gives one address one account."""
    alias = firecrawl_accounts.alias_email
    uuid_id = "6f1c2a9e-0b7d-4c55-9e1a-2f3b4c5d6e7f"
    hashed = alias("User-A", "example.com")

    assert alias(uuid_id, "example.com") == f"{uuid_id}@example.com"
    assert hashed == alias("User-A", "example.com")
    assert hashed != alias("user-a", "example.com")
    assert alias(hashed.split("@")[0], "example.com") != hashed


@pytest.mark.parametrize(("status", "retryable"), [(500, True), (504, True), (502, True), (404, False)])
def test_a_partner_server_error_is_answered_as_one_to_retry(status, retryable):
    """A 500 or 504 from the partner API told the caller to stop, though it
    clears on a later attempt as a 502 does."""
    import httpx

    refused = firecrawl_accounts._unavailable(httpx.Response(status, json={}))

    assert (refused.code, refused.retryable) == ("upstream_error", retryable)
