"""A user's Firecrawl key that Firecrawl revoked is replaced, not dropped.

The stored key was returned forever, so a key Firecrawl rotated or revoked
failed every call and nothing recovered. Firecrawl's MCP reports a dead key
as a tool error inside a 200, so the relay never saw a 401 to act on.
Dropping the row would recover, but the row is the record that the user
accepted Firecrawl's terms.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.server.services import firecrawl_accounts
from src.server.services.egress.firecrawl_meter import FirecrawlMeter
from src.server.services.egress.jsonrpc import CanonicalRequest
from src.server.services.egress.metering import MeteredCall

USER = "user-recheck"


@contextmanager
def _account(stored_key: str, *, usage_status: int, partner_key: str = "fc-new"):
    """Per-user accounts on, with an in-memory row, lock and credit-usage
    endpoint."""
    stored = {USER: stored_key}
    locks: set[str] = set()
    probes: list[str] = []

    async def get_account_key(user_id):
        return stored.get(user_id)

    async def replace_account_key(user_id, *, rejected, api_key):
        if stored.get(user_id) == rejected:
            stored[user_id] = api_key
        return stored.get(user_id)

    async def acquire_lock(key, token, ttl_ms):
        if key in locks:
            return False
        locks.add(key)
        return True

    def _usage(request: httpx.Request) -> httpx.Response:
        probes.append(request.headers["authorization"])
        return httpx.Response(usage_status, json={"success": usage_status == 200})

    partner = AsyncMock(return_value={"apiKey": partner_key})
    client = httpx.AsyncClient(transport=httpx.MockTransport(_usage))
    with (
        patch.object(firecrawl_accounts.env, "FIRECRAWL_PLATFORM_KEY", "pk"),
        patch.object(firecrawl_accounts.env, "FIRECRAWL_ACCOUNT_EMAIL_DOMAIN", "example.com"),
        patch.object(firecrawl_accounts.db, "get_account_key", get_account_key),
        patch.object(firecrawl_accounts.db, "replace_account_key", replace_account_key),
        patch.object(firecrawl_accounts, "_partner_post", partner),
        patch.object(firecrawl_accounts, "_get_client", lambda: client),
        patch(
            "src.utils.cache.redis_cache.get_cache_client",
            lambda: SimpleNamespace(acquire_lock=acquire_lock),
        ),
    ):
        yield SimpleNamespace(stored=stored, probes=probes, partner=partner)


@pytest.mark.asyncio
async def test_a_key_firecrawl_refuses_is_replaced_by_the_accounts_current_one():
    with _account("fc-old", usage_status=401) as account:
        await firecrawl_accounts.recheck_firecrawl_key(USER)

        assert account.probes == ["Bearer fc-old"]
        account.partner.assert_awaited_once_with(
            "/partner/v1/accounts", {"email": f"{USER}@example.com"}
        )
        assert account.stored == {USER: "fc-new"}


@pytest.mark.asyncio
async def test_a_key_firecrawl_accepts_is_left_alone():
    with _account("fc-old", usage_status=200) as account:
        await firecrawl_accounts.recheck_firecrawl_key(USER)

        account.partner.assert_not_awaited()
        assert account.stored == {USER: "fc-old"}


@pytest.mark.asyncio
async def test_a_second_error_within_the_minute_does_not_check_again():
    with _account("fc-old", usage_status=401, partner_key="fc-old") as account:
        await firecrawl_accounts.recheck_firecrawl_key(USER)
        await firecrawl_accounts.recheck_firecrawl_key(USER)

        assert len(account.probes) == 1
        account.partner.assert_awaited_once()
        # The same key back: nothing to replace it with, and the row stays.
        assert account.stored == {USER: "fc-old"}


def _settled_call(reply: dict) -> tuple[MeteredCall, AsyncMock]:
    recheck = AsyncMock()
    prepared = MagicMock()
    prepared.grant = {"user_id": USER, "server_name": "firecrawl"}
    prepared.platform.account.recheck_key = recheck
    prepared.canonical = CanonicalRequest(
        body=b"", method="tools/call", tool_name="firecrawl_map",
        is_notification=False, arguments={"url": "https://example.com"},
    )
    call = MeteredCall(
        meter=FirecrawlMeter(), prepared=prepared, hold_id=7,
        call_id=None, thread_id=None, catalog={},
    )
    call.dispatch.status = 200
    call.observe(json.dumps({"jsonrpc": "2.0", "id": 1, "result": reply}).encode())
    return call, recheck


@pytest.mark.asyncio
async def test_an_error_reply_rechecks_the_users_key_and_a_good_one_never_does():
    failed, failed_recheck = _settled_call(
        {"isError": True, "content": [{"type": "text", "text": "The key is invalid or revoked."}]}
    )
    answered, answered_recheck = _settled_call(
        {"content": [{"type": "text", "text": json.dumps({"success": True, "links": []})}]}
    )
    with (
        patch("src.server.services.egress.metering.release", new=AsyncMock()),
        patch("src.server.services.egress.metering.settle_call", new=AsyncMock()),
        patch("src.server.services.egress.metering.mark_job", new=AsyncMock()),
        patch("src.server.services.egress.metering.stage", new=AsyncMock()),
    ):
        await failed.finish(200, complete=True)
        await answered.finish(200, complete=True)
        # Detached from the settle, so it may still be queued.
        await asyncio.sleep(0)

    failed_recheck.assert_awaited_once_with(USER)
    answered_recheck.assert_not_awaited()
