"""Tests for the orders router (app/orders.py).

Covers the filter passthrough, the keyset cursor echo, the projection that
keeps a vendor's ``raw``/``extras`` off the wire, and that an attempt belonging
to another user is a 404 rather than a readable row.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from urllib.parse import quote

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from src.server.database.order_attempts import decode_cursor, encode_cursor
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

_USER = "test-user-123"
_ATTEMPT = "11111111-2222-4333-8444-555555555555"
_CURSOR = encode_cursor(
    {"created_at": datetime(2026, 9, 9, 6, 51, 19, tzinfo=timezone.utc), "attempt_id": _ATTEMPT}
)

_LIST = "src.server.app.orders.list_attempts"
_GET = "src.server.app.orders.get_attempt"


def _row(**overrides):
    row = {
        "attempt_id": _ATTEMPT,
        "user_id": _USER,
        "workspace_id": "22222222-2222-4222-8222-222222222222",
        "thread_id": "33333333-3333-4333-8333-333333333333",
        "conversation_response_id": "44444444-4444-4444-8444-444444444444",
        "turn_index": 3,
        "tool_call_id": "call_abc",
        "server": "moomoo",
        "vendor": "moomoo",
        "tool": "sim_trade_input_order",
        "action": "place",
        "mode": "paper",
        "account_ref": "1234567",
        "args": {"code": "US.AAPL", "qty": 1},
        "args_sha256": "deadbeef",
        "order_json": {
            "vendor": "moomoo",
            "account_ref": "1234567",
            "mode": "paper",
            "asset_class": "equity",
            "instrument": {"kind": "equity", "symbol": "AAPL", "venue": "US"},
            "side": "buy",
            "qty": "1",
            "order_type": "limit",
            "limit_price": "180.50",
            "time_in_force": "day",
            "currency": "USD",
            "extras": {"trd_env": "SIMULATE"},
            "raw": {"secret_looking_vendor_body": True},
        },
        "approval_required": True,
        "status": "submitted",
        "decided_at": "2026-09-09T06:51:19+00:00",
        "decision_message": "ok",
        "executed_at": "2026-09-09T06:51:20+00:00",
        "completed_at": None,
        "vendor_order_id": "900104",
        "route": {"market": "100"},
        "action_url": None,
        "filled_qty": Decimal("4"),
        "avg_fill_price": Decimal("318.50"),
        "fees": {"amount": "0.35", "currency": "USD"},
        "parent_attempt_id": None,
        "result_sha256": "cafe",
        "failure": None,
        "created_at": "2026-09-09T06:51:19+00:00",
        "updated_at": "2026-09-09T06:51:20+00:00",
    }
    row.update(overrides)
    return row


@pytest_asyncio.fixture
async def client():
    from src.server.app.orders import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def test_list_projects_the_row_without_vendor_payloads(client):
    with patch(_LIST, new=AsyncMock(return_value=([_row()], None))):
        resp = await client.get("/api/v1/orders")
    assert resp.status_code == 200
    body = resp.json()
    assert body["next_cursor"] is None
    item = body["items"][0]
    assert item["attempt_id"] == _ATTEMPT
    assert item["vendor"] == "moomoo"
    assert item["asset_class"] == "equity"
    assert item["vendor_order_id"] == "900104"
    assert item["route"] == {"market": "100"}
    assert item["order"]["instrument"] == {
        "kind": "equity",
        "symbol": "AAPL",
        "venue": "US",
    }
    assert item["order"]["limit_price"] == "180.50"
    # The fill travels as text: NUMERIC is exact and a JSON number is not.
    assert item["filled_qty"] == "4"
    assert item["avg_fill_price"] == "318.50"
    assert item["fees"] == {"amount": "0.35", "currency": "USD"}
    # The vendor's own body and the unmapped fields never leave the ledger,
    # and neither does the args map or the call identity.
    assert "raw" not in item["order"]
    assert "extras" not in item["order"]
    assert "args" not in item
    assert "tool_call_id" not in item
    assert "user_id" not in item


async def test_an_amend_answers_with_the_order_it_acts_on(client):
    """``target_ref`` is how the list pairs a cancel with its placement."""
    row = _row(
        tool="sim_trade_cancel_order",
        action="cancel",
        order_json={
            "vendor": "moomoo",
            "mode": "paper",
            "target_ref": "900001",
            "extras": {"order_id": "900001"},
        },
        vendor_order_id="900001",
        parent_attempt_id="66666666-6666-4666-8666-666666666666",
    )
    with patch(_GET, new=AsyncMock(return_value=row)):
        body = (await client.get(f"/api/v1/orders/{_ATTEMPT}")).json()
    assert body["order"]["target_ref"] == "900001"
    assert body["parent_attempt_id"] == "66666666-6666-4666-8666-666666666666"


async def test_a_staged_instruction_answers_with_the_page_that_releases_it(client):
    """The link is a ledger column, so the list offers it without a receipt."""
    row = _row(
        vendor="ibkr",
        mode="staged",
        action="stage",
        action_url="https://www.example.com/sso/resolver#/orders",
    )
    with patch(_GET, new=AsyncMock(return_value=row)):
        body = (await client.get(f"/api/v1/orders/{_ATTEMPT}")).json()
    assert body["action_url"] == "https://www.example.com/sso/resolver#/orders"


async def test_list_scopes_to_the_caller_and_forwards_every_filter(client):
    spy = AsyncMock(return_value=([], None))
    with patch(_LIST, new=spy):
        resp = await client.get(
            "/api/v1/orders",
            params={
                "vendor": "moomoo",
                "mode": "paper",
                "status": ["submitted", "filled"],
                "asset_class": "equity",
                "cursor": _CURSOR,
                "limit": 7,
            },
        )
    assert resp.status_code == 200
    args, kwargs = spy.call_args
    assert args == (_USER,)
    assert kwargs == {
        "vendor": "moomoo",
        "mode": "paper",
        "status": ["submitted", "filled"],
        "asset_class": "equity",
        "cursor": _CURSOR,
        "limit": 7,
    }


async def test_list_sends_no_filter_when_none_asked_for(client):
    spy = AsyncMock(return_value=([], None))
    with patch(_LIST, new=spy):
        await client.get("/api/v1/orders", params={"vendor": "", "status": ""})
    kwargs = spy.call_args.kwargs
    assert kwargs["vendor"] is None
    assert kwargs["status"] is None
    assert kwargs["cursor"] is None
    assert kwargs["limit"] == 50


async def test_list_forwards_the_page_size_for_the_ledger_to_clamp(client):
    # One bound, and it is the statement's: the ceiling protects the index the
    # page is served from, so the router does not keep a second copy of it.
    spy = AsyncMock(return_value=([], None))
    with patch(_LIST, new=spy):
        await client.get("/api/v1/orders", params={"limit": 5000})
    assert spy.call_args.kwargs["limit"] == 5000


async def test_list_echoes_the_next_cursor(client):
    cursor = _CURSOR
    with patch(_LIST, new=AsyncMock(return_value=([_row()], cursor))):
        resp = await client.get("/api/v1/orders", params={"limit": 1})
    assert resp.json()["next_cursor"] == cursor


async def test_detail_returns_the_owner_row(client):
    with patch(_GET, new=AsyncMock(return_value=_row())):
        resp = await client.get(f"/api/v1/orders/{_ATTEMPT}")
    assert resp.status_code == 200
    assert resp.json()["attempt_id"] == _ATTEMPT


async def test_detail_404s_on_another_users_attempt(client):
    with patch(_GET, new=AsyncMock(return_value=_row(user_id="someone-else"))):
        resp = await client.get(f"/api/v1/orders/{_ATTEMPT}")
    assert resp.status_code == 404


async def test_detail_404s_on_a_missing_or_malformed_id(client):
    with patch(_GET, new=AsyncMock(return_value=None)) as spy:
        assert (await client.get(f"/api/v1/orders/{_ATTEMPT}")).status_code == 404
        # A non-UUID never reaches the ledger: the column is a uuid and the
        # query would raise rather than answer "not found".
        assert (await client.get("/api/v1/orders/not-a-uuid")).status_code == 404
    assert spy.await_count == 1


async def test_detail_carries_the_failure_and_the_timeline(client):
    row = _row(
        status="failed",
        completed_at="2026-09-09T06:51:25+00:00",
        failure={"kind": "vendor", "code": "-1", "message": "insufficient funds"},
    )
    with patch(_GET, new=AsyncMock(return_value=row)):
        body = (await client.get(f"/api/v1/orders/{_ATTEMPT}")).json()
    assert body["status"] == "failed"
    assert body["failure"] == {
        "kind": "vendor",
        "code": "-1",
        "message": "insufficient funds",
    }
    assert body["decided_at"] and body["executed_at"] and body["completed_at"]


async def test_an_attempt_without_a_normalized_order_still_answers(client):
    """An exercise or a cancel has no order shape, and must not 500."""
    with patch(_GET, new=AsyncMock(return_value=_row(order_json=None))):
        body = (await client.get(f"/api/v1/orders/{_ATTEMPT}")).json()
    assert body["order"] is None
    assert body["asset_class"] is None


async def test_list_rejects_a_cursor_it_cannot_read(client):
    # A bare `<created_at>|<attempt_id>` is what the token looks like once its
    # `+` offset has been eaten by a query string. Answering it with page one
    # would page the caller in a circle forever, so it is a 400.
    #
    # Unpatched on purpose: the real ledger refuses the token before it opens a
    # connection, which is why there is one decode rather than a check here and
    # a decode there.
    resp = await client.get(
        "/api/v1/orders",
        params={"cursor": "2026-09-09T06:51:19 00:00|" + _ATTEMPT},
    )
    assert resp.status_code == 400


async def test_a_cursor_needs_no_escaping_to_travel(client):
    # The whole point of the encoding: the token is already url-safe, so a
    # caller that concatenates it into a query string cannot corrupt it, and
    # it still names the keyset it was built from.
    created = datetime(2026, 9, 9, 6, 51, 19, tzinfo=timezone.utc)
    token = encode_cursor({"created_at": created, "attempt_id": _ATTEMPT})
    assert quote(token, safe="") == token
    assert decode_cursor(token) == (created, _ATTEMPT)
