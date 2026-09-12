"""The relay's order gate: one frame, for one attempt that already took its grant.

``prepare_relay`` authorizes every other call from the grant row alone. An order
tool needs more: the execution token minted when the ledger handed out the
attempt's single execution, over arguments the relay hashes again from the body
it is about to forward, against a row that is already ``submitting``.

Seams are patched where ``relay`` imports them, the convention the relay's own
suite keeps.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.provenance.types import hash_args
from src.server.services.brokerage_orders import AttemptStatus
from src.server.services.egress import RelayError, RelayRejection
from src.server.services.egress.execution_token import (
    EXECUTION_HEADER,
    mint_execution_token,
)
from src.server.services.egress.relay import open_upstream, prepare_relay
from src.server.services.egress.relay_jwt import RelayClaims
from src.server.services.mcp_oauth.lifecycle import AccessToken

SECRET = "unit-test-relay-secret-0000000000"
OTHER_SECRET = "unit-test-relay-secret-1111111111"

USER_ID = "usr-order-gate-0001"
WORKSPACE_ID = "11111111-2222-3333-4444-555555555555"
GRANT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
CONNECTION_ID = "ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb"
ATTEMPT_ID = "99999999-8888-7777-6666-555555555555"
TOOL_CALL_ID = "call_order_gate_0001"

# The moomoo address, because the gate keys off the vendor the grant is pinned
# to and moomoo is where ``sim_trade_input_order`` is an order.
DESTINATION = "https://mcp.moomoo.com/mcp"
ORDER_TOOL = "sim_trade_input_order"
READ_TOOL = "get_positions"
ORDER_ARGS = {"code": "US.AAPL", "qty": 1, "price": 50.0, "trd_side": "BUY"}
ARGS_SHA = (hash_args(ORDER_ARGS) or {})["sha256"]

CLAIMS = RelayClaims(
    user_id=USER_ID,
    workspace_id=WORKSPACE_ID,
    sandbox_id="sbx-order-gate-0001",
    jti="jti-order-gate-0001",
    expires_at=2_000_000_000,
)


def _grant(**overrides) -> dict:
    row = {
        "user_id": USER_ID,
        "workspace_id": WORKSPACE_ID,
        "connection_id": CONNECTION_ID,
        "destination_url": DESTINATION,
        "allowed_methods": ["POST"],
        "tool_denylist": [],
        "tool_direct_only": [],
        "grant_status": "active",
        "connection_status": "connected",
    }
    row.update(overrides)
    return row


def _attempt(**overrides) -> dict:
    row = {
        "attempt_id": ATTEMPT_ID,
        "user_id": USER_ID,
        "status": AttemptStatus.SUBMITTING.value,
        "tool": ORDER_TOOL,
        "tool_call_id": TOOL_CALL_ID,
        "vendor": "moomoo",
        "args_sha256": ARGS_SHA,
    }
    row.update(overrides)
    return row


def _body(tool: str = ORDER_TOOL, arguments: dict | None = None) -> bytes:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": tool,
                "arguments": ORDER_ARGS if arguments is None else arguments,
            },
        }
    ).encode()


def _token(secret: str = SECRET, **overrides) -> str:
    kwargs = {
        "attempt_id": ATTEMPT_ID,
        "tool_call_id": TOOL_CALL_ID,
        "tool": ORDER_TOOL,
        "args_sha256": ARGS_SHA,
    }
    kwargs.update(overrides)
    return mint_execution_token(secret, **kwargs)


_UNSET = object()


async def _prepare(*, body: bytes | None = None, headers=None, attempt=_UNSET):
    """Run ``prepare_relay`` with every seam but the order gate stubbed out."""
    with (
        patch(
            "src.server.services.egress.relay.fetch_grant_for_relay",
            AsyncMock(return_value=_grant()),
        ),
        patch(
            "src.server.services.egress.relay.fetch_attempt_status",
            AsyncMock(return_value=_attempt() if attempt is _UNSET else attempt),
        ) as fetch_attempt,
        patch(
            "src.server.services.egress.relay.ensure_fresh_access_token",
            AsyncMock(
                return_value=AccessToken(
                    access_token="vendor-token", token_type="Bearer", generation=1
                )
            ),
        ),
        patch("src.server.services.egress.relay.EGRESS_RELAY_SECRET", SECRET),
    ):
        prepared = await prepare_relay(
            GRANT_ID,
            claims=CLAIMS,
            raw_body=_body() if body is None else body,
            headers=headers,
        )
    return prepared, fetch_attempt


def _rejection(excinfo) -> RelayRejection:
    e = excinfo.value
    assert e.status == 403
    assert e.code == RelayError.EXECUTION_REQUIRED
    return e


@pytest.mark.asyncio
async def test_a_valid_frame_passes_and_names_its_attempt():
    prepared, fetch_attempt = await _prepare(
        headers={EXECUTION_HEADER: _token()}
    )

    assert prepared.order is not None
    assert prepared.order.attempt_id == ATTEMPT_ID
    assert prepared.order.vendor == "moomoo"
    assert prepared.order.tool == ORDER_TOOL
    assert prepared.order.user_id == USER_ID
    fetch_attempt.assert_awaited_once_with(ATTEMPT_ID)


@pytest.mark.asyncio
async def test_the_header_is_matched_case_insensitively():
    prepared, _ = await _prepare(headers={EXECUTION_HEADER.lower(): _token()})
    assert prepared.order is not None


@pytest.mark.asyncio
async def test_a_non_order_tool_never_reaches_the_gate():
    prepared, fetch_attempt = await _prepare(body=_body(READ_TOOL), headers={})

    assert prepared.order is None
    fetch_attempt.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_order_without_the_header_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(headers={})
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_an_order_with_no_headers_at_all_is_refused():
    # The sandbox's own client never sets the header, and this is the shape its
    # frame arrives in: the gate is what keeps that path off an order tool.
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(headers=None)
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_a_malformed_token_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(headers={EXECUTION_HEADER: "not-a-token"})
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_a_token_signed_with_another_secret_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(headers={EXECUTION_HEADER: _token(OTHER_SECRET)})
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_a_tampered_signature_is_refused():
    attempt_id, expires, signature = _token().split(".")
    flipped = ("B" if signature[0] != "B" else "C") + signature[1:]
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(
            headers={EXECUTION_HEADER: f"{attempt_id}.{expires}.{flipped}"}
        )
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_arguments_changed_after_the_approval_are_refused():
    # The token is minted over the approved arguments; the relay hashes the body
    # it is about to forward. A rewritten quantity fails on both the row and the
    # MAC, which is the whole point of hashing here rather than trusting a claim.
    swapped = dict(ORDER_ARGS, qty=1000)
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(body=_body(arguments=swapped), headers={EXECUTION_HEADER: _token()})
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_an_unknown_attempt_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(headers={EXECUTION_HEADER: _token()}, attempt=None)
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_another_users_attempt_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(
            headers={EXECUTION_HEADER: _token()},
            attempt=_attempt(user_id="usr-order-gate-0002"),
        )
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_an_attempt_recorded_at_another_vendor_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(
            headers={EXECUTION_HEADER: _token()},
            attempt=_attempt(vendor="robinhood"),
        )
    _rejection(excinfo)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        AttemptStatus.PROPOSED.value,
        AttemptStatus.APPROVED.value,
        AttemptStatus.SUBMITTED.value,
        AttemptStatus.REJECTED_BY_USER.value,
    ],
)
async def test_a_row_that_is_not_submitting_is_refused(status):
    # ``submitting`` is the one state the ledger's guarded consume produces, so
    # a replay of a live token lands here rather than at the vendor.
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(
            headers={EXECUTION_HEADER: _token()}, attempt=_attempt(status=status)
        )
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_a_token_minted_for_another_call_on_the_same_attempt_is_refused():
    # The tool_call_id in the MAC comes from the row, never from the caller, so
    # a token minted against a different call cannot be presented for this one.
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(
            headers={EXECUTION_HEADER: _token(tool_call_id="call_order_gate_0002")}
        )
    _rejection(excinfo)


@pytest.mark.asyncio
async def test_an_expired_token_is_refused():
    with pytest.raises(RelayRejection) as excinfo:
        await _prepare(
            headers={EXECUTION_HEADER: _token(ttl=-1)},
        )
    _rejection(excinfo)


class _Pinned:
    """``pin_public_url``'s answer, with the triple the sender applies."""

    def pinned_kwargs(self, headers=None):
        return "https://203.0.113.10/mcp", dict(headers or {}), {}


async def _open(prepared, *, claimed: str | None):
    """Run ``open_upstream`` with the network and the claim stubbed out."""
    client = MagicMock()
    client.build_request = MagicMock(return_value=object())
    client.send = AsyncMock(return_value=SimpleNamespace(status_code=200))
    with (
        patch(
            "src.server.services.egress.relay.pin_public_url",
            AsyncMock(return_value=_Pinned()),
        ),
        patch("src.server.services.egress.relay.get_relay_client", lambda: client),
        patch(
            "src.server.services.egress.relay.claim_dispatch",
            AsyncMock(return_value=claimed),
        ) as claim,
    ):
        try:
            response = await open_upstream(prepared, {})
        except RelayRejection as e:
            return e, claim, client
    return response, claim, client


@pytest.mark.asyncio
async def test_an_order_frame_claims_its_dispatch_before_it_is_sent():
    prepared, _ = await _prepare(headers={EXECUTION_HEADER: _token()})
    response, claim, client = await _open(prepared, claimed=ATTEMPT_ID)

    assert response.status_code == 200
    claim.assert_awaited_once_with(ATTEMPT_ID)
    client.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_second_frame_for_a_dispatched_attempt_never_reaches_the_vendor():
    prepared, _ = await _prepare(headers={EXECUTION_HEADER: _token()})
    rejection, claim, client = await _open(prepared, claimed=None)

    assert isinstance(rejection, RelayRejection)
    assert rejection.status == 403
    assert rejection.code == RelayError.EXECUTION_REQUIRED
    claim.assert_awaited_once_with(ATTEMPT_ID)
    client.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_read_frame_claims_nothing():
    prepared, _ = await _prepare(body=_body(tool=READ_TOOL, arguments={}))
    response, claim, client = await _open(prepared, claimed=None)

    assert response.status_code == 200
    claim.assert_not_awaited()
    client.send.assert_awaited_once()
