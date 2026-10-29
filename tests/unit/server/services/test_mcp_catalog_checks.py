"""When an edit is checked, with whose sign-in, and when a switch may turn on.

An edit is dialled only when it changes how the server is reached or what it
is sent, and a connected server is checked with its own token only while the
address it was granted for stays put: a token goes to no endpoint the user did
not consent to. A server that needs a sign-in is switched on only while its
connection stands; once the user lets a sign-in go, the server is asked again.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.mcp_oauth import ConnectionStatus
from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_catalog import sign_in_missing, sign_in_to_enable
from src.server.services.mcp_dial import consented_bearer, dial_moved
from src.server.services.mcp_validation import (
    CheckFailure,
    CheckResult,
    Passed,
    SignInNeeded,
)

_PRIOR = {
    "transport": "http",
    "command": None,
    "args": None,
    "url": "https://api.example.com/mcp",
    "env": None,
    "headers": {"X-Key": "${vault:KEY}"},
    "description": "d",
}


@pytest.mark.parametrize(
    "edit, moved",
    [
        ({}, False),
        ({"description": "reworded", "instruction": "new"}, False),
        # A column the row stores as NULL and the form sends as empty is the
        # same server; treating it as moved would dial on every edit.
        ({"args": [], "env": {}}, False),
        ({"url": "https://api.example.com/v2/mcp"}, True),
        ({"headers": {"X-Key": "${vault:OTHER}"}}, True),
        ({"headers": {}}, True),
        ({"transport": "stdio", "command": "npx", "url": None, "headers": {}}, True),
        ({"env": {"A": "1"}}, True),
    ],
)
def test_an_edit_is_dialled_only_when_it_moves_a_dial_field(edit, moved):
    fields = {**_PRIOR, **edit}
    assert dial_moved(_PRIOR, fields) is moved


def _server(url="https://api.example.com/mcp", transport="http"):
    if transport == "stdio":
        return McpServerInput(name="srv", transport="stdio", command="npx")
    return McpServerInput(name="srv", transport=transport, url=url)


def _connection(status=ConnectionStatus.CONNECTED, url="https://api.example.com/mcp"):
    return SimpleNamespace(connection_id="c-1", server_url=url, status=status)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "server, connection, sends",
    [
        (_server(), _connection(), True),
        # Spelled differently, consented to all the same.
        (_server("https://API.example.com:443/mcp/"), _connection(), True),
        (_server(), _connection(ConnectionStatus.REFRESH_AMBIGUOUS), True),
        (_server("https://moved.example.com/mcp"), _connection(), False),
        (_server(), _connection(ConnectionStatus.NEEDS_REAUTH), False),
        (_server(), _connection(ConnectionStatus.REVOKED), False),
        (_server(), None, False),
    ],
)
async def test_a_connected_server_is_checked_with_its_token_only_where_granted(
    server, connection, sends
):
    fresh = AsyncMock(return_value=SimpleNamespace(access_token="tok"))
    with (
        patch(
            "src.server.services.mcp_dial.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch("src.server.services.mcp_dial.ensure_fresh_access_token", new=fresh),
    ):
        bearer = await consented_bearer("u-1", server)
    assert bearer == ("tok" if sends else None)
    if sends:
        fresh.assert_awaited_once_with("c-1")
    else:
        fresh.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_connection_with_no_usable_token_is_checked_without_one():
    """The server then answers that it wants a sign-in, and the edit is
    refused as one rather than saved past a dead connection."""
    from src.server.services.mcp_oauth.lifecycle import TokenUnavailable

    with (
        patch(
            "src.server.services.mcp_dial.get_connection",
            new=AsyncMock(return_value=_connection()),
        ),
        patch(
            "src.server.services.mcp_dial.ensure_fresh_access_token",
            new=AsyncMock(side_effect=TokenUnavailable("refresh_failed")),
        ),
    ):
        assert await consented_bearer("u-1", _server()) is None


@pytest.mark.asyncio
async def test_a_command_server_has_no_sign_in_to_send():
    lookup = AsyncMock()
    with patch("src.server.services.mcp_dial.get_connection", new=lookup):
        assert await consented_bearer("u-1", _server(transport="stdio")) is None
    lookup.assert_not_awaited()


# ---------------------------------------------------------------------------
# sign_in_missing: what a switch asks before it turns a server on
# ---------------------------------------------------------------------------

_URL = "https://api.example.com/mcp"


def _catalog(name="srv", url=_URL, transport="http", **extra):
    row = {
        "name": name,
        "transport": transport,
        "command": "npx" if transport == "stdio" else None,
        "args": [],
        "url": None if transport == "stdio" else url,
        "env": {},
        "headers": {},
        "enabled": False,
        "plugin_id": None,
    }
    return {**row, **extra}


def _snapshot(row, verdict, *, current=True):
    """A user-tier snapshot row carrying ``verdict``, under the row's current
    fingerprint or a stale one."""
    from src.server.services.mcp_config import user_row_to_server_config
    from src.server.services.mcp_discovery import mcp_discovery_fingerprint

    fingerprint = mcp_discovery_fingerprint(user_row_to_server_config(row))
    return {
        "server_name": row["name"],
        "config_hash": fingerprint if current else "an-earlier-config",
        "status": "error",
        "error": "",
        "tools": [],
        "last_probe": {"verdict": verdict},
    }


async def _missing(row, *, connection=None, snapshots=(), check=None):
    lookup = AsyncMock(return_value=connection)
    check = check if check is not None else AsyncMock(return_value=_answered())
    with (
        patch("src.server.services.mcp_catalog.get_connection", new=lookup),
        patch(
            "src.server.database.mcp_tool_schemas.get_user_tool_schemas",
            new=AsyncMock(return_value=list(snapshots)),
        ),
        patch("src.server.services.mcp_catalog.check_server", new=check),
    ):
        return await sign_in_missing("u-1", row), lookup


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "connection, needs",
    [
        (_connection(), False),
        (_connection(ConnectionStatus.REFRESH_AMBIGUOUS), False),
        # Signed in here once and not now, and never let go of: the grant lapsed.
        (_connection(ConnectionStatus.NEEDS_REAUTH), True),
    ],
)
async def test_a_signed_in_server_is_on_only_while_its_connection_stands(connection, needs):
    check = AsyncMock()
    missing, _ = await _missing(_catalog(), connection=connection, check=check)
    assert missing == (SignInNeeded(server="srv", url=_URL) if needs else None)
    # The stored state decides these; nothing is dialled for a switch.
    check.assert_not_awaited()


def _answered(*, passed=False, sign_in=False) -> CheckResult:
    if passed:
        return Passed("srv")
    if sign_in:
        return SignInNeeded(server="srv", url=_URL)
    return CheckFailure(server="srv", verdict="credential_rejected", reason="no")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer, needs",
    [
        # Disconnected, then a key edited in (or a remade row on an open
        # server): it answers without the sign-in the user let go of.
        (_answered(passed=True), False),
        # A plain disconnect of a server that only signs in with OAuth.
        (_answered(sign_in=True), True),
        # Anything short of a pass leaves the switch where a failed check would.
        (_answered(), True),
    ],
)
async def test_a_revoked_connection_is_history_and_the_server_is_asked(answer, needs):
    row = _catalog(headers={"Authorization": "Bearer ${vault:PAT}"})
    check = AsyncMock(return_value=answer)

    missing, _ = await _missing(
        row, connection=_connection(ConnectionStatus.REVOKED), check=check
    )

    assert missing == (SignInNeeded(server="srv", url=_URL) if needs else None)
    check.assert_awaited_once()
    (user_id, server), kwargs = check.await_args
    # The row as it is now, with no sign-in behind it and no computer.
    assert user_id == "u-1"
    assert (server.name, server.transport, server.url) == ("srv", "http", _URL)
    assert server.headers == {"Authorization": "Bearer ${vault:PAT}"}
    assert kwargs == {}


@pytest.mark.asyncio
async def test_a_revoked_row_that_no_longer_validates_stays_off():
    row = _catalog(url="not a url")
    check = AsyncMock()

    missing, _ = await _missing(
        row,
        connection=_connection(ConnectionStatus.REVOKED, url="not a url"),
        check=check,
    )

    assert missing == SignInNeeded(server="srv", url="not a url")
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_connection_granted_for_another_address_is_history():
    """The row moved since; its new address answers for itself, and a plain
    server there is switched on as any other."""
    moved = _connection(url="https://old.example.com/mcp")
    missing, _ = await _missing(_catalog(), connection=moved)
    assert missing is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, ConnectionStatus.REVOKED])
async def test_a_brokerage_wants_its_sign_in_before_anything_answers(status):
    row = _catalog(name="robinhood", url="https://agent.robinhood.com/mcp/trading")
    connection = None if status is None else _connection(status, url=row["url"])
    check = AsyncMock(return_value=_answered(passed=True))
    missing, _ = await _missing(row, connection=connection, check=check)
    assert missing == SignInNeeded(server="robinhood", url=row["url"])
    check.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_plugins_row_under_a_brokerage_name_is_judged_as_its_own_server():
    row = _catalog(name="robinhood", plugin_id="p-1")
    missing, _ = await _missing(row)
    assert missing is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verdict, current, needs",
    [
        ("oauth", True, True),
        ("ok", True, False),
        ("unreachable", True, False),
        # Asked under a configuration the row no longer has.
        ("oauth", False, False),
    ],
)
async def test_a_server_that_asked_for_a_sign_in_needs_one(verdict, current, needs):
    row = _catalog()
    missing, _ = await _missing(row, snapshots=[_snapshot(row, verdict, current=current)])
    assert missing == (SignInNeeded(server="srv", url=_URL) if needs else None)


@pytest.mark.asyncio
async def test_a_server_nothing_has_probed_switches_as_it_always_did():
    missing, _ = await _missing(_catalog())
    assert missing is None


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["stdio", "sse"])
async def test_a_server_with_no_sign_in_to_miss_is_never_asked(transport):
    missing, lookup = await _missing(_catalog(transport=transport))
    assert missing is None
    lookup.assert_not_awaited()


def test_a_switch_refuses_in_the_shape_a_save_does():
    error = sign_in_to_enable(SignInNeeded(server="srv", url=_URL))
    assert error.status_code == 409
    assert error.detail == {
        "code": "mcp_sign_in_required",
        "message": "Sign in to the server to switch it on.",
        "servers": [{"server": "srv", "url": _URL}],
    }
