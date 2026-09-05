"""The plugin-level enable toggle (app/plugins.py).

Disabling a plugin withdraws every owned component through the delivery join
predicate, which decides only what the NEXT acquire sees. A sandbox holding a
relay JWT keeps reaching an already-minted server for hours, so the toggle owes
its components the same egress cut a per-server disable performs — this pins
that the plugin path does it too, for every server it owns.

One endpoint answers for both kinds of package, so the same debt is owed by a
shipped bundle, whose components are read off disk rather than from the
component rows. The last two tests pin that, and the precedence between them.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tests.conftest import create_test_app

PLUGIN_ID = "22222222-2222-2222-2222-222222222222"


@pytest_asyncio.fixture
async def client():
    from src.server.app.plugins import router

    app = create_test_app(router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def _patches(*, enabled: bool, servers, connection):
    revoke = AsyncMock()
    stack = [
        patch(
            "src.server.app.plugins.set_plugin_enabled",
            new=AsyncMock(
                return_value={
                    "user_plugin_id": PLUGIN_ID,
                    "name": "demo",
                    "enabled": enabled,
                }
            ),
        ),
        patch(
            "src.server.app.plugins.list_plugin_server_names",
            new=AsyncMock(return_value=servers),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.egress_grants.revoke_grants_for_connection",
            new=revoke,
        ),
    ]
    return revoke, stack


async def _toggle(client, *, enabled, servers, connection):
    revoke, stack = _patches(
        enabled=enabled, servers=servers, connection=connection
    )
    for p in stack:
        p.start()
    try:
        resp = await client.patch(
            "/api/v1/plugins/demo/enabled", json={"enabled": enabled}
        )
    finally:
        for p in reversed(stack):
            p.stop()
    return resp, revoke


@pytest.mark.asyncio
async def test_disable_revokes_grants_for_every_owned_server(client):
    resp, revoke = await _toggle(
        client,
        enabled=False,
        servers=[{"name": "a", "plugin_server_key": "a"},
                 {"name": "b", "plugin_server_key": "b"}],
        connection=MagicMock(connection_id="c-1"),
    )
    assert resp.status_code == 200
    # One per owned server: a plugin disable is N server disables at once, and
    # missing any one of them leaves that server reachable.
    assert revoke.await_count == 2


@pytest.mark.asyncio
async def test_enable_does_not_revoke(client):
    resp, revoke = await _toggle(
        client,
        enabled=True,
        servers=[{"name": "a", "plugin_server_key": "a"}],
        connection=MagicMock(connection_id="c-1"),
    )
    assert resp.status_code == 200
    revoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_disable_with_no_owned_servers_is_a_noop(client):
    resp, revoke = await _toggle(
        client, enabled=False, servers=[], connection=None
    )
    assert resp.status_code == 200
    revoke.assert_not_awaited()


def _bundle_patches(*, servers, installed_row, connection):
    """The bundle branch: no installed row, a bundle owning `servers`."""
    from src.server.services.plugins.bundled import ComponentOwners

    revoke = AsyncMock()
    disable = AsyncMock()
    stack = [
        patch(
            "src.server.app.plugins.set_plugin_enabled",
            new=AsyncMock(return_value=installed_row),
        ),
        patch(
            "src.server.app.plugins.bundled_names",
            new=MagicMock(return_value=frozenset({"demo"})),
        ),
        patch(
            "src.server.app.plugins.enforcement_owners",
            new=MagicMock(
                return_value=ComponentOwners(
                    servers={s: "demo" for s in servers}, skills={}
                )
            ),
        ),
        patch("src.server.app.plugins.set_account_disable", new=disable),
        patch(
            "src.server.app.plugins.list_plugin_server_names",
            new=AsyncMock(return_value=[]),
        ),
        patch(
            "src.server.database.mcp_oauth.get_connection",
            new=AsyncMock(return_value=connection),
        ),
        patch(
            "src.server.database.egress_grants.revoke_grants_for_connection",
            new=revoke,
        ),
    ]
    return revoke, disable, stack


async def _toggle_bundle(client, *, enabled, servers, installed_row=None):
    revoke, disable, stack = _bundle_patches(
        servers=servers,
        installed_row=installed_row,
        connection=MagicMock(connection_id="c-1"),
    )
    for p in stack:
        p.start()
    try:
        resp = await client.patch(
            "/api/v1/plugins/demo/enabled", json={"enabled": enabled}
        )
    finally:
        for p in reversed(stack):
            p.stop()
    return resp, revoke, disable


@pytest.mark.asyncio
async def test_disabling_a_bundle_revokes_the_servers_it_ships(client):
    resp, revoke, disable = await _toggle_bundle(
        client, enabled=False, servers=["yf_price", "yf_market"]
    )
    assert resp.status_code == 200
    disable.assert_awaited_once()
    # Same count the installed branch owes: a bundle disable is N server
    # disables at once, and the names come off disk rather than from rows.
    assert revoke.await_count == 2


@pytest.mark.asyncio
async def test_an_installed_row_outranks_a_bundle_of_the_same_name(client):
    # Install refuses a shipped name, so this state only arises when a deploy
    # introduces a name someone installed earlier. The package the user chose
    # is the one their switch has to reach.
    resp, _revoke, disable = await _toggle_bundle(
        client,
        enabled=False,
        servers=["yf_price"],
        installed_row={"user_plugin_id": PLUGIN_ID, "name": "demo",
                       "enabled": False},
    )
    assert resp.status_code == 200
    disable.assert_not_awaited()


async def _toggle_bundle_blind(client, *, enabled):
    """The bundle branch when the boot snapshot cannot say what it owns."""
    from src.server.services.plugins.bundled import ComponentOwners

    disable = AsyncMock()
    stack = [
        patch(
            "src.server.app.plugins.set_plugin_enabled",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "src.server.app.plugins.bundled_names",
            new=MagicMock(return_value=frozenset({"demo"})),
        ),
        patch(
            "src.server.app.plugins.enforcement_owners",
            new=MagicMock(
                return_value=ComponentOwners(
                    servers={}, skills={}, unreadable=frozenset({"demo"})
                )
            ),
        ),
        patch("src.server.app.plugins.set_account_disable", new=disable),
    ]
    for p in stack:
        p.start()
    try:
        resp = await client.patch(
            "/api/v1/plugins/demo/enabled", json={"enabled": enabled}
        )
    finally:
        for p in reversed(stack):
            p.stop()
    return resp, disable


@pytest.mark.asyncio
async def test_a_disable_it_cannot_enforce_is_refused_before_it_is_written(
    client,
):
    """The row must not outlive the refusal.

    owned_by raises when the boot snapshot cannot enumerate a bundle, and a
    row written ahead of that raise is the worst of both: the request fails,
    the disable persists, and every later turn re-raises on a name nothing can
    answer for. Nothing in the response tells the user their account is now
    wedged.
    """
    resp, disable = await _toggle_bundle_blind(client, enabled=False)
    assert resp.status_code == 503
    disable.assert_not_awaited()


@pytest.mark.asyncio
async def test_re_enabling_never_needs_the_answer_that_is_missing(client):
    """The way out of a disable that stopped being enforceable.

    Turning the package back on withdraws nothing, so it owes no component
    list -- and gating it on one would make the wedge permanent for the only
    user who can clear it.
    """
    resp, disable = await _toggle_bundle_blind(client, enabled=True)
    assert resp.status_code == 200
    disable.assert_awaited_once()


async def _toggle_vanished_bundle(client, *, enabled, disabled_bundles):
    """The name is not on disk any more, but a row for it may still be."""
    from src.server.database.account_disables import AccountDisables

    disable = AsyncMock()
    stack = [
        patch(
            "src.server.app.plugins.set_plugin_enabled",
            new=AsyncMock(return_value=None),
        ),
        # Renamed between releases: the live list no longer answers to "demo".
        patch(
            "src.server.app.plugins.bundled_names",
            new=MagicMock(return_value=frozenset({"demos"})),
        ),
        patch(
            "src.server.app.plugins.list_account_disables",
            new=AsyncMock(
                return_value=AccountDisables(
                    servers=frozenset(),
                    bundles=frozenset(disabled_bundles),
                )
            ),
        ),
        patch("src.server.app.plugins.set_account_disable", new=disable),
    ]
    for p in stack:
        p.start()
    try:
        resp = await client.patch(
            "/api/v1/plugins/demo/enabled", json={"enabled": enabled}
        )
    finally:
        for p in reversed(stack):
            p.stop()
    return resp, disable


@pytest.mark.asyncio
async def test_a_disable_outliving_its_name_can_still_be_cleared(client):
    """Rename a bundle and the row the user wrote points at nothing.

    The live-name 404 sits above every other check, so without this the row
    is unreachable from the page that wrote it: enforcement keeps subtracting
    it on any worker that booted before the rename, and the user has no way
    to say otherwise.
    """
    resp, disable = await _toggle_vanished_bundle(
        client, enabled=True, disabled_bundles={"demo"}
    )
    assert resp.status_code == 200
    disable.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_name_with_no_row_behind_it_is_still_not_found(client):
    """The exemption is only a way out, not a way in.

    Enabling something that neither ships nor has a disable is the plain
    404 it always was.
    """
    resp, disable = await _toggle_vanished_bundle(
        client, enabled=True, disabled_bundles=set()
    )
    assert resp.status_code == 404
    disable.assert_not_awaited()


@pytest.mark.asyncio
async def test_disabling_a_name_that_no_longer_ships_is_refused(client):
    """A disable still needs a package that exists."""
    resp, disable = await _toggle_vanished_bundle(
        client, enabled=False, disabled_bundles={"demo"}
    )
    assert resp.status_code == 404
    disable.assert_not_awaited()
