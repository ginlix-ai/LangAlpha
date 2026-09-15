"""A revocation that loses the version race has to land anyway."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.egress_grants import GRANT_KIND_OAUTH_MCP, GrantRef
from src.server.services.egress import flash_binding, grant_resync


def _resolved(version: int, connection_ids: list[str]):
    return SimpleNamespace(
        version=version,
        servers=[SimpleNamespace(oauth_connection_id=c) for c in connection_ids],
    )


async def _refs(resolved):
    return [
        GrantRef(
            kind=GRANT_KIND_OAUTH_MCP,
            server_name=f"server-{s.oauth_connection_id}",
            connection_id=s.oauth_connection_id,
        )
        for s in resolved.servers
    ]


@pytest.mark.asyncio
async def test_a_superseded_sync_re_resolves_and_writes_the_newer_answer():
    # The loser's abort is only safe when the winner synced the whole set, and
    # a binding PATCH bumps the version without doing that.
    resolves = [_resolved(1, ["c1", "c2"]), _resolved(2, ["c1"])]
    sync = AsyncMock(side_effect=[None, object()])
    with (
        patch.object(
            grant_resync, "resolve_mcp_config", AsyncMock(side_effect=resolves)
        ),
        patch.object(grant_resync, "sync_egress_grants", sync),
    ):
        wrote = await grant_resync.sync_grants_until_current(
            object(), user_id="u", workspace_id="w", refs=_refs
        )

    assert wrote is True
    assert sync.await_count == 2
    # The second attempt carries the version the winner left behind, and the
    # narrowed set this call exists to apply.
    assert sync.await_args_list[1].kwargs["config_version"] == 2
    assert [r.connection_id for r in sync.await_args_list[1].kwargs["refs"]] == [
        "c1"
    ]


@pytest.mark.asyncio
async def test_one_clean_sync_does_not_resolve_twice():
    sync = AsyncMock(return_value=object())
    resolve = AsyncMock(return_value=_resolved(1, ["c1"]))
    with (
        patch.object(grant_resync, "resolve_mcp_config", resolve),
        patch.object(grant_resync, "sync_egress_grants", sync),
    ):
        assert await grant_resync.sync_grants_until_current(
            object(), user_id="u", workspace_id="w", refs=_refs
        )

    assert resolve.await_count == 1


@pytest.mark.asyncio
async def test_it_gives_up_rather_than_spinning_on_a_busy_workspace():
    sync = AsyncMock(return_value=None)
    with (
        patch.object(
            grant_resync,
            "resolve_mcp_config",
            AsyncMock(return_value=_resolved(1, [])),
        ),
        patch.object(grant_resync, "sync_egress_grants", sync),
    ):
        wrote = await grant_resync.sync_grants_until_current(
            object(), user_id="u", workspace_id="w", refs=_refs
        )

    assert wrote is False
    assert sync.await_count == grant_resync._MAX_ATTEMPTS


@pytest.mark.asyncio
async def test_the_flash_path_refuses_to_report_a_revocation_it_did_not_make():
    # Nothing converges a flash workspace behind this call, so a quiet failure
    # would leave the toggle's 200 claiming a retirement that never happened.
    with patch.object(
        grant_resync, "sync_grants_until_current", AsyncMock(return_value=False)
    ):
        with pytest.raises(grant_resync.GrantSyncSuperseded):
            await flash_binding.sync_flash_grants(
                object(), user_id="u", workspace_id="w"
            )


@pytest.mark.asyncio
async def test_the_flash_path_is_quiet_when_the_grants_were_written():
    with patch.object(
        grant_resync, "sync_grants_until_current", AsyncMock(return_value=True)
    ):
        await flash_binding.sync_flash_grants(object(), user_id="u", workspace_id="w")
