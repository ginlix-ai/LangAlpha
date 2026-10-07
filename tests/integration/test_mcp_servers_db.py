"""Integration tests for MCP server CRUD against real PostgreSQL.

Covers the user-level catalog, per-workspace selection rows (each write
bumping ``mcp_config_version`` in the same txn), creating a server switched on
in one workspace only, and the version-keyed discovery schema cache.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _version(workspace_id: str) -> int:
    from src.server.database.workspace import get_workspace

    ws = await get_workspace(workspace_id)
    return int(ws["mcp_config_version"])


async def _schema_raw_count(workspace_id: str, server_name: str) -> int:
    """Raw row count for one server's snapshots (bypasses DISTINCT ON)."""
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) AS n FROM workspace_mcp_tool_schemas "
                "WHERE workspace_id = %s AND server_name = %s",
                (workspace_id, server_name),
            )
            return int((await cur.fetchone())["n"])


# ---------------------------------------------------------------------------
# Catalog CRUD
# ---------------------------------------------------------------------------


class TestCatalogCrud:
    async def test_create_and_get(self, seed_user, patched_get_db_connection):
        from src.server.database.mcp_servers import (
            create_catalog_server,
            get_catalog_server,
        )

        await create_catalog_server(
            seed_user["user_id"], "acme",
            transport="http", url="https://example.test/mcp",
            headers={"Authorization": "${vault:TOKEN}"},
            description="d", instruction="i", tool_exposure_mode="detailed",
            discovery_uses_secrets=True,
        )
        row = await get_catalog_server(seed_user["user_id"], "acme")
        assert row["name"] == "acme"
        assert row["headers"] == {"Authorization": "${vault:TOKEN}"}
        assert row["tool_exposure_mode"] == "detailed"
        # discovery_uses_secrets must round-trip through the catalog (it used to be
        # silently dropped, so promoting an auth-at-discovery server lost the flag).
        assert row["discovery_uses_secrets"] is True

    async def test_discovery_uses_secrets_defaults_false_and_updates(
        self, seed_user, patched_get_db_connection
    ):
        from src.server.database.mcp_servers import (
            create_catalog_server,
            get_catalog_server,
            update_catalog_server,
        )

        await create_catalog_server(seed_user["user_id"], "acme", command="npx")
        row = await get_catalog_server(seed_user["user_id"], "acme")
        assert row["discovery_uses_secrets"] is False
        updated = await update_catalog_server(
            seed_user["user_id"], "acme",
            updates={"discovery_uses_secrets": True},
        )
        assert updated["discovery_uses_secrets"] is True

    async def test_duplicate_name_raises(self, seed_user, patched_get_db_connection):
        from src.server.database.mcp_servers import create_catalog_server

        await create_catalog_server(seed_user["user_id"], "dup", command="npx")
        with pytest.raises(ValueError):
            await create_catalog_server(seed_user["user_id"], "dup", command="npx")

    async def test_update_and_list(self, seed_user, patched_get_db_connection):
        from src.server.database.mcp_servers import (
            create_catalog_server,
            list_catalog_servers,
            update_catalog_server,
        )

        await create_catalog_server(seed_user["user_id"], "acme", command="npx")
        updated = await update_catalog_server(
            seed_user["user_id"], "acme",
            updates={"description": "new", "args": ["-y", "pkg"]},
        )
        assert updated["description"] == "new"
        assert updated["args"] == ["-y", "pkg"]
        rows = await list_catalog_servers(seed_user["user_id"])
        assert [r["name"] for r in rows] == ["acme"]

    async def test_delete(self, seed_user, patched_get_db_connection):
        from src.server.database.mcp_servers import (
            create_catalog_server,
            delete_catalog_server,
            get_catalog_server,
        )

        await create_catalog_server(seed_user["user_id"], "acme", command="npx")
        assert await delete_catalog_server(seed_user["user_id"], "acme") is True
        assert await delete_catalog_server(seed_user["user_id"], "acme") is False
        assert await get_catalog_server(seed_user["user_id"], "acme") is None

    async def test_an_enabled_catalog_row_bumps_the_user_workspaces_with_it(
        self, seed_workspace, patched_get_db_connection
    ):
        """An enabled row is live in every workspace the moment it commits, so
        the bump has to commit with it: a caller that bumps later can die first.
        An inert row reaches no workspace and moves no version."""
        from src.server.database.mcp_servers import create_catalog_server
        from src.server.database.workspace import create_workspace

        user_id = seed_workspace["user_id"]
        here = str(seed_workspace["workspace_id"])
        sibling = await create_workspace(
            user_id=user_id, name="Sibling", status="stopped"
        )
        wids = (here, str(sibling["workspace_id"]))
        before = {wid: await _version(wid) for wid in wids}

        await create_catalog_server(user_id, "inert", command="npx")
        assert {wid: await _version(wid) for wid in wids} == before

        await create_catalog_server(user_id, "live", command="npx", enabled=True)
        for wid, version in before.items():
            assert await _version(wid) == version + 1


# ---------------------------------------------------------------------------
# Workspace rows: selection only, version bump in the same txn
# ---------------------------------------------------------------------------


class TestWorkspaceRows:
    async def test_upsert_bumps_version(self, seed_workspace, patched_get_db_connection):
        from src.server.database.mcp_servers import upsert_workspace_server

        wid = seed_workspace["workspace_id"]
        assert await _version(wid) == 0

        await upsert_workspace_server(wid, "acme", source="user", enabled=False)
        assert await _version(wid) == 1

        # Update (same name) bumps again.
        await upsert_workspace_server(wid, "acme", source="user", enabled=False)
        assert await _version(wid) == 2

    async def test_disable_marker_bumps_version(self, seed_workspace, patched_get_db_connection):
        from src.server.database.mcp_servers import (
            list_workspace_servers,
            upsert_workspace_server,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_workspace_server(
            wid, "builtin-x", source="builtin", enabled=False, config=None,
        )
        assert await _version(wid) == 1
        rows = await list_workspace_servers(wid)
        assert rows[0]["source"] == "builtin"
        assert rows[0]["enabled"] is False
        assert rows[0]["config"] is None

    async def test_delete_bumps(self, seed_workspace, patched_get_db_connection):
        from src.server.database.mcp_servers import (
            delete_workspace_server,
            upsert_workspace_server,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_workspace_server(wid, "acme", source="user", enabled=False)  # v1
        assert await delete_workspace_server(wid, "acme") is True  # v2
        assert await _version(wid) == 2
        # Absent rows don't bump.
        assert await delete_workspace_server(wid, "nope") is False
        assert await _version(wid) == 2

    async def test_servers_and_version_snapshot_consistent(
        self, seed_workspace, patched_get_db_connection
    ):
        """get_workspace_servers_and_version returns a (rows, version) pair from
        one snapshot — they always agree with what each separate read sees."""
        from src.server.database.mcp_servers import (
            get_workspace_servers_and_version,
            upsert_workspace_server,
        )

        wid = seed_workspace["workspace_id"]
        rows, version = await get_workspace_servers_and_version(wid)
        assert rows == [] and version == 0

        await upsert_workspace_server(wid, "acme", source="user", enabled=False)
        rows, version = await get_workspace_servers_and_version(wid)
        assert [r["name"] for r in rows] == ["acme"]
        # The version reflects exactly the writes visible in rows (no torn read).
        assert version == 1
        assert version == await _version(wid)


# ---------------------------------------------------------------------------
# A server created from a workspace runs in that workspace only
# ---------------------------------------------------------------------------


class TestWorkspaceCatalogServer:
    async def _workspace(self, user_id: str, name: str, status: str) -> str:
        from src.server.database.workspace import create_workspace

        ws = await create_workspace(user_id=user_id, name=name, status=status)
        return str(ws["workspace_id"])

    async def _slot(self, workspace_id: str, name: str) -> dict | None:
        from src.server.database.mcp_servers import list_workspace_servers

        rows = {r["name"]: r for r in await list_workspace_servers(workspace_id)}
        return rows.get(name)

    async def test_live_in_its_workspace_tombstoned_everywhere_else(
        self, seed_workspace, patched_get_db_connection
    ):
        from src.server.database.mcp_servers import (
            create_workspace_catalog_server,
            get_catalog_server,
            upsert_workspace_server,
        )

        user_id = seed_workspace["user_id"]
        here = str(seed_workspace["workspace_id"])
        sibling = await self._workspace(user_id, "Sibling", "stopped")
        flash = await self._workspace(user_id, "Flash", "flash")
        gone = await self._workspace(user_id, "Gone", "deleted")
        # A leftover tombstone here would switch the new server off in the one
        # workspace that asked for it; a stale enabled row elsewhere must not
        # keep it on.
        await upsert_workspace_server(here, "acme", source="user", enabled=False)
        await upsert_workspace_server(sibling, "acme", source="user", enabled=True)
        before = {wid: await _version(wid) for wid in (here, sibling, flash)}

        row = await create_workspace_catalog_server(
            user_id, here, "acme", transport="stdio", command="npx",
        )

        assert row["name"] == "acme"
        assert (await get_catalog_server(user_id, "acme"))["enabled"] is True
        assert await self._slot(here, "acme") is None
        for wid in (sibling, flash):
            slot = await self._slot(wid, "acme")
            assert (slot["source"], slot["enabled"], slot["config"]) == (
                "user", False, None,
            )
        assert await self._slot(gone, "acme") is None
        for wid, version in before.items():
            assert await _version(wid) == version + 1

    async def test_a_taken_name_writes_nothing(
        self, seed_workspace, patched_get_db_connection
    ):
        """One transaction: the duplicate aborts the tombstones with it."""
        from src.server.database.mcp_servers import (
            create_catalog_server,
            create_workspace_catalog_server,
        )

        user_id = seed_workspace["user_id"]
        here = str(seed_workspace["workspace_id"])
        sibling = await self._workspace(user_id, "Sibling", "running")
        await create_catalog_server(user_id, "acme", command="npx")
        before = await _version(sibling)

        with pytest.raises(ValueError):
            await create_workspace_catalog_server(
                user_id, here, "acme", command="uvx",
            )

        assert await self._slot(sibling, "acme") is None
        assert await _version(sibling) == before


# ---------------------------------------------------------------------------
# Discovery schema cache
# ---------------------------------------------------------------------------


class TestSchemaCache:
    async def test_get_returns_latest_snapshot_per_server(self, seed_workspace, patched_get_db_connection):
        """Each server's snapshot is keyed by its own config_hash; get returns
        one row per server (the most recent), with the hash surfaced so the
        caller can match it against the current config."""
        from src.server.database.mcp_tool_schemas import (
            get_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_tool_schemas(
            wid, "acme", "hash-acme",
            tools=[{"name": "t1", "description": "d", "input_schema": {}}],
            status="ok",
        )
        await upsert_tool_schemas(wid, "beta", "hash-beta", status="pending")

        rows = {r["server_name"]: r for r in await get_tool_schemas(wid)}
        assert rows["acme"]["status"] == "ok"
        assert rows["acme"]["config_hash"] == "hash-acme"
        assert rows["acme"]["tools"][0]["name"] == "t1"
        assert rows["beta"]["status"] == "pending"
        assert rows["beta"]["config_hash"] == "hash-beta"

    async def test_new_hash_replaces_and_purges_stale_rows(self, seed_workspace, patched_get_db_connection):
        """A config change (new hash) replaces the server's snapshot AND
        garbage-collects rows at older hashes, so config iteration doesn't
        accumulate dead rows."""
        from src.server.database.mcp_tool_schemas import (
            get_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_tool_schemas(wid, "acme", "hash-old", status="ok")
        await upsert_tool_schemas(wid, "acme", "hash-mid", status="ok")
        await upsert_tool_schemas(wid, "acme", "hash-new", status="pending")

        rows = await get_tool_schemas(wid)
        assert len(rows) == 1
        assert rows[0]["config_hash"] == "hash-new" and rows[0]["status"] == "pending"
        # The stale hash-old / hash-mid rows are physically gone, not just shadowed.
        assert await _schema_raw_count(wid, "acme") == 1

    async def test_delete_server_purges_schema_rows(self, seed_workspace, patched_get_db_connection):
        """Deleting a workspace row removes its discovery snapshots too."""
        from src.server.database.mcp_servers import (
            delete_workspace_server,
            upsert_workspace_server,
        )
        from src.server.database.mcp_tool_schemas import (
            get_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_workspace_server(wid, "acme", source="user", enabled=False)
        await upsert_tool_schemas(wid, "acme", "hash-1", status="ok")
        await upsert_tool_schemas(wid, "beta", "hash-b", status="ok")

        assert await delete_workspace_server(wid, "acme") is True
        names = {r["server_name"] for r in await get_tool_schemas(wid)}
        assert names == {"beta"}
        assert await _schema_raw_count(wid, "acme") == 0

    async def test_deleting_an_account_server_purges_every_workspace_snapshot(
        self, seed_workspace, patched_get_db_connection
    ):
        """In-sandbox discovery caches an account server under each workspace;
        a same-name recreate with the same config must not be served those."""
        from src.server.database.mcp_servers import (
            create_catalog_server,
            delete_catalog_server,
        )
        from src.server.database.mcp_tool_schemas import upsert_tool_schemas

        wid = seed_workspace["workspace_id"]
        user_id = seed_workspace["user_id"]
        await create_catalog_server(user_id, "acme", command="npx")
        await upsert_tool_schemas(wid, "acme", "hash-1", status="ok")
        await upsert_tool_schemas(wid, "beta", "hash-b", status="ok")

        assert await delete_catalog_server(user_id, "acme") is True
        assert await _schema_raw_count(wid, "acme") == 0
        assert await _schema_raw_count(wid, "beta") == 1

    async def test_upsert_replaces_same_key(self, seed_workspace, patched_get_db_connection):
        from src.server.database.mcp_tool_schemas import (
            get_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_tool_schemas(wid, "acme", "hash-1", status="pending")
        await upsert_tool_schemas(
            wid, "acme", "hash-1", status="error", error="boom",
        )
        rows = await get_tool_schemas(wid)
        assert len(rows) == 1
        assert rows[0]["status"] == "error"
        assert rows[0]["error"] == "boom"

    async def test_transient_error_never_downgrades_same_hash_ok(
        self, seed_workspace, patched_get_db_connection
    ):
        """A flaky probe (same config hash) must not erase a known-good
        snapshot: tools/status/discovered_at are preserved, only the error
        text is recorded for debugging."""
        from src.server.database.mcp_tool_schemas import (
            get_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        tools = [{"name": "t1", "description": "d", "input_schema": {}}]
        await upsert_tool_schemas(wid, "acme", "hash-1", tools=tools, status="ok")
        before = (await get_tool_schemas(wid))[0]

        row = await upsert_tool_schemas(
            wid, "acme", "hash-1", status="error", error="npx fetch hiccup",
        )

        assert row["status"] == "ok"
        assert row["tools"] == tools
        assert row["error"] == "npx fetch hiccup"
        assert row["discovered_at"] == before["discovered_at"]
        assert await _schema_raw_count(wid, "acme") == 1

    async def test_pending_never_downgrades_same_hash_ok(
        self, seed_workspace, patched_get_db_connection
    ):
        """Probing a stopped workspace marks servers pending — that must not
        wipe a working server's cached tools either."""
        from src.server.database.mcp_tool_schemas import upsert_tool_schemas

        wid = seed_workspace["workspace_id"]
        tools = [{"name": "t1", "description": "d", "input_schema": {}}]
        await upsert_tool_schemas(wid, "acme", "hash-1", tools=tools, status="ok")

        row = await upsert_tool_schemas(wid, "acme", "hash-1", status="pending")

        assert row["status"] == "ok"
        assert row["tools"] == tools

    async def test_error_at_new_hash_still_replaces(
        self, seed_workspace, patched_get_db_connection
    ):
        """The no-downgrade guard is same-hash only: a config CHANGE that then
        fails discovery legitimately replaces the old config's snapshot."""
        from src.server.database.mcp_tool_schemas import (
            get_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        tools = [{"name": "t1", "description": "d", "input_schema": {}}]
        await upsert_tool_schemas(wid, "acme", "hash-old", tools=tools, status="ok")

        row = await upsert_tool_schemas(
            wid, "acme", "hash-new", status="error", error="bad new config",
        )

        assert row["status"] == "error"
        rows = await get_tool_schemas(wid)
        assert len(rows) == 1 and rows[0]["config_hash"] == "hash-new"
        assert await _schema_raw_count(wid, "acme") == 1

    async def test_delete_tool_schemas_and_standalone_bump(
        self, seed_workspace, patched_get_db_connection
    ):
        """Vault-mutation invalidation primitives: purge one server's snapshots
        (any hash) and bump every workspace of the user outside a row
        mutation."""
        from src.server.database.mcp_config_version import (
            bump_user_workspaces_mcp_version,
        )
        from src.server.database.mcp_tool_schemas import (
            delete_tool_schemas,
            upsert_tool_schemas,
        )

        wid = seed_workspace["workspace_id"]
        await upsert_tool_schemas(wid, "acme", "hash-1", status="ok")
        await upsert_tool_schemas(wid, "beta", "hash-b", status="ok")

        assert await delete_tool_schemas(wid, "acme") == 1
        assert await _schema_raw_count(wid, "acme") == 0
        assert await _schema_raw_count(wid, "beta") == 1

        v0 = await _version(wid)
        assert await bump_user_workspaces_mcp_version(seed_workspace["user_id"]) == 1
        assert await _version(wid) == v0 + 1
