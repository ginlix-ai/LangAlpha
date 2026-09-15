"""Snapshot-cache purges that a config fingerprint can't trigger on its own,
and the connection guard that stops a late write from undoing one.

Every MCP config hash covers ``${vault:NAME}`` reference strings, never the
resolved values, so a vault VALUE change churns no fingerprint anywhere. These
tests pin the compensating purges — in particular that the user-tier purge also
clears the per-workspace rows, which is where an inherited server's in-sandbox
discovery actually lands — plus the user-tier upsert's under-lock re-read of the
OAuth connection, which runs in both directions: without it a discovery that
overtakes a disconnect writes its snapshot back, and a header probe that
overtakes a connect publishes the wrong account's tools.
"""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

import src.server.database.mcp_tool_schemas as mts


@pytest.fixture
def mock_cursor():
    """Records statements and yields a fresh rowcount per execute, so a purge
    that lost one of its halves shows up in the returned total."""
    cursor = AsyncMock()
    cursor.rowcounts = []

    async def _execute(sql, params=None):
        cursor.rowcount = cursor.rowcounts.pop(0) if cursor.rowcounts else 0

    cursor.execute = AsyncMock(side_effect=_execute)
    return cursor


@pytest.fixture
def schema_mock_db(mock_cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    @asynccontextmanager
    async def _txn_cm():
        yield None

    conn.cursor = _cursor_cm
    conn.transaction = _txn_cm

    @asynccontextmanager
    async def _fake_connection(conn_in=None):
        yield conn_in if conn_in is not None else conn

    with patch(
        "src.server.database.mcp_tool_schemas.get_db_connection", new=_fake_connection
    ):
        yield mock_cursor


def _statements(cursor) -> list[tuple[str, tuple]]:
    """Executed (sql, params) with whitespace collapsed, in order."""
    return [
        (re.sub(r"\s+", " ", call.args[0]).strip(), call.args[1])
        for call in cursor.execute.call_args_list
    ]


def _stored_row(owner_col: str = "user_id", **overrides) -> dict:
    """What RETURNING hands back — enough for ``_row_to_dict`` on either tier."""
    row = {
        owner_col: "user-1" if owner_col == "user_id" else "ws-1",
        "server_name": "authy",
        "config_hash": "hash-1",
        "tools": [],
        "status": "ok",
        "error": "",
        "observed_meta": {},
        "last_probe": {},
        "discovered_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    if owner_col == "user_id":
        row["schema_digest"] = "digest-1"
    return row | overrides


def _prior_row(**overrides) -> dict:
    """What the under-lock read of the row this write is about to replace sees."""
    return {"schema_digest": "digest-0", "last_probe": {}} | overrides


def _unclaimed(*rest) -> list:
    """``fetchone`` returns for a user-tier write holding no connection_id: the
    guard's by-name read runs first and finds nothing claiming the row."""
    return [None, *rest]


# ---------------------------------------------------------------------------
# User tier — the inherited-server purge spans BOTH tiers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_user_purge_also_clears_every_workspace_row(schema_mock_db):
    """The regression: a user server's in-sandbox discovery caches under each
    workspace, so purging only the user tier left a same-hash row to be served
    forever and discovery never reran."""
    schema_mock_db.rowcounts = [1, 4]

    await mts.delete_user_and_workspace_tool_schemas_and_bump("user-1", ["authy"])

    (user_sql, user_params), (workspace_sql, _), _bump = _statements(schema_mock_db)
    assert user_sql == (
        "DELETE FROM user_mcp_tool_schemas "
        "WHERE user_id = %s AND server_name = ANY(%s)"
    )
    assert user_params == ("user-1", ["authy"])
    assert "DELETE FROM workspace_mcp_tool_schemas" in workspace_sql
    assert "server_name = ANY(%s)" in workspace_sql


@pytest.mark.asyncio
async def test_user_purge_is_scoped_to_that_users_workspaces(schema_mock_db):
    """The workspace-tier delete has no user column of its own, so the scope
    rides a subquery — without it the statement would purge the whole table."""
    schema_mock_db.rowcounts = [1, 4]

    await mts.delete_user_and_workspace_tool_schemas_and_bump("user-1", ["authy"])

    _user, (workspace_sql, params), _bump = _statements(schema_mock_db)
    assert "SELECT workspace_id FROM workspaces WHERE user_id = %s" in workspace_sql
    assert params == (["authy"], "user-1")


@pytest.mark.asyncio
async def test_user_purge_covers_idle_workspaces_too(schema_mock_db):
    """No status filter: the sandbox-push half narrows to running workspaces,
    but a cached snapshot outlives the sandbox that wrote it."""
    schema_mock_db.rowcounts = [1, 4]

    await mts.delete_user_and_workspace_tool_schemas_and_bump("user-1", ["authy"])

    _user, (workspace_sql, _params), _bump = _statements(schema_mock_db)
    assert "status" not in workspace_sql


@pytest.mark.asyncio
async def test_user_purge_bumps_after_both_deletes_on_one_cursor(schema_mock_db):
    """One cursor == one transaction: a partial purge with an un-bumped version
    would let live sessions skip re-resolution against the half-purged cache."""
    schema_mock_db.rowcounts = [1, 4]

    await mts.delete_user_and_workspace_tool_schemas_and_bump("user-1", ["authy"])

    statements = _statements(schema_mock_db)
    assert len(statements) == 3
    bump_sql, bump_params = statements[-1]
    assert bump_sql == (
        "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
        "WHERE user_id = %s"
    )
    assert bump_params == ("user-1",)


@pytest.mark.asyncio
async def test_user_purge_totals_both_tiers(schema_mock_db):
    schema_mock_db.rowcounts = [1, 4]

    deleted = await mts.delete_user_and_workspace_tool_schemas_and_bump(
        "user-1", ["authy"]
    )

    assert deleted == 5


@pytest.mark.asyncio
async def test_user_purge_can_join_a_callers_transaction(schema_mock_db):
    """Disconnect folds this purge into the same transaction as the credential
    revoke, so a caller's connection has to be usable in place of a fresh one."""
    caller_conn = AsyncMock()
    caller_cursor = AsyncMock()
    caller_cursor.rowcount = 0

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield caller_cursor

    @asynccontextmanager
    async def _txn_cm():
        yield None

    caller_conn.cursor = _cursor_cm
    caller_conn.transaction = _txn_cm

    await mts.delete_user_and_workspace_tool_schemas_and_bump(
        "user-1", ["authy"], conn=caller_conn
    )

    # Every statement ran on the caller's connection, none on a pooled one.
    assert caller_cursor.execute.await_count == 3
    assert schema_mock_db.execute.await_count == 0


@pytest.mark.asyncio
async def test_user_purge_passes_every_named_server(schema_mock_db):
    schema_mock_db.rowcounts = [2, 6]

    await mts.delete_user_and_workspace_tool_schemas_and_bump(
        "user-1", ["authy", "other"]
    )

    (_user_sql, user_params), (_ws_sql, ws_params), _bump = _statements(schema_mock_db)
    assert user_params == ("user-1", ["authy", "other"])
    assert ws_params == (["authy", "other"], "user-1")


# ---------------------------------------------------------------------------
# Workspace tier
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_workspace_purge_bumps_only_its_own_workspace(schema_mock_db):
    schema_mock_db.rowcounts = [3]

    deleted = await mts.delete_tool_schemas_and_bump("ws-1", ["authy"])

    (purge_sql, purge_params), (bump_sql, bump_params) = _statements(schema_mock_db)
    assert purge_sql == (
        "DELETE FROM workspace_mcp_tool_schemas "
        "WHERE workspace_id = %s AND server_name = ANY(%s)"
    )
    assert purge_params == ("ws-1", ["authy"])
    assert bump_sql.endswith("WHERE workspace_id = %s")
    assert bump_params == ("ws-1",)
    assert deleted == 3


@pytest.mark.asyncio
async def test_workspace_upsert_never_probes_the_connection_table(schema_mock_db):
    """The workspace tier has no OAuth connection to check (in-sandbox
    discovery writes it), so the guard must stay off this path entirely.
    Both arms: this tier passes no connection_id, which is exactly what makes
    the user tier take its by-name read instead."""
    schema_mock_db.fetchone.side_effect = [_stored_row("workspace_id")]

    row = await mts.upsert_tool_schemas("ws-1", "authy", "hash-1", status="ok")

    assert row["status"] == "ok"
    sqls = [sql for sql, _ in _statements(schema_mock_db)]
    assert len(sqls) == 2  # the stale-hash DELETE and the INSERT, nothing else
    assert not any("user_mcp_oauth_connections" in sql for sql in sqls)
    assert not any("FOR SHARE" in sql for sql in sqls)


@pytest.mark.asyncio
async def test_workspace_purge_never_touches_the_user_tier(schema_mock_db):
    """The reverse of the user-tier fix: a workspace secret is not inherited,
    so widening this one would purge snapshots the change can't affect."""
    schema_mock_db.rowcounts = [3]

    await mts.delete_tool_schemas_and_bump("ws-1", ["authy"])

    assert not any(
        "user_mcp_tool_schemas" in sql for sql, _ in _statements(schema_mock_db)
    )


# ---------------------------------------------------------------------------
# User-tier upsert — the connection guard
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["revoked", "needs_reauth"])
async def test_user_upsert_skips_a_connection_that_left_the_servable_set(
    schema_mock_db, status
):
    """The race: discovery spends up to ~40s on the network, a disconnect
    commits in that window and purges both tiers, and this write would put the
    snapshot back — served to the agent with no relay in front of it."""
    schema_mock_db.fetchone.side_effect = [{"status": status}]

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", connection_id="c-1",
        last_probe={},  # required now; this test is about the connection guard
    )

    assert write.row is None
    assert write.connection_status == status
    sqls = [sql for sql, _ in _statements(schema_mock_db)]
    assert len(sqls) == 1  # the probe ran; nothing was written
    assert not any("INSERT INTO" in sql or "DELETE FROM" in sql for sql in sqls)


@pytest.mark.asyncio
async def test_user_upsert_skips_when_the_connection_row_is_gone(schema_mock_db):
    """No row at all is the catalog delete variant — same verdict, and the
    caller needs to tell it apart from a status it can name."""
    schema_mock_db.fetchone.side_effect = [None]

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", connection_id="c-1",
        last_probe={},  # required now; this test is about the connection guard
    )

    assert write.row is None
    assert write.connection_status is None
    assert len(_statements(schema_mock_db)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["connected", "refresh_ambiguous"])
async def test_user_upsert_writes_for_a_servable_connection(schema_mock_db, status):
    # refresh_ambiguous is servable: the old access token works until expiry.
    schema_mock_db.fetchone.side_effect = [
        {"status": status}, _prior_row(), _stored_row()
    ]

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", connection_id="c-1",
        last_probe={},  # required now; this test is about the connection guard
    )

    assert write.row["status"] == "ok"
    assert write.connection_status is None
    probe, delete, prior, insert = _statements(schema_mock_db)
    assert "FOR SHARE" in probe[0]
    assert delete[0].startswith("DELETE FROM user_mcp_tool_schemas")
    assert "FOR UPDATE" in prior[0]
    assert insert[0].startswith("INSERT INTO user_mcp_tool_schemas")


@pytest.mark.asyncio
async def test_user_upsert_locks_the_connection_row_before_writing(schema_mock_db):
    """Statement shape, because the lock semantics can't be: a bare SELECT is
    write-skew under READ COMMITTED — it reads the pre-commit status and
    inserts anyway. FOR SHARE is what serializes this against the disconnect's
    status UPDATE, and it has to be taken before the write, not after."""
    schema_mock_db.fetchone.side_effect = [
        {"status": "connected"}, _prior_row(), _stored_row()
    ]

    await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", connection_id="c-1",
        last_probe={},  # required now; this test is about the connection guard
    )

    (probe_sql, probe_params), *rest = _statements(schema_mock_db)
    assert probe_sql == (
        "SELECT status FROM user_mcp_oauth_connections "
        "WHERE connection_id = %s FOR SHARE"
    )
    assert probe_params == ("c-1",)
    assert all("FOR SHARE" not in sql for sql, _ in rest)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["connected", "needs_reauth", "refresh_ambiguous"])
async def test_a_header_write_is_refused_while_a_connection_claims_the_row(
    schema_mock_db, status
):
    """The same race the other way round, which no fingerprint can see: the row
    had no connection when this probe read it, an OAuth callback created one
    while it was on the network, and execution now sends the relay's token.
    Landing these tools publishes the API-key account's surface to a session
    holding the OAuth account's credential. Anything short of revoked claims
    the row, which is how every other reader of this table claims it."""
    schema_mock_db.fetchone.side_effect = [{"status": status}]

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", last_probe={}
    )

    assert write.row is None
    assert write.connection_status == status
    ((guard_sql, guard_params),) = _statements(schema_mock_db)
    assert guard_sql == (
        "SELECT status FROM user_mcp_oauth_connections "
        "WHERE user_id = %s AND server_name = %s FOR SHARE"
    )
    assert guard_params == ("user-1", "authy")


@pytest.mark.asyncio
async def test_a_header_write_lands_over_a_revoked_connection(schema_mock_db):
    """A revoked row claims nothing: the user disconnected, so the headers are
    what this server is dialled with again and the snapshot is theirs. Rows here
    are never deleted, so this is what a disconnected server looks like
    forever."""
    schema_mock_db.fetchone.side_effect = [
        {"status": "revoked"}, _prior_row(), _stored_row()
    ]

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", last_probe={}
    )

    assert write.row["status"] == "ok"
    assert write.connection_status is None


@pytest.mark.asyncio
async def test_a_header_write_lands_when_nothing_has_connected_the_row(
    schema_mock_db,
):
    """The ordinary header row, and what the extra read buys it: one lock-read
    before the write, then the same three statements as before."""
    schema_mock_db.fetchone.side_effect = _unclaimed(_prior_row(), _stored_row())

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", last_probe={}
    )

    assert write.row["status"] == "ok"
    sqls = [sql for sql, _ in _statements(schema_mock_db)]
    assert len(sqls) == 4
    assert sqls[0].startswith("SELECT status FROM user_mcp_oauth_connections")
    assert not any("user_mcp_oauth_connections" in sql for sql in sqls[1:])


# ---------------------------------------------------------------------------
# User-tier upsert: the probe verdict is the row's one always-fresh field
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_verdict_is_never_kept_on_a_downgrade(schema_mock_db):
    """The blocker this column exists for: a server that discovered fine and now
    answers 401 kept serving ``ok`` with a populated error, because the
    no-downgrade arm held every field the UI read. The tools are still valid, so
    they stay; the verdict describes the probe and must always be the new one."""
    schema_mock_db.fetchone.side_effect = _unclaimed(_prior_row(), _stored_row())

    await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="error",
        last_probe={"verdict": "credential_rejected"},
    )

    insert_sql = _statements(schema_mock_db)[-1][0]
    assert "last_probe = EXCLUDED.last_probe" in insert_sql
    for kept in ("tools", "status", "observed_meta", "discovered_at"):
        assert f"{kept} = CASE WHEN t.status = 'ok'" in insert_sql


@pytest.mark.asyncio
async def test_the_verdict_rides_the_insert_as_its_own_value(schema_mock_db):
    schema_mock_db.fetchone.side_effect = _unclaimed(_prior_row(), _stored_row())

    await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok",
        last_probe={"verdict": "ok_authed"},
    )

    _guard, _delete, _prior, (_sql, params) = _statements(schema_mock_db)
    assert params[-1].obj == {"verdict": "ok_authed"}


@pytest.mark.asyncio
async def test_a_row_with_no_verdict_reads_back_as_an_empty_object(schema_mock_db):
    """Rows written before this column exists, and the workspace tier, which has
    no host-side probe at all. The projection turns both into "unprobed"."""
    schema_mock_db.fetchone.side_effect = _unclaimed(
        _prior_row(), _stored_row(last_probe=None)
    )

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", last_probe={}
    )

    assert write.row["last_probe"] == {}


# ---------------------------------------------------------------------------
# User-tier upsert: what this write replaced, digest and verdict, read under
# its own lock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_user_upsert_reports_the_digest_it_replaced(schema_mock_db):
    """The caller's fan-out decision. Read here rather than before the write
    because two workers may discover one row at once: both see the stored
    digest, and the one that lands second would otherwise compare against a
    value its own write had already replaced and skip the bump."""
    schema_mock_db.fetchone.side_effect = _unclaimed(_prior_row(), _stored_row())

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", schema_digest="digest-1",
        last_probe={},
    )

    assert write.replaced_digest == "digest-0"
    _guard, _delete, (prior_sql, prior_params), (insert_sql, _) = _statements(
        schema_mock_db
    )
    assert prior_sql == (
        "SELECT schema_digest, last_probe FROM user_mcp_tool_schemas "
        "WHERE user_id = %s AND server_name = %s AND config_hash = %s FOR UPDATE"
    )
    assert prior_params == ("user-1", "authy", "hash-1")
    assert insert_sql.startswith("INSERT INTO user_mcp_tool_schemas")


@pytest.mark.asyncio
async def test_a_first_snapshot_replaced_nothing(schema_mock_db):
    """None, not the empty string: nothing was there, so every digest is a
    change and the caller bumps."""
    schema_mock_db.fetchone.side_effect = _unclaimed(None, _stored_row())

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", schema_digest="digest-1",
        last_probe={},
    )

    assert write.replaced_digest is None


@pytest.mark.asyncio
async def test_the_user_upsert_reports_the_verdict_it_replaced(schema_mock_db):
    """Read beside the digest because the digest cannot see it: the
    no-downgrade rule holds tools, status and digest still while ``last_probe``
    moves, and a row that stops answering still has to cost its grant."""
    schema_mock_db.fetchone.side_effect = _unclaimed(
        _prior_row(last_probe={"verdict": "ok_authed"}), _stored_row()
    )

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="error",
        last_probe={"verdict": "credential_rejected"},
    )

    assert write.replaced_verdict == "ok_authed"


@pytest.mark.asyncio
async def test_an_unprobed_row_replaced_no_verdict(schema_mock_db):
    """A row written before the verdict had a column reads back ``{}``, which is
    not a verdict and must not settle a fan-out decision as if it were one."""
    schema_mock_db.fetchone.side_effect = _unclaimed(
        _prior_row(last_probe={}), _stored_row()
    )

    write = await mts.upsert_user_tool_schemas(
        "user-1", "authy", "hash-1", status="ok", last_probe={}
    )

    assert write.replaced_verdict is None


@pytest.mark.asyncio
async def test_the_workspace_tier_takes_no_digest_lock(schema_mock_db):
    """It has no digest column and no fan-out to decide, so the extra statement
    would be a round trip and a row lock bought for nothing."""
    schema_mock_db.fetchone.side_effect = [_stored_row("workspace_id")]

    write_row = await mts.upsert_tool_schemas("ws-1", "authy", "hash-1", status="ok")

    assert write_row["status"] == "ok"
    sqls = [sql for sql, _ in _statements(schema_mock_db)]
    assert len(sqls) == 2
    assert not any("FOR UPDATE" in sql for sql in sqls)
