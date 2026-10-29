"""
Tests for src/server/database/workspace.py

Verifies workspace CRUD operations: create, get, update, delete,
batch sort order, status updates, and the Home upsert.
"""

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database.home_workspace import get_home_workspace_id
from src.server.database.sql_fences import EXCLUDE_HOME


# ---------------------------------------------------------------------------
# Fixture: patch get_db_connection at the workspace module's import location
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cursor():
    """AsyncMock cursor with execute/fetchone/fetchall."""
    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=None)
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.rowcount = 0
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """AsyncMock connection that yields mock_cursor via cursor() context manager."""
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield mock_cursor

    @asynccontextmanager
    async def _transaction_cm():
        yield

    conn.transaction = _transaction_cm
    conn.cursor = _cursor_cm
    conn.execute = AsyncMock()
    return conn


@pytest.fixture
def selection():
    """The per-user lock and the MCP selection a new workspace starts with,
    whose SQL is pinned against a real Postgres in
    tests/integration/test_new_workspace_mcp_selection_db.py."""
    lock, start = AsyncMock(), AsyncMock()
    with (
        patch("src.server.database.workspace.lock_user_writes", new=lock),
        patch("src.server.database.workspace.start_new_workspace_selection", new=start),
    ):
        yield SimpleNamespace(lock=lock, start=start)


@pytest.fixture
def fold():
    """The fold of a former Home row into Home, which every Home resolve
    awaits; its SQL is pinned against a real Postgres in
    tests/integration/test_home_fold_db.py."""
    fold = AsyncMock(return_value=[])
    with patch("src.server.database.workspace.fold_former_homes", new=fold):
        yield fold


@pytest.fixture
def ws_mock_db(mock_connection, selection, fold):
    """Patch get_db_connection where the workspace module and its folder allocation import it."""

    @asynccontextmanager
    async def _fake(conn=None):
        if conn is not None:
            yield conn
            return
        yield mock_connection

    with (
        patch("src.server.database.workspace.get_db_connection", new=_fake),
        patch("src.server.database.workspace_folders.get_db_connection", new=_fake),
    ):
        yield mock_connection


# ---------------------------------------------------------------------------
# Helper: sample workspace row
# ---------------------------------------------------------------------------

def _workspace_row(
    workspace_id=None,
    user_id="user-1",
    name="My Workspace",
    status="running",
    **overrides,
):
    now = datetime.now(timezone.utc)
    row = {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": name,
        "description": None,
        "sandbox_id": "sandbox-abc",
        "status": status,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": None,
        "stopped_at": None,
        "config": {},
        "is_pinned": False,
        "sort_order": 0,
    }
    row.update(overrides)
    return row


# ===========================================================================
# Tests
# ===========================================================================


@pytest.mark.asyncio
async def test_get_home_workspace_id_deterministic():
    """Same user always produces the same Home ID."""
    from src.server.database.home_workspace import get_home_workspace_id

    id1 = get_home_workspace_id("user-42")
    id2 = get_home_workspace_id("user-42")
    assert id1 == id2

    id3 = get_home_workspace_id("user-99")
    assert id3 != id1


@pytest.mark.asyncio
async def test_an_existing_home_costs_one_statement_and_no_lock(
    ws_mock_db, mock_cursor, selection
):
    """Every turn in Home lands here, so the row that exists is only touched."""
    from src.server.database.workspace import get_or_create_home_workspace

    row = _workspace_row(name="Home", status="unbound")
    mock_cursor.fetchone.return_value = row

    result = await get_or_create_home_workspace("user-1")

    assert result["name"] == "Home"
    assert result["status"] == "unbound"
    mock_cursor.execute.assert_awaited_once()
    assert mock_cursor.execute.call_args[0][0].split()[0] == "UPDATE"
    selection.lock.assert_not_awaited()
    selection.start.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_first_home_turn_upserts_and_starts_the_selection_once(
    ws_mock_db, mock_cursor, selection
):
    """A concurrent first turn that inserted it first already started the
    selection; the upsert says which of the two this one was."""
    from src.server.database.home_workspace import get_home_workspace_id
    from src.server.database.workspace import get_or_create_home_workspace

    row = _workspace_row(name="Home", status="unbound")
    mock_cursor.fetchone.side_effect = [None, {**row, "inserted": True}]
    result = await get_or_create_home_workspace("user-1")

    assert "inserted" not in result
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO workspaces" in sql and "ON CONFLICT" in sql
    assert mock_cursor.execute.call_args[0][1][2:] == ("Home", "unbound")
    selection.lock.assert_awaited_once_with(mock_cursor, "user-1")
    selection.start.assert_awaited_once_with(
        mock_cursor, "user-1", get_home_workspace_id("user-1")
    )

    mock_cursor.fetchone.side_effect = [None, {**row, "inserted": False}]
    await get_or_create_home_workspace("user-1")
    selection.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_every_resolve_folds_former_rows_into_home(
    ws_mock_db, mock_cursor, fold
):
    """An account merge leaves the merged account's Home beside this one, which
    the account would list as a second All workspaces entry. Whether there is
    one is the fold's own read, so a first resolve that inserts Home folds too."""
    from src.server.database.home_workspace import get_home_workspace_id
    from src.server.database.workspace import get_or_create_home_workspace

    home_id = get_home_workspace_id("user-1")
    row = _workspace_row(workspace_id=home_id, name="Home", status="unbound")
    mock_cursor.fetchone.return_value = row
    await get_or_create_home_workspace("user-1", conn=ws_mock_db)
    fold.assert_awaited_once_with("user-1", home_id, ws_mock_db)

    mock_cursor.fetchone.side_effect = [None, {**row, "inserted": True}]
    await get_or_create_home_workspace("user-1")
    assert fold.await_args_list[-1].args == ("user-1", home_id, None)


@pytest.mark.asyncio
async def test_a_failed_fold_still_resolves_home(ws_mock_db, mock_cursor, fold):
    """Every turn outside a workspace resolves Home here; the fold runs again
    on the next resolve, so its failure must not take the turn with it."""
    from src.server.database.workspace import get_or_create_home_workspace

    row = _workspace_row(name="Home")
    mock_cursor.fetchone.return_value = row
    fold.side_effect = RuntimeError("unique violation")

    result = await get_or_create_home_workspace("user-1")

    assert result["workspace_id"] == row["workspace_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "create",
    [
        lambda W: W.create_workspace("user-1", "Research"),
        lambda W: W.create_workspace_on_computer(
            "user-1", "Research", "11111111-1111-1111-1111-111111111111"
        ),
        lambda W: W.get_or_create_home_workspace("user-1"),
    ],
    ids=["create_workspace", "create_workspace_on_computer", "home"],
)
async def test_the_user_lock_is_taken_before_the_workspace_insert(
    create, ws_mock_db, mock_cursor, selection
):
    """As its own earlier statement: waiting on it after the INSERT would hold
    the new row's name and folder slots, and a rename blocked on those slots
    holds a row the lock holder's version bump needs, which deadlocks."""
    from src.server.database import workspace as W

    order: list[str] = []
    selection.lock.side_effect = lambda *args: order.append("lock")

    def execute(sql, *args, **kwargs):
        order.append("insert" if "INSERT INTO workspaces" in sql else "other")

    mock_cursor.execute.side_effect = execute
    # Home updates an existing row first; None sends it on to the insert.
    mock_cursor.fetchone.side_effect = lambda: (
        None
        if order[-1] == "other"
        else {**_workspace_row(dir_name="research"), "inserted": True}
    )

    # Which computers take name folders is read on its own connection.
    with patch.object(W, "get_computer", AsyncMock(return_value=None)):
        await create(W)

    assert "insert" in order
    assert order.index("lock") < order.index("insert")


@pytest.mark.asyncio
async def test_create_workspace_auto_id(ws_mock_db, mock_cursor):
    """create_workspace without workspace_id uses auto-generated UUID SQL."""
    from src.server.database.workspace import create_workspace

    row = _workspace_row()
    mock_cursor.fetchone.return_value = row

    result = await create_workspace("user-1", "New WS", description="desc")

    assert result["name"] == "New WS" or result["name"] == row["name"]
    sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO workspaces" in sql
    # Auto-generated path does NOT include workspace_id in VALUES
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == "user-1"  # user_id is first param


@pytest.mark.asyncio
async def test_create_workspace_with_explicit_id(ws_mock_db, mock_cursor):
    """create_workspace with workspace_id passes it in SQL params."""
    from src.server.database.workspace import create_workspace

    ws_id = str(uuid.uuid4())
    row = _workspace_row(workspace_id=ws_id)
    mock_cursor.fetchone.return_value = row

    result = await create_workspace(
        "user-1", "WS", workspace_id=ws_id, status="unbound"
    )

    assert result["workspace_id"] == ws_id
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == ws_id  # workspace_id is first param when explicit


@pytest.mark.asyncio
async def test_get_workspace_found(ws_mock_db, mock_cursor):
    """get_workspace returns dict when row exists."""
    from src.server.database.workspace import get_workspace

    ws_id = str(uuid.uuid4())
    row = _workspace_row(workspace_id=ws_id)
    mock_cursor.fetchone.return_value = row

    result = await get_workspace(ws_id)

    assert result is not None
    assert result["workspace_id"] == ws_id
    sql = mock_cursor.execute.call_args[0][0]
    assert "status != 'deleted'" in sql


@pytest.mark.asyncio
async def test_get_workspace_not_found(ws_mock_db, mock_cursor):
    """get_workspace returns None when no row is found."""
    from src.server.database.workspace import get_workspace

    mock_cursor.fetchone.return_value = None

    result = await get_workspace(str(uuid.uuid4()))
    assert result is None


@pytest.mark.asyncio
async def test_get_workspace_invalid_uuid_short_circuits(ws_mock_db, mock_cursor):
    """A non-UUID id returns None without touching the DB.

    Regression: a memory-file key (e.g. ``my_notes.md``)
    reaching ``WHERE workspace_id = %s`` against the uuid column raised
    psycopg InvalidTextRepresentation (22P02), which handlers surfaced as a
    spurious 500. The guard must short-circuit to None before any query.
    """
    from src.server.database.workspace import get_workspace

    for bad_id in ("my_notes.md", "ws-1", "nonexistent", "", None):
        assert await get_workspace(bad_id) is None

    mock_cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_workspace_normalizes_noncanonical_uuid(ws_mock_db, mock_cursor):
    """A Python-parseable but non-canonical id is bound to the query in canonical
    form, never raw.

    Regression: ``uuid.UUID()`` accepts forms postgres' uuid type rejects (e.g.
    ``urn:uuid:...``), so a validate-only guard let those reach postgres →
    InvalidTextRepresentation (22P02) → 500. The guard must normalize and bind
    the canonical string so postgres always accepts it.
    """
    from src.server.database.workspace import get_workspace

    canonical = "12345678-1234-5678-1234-567812345678"
    mock_cursor.fetchone.return_value = None
    for variant in (
        f"urn:uuid:{canonical}",
        "{%s}" % canonical,
        canonical.replace("-", ""),
        canonical.upper(),
    ):
        mock_cursor.execute.reset_mock()
        await get_workspace(variant)
        params = mock_cursor.execute.call_args[0][1]
        assert params == (canonical,), f"{variant!r} bound {params!r}, not canonical"


@pytest.mark.asyncio
async def test_get_workspaces_for_user(ws_mock_db, mock_cursor):
    """get_workspaces_for_user returns paginated list and total count."""
    from src.server.database.workspace import get_workspaces_for_user

    # First call: COUNT query; second call: paginated SELECT
    ws1 = _workspace_row(workspace_id="ws-1")
    ws2 = _workspace_row(workspace_id="ws-2")
    mock_cursor.fetchone.side_effect = [{"total": 2}]
    mock_cursor.fetchall.return_value = [ws1, ws2]

    workspaces, total = await get_workspaces_for_user("user-1", limit=10, offset=0)

    assert total == 2
    assert len(workspaces) == 2
    # Home stays out by its id and by its status, as every listing leaves it.
    count_sql, params = mock_cursor.execute.call_args_list[0][0]
    assert EXCLUDE_HOME in count_sql
    assert params["home_id"] == get_home_workspace_id("user-1")


@pytest.mark.asyncio
async def test_get_workspace_names_leaves_home_out_as_the_listing_does(ws_mock_db, mock_cursor):
    from src.server.database.workspace import get_workspace_names

    mock_cursor.fetchall.return_value = []
    await get_workspace_names("user-1")

    sql, params = mock_cursor.execute.call_args[0]
    assert EXCLUDE_HOME in sql
    assert params["home_id"] == get_home_workspace_id("user-1")


@pytest.mark.asyncio
async def test_update_workspace_name(ws_mock_db, mock_cursor):
    """update_workspace builds dynamic SET clause for provided fields."""
    from src.server.database.workspace import update_workspace

    row = _workspace_row(name="Renamed")
    mock_cursor.fetchone.return_value = row

    result = await update_workspace("ws-1", name="Renamed")

    assert result is not None
    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE workspaces" in sql
    assert "name = %s" in sql


@pytest.mark.asyncio
async def test_rename_collision_after_the_row_is_gone_is_not_found(ws_mock_db, mock_cursor):
    """The name was taken, and the row went before its owner could be read: a
    missing row answers not found, not a TypeError."""
    from types import SimpleNamespace

    from psycopg.errors import UniqueViolation

    from src.server.database import workspace as W

    class NameTaken(UniqueViolation):
        @property
        def diag(self):
            return SimpleNamespace(constraint_name=W._USER_NAME_INDEX)

    mock_cursor.execute = AsyncMock(side_effect=[NameTaken("duplicate key"), None])
    mock_cursor.fetchone.return_value = None

    assert await W.update_workspace("ws-1", name="Taken") is None


@pytest.mark.asyncio
async def test_update_workspace_status_stopped(ws_mock_db, mock_cursor):
    """update_workspace_status with 'stopped' sets stopped_at."""
    from src.server.database.workspace import update_workspace_status

    row = _workspace_row(status="stopped")
    mock_cursor.fetchone.return_value = row

    result = await update_workspace_status("ws-1", "stopped")

    assert result["status"] == "stopped"
    sql = mock_cursor.execute.call_args[0][0]
    assert "stopped_at" in sql


@pytest.mark.asyncio
async def test_update_workspace_status_running(ws_mock_db, mock_cursor):
    """A status move omits stopped_at, and never touches sandbox_id.

    ``sandbox_id`` shadows ``computers.provider_ref``, and the binding is owned
    solely by ``try_bind_computer_provider_ref``'s compare-and-set; a status
    writer that could also rebind is how a workspace ends up pointing at a
    sandbox that was already replaced.
    """
    from src.server.database.workspace import update_workspace_status

    row = _workspace_row(status="running")
    mock_cursor.fetchone.return_value = row

    result = await update_workspace_status("ws-1", "running")

    assert result["status"] == "running"
    sql = mock_cursor.execute.call_args[0][0]
    # stopped_at, sandbox_id and provider_ref should NOT be in either SET clause
    # (sandbox_id appears in RETURNING). The statement moves two rows now, so
    # check every SET it carries, not just the first.
    set_clauses = [part.split("WHERE")[0] for part in sql.split("SET")[1:]]
    assert set_clauses
    for set_clause in set_clauses:
        assert "stopped_at" not in set_clause
        assert "sandbox_id" not in set_clause
        assert "provider_ref" not in set_clause


@pytest.mark.asyncio
async def test_delete_workspace_soft(ws_mock_db, mock_cursor):
    """delete_workspace (soft) updates status to 'deleted'."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = {
        "workspace_id": "ws-1", "user_id": "user-1", "busy": False, "moving": False,
    }

    deleted = await delete_workspace("ws-1")

    assert deleted is True
    sql = next(
        args.args[0] for args in mock_cursor.execute.call_args_list
        if "UPDATE workspaces" in args.args[0]
    )
    assert "UPDATE workspaces" in sql
    assert "status = 'deleted'" in sql
    assert "folder_cleanup_pending" in sql


def _delete_update(mock_cursor):
    return next(
        call.args for call in mock_cursor.execute.call_args_list
        if "UPDATE workspaces" in call.args[0]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("keep", [False, True])
async def test_a_delete_marks_the_folder_kept_or_queued_never_both(
    ws_mock_db, mock_cursor, keep
):
    """The reaper removes what is queued and the settle keeps what is kept, so
    a row carrying both from an earlier delete would be decided twice."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = {
        "workspace_id": "ws-1", "user_id": "user-1", "computer_id": "comp-a",
        "busy": False, "moving": False,
    }

    assert await delete_workspace("ws-1", bound_to="comp-a", keep_folder=keep)

    sql, params = _delete_update(mock_cursor)
    assert params["keep"] is keep
    kept_branch, queued_branch = sql.split("WHEN %(keep)s")[1].split("ELSE")[:2]
    assert "#- '{folder_cleanup_pending}'" in kept_branch
    assert "'{folder_kept}'" in kept_branch
    assert "#- '{folder_kept}'" in queued_branch
    assert "'{folder_cleanup_pending}'" in queued_branch


@pytest.mark.asyncio
async def test_a_delete_is_refused_once_the_workspace_moved_elsewhere(
    ws_mock_db, mock_cursor
):
    """Whether the folder stays was decided for the computer the caller saw;
    on another one the same choice could delete files its owner kept."""
    from src.server.database.workspace import WorkspaceBusyError, delete_workspace

    mock_cursor.fetchone.return_value = {
        "workspace_id": "ws-1", "user_id": "user-1", "computer_id": "comp-b",
        "busy": False, "moving": False,
    }

    with pytest.raises(WorkspaceBusyError):
        await delete_workspace("ws-1", bound_to="comp-a", keep_folder=True)

    assert not any(
        "UPDATE workspaces" in call.args[0]
        for call in mock_cursor.execute.call_args_list
    )


@pytest.mark.asyncio
async def test_delete_workspace_never_removes_the_row(ws_mock_db, mock_cursor):
    """A removed row takes the layout owner's identity with it, and migration
    046's ON DELETE SET NULL then leaves its folder owned by nobody."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = {
        "workspace_id": "ws-1", "user_id": "user-1", "busy": False, "moving": False,
    }

    await delete_workspace("ws-1")

    sql = next(
        args.args[0] for args in mock_cursor.execute.call_args_list
        if "UPDATE workspaces" in args.args[0]
    )
    assert "DELETE FROM" not in sql


@pytest.mark.asyncio
async def test_pending_folder_cleanup_only_selects_running_computers(
    ws_mock_db, mock_cursor
):
    from src.server.database.workspace import get_pending_workspace_folder_cleanups

    mock_cursor.fetchall.return_value = []

    assert await get_pending_workspace_folder_cleanups() == []

    sql = mock_cursor.execute.call_args.args[0]
    assert "JOIN computers c ON c.computer_id = w.computer_id" in sql
    assert "c.status = 'running'" in sql
    assert "c.provider_ref IS NOT NULL" in sql


@pytest.mark.asyncio
async def test_delete_workspace_not_found(ws_mock_db, mock_cursor):
    """delete_workspace returns False when no row is affected."""
    from src.server.database.workspace import delete_workspace

    mock_cursor.fetchone.return_value = None

    deleted = await delete_workspace("nonexistent")
    assert deleted is False


@pytest.mark.asyncio
async def test_batch_update_sort_order(ws_mock_db, mock_cursor):
    """batch_update_sort_order builds VALUES list and includes user_id check."""
    from src.server.database.workspace import batch_update_sort_order

    mock_cursor.rowcount = 2

    await batch_update_sort_order("user-1", [("ws-1", 0), ("ws-2", 1)])

    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE workspaces" in sql
    assert "v.wid::uuid" in sql
    params = mock_cursor.execute.call_args[0][1]
    # Last param should be user_id
    assert params[-1] == "user-1"


@pytest.mark.asyncio
async def test_batch_update_sort_order_empty(ws_mock_db, mock_cursor):
    """batch_update_sort_order with empty list returns immediately."""
    from src.server.database.workspace import batch_update_sort_order

    await batch_update_sort_order("user-1", [])

    mock_cursor.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_workspaces_by_status(ws_mock_db, mock_cursor):
    """get_workspaces_by_status filters by status and returns list."""
    from src.server.database.workspace import get_workspaces_by_status

    ws1 = _workspace_row(status="stopped")
    mock_cursor.fetchall.return_value = [ws1]

    result = await get_workspaces_by_status("stopped", limit=50)

    assert len(result) == 1
    sql = mock_cursor.execute.call_args[0][0]
    assert "WHERE status = %s" in sql
    params = mock_cursor.execute.call_args[0][1]
    assert params[0] == "stopped"
    assert params[1] == 50


@pytest.mark.asyncio
async def test_update_workspace_activity(ws_mock_db, mock_cursor):
    """update_workspace_activity uses conditional UPDATE (skip if <60s)."""
    from src.server.database.workspace import update_workspace_activity

    # The workspace half of the write-through reports whether it stamped, so a
    # caller cannot read the computer's own cooldown as this workspace's.
    mock_cursor.fetchone.return_value = {"stamped": 1}

    result = await update_workspace_activity("ws-1")

    assert result is True
    sql = mock_cursor.execute.call_args[0][0]
    assert "last_activity_at" in sql
    assert "status <> 'deleted'" in sql
    assert "INTERVAL" in sql  # conditional: skip if <60s

    mock_cursor.fetchone.return_value = {"stamped": 0}
    assert await update_workspace_activity("ws-1") is False


@pytest.mark.asyncio
async def test_update_workspace_activity_not_found(ws_mock_db, mock_cursor):
    """update_workspace_activity returns False when workspace not found (conditional UPDATE, no rows matched)."""
    from src.server.database.workspace import update_workspace_activity

    mock_cursor.rowcount = 0

    result = await update_workspace_activity("nonexistent")
    assert result is False


# ---------------------------------------------------------------------------
# The computer write-through (migration 046)
#
# Every sandbox-lifecycle column left on this table is a shadow of the
# ``computers`` row. What these lock is that each writer stays ONE statement
# with ``comp`` ahead of ``shadow`` -- two statements can half-commit and leave
# a reader seeing a machine that is running on one row and stopped on the other,
# and the shared order is what keeps concurrent writers from deadlocking.
# ---------------------------------------------------------------------------


def _one_write_through_statement(mock_cursor):
    assert mock_cursor.execute.await_count == 1, "two statements can half-commit"
    sql = " ".join(mock_cursor.execute.call_args[0][0].split())
    assert "UPDATE computers" in sql and "UPDATE workspaces" in sql
    assert sql.index("WITH comp AS") < sql.index("shadow AS")
    return sql


@pytest.mark.asyncio
async def test_a_status_move_carries_the_computer_with_it(ws_mock_db, mock_cursor):
    from src.server.database.workspace import update_workspace_status

    mock_cursor.fetchone.return_value = _workspace_row(status="stopping")

    await update_workspace_status("ws-1", "stopping")

    sql = _one_write_through_statement(mock_cursor)
    # One spelling of the fence, from sql_fences, on both sides of the shadow.
    assert "c.status <> 'deleted'" in sql
    assert "w.status <> 'deleted'" in sql


@pytest.mark.asyncio
async def test_running_publication_cas_fences_the_workspace_shadow(
    ws_mock_db, mock_cursor
):
    from src.server.database.workspace import update_workspace_status

    mock_cursor.fetchone.return_value = _workspace_row(status="running")
    await update_workspace_status(
        "ws-1", "running", expected_computer_always_on=True
    )

    sql = _one_write_through_statement(mock_cursor)
    assert "c.is_always_on = %(expected_always_on)s" in sql
    assert "(SELECT count(*) FROM comp) > 0" in sql


@pytest.mark.asyncio
async def test_a_soft_delete_leaves_the_machine_alone(ws_mock_db, mock_cursor):
    """Deleting a project is not deleting the machine: several projects will
    share one, and the teardown belongs where the sandbox is destroyed."""
    from src.server.database.workspace import delete_workspace, update_workspace_status

    mock_cursor.fetchone.return_value = {
        "workspace_id": "ws-1", "user_id": "user-1", "busy": False, "moving": False,
    }
    await delete_workspace("ws-1")
    assert "computers" not in mock_cursor.execute.call_args[0][0]

    mock_cursor.fetchone.return_value = _workspace_row(status="deleted")
    await update_workspace_status("ws-1", "deleted")
    assert "computers" not in mock_cursor.execute.call_args[0][0]


@pytest.mark.asyncio
async def test_an_unbound_home_has_no_machine_to_move(ws_mock_db, mock_cursor):
    """'unbound' is not in 046's CHECK set, so mirroring it would fail the
    statement after it had already moved the workspace row."""
    from src.server.database.workspace import update_workspace_status

    mock_cursor.fetchone.return_value = _workspace_row(status="unbound")

    await update_workspace_status("ws-1", "unbound")

    assert "computers" not in mock_cursor.execute.call_args[0][0]


@pytest.mark.asyncio
async def test_the_activity_stamp_moves_both_rows_on_their_own_cooldowns(
    ws_mock_db, mock_cursor
):
    """A second project's turn has to refresh the machine even inside this
    workspace's quiet minute, so each side carries the cooldown itself."""
    from src.server.database.workspace import update_workspace_activity

    mock_cursor.fetchone.return_value = {"stamped": 1}

    await update_workspace_activity("ws-1")

    sql = _one_write_through_statement(mock_cursor)
    assert sql.count("INTERVAL '60 seconds'") == 2
    assert "c.last_activity_at IS NULL" in sql


def _shadow_set(sql: str) -> str:
    shadow = sql[sql.index("shadow AS") :]
    return shadow[shadow.index("SET") : shadow.index("FROM")]


@pytest.mark.asyncio
async def test_only_the_workspace_s_own_use_moves_it_in_the_gallery(
    ws_mock_db, mock_cursor
):
    """The gallery orders by updated_at. A machine write reaches every folder
    on the computer, and stamping them all gave them one timestamp after each
    idle stop; the workspace's own activity is what should move it."""
    from src.server.database.workspace import (
        update_workspace_activity,
        update_workspace_status,
    )

    mock_cursor.fetchone.return_value = _workspace_row(status="stopped")
    await update_workspace_status("ws-1", "stopped")
    assert "updated_at" not in _shadow_set(_one_write_through_statement(mock_cursor))

    mock_cursor.execute.reset_mock()
    mock_cursor.fetchone.return_value = {"stamped": 1}
    await update_workspace_activity("ws-1")
    assert "updated_at = %(now)s" in _shadow_set(_one_write_through_statement(mock_cursor))


@pytest.mark.asyncio
async def test_the_preview_cache_is_written_to_the_machine_too(ws_mock_db, mock_cursor):
    """A port is a namespace of the machine, not of one project."""
    from src.server.database.workspace import save_preview_command

    await save_preview_command("ws-1", 8080, "python -m http.server 8080")

    sql = " ".join(mock_cursor.execute.call_args[0][0].split())
    assert sql.index("UPDATE computers") < sql.index("UPDATE workspaces")
    assert sql.count("'{preview_servers}'") == 2
