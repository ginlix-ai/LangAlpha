"""Which folders on a computer must move, and the rows that record each move.

``dir_name`` is where a workspace's folder physically is; its name decides
where it should be. A settle closes the gap in two recorded steps. Staging
points the row at ``_internal/moving/<id>``, prepends the old folder to
``previous_dir_names`` and adds the folder the pass plans to land on to
``config.folder_landings``, all before anything moves, so a crash anywhere
leaves the content in one of three places the next settle checks in order:
where the move script journaled its last landing, which counts only if the row
records a pass planning it, the staging folder, the old folder.
"""

from __future__ import annotations

import asyncio
import logging
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

from ptc_agent.core.paths import SandboxLayout
from src.server.database import pool
from src.server.database.home_workspace import is_home_row
from src.server.database.pool import get_db_connection
from src.server.database.session_lock import (
    SharedLockBusy,
    await_settled,
    shared_lock_session,
)
from src.server.database.sql_fences import MOVE_SET_ASIDE_PHASES, workspace_move_fences
from src.server.database.workspace_names import (
    HOME_FOLDER,
    WorkspaceNameInvalid,
    placeholder_dir_name,
    workspace_folder_name,
)

logger = logging.getLogger(__name__)

MOVING_DIR = "_internal/moving"
# One pass holds the folder lock, and the acquisition waiting on it, until it
# ends; the rest of a long backlog clears on later acquisitions.
TOMBSTONES_PER_PASS = 200
LEFTOVERS_DIR = SandboxLayout.LEFTOVERS_DIR
PREVIOUS_DIR_NAMES_MAX = 8
# The layout the folder-per-workspace split landed in; older machines keep
# files at the root, where no folder of theirs can move.
FOLDER_LAYOUT_VERSION = 4


def takes_name_folders(computer: Mapping[str, Any]) -> bool:
    """Whether a folder placed on this computer now may be spelled as its name.

    A sandbox not yet on the folder layout keeps its origin workspace's files
    at the root, and the move to it skips every root entry a row names, so a
    row named like one of those entries would take it. Such a row starts on a
    placeholder, which the first settle after the move renames.
    """
    return not computer.get("provider_ref") or int(
        computer.get("layout_version") or 0
    ) >= FOLDER_LAYOUT_VERSION


_LOCK_NS = "WORKSPACE_FOLDERS"
_IN_USE_NS = "WORKSPACE_FOLDER_IN_USE"
_ALLOC_NS = "WORKSPACE_FOLDER_ALLOC"
_LOCK_WAIT_S = 30
_LOCK_RETRY_S = 0.25
# Folder-lock sessions one process holds at once, across computers: they are
# outside the pool, so this is their bound.
_LOCK_SESSIONS_MAX = 4


class WorkspaceFolderMoving(RuntimeError):
    """A settle held the workspace's folder past the wait."""


def moving_path(workspace_id: str) -> str:
    return f"{MOVING_DIR}/{workspace_id}"


def leftovers_path(workspace_id: str) -> str:
    return f"{LEFTOVERS_DIR}/{workspace_id}"


def is_top_level(dir_name: str) -> bool:
    return bool(dir_name) and "/" not in dir_name


def recorded_landings(value: Any) -> tuple[str, ...]:
    """``config.folder_landings`` as stored; a client writes ``config`` too."""
    return tuple(f for f in value if isinstance(f, str)) if isinstance(value, list) else ()


async def release_former_folder(
    cur, *, computer_id: str, workspace_id: str, folder: str
) -> None:
    """A folder a workspace takes stops being any sibling's former folder or landing.

    Every reader folds a former folder's paths into its workspace, and one a
    sibling now holds would send the sibling's files to the wrong workspace.
    A staged sibling's move script takes back what sits on any landing its row
    records, which here is this workspace's content. Readers match with
    Python's casefold, which Postgres's ``lower`` is not (``Straße`` and
    ``STRASSE``), so the spellings to drop are picked here. Takes a
    ``dict_row`` cursor.
    """
    scope = {"computer": computer_id, "id": workspace_id}
    await cur.execute(
        """
        SELECT DISTINCT f.p AS folder
        FROM workspaces, unnest(previous_dir_names) AS f(p)
        WHERE computer_id = %(computer)s AND workspace_id <> %(id)s
        """,
        scope,
    )
    folded = folder.casefold()
    spellings = [
        row["folder"]
        for row in await cur.fetchall()
        if row["folder"] and row["folder"].casefold() == folded
    ]
    if spellings:
        await cur.execute(
            """
            UPDATE workspaces
            SET previous_dir_names = ARRAY(
                SELECT f.p FROM unnest(previous_dir_names) WITH ORDINALITY AS f(p, i)
                WHERE f.p <> ALL(%(spellings)s::text[]) ORDER BY f.i
            )
            WHERE computer_id = %(computer)s AND workspace_id <> %(id)s
              AND previous_dir_names && %(spellings)s::text[]
            """,
            {**scope, "spellings": spellings},
        )
    await cur.execute(
        """
        SELECT workspace_id, config->'folder_landings' AS landings FROM workspaces
        WHERE computer_id = %(computer)s AND workspace_id <> %(id)s
          AND dir_name = %(moving)s || workspace_id::text
        """,
        {**scope, "moving": f"{MOVING_DIR}/"},
    )
    for row in await cur.fetchall():
        landings = recorded_landings(row.get("landings"))
        kept = [f for f in landings if f.casefold() != folded]
        if len(kept) < len(landings):
            await cur.execute(
                """
                UPDATE workspaces SET config = jsonb_set(config, '{folder_landings}', %s::jsonb)
                WHERE workspace_id = %s
                """,
                (Json(kept), row["workspace_id"]),
            )


@dataclass(frozen=True, slots=True)
class FolderRow:
    workspace_id: str
    name: Optional[str]
    dir_name: str
    deleted: bool
    previous_dir_names: tuple[str, ...] = ()
    # Every folder a pass planned to land this row on since it was staged.
    landings: tuple[str, ...] = ()
    # Home's folder is named for Home whatever its row is called.
    home: bool = False
    # A target the last pass had no disk to copy this row's folder to, while
    # its backoff lasts: each acquisition would copy and fail the same way.
    blocked: Optional[str] = None
    # A deleted workspace whose folder its owner kept on their machine.
    kept: bool = False

    @property
    def folder_label(self) -> Optional[str]:
        # Home bound beside a folder already called Home waits on a
        # placeholder, and moves in once that folder is free.
        return HOME_FOLDER if self.home else self.name

    @property
    def staged(self) -> bool:
        return self.dir_name == moving_path(self.workspace_id)

    @property
    def source(self) -> Optional[str]:
        """Where the content was before staging, when this row is mid-move."""
        if self.staged:
            return self.previous_dir_names[0] if self.previous_dir_names else None
        return self.dir_name

    def target(self) -> Optional[str]:
        if self.deleted:
            return None
        try:
            return workspace_folder_name(self.folder_label)
        except WorkspaceNameInvalid:
            # A name an older build let through; it keeps the folder it has.
            return None


@dataclass(frozen=True, slots=True)
class FolderMove:
    workspace_id: str
    source: Optional[str]
    target: str
    staged: bool
    # What earlier passes planned: the move script's journal is
    # sandbox-writable, and an entry naming any other folder is not a landing.
    landings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class FolderPlan:
    moves: tuple[FolderMove, ...]
    tombstones: tuple[FolderRow, ...]
    # Deleted workspaces whose folders this pass leaves: an earlier pass may
    # have moved one aside unrecorded, and only its journal entry says so.
    deferred: tuple[str, ...] = ()
    # Rows still to move, here or on a later pass: a copy one began where a
    # rename could not move its folder is kept for the pass that finishes it.
    copying: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.moves or self.tombstones)


def plan_folder_moves(rows: Iterable[FolderRow], busy: set[str] = frozenset()) -> FolderPlan:
    """Every move that can land now, and the tombstone folders to clear first.

    A busy workspace stays put, and so does one whose target another staying
    row holds, repeated until nothing changes: a row that stays keeps holding
    its folder, which can strand the next one. A staged row always moves, back
    to its old folder or to a placeholder when its target is held, because a
    folder under ``_internal`` is one the agent cannot use. The exception is
    one the last pass had no room to copy out, while that backoff lasts:
    staged across an archive, it leaves staging only by a copy, and every
    acquisition on the computer would copy until the disk filled, then delete.
    """
    rows = list(rows)
    live = [r for r in rows if not r.deleted]
    fold = str.casefold
    movers: dict[str, FolderRow] = {}
    seen_targets: set[str] = set()
    # A staged row moves whatever it meets, so it claims its target first and
    # a row whose target folds alike waits where it is.
    for row in sorted(live, key=lambda r: not r.staged):
        target = row.target()
        if (row.staged and not row.blocked) or (
            not row.staged
            and target is not None
            and target != row.dir_name
            and row.workspace_id not in busy
            and target != row.blocked
            # Two rows folding alike only happens across an old build's write.
            and fold(target) not in seen_targets
        ):
            movers[row.workspace_id] = row
            if target is not None:
                seen_targets.add(fold(target))

    # A tombstone this pass leaves, past its cap or busy with a file change
    # still writing into it, holds its folder like a staying row: landing
    # there would evict it now and have its own clearing take the landed
    # content later. A staged one always goes, since only this pass's journal
    # remembers its landing.
    staged_dead = [r for r in rows if r.deleted and r.staged]
    top_dead = [r for r in rows if r.deleted and is_top_level(r.dir_name)]
    # A kept folder stays where its owner left it, until a live row needs the
    # name; then it goes aside under leftovers, as the owner's still.
    clearable = sorted(
        (
            r
            for r in top_dead
            if r.workspace_id not in busy
            and (not r.kept or fold(r.dir_name) in seen_targets)
        ),
        key=lambda r: fold(r.dir_name) not in seen_targets,
    )
    room = max(TOMBSTONES_PER_PASS - len(staged_dead), 0)
    tombstones = staged_dead + clearable[:room]
    cleared = {r.workspace_id for r in tombstones}
    left = [r for r in top_dead if r.workspace_id not in cleared]
    left_folders = {fold(r.dir_name) for r in left}

    def held_by_stayer(folder: str, mover_id: str) -> bool:
        return fold(folder) in left_folders or any(
            fold(r.dir_name) == fold(folder)
            and r.workspace_id not in movers
            and r.workspace_id != mover_id
            for r in live
        )

    changed = True
    while changed:
        changed = False
        for row in list(movers.values()):
            target = row.target()
            if not row.staged and (target is None or held_by_stayer(target, row.workspace_id)):
                del movers[row.workspace_id]
                changed = True

    landing = {fold(t) for r in movers.values() if (t := r.target()) is not None}
    taken: set[str] = set()

    def free(folder: str, mover_id: str) -> bool:
        return not held_by_stayer(folder, mover_id) and fold(folder) not in landing | taken

    moves = []
    for row in movers.values():
        destination = row.target()
        # Two staged rows can fold alike; the later one goes back, not onto it.
        if (
            destination is None
            or fold(destination) in taken
            or held_by_stayer(destination, row.workspace_id)
        ):
            back = row.source
            if back and is_top_level(back) and free(back, row.workspace_id):
                destination = back
            else:
                salt = 0
                destination = placeholder_dir_name(row.folder_label, row.workspace_id, hex_chars=8)
                while not free(destination, row.workspace_id):
                    # A staying row holds this spelling (a workspace named
                    # so); another digest of the same id is as good.
                    salt += 1
                    destination = placeholder_dir_name(
                        row.folder_label, f"{row.workspace_id}/{salt}", hex_chars=8
                    )
        taken.add(fold(destination))
        moves.append(
            FolderMove(row.workspace_id, row.source, destination, row.staged, row.landings)
        )
    copying = tuple(
        r.workspace_id for r in live if r.staged or r.target() not in (None, r.dir_name)
    )
    return FolderPlan(
        tuple(moves), tuple(tombstones), tuple(r.workspace_id for r in left), copying
    )


def _row(record: Mapping[str, Any]) -> FolderRow:
    workspace_id = str(record["workspace_id"])
    staged = record["dir_name"] == moving_path(workspace_id)
    return FolderRow(
        workspace_id=workspace_id,
        name=record["name"],
        dir_name=record["dir_name"],
        deleted=bool(record["deleted"]),
        previous_dir_names=tuple(record.get("previous_dir_names") or ()),
        # Staging writes a fresh list, so any other row's is nothing a pass planned.
        landings=recorded_landings(record.get("landings")) if staged else (),
        home=is_home_row(record),
        blocked=record.get("move_blocked"),
        kept=bool(record.get("folder_kept")),
    )


# A deleted row counts while its cleanup is pending, and while it still names
# a top-level folder: a build before this layout cleared the folder but kept
# the name, which would hold it on the unique folder index forever.
_HOLDS_FOLDER = """
    w.dir_name IS NOT NULL
    AND (w.status <> 'deleted'
         OR strpos(w.dir_name, '/') = 0
         OR COALESCE(w.config, '{}'::jsonb) @> '{"folder_cleanup_pending": true}')
"""

# Epoch seconds by the database clock, which wrote them. Nested, as a guard
# beside the cast in one condition would not keep the cast from running.
_MOVE_BLOCKED = """
    CASE WHEN jsonb_typeof(w.config->'folder_move_blocked'->'until') = 'number'
         THEN CASE WHEN (w.config->'folder_move_blocked'->>'until')::numeric
                        > extract(epoch FROM now())
                   THEN w.config->'folder_move_blocked'->>'target' END
    END AS move_blocked
"""

_FOLDER_KEPT = """
    COALESCE(w.config, '{}'::jsonb) @> '{"folder_kept": true}' AS folder_kept
"""

_ROWS_SQL = f"""
    SELECT w.workspace_id, w.name, w.dir_name, w.status = 'deleted' AS deleted,
           w.previous_dir_names, w.config->'folder_landings' AS landings,
           w.user_id, w.status, {_MOVE_BLOCKED}, {_FOLDER_KEPT}
    FROM workspaces w
    WHERE w.computer_id = %s AND {_HOLDS_FOLDER}
    ORDER BY w.created_at, w.workspace_id
"""

_RUNNING_SQL = """
    SELECT 1 FROM conversation_responses r
    JOIN conversation_threads t
      ON t.conversation_thread_id = r.conversation_thread_id
    JOIN workspaces w ON w.workspace_id = t.workspace_id
    WHERE w.computer_id = %(computer)s
      AND r.status = 'in_progress'
      AND r.conversation_response_id IS DISTINCT FROM %(own)s::uuid
    UNION ALL
    SELECT 1 FROM subagent_runs r
    JOIN conversation_threads t
      ON t.conversation_thread_id = r.thread_id
    JOIN workspaces w ON w.workspace_id = t.workspace_id
    WHERE w.computer_id = %(computer)s
      AND r.status = 'in_progress'
    LIMIT 1
"""

_COMMAND_RUNNING_SQL = """
    SELECT 1 FROM command_runs
    WHERE computer_id = %(computer)s AND status = 'running'
    LIMIT 1
"""


async def read_folder_rows(computer_id: str) -> tuple[Optional[int], list[FolderRow]]:
    """The computer's observed layout version and every row holding a folder on it."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT c.layout_version, w.workspace_id, w.name, w.dir_name,
                   w.status = 'deleted' AS deleted, w.previous_dir_names,
                   w.config->'folder_landings' AS landings,
                   w.user_id, w.status, {_MOVE_BLOCKED}, {_FOLDER_KEPT}
            FROM computers c
            LEFT JOIN workspaces w
              ON w.computer_id = c.computer_id AND {_HOLDS_FOLDER}
            WHERE c.computer_id = %s
            ORDER BY w.created_at, w.workspace_id
            """,
            (computer_id,),
        )
        records = await cur.fetchall()
    if not records:
        return None, []
    version = records[0]["layout_version"]
    rows = [_row(r) for r in records if r["workspace_id"] is not None]
    return (int(version) if version else None), rows


async def computer_run_in_progress(
    db, computer_id: str, *, own_run_id: Optional[str] = None
) -> bool:
    """Whether a run other than ``own_run_id`` is in progress on the computer.

    An agent reaches a sibling's folder as ``../<folder>`` from its own
    code, so a run anywhere on the computer may be using any folder on it.
    """
    cur = await db.execute(_RUNNING_SQL, {"computer": computer_id, "own": own_run_id})
    return await cur.fetchone() is not None


async def computer_command_running(db, computer_id: str) -> bool:
    """Whether a handed-off command still runs on the computer. Its code
    reaches the folders there as a run's does, and it can outlive every turn."""
    cur = await db.execute(_COMMAND_RUNNING_SQL, {"computer": computer_id})
    return await cur.fetchone() is not None


async def _busy(
    cur, computer_id: str, rows: Iterable[FolderRow], own_run_id: Optional[str]
) -> set[str]:
    """Every row while any run or command on the computer is in progress, else
    any being moved off it, whose folder the move sets aside by the name it has now."""
    rows = list(rows)
    if await computer_run_in_progress(
        cur, computer_id, own_run_id=own_run_id
    ) or await computer_command_running(cur, computer_id):
        return {r.workspace_id for r in rows}
    if not rows:
        return set()
    await cur.execute(
        f"""
        SELECT workspace_id FROM workspaces
        WHERE workspace_id = ANY(%s::uuid[])
          AND {workspace_move_fences("workspaces.workspace_id")}
        """,
        ([r.workspace_id for r in rows],),
    )
    return {str(row["workspace_id"]) for row in await cur.fetchall()}


async def busy_workspace_ids(
    computer_id: str, rows: Iterable[FolderRow], *, own_run_id: Optional[str] = None
) -> set[str]:
    """Workspaces whose folders stay put this settle, because a run may use them."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        return await _busy(cur, computer_id, rows, own_run_id)


_LOCK_HELD_SQL = """
    SELECT EXISTS (
        SELECT 1 FROM pg_locks, hashtextextended(%s, 0) AS k(h)
        WHERE locktype = 'advisory' AND granted AND objsubid = 1
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = ((h >> 32) & 4294967295)::oid
          AND objid = (h & 4294967295)::oid
    ) AS held
"""


async def _lock_held(key: str) -> bool:
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_LOCK_HELD_SQL, (key,))
        return bool((await cur.fetchone())["held"])


async def _try_lock(conn, key: str) -> bool:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS held", (key,)
        )
        return bool((await cur.fetchone())["held"])


_contenders: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, set[str]] = (
    weakref.WeakKeyDictionary()
)


def _claim_contention(computer_id: str) -> bool:
    """Be this process's one contender for the computer's lock, if no other
    is and the process is under its session bound. Never awaits, so the
    look and the claim cannot be split."""
    active = _contenders.setdefault(asyncio.get_running_loop(), set())
    if computer_id in active or len(active) >= _LOCK_SESSIONS_MAX:
        return False
    active.add(computer_id)
    return True


def _drop_contention(computer_id: str) -> None:
    _contenders.get(asyncio.get_running_loop(), set()).discard(computer_id)


@asynccontextmanager
async def workspace_folders_lock(computer_id: str, *, wait_s: float = _LOCK_WAIT_S):
    """One settle or folder removal per computer, across workers.

    Held on a connection of its own, closed on exit with every folder hold
    ``stage_folder_moves`` took on it: the holder keeps it through a sandbox
    script that can outlast a minute, and a pool slot kept that long, or one
    per caller waiting behind it, is one no other request can use. A waiter
    watches ``pg_locks`` through brief checkouts and opens its connection once
    the lock is free; within a process only one caller per computer looks,
    so the callers queued behind a settle open one connection when it ends,
    not one each. Yields that connection, or None when another holder kept
    the lock past ``wait_s``: the caller leaves folders where they are this
    time. A caller on a turn's path waits briefly, as a settle can hold the
    lock for as long as its sandbox script runs.
    """
    key = f"{_LOCK_NS}:{computer_id}"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    conn = None
    claimed = False
    try:
        while True:
            if _claim_contention(computer_id):
                claimed = True
                if not await _lock_held(key):
                    conn = await pool.open_session_connection()
                    if await _try_lock(conn, key):
                        break
                    # Another worker took it between the look and the try.
                    await conn.close()
                    conn = None
                _drop_contention(computer_id)
                claimed = False
            if loop.time() >= deadline:
                logger.warning(
                    f"Folder lock on computer {computer_id} still held after {wait_s:g}s"
                )
                break
            await asyncio.sleep(_LOCK_RETRY_S)
        yield conn
    finally:
        try:
            if conn is not None:
                await await_settled(asyncio.ensure_future(conn.close()))
        finally:
            if claimed:
                # Once the session is closed, so the bound counts it, and
                # even when the close re-raises a cancellation.
                _drop_contention(computer_id)


async def _lock_folder_allocation(db, computer_id: str, *, shared: bool) -> None:
    lock = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
    key = f"{_ALLOC_NS}:{computer_id}"
    await db.execute(f"SELECT {lock}(hashtextextended(%s, 0))", (key,))


@asynccontextmanager
async def folder_allocation(computer_id: str, *, conn=None):
    """A transaction to pick a new folder on the computer and take it in.

    A stage records the folder it plans for a staged row only in that row's
    config, where the folder index cannot see it, so a folder picked from rows
    read before the stage commits, and taken after, can be the one it plans.
    Allocations share the lock, as the index already orders them, and keep it
    until their row is visible: joined to a caller's transaction, until that
    commits. A stage takes it alone, for its database window only, never across
    the sandbox exec.
    """
    async with get_db_connection(conn) as owned, owned.transaction():
        await _lock_folder_allocation(owned, computer_id, shared=True)
        yield owned


@asynccontextmanager
async def workspace_folder_in_use(workspace_id: str):
    """Keep a workspace's folder where it is for one file change.

    A settle moves only folders it can hold exclusively, so a change whose
    paths were read under this lands before the move or after it. Between the
    two, ``mkdir -p`` would recreate the folder the move left, and the file
    would land where nothing reads it. A move to another computer sets the
    folder aside under this hold too, but the row names the folder it left
    until the move ends, so from staging on the hold is refused. Held on the
    process's shared lock session rather than a pooled connection, so the work
    under it reads through the pool freely, and a hold taken inside another
    stacks on it.
    """
    key = f"{_IN_USE_NS}:{workspace_id}"
    locks = shared_lock_session()
    try:
        conn = await locks.acquire(key, wait_s=_LOCK_WAIT_S)
    except SharedLockBusy as e:
        raise WorkspaceFolderMoving(workspace_id) from e
    try:
        if await _folder_set_aside(workspace_id):
            raise WorkspaceFolderMoving(workspace_id)
        yield
    finally:
        await locks.release(conn, key)


async def _folder_set_aside(workspace_id: str) -> bool:
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT EXISTS (SELECT 1 FROM workspace_moves"
            " WHERE workspace_id = %s AND phase = ANY(%s)) AS set_aside",
            (workspace_id, list(MOVE_SET_ASIDE_PHASES)),
        )
        return bool((await cur.fetchone())["set_aside"])


async def hold_workspace_folder(conn, workspace_id: str) -> bool:
    """Hold a folder for the session ``workspace_folders_lock`` gave, unless a
    file change has it; released with that lock."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS held",
            (f"{_IN_USE_NS}:{workspace_id}",),
        )
        return bool((await cur.fetchone())["held"])


async def stage_folder_moves(
    conn,
    computer_id: str,
    *,
    own_run_id: Optional[str] = None,
    ignore_busy: bool = False,
) -> FolderPlan:
    """Lock the computer's rows, plan against who is busy, and record staging.

    ``FOR UPDATE`` holds a turn's START (``FOR SHARE``) on these rows until the
    plan commits; a turn that starts after that acquires, and its own settle
    waits on the folder lock this caller holds. The caller's own run is not
    work in progress here: it is the turn this acquisition serves. A file
    change in flight keeps its folder like a run; the holds taken here last
    until ``workspace_folders_lock`` closes its connection, after the landings
    are recorded. A ``folder_allocation`` in flight finishes first, so the plan
    reads every folder one took.
    """
    async with conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
        await _lock_folder_allocation(cur, computer_id, shared=False)
        await cur.execute(_ROWS_SQL.rstrip() + " FOR UPDATE", (computer_id,))
        rows = [_row(r) for r in await cur.fetchall()]
        busy = set() if ignore_busy else await _busy(cur, computer_id, rows, own_run_id)
        while True:
            plan = plan_folder_moves(rows, busy)
            changing = {
                r.workspace_id
                for r in (*plan.moves, *plan.tombstones)
                if not r.staged and not await hold_workspace_folder(conn, r.workspace_id)
            }
            if not changing:
                break
            busy |= changing
        for move in plan.moves:
            # Every folder a pass plans for a staged row stays recorded until
            # it lands, as the move script's journal of its last landing counts
            # only when it names one: a later pass may plan another folder, its
            # target held, and move nothing.
            params = {
                "moving": moving_path(move.workspace_id),
                "id": move.workspace_id,
                "landings": Json(list(dict.fromkeys((*move.landings, move.target)))),
            }
            if move.staged:
                if move.target not in move.landings:
                    await cur.execute(
                        """
                        UPDATE workspaces
                        SET config = jsonb_set(
                            COALESCE(config, '{}'::jsonb), '{folder_landings}', %(landings)s::jsonb
                        )
                        WHERE workspace_id = %(id)s AND dir_name = %(moving)s
                        """,
                        params,
                    )
                continue
            await cur.execute(
                """
                UPDATE workspaces
                SET dir_name = %(moving)s,
                    previous_dir_names = (array_prepend(
                        %(old)s::text, array_remove(previous_dir_names, %(old)s::text)
                    ))[1:%(cap)s],
                    config = jsonb_set(
                        COALESCE(config, '{}'::jsonb), '{folder_landings}', %(landings)s::jsonb
                    )
                WHERE workspace_id = %(id)s AND dir_name = %(old)s::text
                  AND status <> 'deleted'
                """,
                {**params, "old": move.source, "cap": PREVIOUS_DIR_NAMES_MAX},
            )
    return plan


def accepted_landings(
    plan: FolderPlan, report: Mapping[str, Any]
) -> tuple[dict[str, str], dict[str, Optional[str]]]:
    """The answers this plan asked the move script for, and nothing else.

    The script runs where the workspace's own code runs, so its report is
    input, not a record: an id outside the plan, or a folder that is not the
    row's staging, old folder, target or a landing a pass recorded planning
    for it, is dropped and logged, and that row stays as it is. Other moves'
    plans are not consulted: a pass that never moved, or a row that could not
    stage, reports the row on its old folder while another move was planned
    there, and the script lands nothing on a folder so held.
    """
    moves_in = report.get("moves") if isinstance(report.get("moves"), dict) else {}
    tombs_in = report.get("tombstones") if isinstance(report.get("tombstones"), dict) else {}
    tombstones: dict[str, Optional[str]] = {}
    for row in plan.tombstones:
        if row.workspace_id not in tombs_in:
            continue
        folder = tombs_in[row.workspace_id]
        if folder is None or folder == leftovers_path(row.workspace_id):
            tombstones[row.workspace_id] = folder
        else:
            logger.warning(f"Dropped folder {folder!r} reported for deleted workspace {row.workspace_id}")
    moves: dict[str, str] = {}
    for move in plan.moves:
        folder = moves_in.get(move.workspace_id)
        if folder is None:
            continue
        if folder in (moving_path(move.workspace_id), move.source, move.target, *move.landings):
            moves[move.workspace_id] = folder
        else:
            logger.warning(f"Dropped folder {folder!r} reported for workspace {move.workspace_id}")
    return moves, tombstones


async def record_folder_landings(
    conn,
    computer_id: str,
    *,
    moves: Mapping[str, str],
    tombstones: Mapping[str, Optional[str]],
) -> set[str]:
    """Point each row at where the move script left it; returns the rows that took.

    Tombstones go first: they release the names the moves land on. A
    tombstone whose folder was never on this disk has nothing left to clean.
    A row that leaves staging drops its planned landings with it.
    """
    landed: set[str] = set()
    async with conn.cursor(row_factory=dict_row) as cur:
        for workspace_id, folder in tombstones.items():
            if folder is None:
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET dir_name = NULL,
                        config = COALESCE(config, '{}'::jsonb)
                            #- '{folder_cleanup_pending}' #- '{folder_landings}'
                    WHERE workspace_id = %s AND computer_id = %s AND status = 'deleted'
                    """,
                    (workspace_id, computer_id),
                )
            else:
                # A kept folder is set aside for its owner, never queued for removal.
                await cur.execute(
                    """
                    UPDATE workspaces
                    SET dir_name = %s,
                        config = CASE
                            WHEN COALESCE(config, '{}'::jsonb) @> '{"folder_kept": true}'
                            THEN config #- '{folder_landings}'
                            ELSE jsonb_set(
                                COALESCE(config, '{}'::jsonb) #- '{folder_landings}',
                                '{folder_cleanup_pending}', 'true'
                            )
                        END
                    WHERE workspace_id = %s AND computer_id = %s AND status = 'deleted'
                    """,
                    (folder, workspace_id, computer_id),
                )
        for workspace_id, folder in moves.items():
            try:
                async with conn.transaction():
                    await cur.execute(
                        """
                        UPDATE workspaces
                        SET dir_name = %(folder)s::text,
                            previous_dir_names = array_remove(previous_dir_names, %(folder)s::text),
                            config = CASE WHEN %(landed)s
                                THEN config #- '{folder_landings}' #- '{folder_move_blocked}'
                                ELSE config END
                        WHERE workspace_id = %(id)s AND computer_id = %(computer)s
                          AND dir_name = %(moving)s
                        """,
                        {
                            "folder": folder,
                            "id": workspace_id,
                            "computer": computer_id,
                            "moving": moving_path(workspace_id),
                            # Reported back at its staging, it is still mid-move.
                            "landed": is_top_level(folder),
                        },
                    )
                    if cur.rowcount:
                        landed.add(workspace_id)
                        if is_top_level(folder):
                            await release_former_folder(
                                cur, computer_id=computer_id, workspace_id=workspace_id, folder=folder
                            )
            except UniqueViolation:
                # Left staged; the next settle finds it in the staging folder.
                logger.warning(
                    f"Folder {folder!r} for workspace {workspace_id} was taken "
                    f"before its move recorded"
                )
    return landed


FOLDER_MOVE_BACKOFF = "6 hours"


async def record_folder_blocked(
    conn, computer_id: str, *, blocked: Mapping[str, str]
) -> None:
    """Hold each row on its folder until the backoff ends or it is renamed again.

    ``blocked`` maps a row to the target its folder had no room to be copied
    to; only a target the pass planned for that row counts, since the report
    comes from the sandbox.
    """
    async with conn.cursor() as cur:
        for workspace_id, target in blocked.items():
            await cur.execute(
                """
                UPDATE workspaces
                SET config = jsonb_set(
                    COALESCE(config, '{}'::jsonb), '{folder_move_blocked}',
                    jsonb_build_object(
                        'target', %(target)s::text,
                        'until', extract(epoch FROM now() + %(backoff)s::interval)
                    )
                )
                WHERE workspace_id = %(id)s AND computer_id = %(computer)s
                  AND status <> 'deleted'
                """,
                {
                    "target": target,
                    "backoff": FOLDER_MOVE_BACKOFF,
                    "id": workspace_id,
                    "computer": computer_id,
                },
            )


async def retarget_folder_cleanup(
    workspace_id: str, *, computer_id: str, dir_name: str, folder: str
) -> bool:
    """Record a deleted workspace's folder at its leftovers path, freeing the name."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE workspaces SET dir_name = %s
            WHERE workspace_id = %s AND computer_id = %s AND dir_name = %s
              AND status = 'deleted'
            """,
            (folder, workspace_id, computer_id, dir_name),
        )
        return cur.rowcount > 0
