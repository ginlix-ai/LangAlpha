"""The record of a workspace moving to another of its owner's computers.

One ``workspace_moves`` row per move. Its phase walks preparing, backing_up,
staged (the folder set aside on the source), committed (the workspace bound
to the target), then done, or ends failed or cancelled. Every write after the
claim names the runner, so a runner whose row was cancelled or taken over as
stale changes nothing more. Before staging, a stale row reads as failed and
fences nothing, so its runner cannot renew or advance it either.

Computer rows are locked before workspace rows, the order a machine write and
a bind take them in.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.computer import MAKE_PRIMARY_INTENT, REMOVAL_INTENT
from src.server.database.egress_grants import (
    lock_user_egress_state,
    retire_workspace_grants,
)
from src.server.database.home_workspace import (
    get_home_workspace_id,
    is_home_row,
    is_home_stand_in_id,
)
from src.server.database.mcp_config_version import bump_workspace_version
from src.server.database.pool import get_db_connection
from src.server.database.runs.lifecycle import workspace_has_live_runs
from src.server.database.sql_fences import (
    COMPUTER_COLS,
    FENCE_NOT_DELETED,
    MOVE_ACTIVE_PHASES,
    MOVE_EARLY_PHASES,
    MOVE_SET_ASIDE_PHASES,
    move_stale,
    takes_a_workspace_sql,
)
from src.server.database.workspace import (
    SANDBOX_STAMP_KEYS,
    WorkspaceDirNameTaken,
    bind_workspace_to_computer,
    get_workspace_dir_names_for_computer,
    workspace_is_moving,
)
from src.server.database.workspace_folders import (
    PREVIOUS_DIR_NAMES_MAX,
    computer_command_running,
    computer_run_in_progress,
    folder_allocation,
    is_top_level,
    takes_name_folders,
)
from src.server.database.workspace_names import HOME_FOLDER, candidate_dir_names
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)

_ACTIVE = list(MOVE_ACTIVE_PHASES)
_EARLY = list(MOVE_EARLY_PHASES)
_RECOVERABLE = list(MOVE_SET_ASIDE_PHASES)

# A computer a workspace can land on now: the user's, taking workspaces, able
# to start, and past its first build, which a workspace on it would have to
# wait out or race, as WorkspaceManager.create_workspace refuses too.
_TAKES_A_WORKSPACE = takes_a_workspace_sql()
_ENDED = ["done", "failed", "cancelled"]
# The stamp of the make-primary intent a worker took on the computer, read on
# its locked row (``database.computer.take_make_primary_intent``).
_INTENT_TAKEN_AT = f"config->'{MAKE_PRIMARY_INTENT}'->>'taken_at' AS intent_taken_at"


def _intent_superseded(row: Dict[str, Any], intent_taken_at: Optional[str]) -> bool:
    """Whether a make-primary applied from an intent lost it while it ran:
    a Make primary since, which ends any pending intent (``_flip_primary``),
    must not be undone by the older request."""
    return intent_taken_at is not None and row.get("intent_taken_at") != intent_taken_at
_SETTABLE = frozenset({"leftovers_path", "source_folder", "target_folder"})
# What a runner's write requires beyond its id: the fence lifted from a stale
# row before staging, and a turn may have started since.
_NOT_LAPSED = f"NOT (phase = ANY(%(early)s) AND {move_stale()})"

#: How long a moved workspace's old folder is kept on its old computer once
#: the workspace's files are back in place. The mirror skips dependency and
#: version-control folders, and this is the only copy of those. On a machine
#: its owner brought, the folder is theirs and is never removed.
LEFTOVERS_KEEP_DAYS = 7
#: How long a folder a move from the backup left waits after a try before the
#: sweep claims it again.
LEFT_FOLDER_RETRY_S = 15 * 60


class MoveRefused(Exception):
    """A move that cannot start or commit, with the code the client words."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _stale_projection(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        f"({prefix}phase = ANY(%(active)s) AND {move_stale(prefix)}) AS stale"
    )


async def claim_workspace_move(
    *,
    user_id: str,
    workspace_id: str,
    target_computer_id: str,
    make_primary: bool = False,
    from_backup: bool = False,
    may_be_left: Callable[[str], bool] = lambda kind: False,
    removal: Optional[tuple[str, Dict[str, Any]]] = None,
    expected_source: Optional[str] = None,
    intent_taken_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Record a move, or raise ``MoveRefused`` with why it cannot start.

    The workspace row is held FOR UPDATE, which a turn's admission holds FOR
    SHARE until its run row commits, so a turn either started first and this
    sees it, or meets the move row this inserts. A run elsewhere on the source
    refuses it too, since sibling workspaces use each other's folders; staging
    checks that again, as the authority. ``may_be_left`` answers for the
    source's kind, which is the providers' rule, not the rows': a move
    ``from_backup`` never reaches it, and a machine the server rebuilds is
    never left behind for its backup. Home moves only onto a computer whose
    files are backed up, which the commit's flip holds to as well.

    ``removal`` is a computer to remove once this move commits and the intent
    to keep on it (``database.computer.REMOVAL_INTENT``), written with the
    move so neither commits without the other.

    ``expected_source`` is the computer the caller judged ``from_backup`` by:
    a workspace that moved off it since is refused as ``move_in_progress``,
    to be read again. ``intent_taken_at`` is a make-primary intent's stamp
    (``_intent_superseded``).
    """
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        raise MoveRefused("not_found")
    target = normalize_uuid(target_computer_id)
    if target is None:
        raise MoveRefused("target_unavailable")
    params = {
        "move_id": str(uuid.uuid4()),
        "runner_id": str(uuid.uuid4()),
        "user_id": user_id,
        "workspace_id": workspace_id,
        "target": target,
        "make_primary": make_primary,
        "from_backup": from_backup,
        "removing": normalize_uuid(removal[0]) if removal else None,
        "early": _EARLY,
        "active": _ACTIVE,
    }
    try:
        async with get_db_connection() as conn, conn.transaction(), conn.cursor(
            row_factory=dict_row
        ) as cur:
            # A stale move that never set its folder aside changed nothing;
            # ended here, it no longer holds the user's one slot.
            await cur.execute(
                f"""
                UPDATE workspace_moves
                SET phase = 'failed', error = %(interrupted)s,
                    finished_at = NOW(), updated_at = NOW()
                WHERE user_id = %(user_id)s AND phase = ANY(%(early)s)
                  AND {move_stale()}
                """,
                {**params, "interrupted": Json({"code": "interrupted"})},
            )
            # The computer a removal waits on is written below; it is locked
            # here, with the target and in the commit's order, ahead of the
            # workspace.
            await cur.execute(
                f"""
                SELECT computer_id, files_backup, {_INTENT_TAKEN_AT}
                FROM computers
                WHERE user_id = %(user_id)s AND (
                    (computer_id = %(target)s AND {_TAKES_A_WORKSPACE})
                    OR (computer_id = %(removing)s::uuid AND {FENCE_NOT_DELETED}))
                ORDER BY computer_id
                FOR {"UPDATE" if removal else "SHARE"}
                """,
                params,
            )
            locked = {str(r["computer_id"]): r for r in await cur.fetchall()}
            computer = locked.get(target)
            if removal and params["removing"] not in locked:
                raise MoveRefused("not_found")
            await cur.execute(
                """
                SELECT workspace_id, user_id, computer_id, dir_name, status
                FROM workspaces WHERE workspace_id = %(workspace_id)s
                FOR UPDATE
                """,
                params,
            )
            workspace = await cur.fetchone()
            if (
                workspace is None
                or str(workspace["user_id"]) != str(user_id)
                or workspace["status"] == "deleted"
            ):
                raise MoveRefused("not_found")
            if workspace["computer_id"] is None or workspace["status"] == "unbound":
                raise MoveRefused("not_movable")
            if is_home_stand_in_id(user_id, workspace_id):
                # Never moved with its files: they are what Home's next
                # hand-back reads, where Home's turns on the cloud left them.
                raise MoveRefused("not_movable")
            # Home lives on the primary, so it moves only as the primary changes,
            # and making a computer primary moves nothing but Home.
            if is_home_row(workspace) != make_primary:
                raise MoveRefused("not_movable")
            if expected_source is not None and str(workspace["computer_id"]) != str(
                expected_source
            ):
                raise MoveRefused("move_in_progress")
            if str(workspace["computer_id"]) == target:
                raise MoveRefused("same_computer")
            if computer is None:
                raise MoveRefused("target_unavailable")
            if _intent_superseded(computer, intent_taken_at):
                raise MoveRefused("superseded")
            if make_primary and not computer["files_backup"]:
                # Home would live only on that machine's disk.
                raise MoveRefused("not_backed_up")
            if from_backup:
                await _check_left_for_backup(cur, workspace, may_be_left)
            if await workspace_has_live_runs(cur, workspace_id):
                raise MoveRefused("turn_active")
            if await computer_run_in_progress(cur, str(workspace["computer_id"])):
                raise MoveRefused("busy")
            if not from_backup and await computer_command_running(
                cur, str(workspace["computer_id"])
            ):
                raise MoveRefused("command_running")
            await cur.execute(
                f"""
                INSERT INTO workspace_moves (
                    move_id, user_id, workspace_id, source_computer_id,
                    target_computer_id, make_primary, from_backup, phase,
                    runner_id, source_folder
                ) VALUES (
                    %(move_id)s, %(user_id)s, %(workspace_id)s, %(source)s,
                    %(target)s, %(make_primary)s, %(from_backup)s, 'preparing',
                    %(runner_id)s, %(source_folder)s
                )
                RETURNING *, {_stale_projection()}
                """,
                {
                    **params,
                    "source": workspace["computer_id"],
                    "source_folder": workspace["dir_name"],
                },
            )
            move = dict(await cur.fetchone())
            if removal:
                await cur.execute(
                    f"""
                    UPDATE computers
                    SET config = jsonb_set(
                            COALESCE(config, '{{}}'::jsonb), '{{{REMOVAL_INTENT}}}', %s::jsonb
                        ),
                        updated_at = NOW()
                    WHERE computer_id = %s
                    """,
                    (Json({**removal[1], "move_id": str(move["move_id"])}), params["removing"]),
                )
            return move
    except UniqueViolation:
        raise MoveRefused("move_in_progress") from None


async def _check_left_for_backup(
    cur, workspace: Mapping[str, Any], may_be_left: Callable[[str], bool]
) -> None:
    """Refuse a move from the backup off a machine that may not be left, or with none.

    Any file row will do. A workspace never backed up has none, and laid
    from nothing it would read as moved while its files stayed behind.
    """
    await cur.execute(
        "SELECT kind FROM computers WHERE computer_id = %s",
        (workspace["computer_id"],),
    )
    source = await cur.fetchone()
    if source is None or not may_be_left(source["kind"]):
        raise MoveRefused("not_movable")
    await cur.execute(
        "SELECT EXISTS (SELECT 1 FROM workspace_files WHERE workspace_id = %s) AS held",
        (workspace["workspace_id"],),
    )
    if not (await cur.fetchone())["held"]:
        raise MoveRefused("no_backup")


async def heartbeat_workspace_move(move_id: str, runner_id: str) -> bool:
    """Whether the move is still live and this runner's; renews it if so."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE workspace_moves SET heartbeat_at = NOW()
            WHERE move_id = %(move_id)s AND runner_id = %(runner_id)s
              AND phase = ANY(%(active)s) AND {_NOT_LAPSED}
            """,
            {"move_id": move_id, "runner_id": runner_id, "active": _ACTIVE, "early": _EARLY},
        )
        return cur.rowcount > 0


async def advance_workspace_move(
    move_id: str, runner_id: str, phase: str, **fields: Any
) -> Optional[Dict[str, Any]]:
    """Set the next phase; None once the move is not live or not this runner's."""
    unknown = set(fields) - _SETTABLE
    if unknown or phase not in MOVE_ACTIVE_PHASES:
        raise ValueError(f"Cannot advance a move to {phase!r} with {sorted(unknown)}")
    sets = "".join(f", {name} = %({name})s" for name in fields)
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET phase = %(phase)s, heartbeat_at = NOW(), updated_at = NOW(){sets}
            WHERE move_id = %(move_id)s AND runner_id = %(runner_id)s
              AND phase = ANY(%(active)s) AND {_NOT_LAPSED}
            RETURNING *, {_stale_projection()}
            """,
            {
                **fields,
                "phase": phase,
                "move_id": move_id,
                "runner_id": runner_id,
                "active": _ACTIVE,
                "early": _EARLY,
            },
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def stage_workspace_move(
    move_id: str, runner_id: str, *, source_folder: str, leftovers_path: Optional[str]
) -> Optional[Dict[str, Any]]:
    """Record the folder as about to leave its name; None once the move is not this runner's.

    The point after which the folder is not where the row says, so it is
    checked as a settle checks a folder move: the source's rows are held FOR
    UPDATE, which keeps every turn's START (FOR SHARE) out until this commits,
    and no run may be in progress anywhere on the source, since sibling
    workspaces use each other's folders. A turn admitted while a lapsed row
    fenced nothing is seen here too. Raises ``MoveRefused("busy")`` while one
    runs, and ``MoveRefused("command_running")`` while a command does, unless
    the move is from the backup, which leaves the source's folder as it is.
    """
    async with get_db_connection() as conn, conn.transaction(), conn.cursor(
        row_factory=dict_row
    ) as cur:
        await cur.execute(
            "SELECT source_computer_id, from_backup FROM workspace_moves WHERE move_id = %s",
            (move_id,),
        )
        move = await cur.fetchone()
        if move is None:
            return None
        source = str(move["source_computer_id"])
        await cur.execute(
            """
            SELECT 1 FROM workspaces WHERE computer_id = %s
            ORDER BY created_at, workspace_id
            FOR UPDATE
            """,
            (source,),
        )
        if await computer_run_in_progress(cur, source):
            raise MoveRefused("busy")
        if not move["from_backup"] and await computer_command_running(cur, source):
            raise MoveRefused("command_running")
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET phase = 'staged', source_folder = %(source_folder)s,
                leftovers_path = %(leftovers_path)s, staged_at = NOW(),
                heartbeat_at = NOW(), updated_at = NOW()
            WHERE move_id = %(move_id)s AND runner_id = %(runner_id)s
              AND phase = 'backing_up' AND {_NOT_LAPSED}
            RETURNING *, {_stale_projection()}
            """,
            {
                "source_folder": source_folder,
                "leftovers_path": leftovers_path,
                "move_id": move_id,
                "runner_id": runner_id,
                "active": _ACTIVE,
                "early": _EARLY,
            },
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def finish_workspace_move(
    move_id: str,
    runner_id: str,
    phase: str,
    *,
    error: Optional[Dict[str, Any]] = None,
    **fields: Any,
) -> Optional[Dict[str, Any]]:
    """End a live move as done, failed or cancelled; None if it was not this runner's.

    A rollback that put the folder back passes ``leftovers_path=None``.
    """
    unknown = set(fields) - _SETTABLE
    if unknown or phase not in _ENDED:
        raise ValueError(f"Cannot finish a move as {phase!r} with {sorted(unknown)}")
    sets = "".join(f", {name} = %({name})s" for name in fields)
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET phase = %(phase)s, error = %(error)s, finished_at = NOW(),
                updated_at = NOW(){sets}
            WHERE move_id = %(move_id)s AND runner_id = %(runner_id)s
              AND phase = ANY(%(active)s)
            RETURNING *, {_stale_projection()}
            """,
            {
                **fields,
                "phase": phase,
                "error": Json(error) if error else None,
                "move_id": move_id,
                "runner_id": runner_id,
                "active": _ACTIVE,
            },
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def cancel_workspace_move(
    workspace_id: str, user_id: str
) -> Optional[Dict[str, Any]]:
    """Cancel a move that has not set its folder aside; None when there is none.

    Its runner learns at its next step, whose write then matches nothing; a
    backup it is still running finishes and is kept, which is harmless.
    """
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET phase = 'cancelled', finished_at = NOW(), updated_at = NOW()
            WHERE workspace_id = %(workspace_id)s AND user_id = %(user_id)s
              AND phase = ANY(%(early)s)
            RETURNING *, {_stale_projection()}
            """,
            {
                "workspace_id": workspace_id,
                "user_id": user_id,
                "early": _EARLY,
                "active": _ACTIVE,
            },
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_latest_workspace_move(workspace_id: str) -> Optional[Dict[str, Any]]:
    workspace_id = normalize_uuid(workspace_id)
    if workspace_id is None:
        return None
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            SELECT *, {_stale_projection()} FROM workspace_moves
            WHERE workspace_id = %(workspace_id)s
            ORDER BY created_at DESC LIMIT 1
            """,
            {"workspace_id": workspace_id, "active": _ACTIVE},
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def get_workspace_move(move_id: str) -> Optional[Dict[str, Any]]:
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT *, {_stale_projection()} FROM workspace_moves WHERE move_id = %(move_id)s",
            {"move_id": move_id, "active": _ACTIVE},
        )
        row = await cur.fetchone()
    return dict(row) if row else None


def carried_previous(
    source_folder: Optional[str],
    previous: Optional[Sequence[str]],
    dir_name: str,
    held: Iterable[str],
) -> List[str]:
    """The folders an old path in the workspace's threads may name, less any the target holds.

    Readers fold a former folder's paths into its workspace, so one a sibling
    on the target holds would send that sibling's files here. The folder left
    leads, as a rename's does.
    """
    carried = [source_folder] if source_folder and is_top_level(source_folder) else []
    carried += list(previous or ())
    taken = {folder.casefold() for folder in held}
    seen = {dir_name.casefold()}
    kept: List[str] = []
    for folder in carried:
        key = (folder or "").casefold()
        if key and key not in taken and key not in seen:
            kept.append(folder)
            seen.add(key)
    return kept[:PREVIOUS_DIR_NAMES_MAX]


#: How many roots a workspace keeps of the computers it left. Each is a read a
#: path fold tries, and a workspace that outlives this many moves has turns
#: old enough that their links were never going to open anyway.
PREVIOUS_ROOT_DIRS_MAX = 16


def carried_roots(
    left: Optional[str], landed: Optional[str], previous: Optional[Sequence[str]]
) -> List[str]:
    """The computer roots an old path in the workspace's threads may start with.

    The live computer list stops naming a root once its computer is removed,
    so the workspace keeps its own. The root left leads, as the folder left
    does; one the target shares adds nothing, since its paths read the same.
    """
    kept = [root for root in previous or () if root]
    left = (left or "").rstrip("/")
    if left and left != (landed or "").rstrip("/"):
        kept = [left, *(root for root in kept if root != left)]
    return kept[:PREVIOUS_ROOT_DIRS_MAX]


async def _flip_primary(cur, user_id: str, computer_id: str) -> None:
    """Make ``computer_id`` the user's primary; the old one is cleared first.

    The one-primary index is not deferrable, so the order is what lets both
    writes land in one transaction. Home lives on the primary, so a computer
    whose files are not backed up is refused here, under the write: turning
    backup off refuses a primary in its own update, and whichever lands
    second sees the other. A make primary another computer's create asked
    for and that is still pending is the older choice, so it goes.
    """
    await cur.execute(
        """
        UPDATE computers SET is_primary = FALSE
        WHERE user_id = %s AND is_primary AND computer_id <> %s
        """,
        (user_id, computer_id),
    )
    await cur.execute(
        f"""
        UPDATE computers SET config = config - '{MAKE_PRIMARY_INTENT}'
        WHERE user_id = %s AND computer_id <> %s
          AND config->'{MAKE_PRIMARY_INTENT}'->>'state' = 'pending'
        """,
        (user_id, computer_id),
    )
    await cur.execute(
        f"""
        UPDATE computers SET is_primary = TRUE
        WHERE computer_id = %s AND user_id = %s AND {FENCE_NOT_DELETED}
        RETURNING files_backup
        """,
        (computer_id, user_id),
    )
    row = await cur.fetchone()
    if row is None:
        raise MoveRefused("target_unavailable")
    if not row["files_backup"]:
        raise MoveRefused("not_backed_up")


# The computer that becomes the user's primary while they have none: one that
# takes a workspace now, a backed-up one before any other (Home lives on the
# primary and is restored from its backup), then the one Home is on, then the
# oldest. The caller binds ``user``, ``home`` and ``excluding``.
_PRIMARY_CANDIDATE = f"""
    SELECT c.computer_id
    FROM computers c
    LEFT JOIN workspaces home
        ON home.workspace_id = %(home)s AND home.computer_id = c.computer_id
    WHERE c.user_id = %(user)s AND {takes_a_workspace_sql("c")}
      AND c.computer_id IS DISTINCT FROM %(excluding)s::uuid
    ORDER BY c.files_backup DESC, (home.workspace_id IS NOT NULL) DESC,
             c.created_at, c.computer_id
    LIMIT 1
"""


def _candidate_params(user_id: str, excluding: Optional[str]) -> Dict[str, Any]:
    return {
        "user": user_id,
        "home": get_home_workspace_id(user_id),
        "excluding": normalize_uuid(excluding) if excluding else None,
    }


async def primary_candidate(
    user_id: str, *, excluding: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """The computer that would become the user's primary, whatever their
    primary is now; None when none could. One with backup off is answered
    only when no backed-up one could, and is the one turning backup on
    makes primary. ``excluding`` leaves one out, as a removal asks whether
    another could take over from it."""
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {COMPUTER_COLS} FROM computers"
            f" WHERE computer_id = ({_PRIMARY_CANDIDATE})",
            _candidate_params(user_id, excluding),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def promote_primary_computer(user_id: str) -> Optional[Dict[str, Any]]:
    """Make the user's ``primary_candidate`` their primary, while they have none.

    None when no computer qualifies, its backup on. A primary that appeared
    meanwhile, or wins the one-primary index, is returned instead.
    """
    try:
        async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE computers
                SET is_primary = TRUE, updated_at = NOW()
                WHERE computer_id = ({_PRIMARY_CANDIDATE}) AND files_backup
                  -- Again on the row it locks: the candidate is chosen from
                  -- the statement's snapshot, and a retirement may mark it
                  -- while this waits for its lock.
                  AND {_TAKES_A_WORKSPACE}
                  AND NOT EXISTS (
                      SELECT 1 FROM computers
                      WHERE user_id = %(user)s AND is_primary AND {FENCE_NOT_DELETED}
                  )
                RETURNING {COMPUTER_COLS}
                """,
                _candidate_params(user_id, None),
            )
            row = await cur.fetchone()
    except UniqueViolation:
        row = None
    if row is not None:
        logger.info(
            f"Made computer {row['computer_id']} (kind={row['kind']}) the "
            f"primary of user {user_id}, who had none"
        )
        return dict(row)
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {COMPUTER_COLS} FROM computers"
            f" WHERE user_id = %s AND is_primary AND {FENCE_NOT_DELETED}",
            (user_id,),
        )
        row = await cur.fetchone()
    return dict(row) if row else None


async def commit_workspace_move(move_id: str, runner_id: str) -> Optional[Dict[str, Any]]:
    """Bind the workspace to the target, with everything that follows from it, at once.

    The files come from the mirror, so the restore flag goes up and the epoch
    changes; the settings stamp is the target's, since a folder vouches for
    the sandbox it sits on; previews and grants of the old sandbox end; the
    MCP version moves so every worker resolves the workspace's tools again.
    None when the move is no longer this runner's to commit.
    """
    move = await get_workspace_move(move_id)
    if move is None:
        return None
    workspace_id = str(move["workspace_id"])
    source = str(move["source_computer_id"])
    target = str(move["target_computer_id"])
    async with folder_allocation(target) as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT * FROM workspace_moves WHERE move_id = %s FOR UPDATE", (move_id,)
        )
        move = await cur.fetchone()
        if move is None or move["phase"] != "staged" or str(move["runner_id"]) != runner_id:
            return None
        user_id = str(move["user_id"])
        # The target, and for Make primary the primary it replaces, ahead of
        # the workspace; held for update when the flip will write them.
        await cur.execute(
            f"""
            SELECT computer_id, provider_ref, layout_version, root_dir,
                   ({_TAKES_A_WORKSPACE}) AS takes_a_workspace
            FROM computers
            WHERE computer_id = %(target)s
               OR (%(make_primary)s AND user_id = %(user_id)s AND is_primary)
            ORDER BY computer_id
            FOR {"UPDATE" if move["make_primary"] else "SHARE"}
            """,
            {"target": target, "make_primary": bool(move["make_primary"]), "user_id": user_id},
        )
        computer = next(
            (r for r in await cur.fetchall() if str(r["computer_id"]) == target), None
        )
        # Owner first, workspace second, as every grant writer orders them.
        await lock_user_egress_state(cur, user_id)
        # The source's root is read, not locked: a computer's root is fixed
        # when it is made, and computer rows are locked ahead of this one.
        await cur.execute(
            """
            SELECT w.workspace_id, w.user_id, w.name, w.computer_id,
                   w.previous_dir_names, w.previous_root_dirs, w.status,
                   c.root_dir AS source_root_dir
            FROM workspaces w
            LEFT JOIN computers c ON c.computer_id = w.computer_id
            WHERE w.workspace_id = %s
            FOR UPDATE OF w
            """,
            (workspace_id,),
        )
        workspace = await cur.fetchone()
        if workspace is None or workspace["status"] == "deleted":
            raise MoveRefused("not_found")
        if str(workspace["computer_id"]) != source:
            raise MoveRefused("not_movable")
        # As at the claim, read again: the target may have fallen into error.
        if computer is None or not computer["takes_a_workspace"]:
            raise MoveRefused("target_unavailable")
        held = await get_workspace_dir_names_for_computer(target, conn=conn)
        label = HOME_FOLDER if is_home_row(workspace) else workspace["name"]
        bound = None
        for dir_name in candidate_dir_names(
            label, workspace_id, held, own_folder=takes_name_folders(computer)
        ):
            try:
                bound = await bind_workspace_to_computer(
                    workspace_id,
                    target,
                    expected_computer_id=source,
                    dir_name=dir_name,
                    conn=conn,
                )
            except WorkspaceDirNameTaken:
                continue
            break
        if bound is None:
            raise MoveRefused("target_unavailable")
        await cur.execute(
            """
            UPDATE workspaces w
            SET previous_dir_names = %(carried)s::text[],
                previous_root_dirs = %(roots)s::text[],
                files_restore_incomplete_at = COALESCE(w.files_restore_incomplete_at, NOW()),
                files_restore_offered_at = NULL,
                files_epoch = %(move_id)s,
                config = (COALESCE(w.config, '{}'::jsonb) - %(stamp_keys)s::text[])
                    || COALESCE((
                        SELECT jsonb_object_agg(k, s.config->k)
                        FROM (
                            SELECT config FROM workspaces
                            WHERE computer_id = %(target)s AND workspace_id <> w.workspace_id
                              AND status <> 'deleted' AND config ? 'sandbox_config_hash'
                            LIMIT 1
                        ) s, unnest(%(stamp_keys)s::text[]) AS k
                        WHERE s.config ? k
                    ), '{}'::jsonb),
                artifacts = COALESCE(w.artifacts, '{}'::jsonb) - 'preview_servers'
            WHERE w.workspace_id = %(workspace_id)s
            """,
            {
                "carried": carried_previous(
                    move["source_folder"],
                    workspace["previous_dir_names"],
                    bound["dir_name"],
                    held,
                ),
                "roots": carried_roots(
                    workspace["source_root_dir"],
                    computer["root_dir"],
                    workspace["previous_root_dirs"],
                ),
                "move_id": move_id,
                "stamp_keys": list(SANDBOX_STAMP_KEYS),
                "target": target,
                "workspace_id": workspace_id,
            },
        )
        await bump_workspace_version(cur, workspace_id)
        await retire_workspace_grants(cur, workspace_id, source)
        if move["make_primary"]:
            await _flip_primary(cur, user_id, target)
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET phase = 'committed', target_folder = %(folder)s,
                committed_at = NOW(), heartbeat_at = NOW(), updated_at = NOW()
            WHERE move_id = %(move_id)s
            RETURNING *, {_stale_projection()}
            """,
            {"folder": bound["dir_name"], "move_id": move_id, "active": _ACTIVE},
        )
        committed = dict(await cur.fetchone())
    logger.info(
        f"Moved workspace {workspace_id} from computer {source} to {target} "
        f"as {bound['dir_name']} (move {move_id})"
    )
    return committed


async def flag_rollback_restore(move_id: str, workspace_id: str) -> None:
    """A rollback that could not put the folder back: restore it from the mirror.

    The workspace is still on its computer with its folder set aside, and a
    worker that attached it there would take the empty folder as current, so
    the epoch changes as a committed move's does.
    """
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE workspaces
            SET files_restore_incomplete_at = COALESCE(files_restore_incomplete_at, NOW()),
                files_restore_offered_at = NULL,
                files_epoch = %s
            WHERE workspace_id = %s
            """,
            (move_id, workspace_id),
        )


async def set_primary_computer(
    user_id: str, computer_id: str, *, intent_taken_at: Optional[str] = None
) -> bool:
    """Flip the primary without moving Home; False when Home must move with it.

    The old primary is held before Home, as Home's first bind holds it: that
    bind takes only a computer still primary, so it either landed first and
    this refuses, or waits and then binds Home to the new primary.
    ``intent_taken_at`` is a make-primary intent's stamp (``_intent_superseded``).
    """
    computer_id = normalize_uuid(computer_id)
    if computer_id is None:
        raise MoveRefused("not_found")
    async with get_db_connection() as conn, conn.transaction(), conn.cursor(
        row_factory=dict_row
    ) as cur:
        await cur.execute(
            f"""
            SELECT computer_id, ({_TAKES_A_WORKSPACE}) AS takes_workspaces,
                   {_INTENT_TAKEN_AT}
            FROM computers
            WHERE user_id = %(user_id)s AND (computer_id = %(target)s OR is_primary)
            ORDER BY computer_id
            FOR UPDATE
            """,
            {"user_id": user_id, "target": computer_id},
        )
        # As a claim to move onto it reads: Home and new workspaces would land
        # on a computer that cannot start or is still building its first sandbox.
        target = next(
            (r for r in await cur.fetchall() if str(r["computer_id"]) == computer_id), None
        )
        if target is None or not target["takes_workspaces"]:
            raise MoveRefused("target_unavailable")
        if _intent_superseded(target, intent_taken_at):
            raise MoveRefused("superseded")
        await cur.execute(
            "SELECT computer_id FROM workspaces WHERE workspace_id = %s FOR UPDATE",
            (get_home_workspace_id(user_id),),
        )
        home = await cur.fetchone()
        # Its own statement, as at a turn's admission: one that waited on a
        # move's claim for Home would read the move table from before it.
        if home is not None and await workspace_is_moving(
            get_home_workspace_id(user_id), conn=conn
        ):
            raise MoveRefused("move_in_progress")
        if home is not None and home["computer_id"] is not None and str(
            home["computer_id"]
        ) != computer_id:
            return False
        await _flip_primary(cur, user_id, computer_id)
    return True


async def fail_stale_early_moves() -> int:
    """End moves whose runner died before setting the folder aside."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET phase = 'failed', error = %s, finished_at = NOW(), updated_at = NOW()
            WHERE phase = ANY(%s) AND {move_stale()}
            """,
            (Json({"code": "interrupted"}), _EARLY),
        )
        return cur.rowcount


async def take_over_stale_moves(limit: int = 20) -> List[Dict[str, Any]]:
    """Claim moves whose runner died after setting the folder aside, for recovery.

    The new runner id fences the old one, should it only have stalled.
    """
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"""
            UPDATE workspace_moves
            SET runner_id = gen_random_uuid(), heartbeat_at = NOW(), updated_at = NOW()
            WHERE move_id IN (
                SELECT move_id FROM workspace_moves
                WHERE phase = ANY(%(recoverable)s) AND {move_stale()}
                ORDER BY heartbeat_at
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *, {_stale_projection()}
            """,
            {"recoverable": _RECOVERABLE, "limit": limit, "active": _ACTIVE},
        )
        return [dict(row) for row in await cur.fetchall()]


async def list_moves_with_leftovers(limit: int = 50) -> List[Dict[str, Any]]:
    """Folders moves set aside that the sweep can act on now, with what to do.

    ``source_gone``: the computer went, the folder with it. ``remove``: the
    computer is running and the workspace is deleted or restored long enough
    ago. ``restored``: the workspace's files are back in place, which starts
    the wait. A folder waiting out its days or on a stopped computer is not
    listed, so those can never crowd out one that is due. The source's kind
    and root ride along, since whether the folder may ever be removed is the
    providers' rule, not the rows'.
    """
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            SELECT * FROM (
                SELECT m.move_id, m.source_computer_id, m.leftovers_path, m.finished_at,
                    c.kind AS source_kind, c.root_dir AS source_root_dir,
                    CASE
                        WHEN c.computer_id IS NULL OR c.status = 'deleted'
                            THEN 'source_gone'
                        WHEN c.status = 'running' AND (
                            w.workspace_id IS NULL OR w.status = 'deleted'
                            OR m.restored_at < NOW() - make_interval(days => %(keep_days)s)
                        ) THEN 'remove'
                        WHEN m.restored_at IS NULL AND w.status <> 'deleted'
                            AND w.files_restore_incomplete_at IS NULL
                            THEN 'restored'
                    END AS action
                FROM workspace_moves m
                LEFT JOIN workspaces w ON w.workspace_id = m.workspace_id
                LEFT JOIN computers c ON c.computer_id = m.source_computer_id
                WHERE m.leftovers_path IS NOT NULL AND m.leftovers_removed_at IS NULL
                  AND m.phase = ANY(%(ended)s)
            ) actionable
            WHERE action IS NOT NULL
            ORDER BY finished_at
            LIMIT %(limit)s
            """,
            {"ended": _ENDED, "limit": limit, "keep_days": LEFTOVERS_KEEP_DAYS},
        )
        return [dict(row) for row in await cur.fetchall()]


async def claim_folders_left_in_place(limit: int = 1) -> List[Dict[str, Any]]:
    """Claim folders moves from the backup left, on a computer running again or gone.

    Each holds its name there (``get_workspace_dir_names_for_computer``) until
    ``set_left_folder_aside`` records it moved aside, or its computer went.
    Setting one aside dials its owner's machine, which may not answer, so the
    claim stamps the row and it is due again only ``LEFT_FOLDER_RETRY_S``
    later: one worker tries it per window, and a row that never clears goes
    behind those untried. One at a time by default, so the window starts when
    the row's try does.
    """
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE workspace_moves claimed
            SET left_folder_tried_at = NOW()
            FROM (
                SELECT m.move_id,
                    (c.computer_id IS NULL OR c.status = 'deleted') AS source_gone
                FROM workspace_moves m
                LEFT JOIN computers c ON c.computer_id = m.source_computer_id
                WHERE m.from_backup AND m.leftovers_removed_at IS NULL
                  AND m.phase = 'done'
                  AND (
                      c.computer_id IS NULL OR c.status = 'deleted'
                      OR (c.status = 'running' AND c.host_unavailable_since IS NULL)
                  )
                  AND (
                      m.left_folder_tried_at IS NULL
                      OR m.left_folder_tried_at < NOW() - make_interval(secs => %(retry_s)s)
                  )
                ORDER BY m.left_folder_tried_at NULLS FIRST, m.finished_at
                LIMIT %(limit)s
                FOR UPDATE OF m SKIP LOCKED
            ) due
            WHERE claimed.move_id = due.move_id
            RETURNING claimed.move_id, claimed.source_computer_id, claimed.source_folder,
                due.source_gone
            """,
            {"retry_s": LEFT_FOLDER_RETRY_S, "limit": limit},
        )
        return [dict(row) for row in await cur.fetchall()]


async def left_folder_taken(conn, computer_id: str, folder: str) -> bool:
    """Whether a workspace holds the name now: a rename's landing sets what it
    finds there aside on its way in, so the folder there is that workspace's.

    By any name a row holds there, a staged row's landings included, and by
    any spelling folding alike, as folder allocation compares them: the
    owner's disk may not tell case apart.
    """
    folded = folder.casefold()
    held = await get_workspace_dir_names_for_computer(
        computer_id, conn=conn, left_by_moves=False
    )
    return any(name.casefold() == folded for name in held)


async def set_left_folder_aside(move_id: str, leftovers_path: Optional[str]) -> None:
    """Release a folder a move from the backup left: kept at ``leftovers_path``
    on its owner's machine, or None once nothing of it is left."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE workspace_moves
            SET leftovers_path = %s, leftovers_removed_at = NOW()
            WHERE move_id = %s AND leftovers_removed_at IS NULL
            """,
            (leftovers_path, move_id),
        )


async def mark_move_restored(move_id: str) -> None:
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE workspace_moves SET restored_at = NOW() WHERE move_id = %s AND restored_at IS NULL",
            (move_id,),
        )


async def mark_leftovers_removed(move_id: str) -> None:
    """The sweep is done with a move's folder: removed, gone with its computer,
    or kept on its owner's machine, where ``leftovers_path`` still finds it."""
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE workspace_moves SET leftovers_removed_at = NOW() WHERE move_id = %s",
            (move_id,),
        )
