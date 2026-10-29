"""Migration 072: retire the Flash agent's names from the schema and its data.

'flash' named the agent that once ran outside a workspace. That work runs in
the user's Home now, whose status reads 'unbound' until its first computer,
and every automation runs in the workspace it names; the model slot it ran
on is the Background model. This renames what is left and narrows the
constraints, so nothing can write 'flash' again:

- workspaces: status 'flash' becomes 'unbound', a Home still called 'Flash'
  (on no computer, or at the id Home derives from) is called 'Home', and the
  ``flash_mode`` config key, which nothing reads, goes. The status CHECK
  trades 'flash' for 'unbound', and the name index (054) leaves out
  'unbound' rows, which carry no name key, in its place.
- automations: each row is stored where the previous build runs it, since
  from here on a run happens in the stored workspace and continues its
  pinned thread only there. A row pinned to a thread in a live workspace of
  its owner's moves to that workspace. A pin into a deleted workspace is
  cleared, as the previous build started a new thread where the row lives,
  and so is one into another account's, whose runs failed; a pinned thread
  that is gone stays pinned, to be made again where the row lives. That is
  Home for a row stored as agent_mode 'flash', its Home row made if the user
  has none, and the stored workspace otherwise. A Home id another account
  holds is not this user's Home, and the row names no workspace instead, as
  its runs already failed. Then ``agent_mode`` goes.
- preferences: ``preferred_flash_model`` and ``flash_follows`` in
  ``model_preference`` become ``preferred_background_model`` and
  ``background_follows``, as does the copy of the first that a row from
  before 034 keeps in ``other_preference``, which is read underneath. A row
  that holds both keeps the new name's value.
- market insights: a brief made on the Background model says so in its
  ``model``, which read 'flash'.
- history: the rewrites of the earlier history backfill, again, for what it
  missed, and ``origin_flash_thread_id``, which names the thread a
  dispatched run reports back to, becomes ``origin_home_thread_id``: in a
  run's metadata, which a later turn of the thread inherits, and in a
  hand-off's steer, which stamps the run that takes it or starts a turn
  carrying it. A steer Flash sent reads as the Chief of Staff's, as a
  thread's origin does. Then msg_type CHECKs without 'flash' on both tables
  that have one.

It needs a downtime deploy. The previous build selects ``agent_mode`` in
every automation query, writes 'flash' for a new Home and reads the old
preference and metadata names, so no process of it may serve against this
schema, and the history rewrite scans the conversation tables.
``updated_at`` is left as it was on every rewritten row: lists order by it,
and this is not activity.

Lock discipline follows 052. History is rewritten first, before anything
locks workspaces or automations, as it scans the conversation tables and the
outbox. The
status CHECK is dropped before the rows are rewritten to a value it refuses;
every CHECK is then added NOT VALID inside the transaction and validated
afterwards in an autocommit block with no lock timeout, where the name index
is rebuilt CONCURRENTLY, as 054 builds it. An upgrade that dies in that block
has committed everything before it with 071 still recorded: run it again,
which is safe, before anything else, or a downgrade does nothing.

``downgrade`` restores the 071 schema, and Home's status with it: an
'unbound' row reads 'flash' again, which is all the previous build knows.
The old preference, metadata and steer names and an insight's 'flash' come
back too, as the previous build reads only those; a steer or a thread origin
naming the Chief of Staff stays, which it reads as well. Placements stay as this left
them, which the previous build runs in the same place, and ``agent_mode``
comes back as 'ptc' on every row, which with Home's id still means Home.
Stop this build first: it writes 'unbound' for a new Home, which the
restored CHECK refuses, and names no ``agent_mode`` for a new automation,
which the restored default would make 'flash'.
"""

import logging
import uuid

from alembic import op
from sqlalchemy import text

revision = "072"
down_revision = "071"
branch_labels = None
depends_on = None

_log = logging.getLogger("alembic.runtime.migration")

# The value every stored Home id derives from (``get_home_workspace_id``).
_HOME_NAMESPACE = uuid.UUID("f1a50000-0000-5000-e000-f1a500000000")

_NAME_INDEX = "idx_workspaces_user_name_key"
_STATUS_CHECK = "workspaces_status_check"

_WORKSPACE_STATUSES = (
    "'creating', 'running', 'starting', 'stopping', 'stopped', 'error', 'deleted'"
)
_MSG_TYPES = "'ptc', 'interrupted', 'task'"

# (table, constraint, column, values after upgrade, values after downgrade)
_CHECKS = (
    (
        "workspaces",
        _STATUS_CHECK,
        "status",
        f"{_WORKSPACE_STATUSES}, 'unbound'",
        f"{_WORKSPACE_STATUSES}, 'flash'",
    ),
    (
        "conversation_threads",
        "conversation_threads_msg_type_check",
        "msg_type",
        _MSG_TYPES,
        f"'flash', {_MSG_TYPES}",
    ),
    (
        "conversation_usages",
        "conversation_usages_msg_type_check",
        "msg_type",
        _MSG_TYPES,
        f"'flash', {_MSG_TYPES}",
    ),
)


def _rename_key(
    table: str,
    column: str,
    old: str,
    new: str,
    *,
    back: bool = False,
    within: str | None = None,
    where: str | None = None,
) -> str:
    """Move a JSONB key to its other name, ``new`` unless ``back``: at the top
    of ``column``, or in its object ``within`` on the rows ``where`` picks. A
    row that already holds the name it moves to keeps that value."""
    src, dst = (new, old) if back else (old, new)
    obj = f"({column}->'{within}')" if within else column
    moved = (
        f"({obj} - '{src}') || "
        f"jsonb_build_object('{dst}', COALESCE({obj}->'{dst}', {obj}->'{src}'))"
    )
    if within:
        moved = f"jsonb_set({column}, '{{{within}}}', {moved})"
    match = f"{obj} ? '{src}'"
    return f"UPDATE {table} SET {column} = {moved} WHERE " + (
        f"{where} AND {match}" if where else match
    )


_STEER = "hook_type = 'steer'"
_STAMP = ("origin_flash_thread_id", "origin_home_thread_id")


def _stamp_renames(*, back: bool = False) -> tuple[tuple[str, str], ...]:
    """The name of the thread a dispatched run reports back to, on the run and
    in a hand-off's steer, moved as ``_rename_key`` moves it."""
    return (
        (
            "response_origin_thread",
            _rename_key("conversation_responses", "metadata", *_STAMP, back=back),
        ),
        (
            "steer_origin_thread",
            _rename_key(
                "hook_outbox", "payload", *_STAMP, back=back, within="origin", where=_STEER
            ),
        ),
    )

# The earlier history backfill's rewrites, then the stamps and the steers Flash
# sent. A thread's model was picked for the retired Flash agent, so it is
# cleared and the thread follows the account default.
_HISTORY = (
    (
        "thread_msg_type",
        "UPDATE conversation_threads SET msg_type = 'ptc', llm_model = NULL "
        "WHERE msg_type = 'flash'",
    ),
    (
        "thread_origin",
        "UPDATE conversation_threads "
        "SET metadata = jsonb_set(metadata, '{origin,agent}', '\"chief_of_staff\"') "
        "WHERE metadata->'origin'->>'agent' = 'flash'",
    ),
    (
        "usage_msg_type",
        "UPDATE conversation_usages SET msg_type = 'ptc' WHERE msg_type = 'flash'",
    ),
    (
        "response_msg_type",
        "UPDATE conversation_responses "
        "SET metadata = jsonb_set(metadata, '{msg_type}', '\"ptc\"') "
        "WHERE metadata->>'msg_type' = 'flash'",
    ),
    (
        "query_msg_type",
        "UPDATE conversation_queries "
        "SET metadata = jsonb_set(metadata, '{msg_type}', '\"ptc\"') "
        "WHERE metadata->>'msg_type' = 'flash'",
    ),
    *_stamp_renames(),
    (
        "steer_sender",
        "UPDATE hook_outbox SET payload = jsonb_set(payload, '{sender}', '\"chief_of_staff\"') "
        f"WHERE {_STEER} AND payload->>'sender' = 'flash'",
    ),
    (
        "steer_origin_agent",
        "UPDATE hook_outbox "
        "SET payload = jsonb_set(payload, '{origin,agent}', '\"chief_of_staff\"') "
        f"WHERE {_STEER} AND payload->'origin'->>'agent' = 'flash'",
    ),
)

# The Background slot's keys in each preferences column that holds them:
# ``model_preference``, and the copy a row from before 034 keeps in
# ``other_preference``. ``flash_follows`` came after 034, so only the first.
_SLOT_KEYS = (
    ("model_preference", "preferred_flash_model", "preferred_background_model"),
    ("model_preference", "flash_follows", "background_follows"),
    ("other_preference", "preferred_flash_model", "preferred_background_model"),
)

_UPDATED_AT_TRIGGERS = (
    ("workspaces", "trg_workspaces_updated_at"),
    ("automations", "trg_automations_updated_at"),
    ("user_preferences", "trg_user_preferences_updated_at"),
)


def home_workspace_id(user_id: str) -> str:
    return str(uuid.uuid5(_HOME_NAMESPACE, user_id))


def reads_as_home(workspace_id: str, user_id: str, status: str) -> bool:
    """Whether the app takes a row for its owner's Home once this has run.

    A ``flash_mode`` flag does not make one: a former Home bound to a computer
    at another id reads as an ordinary workspace, and named 'Home', its folder
    would follow the name to Home's folder at its next settle.
    """
    return status in ("flash", "unbound") or workspace_id == home_workspace_id(user_id)


def place(
    *,
    for_home: bool,
    stored: str | None,
    pinned: bool,
    thread_workspace: str | None,
    reachable: bool,
) -> tuple[str | None, bool, bool]:
    """Where the previous build runs an automation, as (workspace id or None
    for Home, whether that is Home, whether the pin is cleared).

    ``for_home`` is a row stored as agent_mode 'flash', which ran outside any
    workspace. ``thread_workspace`` is the pinned thread's workspace, None
    when the row is not pinned or its thread is gone; ``reachable`` says the
    thread's workspace is live and the automation's owner holds it.
    """
    if pinned and thread_workspace is not None and reachable:
        return thread_workspace, False, False
    cleared = pinned and thread_workspace is not None
    if for_home:
        return None, True, cleared
    return stored, False, cleared


def _set_updated_at_triggers(bind, state: str) -> None:
    for table, trigger in _UPDATED_AT_TRIGGERS:
        bind.execute(text(f"ALTER TABLE {table} {state} TRIGGER {trigger}"))


def _rewrite_workspaces(bind) -> None:
    rows = bind.execute(
        text(
            """
            SELECT workspace_id::text, user_id, status
            FROM workspaces WHERE name = 'Flash'
            """
        )
    ).fetchall()
    # Any other row keeps its name, a workspace a user named 'Flash' included.
    homes = [r[0] for r in rows if reads_as_home(*r)]
    renamed = bind.execute(
        text(
            """
            UPDATE workspaces
            SET name = 'Home',
                description = NULLIF(description, 'Flash mode conversations')
            WHERE workspace_id = ANY(CAST(:ids AS uuid[]))
            """
        ),
        {"ids": homes},
    ).rowcount
    unbound = bind.execute(
        text("UPDATE workspaces SET status = 'unbound' WHERE status = 'flash'")
    ).rowcount
    unflagged = bind.execute(
        text(
            "UPDATE workspaces SET config = config - 'flash_mode' "
            "WHERE config ? 'flash_mode'"
        )
    ).rowcount
    _log.info(
        f"072: workspaces renamed Home {renamed}, set unbound {unbound}, "
        f"flash_mode dropped {unflagged}"
    )


def _rewrite_automations(bind) -> None:
    # Gone already when an abort in the autocommit block left 071 recorded
    # after this part committed.
    if not bind.execute(
        text(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'automations' AND column_name = 'agent_mode'
            """
        )
    ).first():
        return
    # The thread's owner is its workspace's, as the app reads it.
    rows = bind.execute(
        text(
            """
            SELECT a.automation_id::text, a.user_id, a.agent_mode = 'flash',
                   a.workspace_id::text,
                   a.thread_strategy = 'continue'
                       AND a.conversation_thread_id IS NOT NULL,
                   w.workspace_id::text,
                   w.status <> 'deleted' AND w.user_id = a.user_id
            FROM automations a
            LEFT JOIN conversation_threads t
                ON t.conversation_thread_id = a.conversation_thread_id
               AND a.thread_strategy = 'continue'
            LEFT JOIN workspaces w ON w.workspace_id = t.workspace_id
            WHERE a.agent_mode = 'flash' OR w.workspace_id IS NOT NULL
            """
        )
    ).fetchall()
    ids, targets, at_home, unpin, users = [], [], [], [], set()
    to_thread = 0
    for automation_id, user_id, for_home, stored, pinned, thread_ws, reachable in rows:
        target, home, cleared = place(
            for_home=for_home,
            stored=stored,
            pinned=pinned,
            thread_workspace=thread_ws,
            reachable=bool(reachable),
        )
        if home:
            target = home_workspace_id(user_id)
            users.add(user_id)
        if target == stored and not home and not cleared:
            continue
        to_thread += not home and thread_ws is not None and target == thread_ws
        ids.append(automation_id)
        targets.append(target)
        at_home.append(home)
        unpin.append(cleared)
    # The app's Home row: its insert trigger starts the workspace's MCP
    # selection, as the app's own insert does.
    owners = sorted(users)
    made = bind.execute(
        text(
            """
            INSERT INTO workspaces (workspace_id, user_id, name, status, is_pinned)
            SELECT h.home_id, h.user_id, 'Home', 'unbound', TRUE
            FROM unnest(CAST(:users AS text[]), CAST(:homes AS uuid[]))
                AS h(user_id, home_id)
            ON CONFLICT (workspace_id) DO NOTHING
            """
        ),
        {"users": owners, "homes": [home_workspace_id(u) for u in owners]},
    ).rowcount
    placed = bind.execute(
        text(
            """
            UPDATE automations a
            SET workspace_id = CASE
                    WHEN p.home THEN (
                        SELECT w.workspace_id FROM workspaces w
                        WHERE w.workspace_id = p.target AND w.user_id = a.user_id
                    )
                    ELSE p.target
                END,
                conversation_thread_id = CASE
                    WHEN p.unpin THEN NULL ELSE a.conversation_thread_id
                END
            FROM unnest(
                CAST(:ids AS uuid[]), CAST(:targets AS uuid[]),
                CAST(:homes AS boolean[]), CAST(:unpin AS boolean[])
            ) AS p(automation_id, target, home, unpin)
            WHERE a.automation_id = p.automation_id
            RETURNING p.home, a.workspace_id IS NULL
            """
        ),
        {"ids": ids, "targets": targets, "homes": at_home, "unpin": unpin},
    ).fetchall()
    homeless = sum(1 for home, missing in placed if home and missing)
    _log.info(
        f"072: automations moved to their thread's workspace {to_thread}, "
        f"placed in Home {sum(1 for home, _ in placed if home) - homeless}, "
        f"left with no workspace {homeless}, pins cleared {sum(unpin)}, "
        f"Home rows made {made}"
    )
    op.execute("ALTER TABLE automations DROP COLUMN agent_mode")


def _rename_slot(bind, *, back: bool = False) -> None:
    """The Background slot's names in preferences and market insights, or
    with ``back`` the names the previous build reads."""
    counts = {
        f"{column}.{old if back else new}": bind.execute(
            text(_rename_key("user_preferences", column, old, new, back=back))
        ).rowcount
        for column, old, new in _SLOT_KEYS
    }
    src, dst = ("background", "flash") if back else ("flash", "background")
    counts["market_insights.model"] = bind.execute(
        text(f"UPDATE market_insights SET model = '{dst}' WHERE model = '{src}'")
    ).rowcount
    _log.info(f"072: Background slot {'named back' if back else 'renamed'} {counts}")


def _rewrite_history(bind) -> None:
    counts = {name: bind.execute(text(sql)).rowcount for name, sql in _HISTORY}
    _log.info(f"072: history rewritten {counts}")


def _replace_checks(which: int) -> None:
    for table, name, column, *values in _CHECKS:
        # ADD CONSTRAINT has no IF NOT EXISTS; the DROP makes this re-runnable
        # after an abort in the autocommit block.
        op.execute(
            f"""
            ALTER TABLE {table}
                DROP CONSTRAINT IF EXISTS {name},
                ADD CONSTRAINT {name}
                    CHECK ({column} IN ({values[which]})) NOT VALID
            """
        )


def _validate_and_index(predicate: str) -> None:
    # workspaces is written on every turn: the index builds without blocking
    # writers, and drop-before-create repairs an INVALID index an interrupted
    # build left, instead of adopting it.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        for table, name, *_ in _CHECKS:
            op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {name}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_NAME_INDEX}")
        op.execute(
            f"""
            CREATE UNIQUE INDEX CONCURRENTLY {_NAME_INDEX}
            ON workspaces (user_id, name_key)
            WHERE status NOT IN ({predicate})
            """
        )


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    _rewrite_history(bind)
    # 071's CHECK refuses 'unbound', which the rewrite writes.
    bind.execute(text(f"ALTER TABLE workspaces DROP CONSTRAINT IF EXISTS {_STATUS_CHECK}"))
    _set_updated_at_triggers(bind, "DISABLE")
    _rewrite_workspaces(bind)
    _rewrite_automations(bind)
    _rename_slot(bind)
    _set_updated_at_triggers(bind, "ENABLE")
    # After the rewrites: a CHECK added NOT VALID still holds every later write.
    _replace_checks(0)
    _validate_and_index("'deleted', 'unbound'")


def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    # First, as the upgrade's history: it scans the conversation tables.
    stamps = {
        name: bind.execute(text(sql)).rowcount for name, sql in _stamp_renames(back=True)
    }
    _log.info(f"072: origin stamps named back {stamps}")
    # This schema's CHECK refuses 'flash', which the rewrite writes.
    bind.execute(text(f"ALTER TABLE workspaces DROP CONSTRAINT IF EXISTS {_STATUS_CHECK}"))
    _set_updated_at_triggers(bind, "DISABLE")
    restored = bind.execute(
        text("UPDATE workspaces SET status = 'flash' WHERE status = 'unbound'")
    ).rowcount
    _rename_slot(bind, back=True)
    _set_updated_at_triggers(bind, "ENABLE")
    _log.info(f"072: workspaces given status 'flash' again {restored}")
    # 'ptc' on every row, then the previous build's default for a new one.
    op.execute(
        """
        ALTER TABLE automations
            ADD COLUMN IF NOT EXISTS agent_mode VARCHAR(20) NOT NULL DEFAULT 'ptc'
                CONSTRAINT automations_agent_mode_check
                CHECK (agent_mode IN ('ptc', 'flash'))
        """
    )
    op.execute("ALTER TABLE automations ALTER COLUMN agent_mode SET DEFAULT 'flash'")
    _replace_checks(1)
    _validate_and_index("'deleted', 'flash'")
