"""Shared SQL fragments for the conversation thread and response queries."""


def sql_literals(statuses: tuple) -> str:
    """Render a status vocabulary as a SQL IN-list.

    The tuples come from ``contracts.status`` — every status IN-filter in this
    package must build from those constants through here, never a hand-typed
    list, so the SQL cannot drift from the Python classification. Nothing
    user-supplied reaches this, and binding as parameters would cost the
    planner the constant folding that keeps the branch filters index-friendly.
    """
    return ", ".join(f"'{s}'" for s in statuses)

# The thread row that every thread response model is built from. A query
# appends what only it serves (the seen cursor, the share fields), so a new
# column added here reaches every reader and writer that returns a whole
# thread. Writes that answer only their own fields (the title CAS, the share
# update) project those alone.
_THREAD_COLUMN_NAMES = (
    "conversation_thread_id", "workspace_id", "current_status", "msg_type",
    "thread_index", "title", "platform", "metadata", "is_shared", "is_pinned",
    "archived_at", "llm_model", "subagents_allowed", "reasoning_effort",
    "fast_mode", "created_at", "updated_at",
)
_THREAD_COLUMNS = ", ".join(_THREAD_COLUMN_NAMES)
# For a query that joins another table under the ``t`` alias.
_THREAD_COLUMNS_T = ", ".join(f"t.{c}" for c in _THREAD_COLUMN_NAMES)

def first_query(*, untitled_only: bool = False) -> str:
    """The first message of thread ``t``, which an untitled thread shows as
    its title: a query for one row of one column, ``content``.
    ``untitled_only`` reads nothing for a titled thread, for a lateral join,
    whose ``ON`` would read it for every thread first."""
    gate = "NULLIF(t.title, '') IS NULL AND " if untitled_only else ""
    return f"""
        SELECT left(q.content, 120) AS content FROM conversation_queries q
        WHERE {gate}q.conversation_thread_id = t.conversation_thread_id
        ORDER BY q.turn_index ASC LIMIT 1"""


# A thread's title, or for an untitled one its first message, as every
# listing shows it: a hand-off's thread runs before its title is written. For
# a query that joins the thread under the ``t`` alias.
TITLE_OR_FIRST_QUERY = f"COALESCE(NULLIF(t.title, ''), ({first_query()}))"


def owner_subagents_default(workspace_id_sql: str) -> str:
    """The subagents default of a workspace's owner, as a SQL boolean.

    ``other_preference.subagents_default``, where only a stored JSON ``false``
    turns it off: no preferences row, no key, or a value no writer should have
    stored all compare distinct from it, so a bad preference never fails a
    statement. Kept out of ``agent_preference``, which the agent can write.
    """
    return f"""((
        SELECT p.other_preference -> 'subagents_default'
        FROM workspaces w
        JOIN user_preferences p ON p.user_id = w.user_id
        WHERE w.workspace_id = {workspace_id_sql}
    ) IS DISTINCT FROM 'false'::jsonb)"""


def stored_subagents_allowed(value_sql: str, workspace_id_sql: str) -> str:
    """The subagent switch a write stores: NULL when it equals the default.

    A thread set to the side its owner's default is on follows the default,
    so it moves when the default does. Decided in the statement, against the
    default as committed, never by a client: one tab's copy of the default can
    be behind a change made in another.
    """
    return f"NULLIF({value_sql}::boolean, {owner_subagents_default(workspace_id_sql)})"


# ``usage_settled_at`` rides along because it is the settle instant itself, and
# replay pairs it with the query timestamp to say how long a turn took. Readers
# that go through ``_SETTLED_ATTEMPTS`` directly get it from the ``*``; one that
# projects these lists would otherwise be left with the start-plus-duration
# estimate, which measures from before the row existed.
#
# Replay reads every settled row of a thread on each open, but needs a row's
# ``replay_facts`` only for a turn it projects afresh or stubs. Those re-read
# their rows by id (``get_replay_responses``), so a cached open never moves the
# facts of its whole history over the wire, the legacy facts of an older turn
# among them. A row read without ``replay_facts`` has them still to read.
_LIGHT_RESPONSE_COLUMNS = (
    "conversation_response_id, conversation_thread_id, turn_index, status, "
    "interrupt_reason, metadata, warnings, errors, execution_time, created_at, "
    "usage_settled_at, attempt_no, retry_of_run_id"
)

# 1.6: retries append attempt rows at the SAME turn_index, and the live run is
# an in_progress row. History readers must see ONE row per turn — the newest
# attempt that has settled. in_progress is the slot, not history (pre-v4 no
# row existed until finalize, so excluding it preserves reader semantics).
# DISTINCT ON (turn_index) + attempt_no DESC picks that row without leaking a
# rank column into SELECT-* consumers.
#
# ``xmin`` rides along for the replay projection cache, which fingerprints a
# turn's table-sourced inputs: every one of them lives on this row, and any
# UPDATE (the finalize among them) writes a new tuple version. It is a
# tuple-header read, so it costs nothing — and a subquery cannot expose it,
# which is why it belongs here rather than at the replay call site.
def settled_attempts(columns: str, thread: str = "%s") -> str:
    """The query for the row each turn of a thread reads. ``thread`` is what
    the thread id is matched against: a bound parameter, or an outer column
    such as ``t.conversation_thread_id`` when this is a lateral join, so a
    job counting turns across threads selects the rows replay reads."""
    return f"""
    SELECT DISTINCT ON (turn_index) {columns}, xmin
    FROM conversation_responses
    WHERE conversation_thread_id = {thread} AND status <> 'in_progress'
    ORDER BY turn_index ASC, attempt_no DESC
"""


_SETTLED_ATTEMPTS = settled_attempts("*")
