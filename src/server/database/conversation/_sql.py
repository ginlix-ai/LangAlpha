"""Shared SQL fragments for conversation_responses readers."""


def sql_literals(statuses: tuple) -> str:
    """Render a status vocabulary as a SQL IN-list.

    The tuples come from ``contracts.status`` — every status IN-filter in this
    package must build from those constants through here, never a hand-typed
    list, so the SQL cannot drift from the Python classification. Nothing
    user-supplied reaches this, and binding as parameters would cost the
    planner the constant folding that keeps the branch filters index-friendly.
    """
    return ", ".join(f"'{s}'" for s in statuses)

_RESPONSE_COLUMNS = (
    "conversation_response_id, conversation_thread_id, turn_index, status, "
    "interrupt_reason, metadata, warnings, errors, execution_time, created_at, "
    "sse_events, attempt_no, retry_of_run_id"
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
# UPDATE (the archive drain rewriting sse_events, an atomic context_window
# append) writes a new tuple version. It is a tuple-header read, so it costs
# nothing — and a subquery cannot expose it, which is why it belongs here
# rather than at the replay call site.
_SETTLED_ATTEMPTS = """
    SELECT DISTINCT ON (turn_index) *, xmin
    FROM conversation_responses
    WHERE conversation_thread_id = %s AND status <> 'in_progress'
    ORDER BY turn_index ASC, attempt_no DESC
"""
