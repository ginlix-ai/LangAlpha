"""Shared SQL fragments for conversation_responses readers."""

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
_SETTLED_ATTEMPTS = """
    SELECT DISTINCT ON (turn_index) *
    FROM conversation_responses
    WHERE conversation_thread_id = %s AND status <> 'in_progress'
    ORDER BY turn_index ASC, attempt_no DESC
"""
