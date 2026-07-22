"""Conversation store: threads, queries, responses, usage, feedback, replay.

Split by table/concern — threads_read / threads_write / queries / responses /
usage / feedback / replay_rows, with shared leaves errors and _sql. The package
root re-exports the legacy surface below; new code should import the owning
submodule directly.
"""

from src.server.database.conversation._sql import (
    _RESPONSE_COLUMNS,
    _SETTLED_ATTEMPTS,
)
from src.server.database.conversation.errors import (
    EXTERNAL_ID_CONFLICT_ERROR_TYPE,
    ExternalIdConflictError,
    QueryConflictError,
    _EXTERNAL_ID_CONFLICT_MESSAGE,
    _EXTERNAL_ID_INDEX,
    _unique_violation_constraint,
    external_id_conflict_payload,
)
from src.server.database.conversation.threads_read import (
    get_thread_auth_meta,
    get_thread_by_id,
    get_thread_by_share_token,
    get_thread_checkpoint_id,
    get_thread_owner_id,
    get_thread_with_summary,
    get_threads_for_user,
    get_workspace_threads,
    _like_escape,
    lookup_thread_by_external_id,
)
from src.server.database.conversation.threads_write import (
    calculate_next_thread_index,
    create_thread,
    delete_thread,
    ensure_thread_exists,
    _EXISTS_TTL,
    thread_exists_key,
    truncate_thread_from_turn,
    update_thread_checkpoint_id,
    update_thread_external_id,
    update_thread_sharing,
    update_thread_status,
    update_thread_title,
    ws_exists_key,
)
from src.server.database.conversation.queries import (
    create_query,
    get_latest_turn_index,
    get_queries_for_thread,
)
from src.server.database.conversation.responses import (
    append_sse_event,
    get_recent_responses_for_thread,
    get_responses_for_thread,
    rebase_sse_events,
    _sse_has_provenance,
    _sync_provenance_for_response,
)
from src.server.database.conversation.usage import (
    create_usage_record,
)
from src.server.database.conversation.feedback import (
    delete_feedback,
    get_feedback_for_thread,
    upsert_feedback,
)
from src.server.database.conversation.replay_rows import (
    get_replay_thread_data,
)

# Legacy façade: symbols extracted into submodules stay importable from the
# package root. New code should import the owning submodule directly.
__all__ = [
    "append_sse_event",
    "calculate_next_thread_index",
    "create_query",
    "create_thread",
    "create_usage_record",
    "delete_feedback",
    "delete_thread",
    "ensure_thread_exists",
    "_EXISTS_TTL",
    "EXTERNAL_ID_CONFLICT_ERROR_TYPE",
    "_EXTERNAL_ID_CONFLICT_MESSAGE",
    "external_id_conflict_payload",
    "_EXTERNAL_ID_INDEX",
    "ExternalIdConflictError",
    "get_feedback_for_thread",
    "get_latest_turn_index",
    "get_queries_for_thread",
    "get_recent_responses_for_thread",
    "get_replay_thread_data",
    "get_responses_for_thread",
    "get_thread_auth_meta",
    "get_thread_by_id",
    "get_thread_by_share_token",
    "get_thread_checkpoint_id",
    "get_thread_owner_id",
    "get_thread_with_summary",
    "get_threads_for_user",
    "get_workspace_threads",
    "_like_escape",
    "lookup_thread_by_external_id",
    "QueryConflictError",
    "rebase_sse_events",
    "_RESPONSE_COLUMNS",
    "_SETTLED_ATTEMPTS",
    "_sse_has_provenance",
    "_sync_provenance_for_response",
    "thread_exists_key",
    "truncate_thread_from_turn",
    "_unique_violation_constraint",
    "update_thread_checkpoint_id",
    "update_thread_external_id",
    "update_thread_sharing",
    "update_thread_status",
    "update_thread_title",
    "upsert_feedback",
    "ws_exists_key",
]

