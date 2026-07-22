"""Conversation-store exceptions and the external-id conflict contract."""

from typing import Optional, Dict, Any


class QueryConflictError(Exception):
    """Raised when create_query (idempotent) collides with an existing
    row whose content differs from the new write.

    The idempotent ``ON CONFLICT DO UPDATE`` is gated on
    ``content IS NOT DISTINCT FROM EXCLUDED.content``, so legitimate
    retry-of-same-content stays a silent no-op while different-content
    races (e.g. a concurrent POST that bypassed the in-process admission
    lock) surface here instead of silently overwriting the loser's row.
    """

    def __init__(self, thread_id: str, turn_index: int, existing_content: Optional[str]):
        self.thread_id = thread_id
        self.turn_index = turn_index
        self.existing_content = existing_content
        super().__init__(
            f"conversation_queries collision for thread_id={thread_id} "
            f"turn_index={turn_index}: existing row has different content"
        )


class ExternalIdConflictError(Exception):
    """Raised when a ``(platform, external_id)`` pair is already claimed by a
    DIFFERENT thread, violating ``idx_conversation_threads_external`` (the global
    partial-unique index on ``(platform, external_id) WHERE external_id IS NOT NULL``).

    Surfaced by:
      - ``update_thread_external_id`` (the stamp API) when the UPDATE collides.
      - ``create_thread`` when a create race loses the ``(platform, external_id)``
        unique-index check; the winner resolves upstream via
        ``lookup_thread_by_external_id`` and the loser regenerates a fresh key.

    Routers convert it to an HTTP 409 whose body carries
    ``error_type="external_id_conflict"`` plus the offending pair, so channel
    clients can detect the collision and fall back to their own dedup.
    """

    def __init__(self, platform: str, external_id: str):
        self.platform = platform
        self.external_id = external_id
        super().__init__(
            f"external_id already in use: platform={platform} external_id={external_id}"
        )


# Name of the global partial-unique index on (platform, external_id). Matched
# against ``UniqueViolation.diag.constraint_name`` to tell an external-id dedup
# collision apart from the per-workspace thread_index uniqueness constraint.
_EXTERNAL_ID_INDEX = "idx_conversation_threads_external"

# Stable ``error_type`` discriminator emitted whenever a (platform, external_id)
# pair collides. Channel clients key on this exact string to fall back to their
# own dedup, so every surface that reports the conflict — the HTTP 409 detail and
# the in-stream SSE error frame — MUST carry it. Centralized here (next to the
# error it describes) so the two surfaces can't drift.
EXTERNAL_ID_CONFLICT_ERROR_TYPE = "external_id_conflict"

# Canonical human-facing wording for the conflict (originally the HTTP 409
# detail's message). Shared so the SSE frame reads identically.
_EXTERNAL_ID_CONFLICT_MESSAGE = (
    "This (platform, external_id) is already linked to another thread."
)


def external_id_conflict_payload(platform: str, external_id: str) -> Dict[str, Any]:
    """Core fields describing a (platform, external_id) collision.

    Single source of truth shared by the HTTP 409 detail body and the SSE error
    frame so their ``error_type``, offending pair, and human message stay in
    lockstep. Callers layer any surface-specific fields on top.
    """
    return {
        "error_type": EXTERNAL_ID_CONFLICT_ERROR_TYPE,
        "message": _EXTERNAL_ID_CONFLICT_MESSAGE,
        "platform": platform,
        "external_id": external_id,
    }


def _unique_violation_constraint(exc: Exception) -> Optional[str]:
    """Best-effort constraint/index name from a psycopg UniqueViolation.

    For a standalone unique index (like ``idx_conversation_threads_external``)
    psycopg reports the index name in ``diag.constraint_name``; returns ``None``
    when the diagnostic is unavailable.
    """
    return getattr(getattr(exc, "diag", None), "constraint_name", None)
