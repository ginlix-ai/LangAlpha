"""Hints on the owner's user feed that an artifact changed.

Best effort: the web also reads its artifacts again when a thread's turn
ends. Both events reach the user feed only, never the agent.
"""

from __future__ import annotations

from collections.abc import Iterable

from src.server.database.artifacts import Artifact
from src.server.services.thread_lifecycle_feed import EVENT_VERSION, publish_user_event

#: A comment changed: written, queued, sent, given back, answered or read.
ARTIFACT_COMMENT_EVENT = "artifact_comment"
#: The record changed: a version, a share or unshare, a setting, a binding.
ARTIFACT_EVENT = "artifact"


async def publish_comment_change(
    user_id: str,
    *,
    artifact_id: str,
    workspace_id: str,
    path: str,
    thread_id: str | None,
    comment_ids: Iterable[str],
) -> None:
    """``thread_id`` is the changed comments' destination, whose queue the
    owner's tabs read again."""
    await publish_user_event(
        user_id,
        {
            "v": EVENT_VERSION,
            "type": ARTIFACT_COMMENT_EVENT,
            "artifact_id": artifact_id,
            "workspace_id": workspace_id,
            "path": path,
            "thread_id": thread_id,
            "comment_ids": list(comment_ids),
        },
    )


async def publish_artifact_change(user_id: str, artifact: Artifact) -> None:
    await publish_user_event(
        user_id,
        {
            "v": EVENT_VERSION,
            "type": ARTIFACT_EVENT,
            "artifact_id": artifact.artifact_id,
            "workspace_id": artifact.workspace_id,
            "path": artifact.path,
            "shared": artifact.shared,
            "latest_seq": artifact.latest_seq,
        },
    )
