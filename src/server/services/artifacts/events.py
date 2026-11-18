"""Hints on the owner's user feed that an artifact changed.

Best effort: the web also reads its artifacts again when a thread's turn
ends. Both events reach the user feed only, never the agent. A shared
workspace's records are its holder's, whom no feed listens for, so their
events go to the organization's channel, where each member's feed passes
them on while it reaches the workspace.
"""

from __future__ import annotations

from collections.abc import Iterable

from src.server.database.artifacts import Artifact
from src.server.database.shared_workspaces import is_holder
from src.server.services.thread_lifecycle_feed import EVENT_VERSION, publish_user_event
from src.server.services.shared_workspaces.access import shared_org_of

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
    await _publish(
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
    await _publish(
        user_id,
        {
            "v": EVENT_VERSION,
            "type": ARTIFACT_EVENT,
            "artifact_id": artifact.artifact_id,
            "workspace_id": artifact.workspace_id,
            "path": artifact.path,
            "shared": artifact.shared,
            "latest_seq": artifact.latest_seq,
            "shared_seq": artifact.shared_seq,
        },
    )


async def _publish(owner_id: str, event: dict) -> None:
    if not is_holder(owner_id):
        await publish_user_event(owner_id, event)
        return
    try:
        org_id = await shared_org_of(event.get("workspace_id"))
    except Exception:
        return  # best effort, as every publish here is
    if org_id is not None:
        await publish_user_event(None, event, shared_org=org_id)
