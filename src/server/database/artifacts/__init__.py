"""Artifacts: a workspace file's record, its published versions, and the owner's
comments on it.

The record is the file's ``share_links`` row (migration 101), so a file has one
``/a/`` code whether or not it was ever shared. Rows are made on first need:
the agent writing the file (its creator), a comment, a binding or a publish.
Ownership is the workspace's: every read and write keyed by ``artifact_id``
joins the workspace and checks its user.

A version is cut in one transaction under the record's row lock, which also
numbers comments, so two publishes never take one ``seq`` and two comments
never take one pin. A shared record's ``files`` is always its latest
version's paths, which 049's check and a previous build's serving both read.

Every change to a comment's messages locks the comment row first, so its
``delivery`` stays true to them across workers: ``sent`` exactly when none of
the owner's messages in it is unsent. The lock also orders the thread: each
message takes the next ``seq`` once it holds the lock, so seq order is commit
order, which ``created_at`` (the transaction's start) is not.

Where a comment goes is a thread the owner may send it to: theirs, not
archived, and in the artifact's workspace or the owner's Home, whose Chief of
Staff reaches every workspace. A sent message names the thread it went to,
and only that thread's agent may answer the comment.

How far the owner has read a comment is a cursor in
``artifact_comment_reads``, never on the comment row, since reading is not an
edit. The agent's messages past it are unread; the owner's own never are,
because writing one moves the cursor to it.

``records`` is the row itself, ``routing`` where comments go,
``versions`` publishing, ``comments`` the owner's side of a comment and
``delivery`` a chat send carrying comments to the agent and its answer.
"""

from src.server.database.artifacts.comments import (
    Comment,
    CommentConflict,
    CommentCounts,
    CommentMessage,
    CommentNotFound,
    Delivery,
    Status,
    add_owner_message,
    comment_counts,
    create_comment,
    delete_comment,
    list_comments,
    list_queued_comments,
    mark_artifact_read,
    mark_comment_read,
    update_comment,
)
from src.server.database.artifacts.delivery import (
    AgentReply,
    ClaimedComment,
    add_agent_reply,
    claim_comments,
    confirm_claim,
    list_abandoned_claims,
    release_claim,
    release_unread,
    release_unread_send,
)
from src.server.database.artifacts.routing import (
    CommentDestination,
    DestinationRule,
    Destinations,
    DestinationThread,
    DestinationUnavailable,
    bind,
    destinations,
    unbind,
)
from src.server.database.artifacts.records import (
    ARTIFACT_COLUMNS,
    Artifact,
    ArtifactNotFound,
    artifact_from_row,
    clip_title,
    ensure_artifact,
    find_artifact,
    get_artifact,
    list_kept_updated,
    one_line,
    record_creator,
    set_keep_updated,
    unshare,
)
from src.server.database.artifacts.versions import (
    KEEP_UPDATED_VERSIONS,
    Origin,
    PublishResult,
    SharingChanged,
    VersionMoved,
    StoredVersionFile,
    Version,
    VersionFile,
    get_shared_version_file,
    get_version,
    get_version_file,
    get_version_hashes,
    list_versions,
    publish_version,
    refuse_legacy,
)

__all__ = [
    "ARTIFACT_COLUMNS",
    "KEEP_UPDATED_VERSIONS",
    "AgentReply",
    "Artifact",
    "ArtifactNotFound",
    "ClaimedComment",
    "Comment",
    "CommentConflict",
    "CommentCounts",
    "CommentDestination",
    "CommentMessage",
    "CommentNotFound",
    "Delivery",
    "DestinationRule",
    "DestinationThread",
    "DestinationUnavailable",
    "Destinations",
    "Origin",
    "PublishResult",
    "SharingChanged",
    "VersionMoved",
    "Status",
    "StoredVersionFile",
    "Version",
    "VersionFile",
    "add_agent_reply",
    "add_owner_message",
    "artifact_from_row",
    "bind",
    "claim_comments",
    "clip_title",
    "comment_counts",
    "confirm_claim",
    "create_comment",
    "delete_comment",
    "destinations",
    "ensure_artifact",
    "find_artifact",
    "get_artifact",
    "get_shared_version_file",
    "get_version",
    "get_version_file",
    "get_version_hashes",
    "list_comments",
    "list_kept_updated",
    "list_queued_comments",
    "list_versions",
    "mark_artifact_read",
    "mark_comment_read",
    "one_line",
    "publish_version",
    "record_creator",
    "refuse_legacy",
    "list_abandoned_claims",
    "release_claim",
    "release_unread",
    "release_unread_send",
    "set_keep_updated",
    "unbind",
    "unshare",
    "update_comment",
]
