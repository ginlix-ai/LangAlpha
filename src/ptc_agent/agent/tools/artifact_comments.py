"""Answer the user's comments on a file, under the comment."""

from typing import Any

import structlog
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)

# The comment message column's limit.
_MAX_REPLY_CHARS = 8000
# What the chat's "Replied to comment N" line can expand; the full reply is
# under the comment.
_SHOWN_CHARS = 1000


def _shown(body: str) -> tuple[str, bool]:
    """The reply as the chat shows it, cut at a word, and whether it was cut."""
    if len(body) <= _SHOWN_CHARS:
        return body, False
    cut = body[:_SHOWN_CHARS]
    at = max(cut.rfind(" "), cut.rfind("\n"))
    return (cut[:at] if at > 0 else cut).rstrip(), True


def create_reply_to_comment_tool() -> BaseTool:
    @tool(response_format="content_and_artifact")
    async def reply_to_comment(
        comment_id: str,
        text: str,
        config: RunnableConfig,
        resolve: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        """Answer a comment the user left on a file. The answer shows under that comment beside the file.

        If the comment asked for a change, edit the file in <file>, then say in the reply what changed.

        Args:
            comment_id: The comment's id, from its block in the user's message.
            text: The answer, up to 8000 characters.
            resolve: True when you made the change the comment asked for, which closes the comment. Leave it false when the user still has something to read or decide: an answer to a question, a change you declined or only partly made, or a question back.
        """
        from src.server.database import artifacts as artifacts_db
        from src.server.services.artifacts.events import publish_comment_change
        from src.server.utils.pg_sanitize import normalize_uuid

        configurable = (config or {}).get("configurable") or {}
        user_id = configurable.get("user_id")
        thread_id = configurable.get("thread_id")
        if not user_id or not thread_id:
            return (
                "ERROR: Replying to a comment needs a thread, and this turn has none.",
                {},
            )
        body = text.replace("\x00", "").strip()
        if not body:
            return "ERROR: The reply is empty.", {}
        if len(body) > _MAX_REPLY_CHARS:
            return (
                f"ERROR: The reply is {len(body)} characters; the limit is "
                f"{_MAX_REPLY_CHARS}.",
                {},
            )
        missing = (f"ERROR: No comment {comment_id} was sent to this thread.", {})
        comment_id = normalize_uuid(comment_id.strip())
        if comment_id is None:
            return missing
        try:
            reply = await artifacts_db.add_agent_reply(
                user_id=str(user_id),
                thread_id=str(thread_id),
                comment_id=comment_id,
                body=body,
                resolve=resolve,
            )
        except artifacts_db.CommentNotFound:
            return missing
        except Exception as e:
            logger.warning(
                "reply_to_comment failed", comment_id=comment_id, exc_info=True
            )
            return f"ERROR: The reply could not be saved: {e!s}", {}

        await publish_comment_change(
            str(user_id),
            artifact_id=reply.artifact_id,
            workspace_id=reply.workspace_id,
            path=reply.path,
            thread_id=str(thread_id),
            comment_ids=[comment_id],
        )
        where = f"comment {reply.number} on {reply.path}"
        if reply.kept_open:
            content = (
                f"Replied to {where}. It stays open: the user has written more in "
                f"it since, which reaches you when they send it."
            )
        elif resolve:
            content = f"Replied to {where} and resolved it."
        else:
            content = f"Replied to {where}."
        shown, clipped = _shown(body)
        artifact = {
            "type": "artifact_comment",
            "artifact_id": reply.artifact_id,
            "workspace_id": reply.workspace_id,
            "path": reply.path,
            "comment_id": comment_id,
            "number": reply.number,
            "resolved": reply.resolved,
            "text": shown,
            "clipped": clipped,
        }
        return content, artifact

    return reply_to_comment
