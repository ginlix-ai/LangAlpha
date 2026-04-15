"""Middleware for dynamically injecting workspace context (agent.md) into system prompt.

Reads agent.md from the sandbox on every model call, ensuring the agent always
sees the latest workspace context — even after it creates or updates agent.md
mid-conversation. The content is appended as the last content block in the
system message, after skills and all other injections.

When the YAML front matter in agent.md changes (e.g. agent updates
workspace_name or description), the middleware syncs those changes back to
the workspace record in the database.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage

logger = structlog.get_logger(__name__)

MAX_AGENT_MD_SIZE = 8192


def _parse_yaml_front_matter(content: str) -> dict[str, str] | None:
    """Extract YAML front matter from markdown content.

    Returns a dict of key-value pairs, or None if no front matter found.
    Only handles simple `key: value` lines (no nested structures).
    """
    if not content.startswith("---\n"):
        return None
    end = content.find("\n---", 3)
    if end == -1:
        return None
    # Start after first newline (handles "---\n" = 4 chars)
    start = content.index("\n") + 1
    block = content[start:end]
    result = {}
    for line in block.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        key, _, value = line.partition(":")
        result[key.strip()] = value.strip()
    return result


def _append_content_block(system_message: SystemMessage | None, text: str) -> SystemMessage:
    """Append a text content block to a system message."""
    new_content: list[dict[str, str]] = (
        list(system_message.content_blocks) if system_message else []
    )
    prefix = "\n\n" if new_content else ""
    new_content.append({"type": "text", "text": f"{prefix}{text}"})
    return SystemMessage(content_blocks=new_content)


class WorkspaceContextMiddleware(AgentMiddleware):
    """Dynamically injects agent.md content into the system prompt on every model call.

    This ensures:
    1. agent.md is always the LAST content block (after skills)
    2. Changes to agent.md mid-conversation are reflected immediately
    3. YAML front matter changes are synced back to the workspace DB

    Args:
        session: The Session object (has get_agent_md() with caching/invalidation).
    """

    def __init__(self, *, session: Any) -> None:
        self._session = session
        # Cache last-seen front matter to detect changes
        self._last_front_matter: dict[str, str] | None = None

    @property
    def _workspace_id(self) -> str | None:
        return getattr(self._session, "conversation_id", None)

    async def _sync_front_matter_to_db(
        self, front_matter: dict[str, str], *, prev: dict[str, str] | None = None
    ) -> None:
        """Sync changed YAML front matter fields back to the workspace DB record."""
        if not self._workspace_id:
            return

        updates: dict[str, str] = {}
        prev = prev or {}

        for key, db_field in (
            ("workspace_name", "name"),
            ("description", "description"),
        ):
            new_val = front_matter.get(key, "")
            old_val = prev.get(key, "")
            if new_val and new_val != old_val:
                updates[db_field] = new_val

        if not updates:
            return

        try:
            from src.server.database.workspace import update_workspace

            await update_workspace(workspace_id=self._workspace_id, **updates)
            logger.debug(
                "Synced agent.md front matter to workspace DB",
                workspace_id=self._workspace_id,
                updates=list(updates.keys()),
            )
        except Exception as e:
            logger.warning(
                "Failed to sync agent.md front matter to DB",
                workspace_id=self._workspace_id,
                error=str(e),
            )

    async def _get_workspace_context_block(self) -> str:
        """Build the workspace context block from agent.md."""
        agent_md = await self._session.get_agent_md()
        if agent_md:
            # Check for front matter changes and sync to DB
            front_matter = _parse_yaml_front_matter(agent_md)
            if front_matter is not None and front_matter != self._last_front_matter:
                # Capture prev before overwriting — task runs async after this line
                prev = self._last_front_matter
                self._last_front_matter = front_matter
                # Fire-and-forget — don't block the model call
                asyncio.create_task(self._sync_front_matter_to_db(front_matter, prev=prev))

            if len(agent_md) > MAX_AGENT_MD_SIZE:
                agent_md = agent_md[:MAX_AGENT_MD_SIZE] + "\n\n[... truncated ...]"
            return f'<agentmd path="/agent.md">\n{agent_md}\n</agentmd>'
        return (
            '<agentmd path="/agent.md">\n'
            "No agent.md exists yet. Create /agent.md at the workspace root with:\n"
            "- Workspace purpose based on the user's query\n"
            "- Initial goals and planned artifacts\n"
            "- Section stubs for Thread Index, Key Findings, File Index\n"
            "</agentmd>"
        )

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback — shouldn't be hit in async agent, but keep for safety
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        """Inject latest agent.md into system message before each model call."""
        context_block = await self._get_workspace_context_block()
        new_system_message = _append_content_block(request.system_message, context_block)
        modified_request = request.override(system_message=new_system_message)
        return await handler(modified_request)
