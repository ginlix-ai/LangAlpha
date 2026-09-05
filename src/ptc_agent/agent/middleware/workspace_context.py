"""Injects the workspace's identity and its agent.md into the system prompt.

The name and description are read when the turn's agent is built and held for
that turn, so what the model is told is what the user last set. agent.md is read
from the sandbox on every model call, so an edit the agent makes mid-conversation
is visible to the call after it.

Both blocks land after the prompt-cache breakpoint (whichever of the provider
caching middlewares is live pins the static prefix earlier in the stack), so
neither costs a cache miss when it changes.
"""

from collections.abc import Awaitable, Callable
from html import escape
from typing import Any

import structlog
from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage

logger = structlog.get_logger(__name__)

MAX_AGENT_MD_SIZE = 8192

_NO_AGENT_MD_BLOCK = (
    '<agentmd path="/agent.md">\n'
    "No agent.md exists yet. Create /agent.md at the workspace root with:\n"
    "- Workspace purpose based on the user's query\n"
    "- Initial goals and planned artifacts\n"
    "- Section stubs for Thread Index, Key Findings, File Index\n"
    "</agentmd>"
)


def _append_content_block(system_message: SystemMessage | None, text: str) -> SystemMessage:
    """Append a text content block to a system message."""
    new_content: list[dict[str, str]] = (
        list(system_message.content_blocks) if system_message else []
    )
    prefix = "\n\n" if new_content else ""
    new_content.append({"type": "text", "text": f"{prefix}{text}"})
    return SystemMessage(content_blocks=new_content)


class WorkspaceContextMiddleware(AgentMiddleware):
    """Injects the workspace block and agent.md on every model call.

    Args:
        session: The Session object (has get_agent_md() with caching/invalidation).
        name: The workspace's name, as its row had it when this agent was built.
        description: The workspace's description, from the same read.
    """

    def __init__(self, *, session: Any, name: str = "", description: str = "") -> None:
        self._session = session
        self._name = (name or "").strip()
        self._description = (description or "").strip()

    def _workspace_block(self) -> str:
        """What the workspace is called, as element text.

        Text rather than attributes: a name is free text the user typed, and as
        text an escape of `<` and `&` is the whole obligation — no quoting rule
        to get wrong, and an apostrophe survives as an apostrophe.
        """
        if not self._name:
            return ""
        lines = [f"Name: {escape(self._name, quote=False)}"]
        if self._description:
            lines.append(f"Description: {escape(self._description, quote=False)}")
        body = "\n".join(lines)
        return f"<workspace>\n{body}\n</workspace>"

    async def _get_agent_md_block(self) -> str:
        """Build the workspace context block from agent.md."""
        content = await self._session.get_agent_md()
        if not content:
            return _NO_AGENT_MD_BLOCK
        if len(content) > MAX_AGENT_MD_SIZE:
            content = content[:MAX_AGENT_MD_SIZE] + "\n\n[... truncated ...]"
        return f'<agentmd path="/agent.md">\n{content}\n</agentmd>'

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
        """Inject the workspace block and the latest agent.md before each call."""
        workspace = self._workspace_block()
        agent_md = await self._get_agent_md_block()
        blocks = "\n\n".join(b for b in (workspace, agent_md) if b)
        new_system_message = _append_content_block(request.system_message, blocks)
        modified_request = request.override(system_message=new_system_message)
        return await handler(modified_request)
