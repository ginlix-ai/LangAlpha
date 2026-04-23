"""Middleware that injects user- and workspace-tier ``memory.md`` into the system prompt."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import structlog
from langchain.agents.middleware.types import AgentMiddleware, ModelRequest, ModelResponse
from langchain_core.messages import SystemMessage
from langgraph.store.base import BaseStore

logger = structlog.get_logger(__name__)

MAX_MEMORY_BLOCK_SIZE = 8192

# Memory injection runs every model call; a slow store must not stall the turn.
_READ_TIMEOUT_S = 2.0

NamespaceFactory = Callable[[], tuple[str, ...]]


def _append_content_block(system_message: SystemMessage | None, text: str) -> SystemMessage:
    new_content: list[dict[str, str]] = (
        list(system_message.content_blocks) if system_message else []
    )
    prefix = "\n\n" if new_content else ""
    new_content.append({"type": "text", "text": f"{prefix}{text}"})
    return SystemMessage(content_blocks=new_content)


class MemoryContextMiddleware(AgentMiddleware):
    """Inject one or both tiers of ``memory.md`` into the system prompt on every model call.

    Either namespace factory can be ``None`` when that tier's identity isn't
    available; that tier is simply omitted.
    """

    def __init__(
        self,
        *,
        store: BaseStore,
        user_namespace_factory: NamespaceFactory | None = None,
        workspace_namespace_factory: NamespaceFactory | None = None,
        user_display_path: str = ".agents/user/memory/memory.md",
        workspace_display_path: str = ".agents/workspace/memory/memory.md",
        index_key: str = "memory.md",
    ) -> None:
        self._store = store
        self._user_ns = user_namespace_factory
        self._workspace_ns = workspace_namespace_factory
        self._user_display_path = user_display_path
        self._workspace_display_path = workspace_display_path
        self._index_key = index_key

    @staticmethod
    def _content_from_value(value: object) -> str | None:
        if not isinstance(value, dict):
            return None
        raw = value.get("content")
        if isinstance(raw, str):
            return raw
        if isinstance(raw, list):
            return "\n".join(raw)
        return None

    async def _read_memory(
        self, namespace_factory: NamespaceFactory
    ) -> str | None:
        try:
            namespace = namespace_factory()
        except Exception:
            logger.exception("memory namespace resolution failed")
            return None
        try:
            item = await asyncio.wait_for(
                self._store.aget(namespace, self._index_key),
                timeout=_READ_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "memory.md read timed out",
                namespace=namespace,
                timeout_s=_READ_TIMEOUT_S,
            )
            return None
        except Exception:
            logger.exception(
                "memory.md store.aget failed", namespace=namespace
            )
            return None
        if item is None:
            return None
        return self._content_from_value(item.value)

    @staticmethod
    def _format_block(display_path: str, content: str | None) -> str:
        if content is None:
            return (
                f'<memory path="{display_path}">\n'
                f"No {display_path} exists yet. When you learn something durable, "
                "create a typed detail file with frontmatter and add a one-line "
                f"pointer to it here. Keep {display_path} as an index, not a memory.\n"
                f"</memory>"
            )
        if len(content) > MAX_MEMORY_BLOCK_SIZE:
            # Cut at a newline so we don't slice mid-markdown-fence and tempt
            # the model to "repair" attacker-controlled content.
            cut = content.rfind("\n", 0, MAX_MEMORY_BLOCK_SIZE)
            if cut <= 0:
                cut = MAX_MEMORY_BLOCK_SIZE
            content = content[:cut] + "\n\n[... truncated ...]"
        return f'<memory path="{display_path}">\n{content}\n</memory>'

    async def _build_block(self) -> str:
        # Fetch both tiers in parallel for a single round-trip wall cost.
        async def _maybe_read(factory: NamespaceFactory | None) -> str | None:
            if factory is None:
                return None
            return await self._read_memory(factory)

        user_content, workspace_content = await asyncio.gather(
            _maybe_read(self._user_ns),
            _maybe_read(self._workspace_ns),
        )
        blocks: list[str] = []
        if self._user_ns is not None:
            blocks.append(self._format_block(self._user_display_path, user_content))
        if self._workspace_ns is not None:
            blocks.append(
                self._format_block(self._workspace_display_path, workspace_content)
            )
        return "\n".join(blocks)

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse:
        # Sync fallback; langalpha is async end-to-end.
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        block = await self._build_block()
        new_system_message = _append_content_block(request.system_message, block)
        modified_request = request.override(system_message=new_system_message)
        return await handler(modified_request)
