"""Background subagent registry store.

Keeps BackgroundTaskRegistry instances keyed by thread_id so that
background subagent tasks survive reconnects for the same thread.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTaskRegistry

logger = logging.getLogger(__name__)


def _message_text(msg) -> str:
    """Plain text of a LangChain message (string or block-list content)."""
    content = getattr(msg, "content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = [
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        return "\n".join(p for p in parts if p).strip()
    return ""


async def resolve_task_result_text(thread_id: str, task_id: str) -> str | None:
    """Durable result derivation: the subagent's final answer, read from its
    ``task:{task_id}`` checkpoint namespace. This is the primary delivery
    source for TaskOutput — it survives registry eviction, user stops,
    restarts, and other-worker reads, all of which lose the in-memory entry.
    """
    from src.server.services.history.reader import CheckpointHistoryReader

    reader = CheckpointHistoryReader.get_instance()
    messages = await reader.aget_task_messages(thread_id, task_id)
    for msg in reversed(messages):
        if getattr(msg, "type", None) != "ai":
            continue
        text = _message_text(msg)
        if text:
            return text
    return None


class BackgroundRegistryStore:
    """Singleton store for per-thread background registries."""

    _instance: Optional["BackgroundRegistryStore"] = None

    def __init__(self) -> None:
        self._registries: Dict[str, BackgroundTaskRegistry] = {}
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "BackgroundRegistryStore":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def get_or_create_registry(self, thread_id: str) -> BackgroundTaskRegistry:
        async with self._lock:
            registry = self._registries.get(thread_id)
            if registry is None:
                registry = BackgroundTaskRegistry(thread_id=thread_id)
                registry.result_resolver = resolve_task_result_text
                self._registries[thread_id] = registry
                logger.debug(
                    "Created background registry",
                    extra={"thread_id": thread_id},
                )
            return registry

    async def get_registry(self, thread_id: str) -> BackgroundTaskRegistry | None:
        async with self._lock:
            return self._registries.get(thread_id)

    async def cancel_run_tasks(
        self, thread_id: str, run_id: str, *, force: bool = False
    ) -> int:
        """Cancel only the tasks spawned by ``run_id``; the registry and any
        prior-turn tasks/claims survive (unlike ``cancel_and_clear``)."""
        async with self._lock:
            registry = self._registries.get(thread_id)
        if registry is None:
            return 0

        # registry.cancel_run_tasks logs the cancellation with task detail.
        return await registry.cancel_run_tasks(run_id, force=force)

    async def cancel_and_clear(self, thread_id: str, *, force: bool = False) -> int:
        async with self._lock:
            registry = self._registries.get(thread_id)
            if registry is None:
                return 0

        cancelled = await registry.cancel_all(force=force)
        # Lock-held clear: the stop teardown can race a concurrent drain /
        # collector still reading the registry.
        await registry.clear_locked()

        async with self._lock:
            self._registries.pop(thread_id, None)

        logger.info(
            "Cleared background registry",
            extra={"thread_id": thread_id, "cancelled": cancelled, "force": force},
        )
        return cancelled

    async def cancel_all(self, *, force: bool = False) -> int:
        async with self._lock:
            registries = list(self._registries.items())

        cancelled_total = 0
        for thread_id, registry in registries:
            cancelled_total += await registry.cancel_all(force=force)
            registry.clear()
            logger.info(
                "Cleared background registry",
                extra={"thread_id": thread_id, "force": force},
            )

        async with self._lock:
            self._registries.clear()

        return cancelled_total
