"""Background subagent registry store.

Keeps BackgroundTaskRegistry instances keyed by thread_id so that
background subagent tasks survive reconnects for the same thread.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

from ptc_agent.agent.middleware.background_subagent.registry import BackgroundTaskRegistry
from ptc_agent.agent.middleware.background_subagent.workflow.ui_snapshot import (
    read_task_result,
)

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


async def _task_history(thread_id: str, task_id: str):
    from src.server.services.history.reader import CheckpointHistoryReader

    return await CheckpointHistoryReader.get_instance().aget_task_history(
        thread_id, task_id
    )


async def resolve_archived_result_text(
    thread_id: str, task_id: str, task_run_id: str
) -> str | None:
    """An explicitly archived result for exactly this run, or None — never
    transcript-derived.

    The source for callers that must not fabricate. A run that failed still
    left a transcript, so deriving from one would present mid-work text as
    the answer; an archive is only ever written by a run that got far enough
    to produce a result, which is what makes its presence trustworthy where
    the ledger's verdict is not.
    """
    history = await _task_history(thread_id, task_id)
    return read_task_result(history.new_ui_records, task_run_id)


async def resolve_task_result_text(
    thread_id: str, task_id: str, task_run_id: str | None = None
) -> str | None:
    """Durable result derivation from a task's ``task:{task_id}`` namespace.
    The primary delivery source for TaskOutput — it survives registry
    eviction, user stops, restarts, and other-worker reads, all of which lose
    the in-memory entry.

    An explicitly archived result answers for the run that wrote it; otherwise
    the answer is derived from the transcript, which is how a subagent (whose
    final AI message *is* its answer) has always been read. Presence decides,
    not task kind, so a kind that later starts archiving needs no change here.
    Only sound once the run is known to have finished — a caller holding a
    failed/unknown verdict wants ``resolve_archived_result_text`` instead.
    """
    history = await _task_history(thread_id, task_id)
    if task_run_id:
        archived = read_task_result(history.new_ui_records, task_run_id)
        if archived:
            return archived
    for msg in reversed(history.messages):
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
                from src.server.services.subagent_run_coordinator import SubagentRunCoordinator

                registry = BackgroundTaskRegistry(thread_id=thread_id)
                registry.result_resolver = resolve_task_result_text
                registry.archived_result_resolver = resolve_archived_result_text
                registry.run_ledger = SubagentRunCoordinator(thread_id)
                self._registries[thread_id] = registry
                logger.debug(
                    "Created background registry",
                    extra={"thread_id": thread_id},
                )
            return registry

    async def get_registry(self, thread_id: str) -> BackgroundTaskRegistry | None:
        async with self._lock:
            return self._registries.get(thread_id)

    async def cancel_task(self, thread_id: str, task_id: str) -> bool:
        """Cancel one task if this worker owns its live writer; False otherwise.

        force=True: a user-targeted stop must interrupt the handler itself —
        a plain wrapper cancel is absorbed by the writer shield and the task
        would run on to completion.
        """
        async with self._lock:
            registry = self._registries.get(thread_id)
        if registry is None:
            return False
        return await registry.cancel_task(task_id, force=True)

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
