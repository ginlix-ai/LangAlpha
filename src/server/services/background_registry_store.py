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
                self._registries[thread_id] = registry
                logger.debug(
                    "Created background registry",
                    extra={"thread_id": thread_id},
                )
            return registry

    async def get_registry(self, thread_id: str) -> BackgroundTaskRegistry | None:
        async with self._lock:
            return self._registries.get(thread_id)

    async def cancel_and_clear(self, thread_id: str, *, force: bool = False) -> int:
        async with self._lock:
            registry = self._registries.get(thread_id)
            if registry is None:
                return 0

        cancelled = await registry.cancel_all(force=force)
        registry.clear()

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
