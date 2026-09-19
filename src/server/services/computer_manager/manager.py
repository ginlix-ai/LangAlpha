"""Assembly point for ComputerManager.

The mixins in this package are a file split of one object, not a class
hierarchy. They all bind to the same ``self`` and read attributes created in
``ComputerManager.__init__``, so none of them is usable on its own."""

import asyncio
import logging
from typing import Any, Dict, Optional, Tuple

from ptc_agent.config import AgentConfig

from src.server.services.workspace_entitlements import WorkspaceEntitlementsMixin

from src.server.services.computer_manager._types import MachineState
from src.server.services.computer_manager._lifecycle import SessionLifecycleMixin
from src.server.services.computer_manager._machines import MachineLifecycleMixin
from src.server.services.computer_manager._mcp import McpSecretsMixin
from src.server.services.computer_manager._providers import ProviderMixin
from src.server.services.computer_manager._provisioning import ProvisioningMixin
from src.server.services.computer_manager._sessions import SessionCacheMixin

logger = logging.getLogger(__name__)


class ComputerManager(
    SessionCacheMixin,
    ProviderMixin,
    ProvisioningMixin,
    McpSecretsMixin,
    SessionLifecycleMixin,
    MachineLifecycleMixin,
    WorkspaceEntitlementsMixin,
):
    _instance: Optional["ComputerManager"] = None

    def __init__(
        self,
        config: AgentConfig,
        idle_timeout: int = 1800,
        cleanup_interval: int = 300,
        start_wait_timeout: float = 300.0,
        start_wait_poll_interval: float = 0.5,
        reap_stuck_after: float | None = None,
    ):
        self.config = config
        self.idle_timeout = idle_timeout
        self.cleanup_interval = cleanup_interval
        # 300s covers the worst-case archived-sandbox restore before a waiter gives up.
        self.start_wait_timeout = start_wait_timeout
        self.start_wait_poll_interval = start_wait_poll_interval
        # Must exceed start_wait_timeout: reaping a live restore clears the
        # record's pending_lazy_sync, preventing promotion and triggering
        # duplicate restarts.
        # 2x leaves headroom beyond the 60-300s restore window.
        self.reap_stuck_after = (
            reap_stuck_after if reap_stuck_after is not None else start_wait_timeout * 2
        )

        # Which machine this worker currently holds a session for, per project.
        # The two synchronous route-facing accessors need a workspace-to-machine
        # answer without a round trip; every other path resolves the binding.
        self._session_computer: Dict[str, str] = {}

        # Everything this worker holds for one machine, created on first touch
        # and dropped as a unit. Per-field maps drifted apart: a teardown path
        # had to remember all twelve, and the one it forgot outlived the
        # session. Execution context only; Postgres remains the authority.
        self._machines: Dict[str, MachineState] = {}

        # Startup materialises only its initiating project. Check siblings
        # separately, keyed by sandbox so a rebuilt machine cannot reuse an old
        # check.
        self._projects_attached: set[tuple[str, str | None]] = set()

        # asyncio holds weak task refs; retain publishes until PUBLISH completes.
        self._status_publish_tasks: set[asyncio.Task] = set()

        # Cache one classifier per backend: SDK error shapes differ and constructing
        # a provider for every failed teardown leaks HTTP clients.
        self._error_classifiers: Dict[Tuple[Optional[str], str], Any] = {}

        # Strong refs prevent asyncio task GC of a discovery probe between its
        # cancellation and the callback that unregisters it; the per-machine
        # record holds the same tasks for targeted cancellation.
        self._mcp_discovery_tasks: set[asyncio.Task] = set()

        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown = False

        logger.info(
            "ComputerManager initialized",
            extra={
                "idle_timeout": idle_timeout,
                "cleanup_interval": cleanup_interval,
            },
        )

    @classmethod
    def get_instance(
        cls,
        config: Optional[AgentConfig] = None,
        **kwargs,
    ) -> "ComputerManager":
        """Both manager entry points must share one facade instance.

        Separate instances would split the session caches and compete for machine
        ownership; always constructing WorkspaceManager makes call order irrelevant."""
        if ComputerManager._instance is None:
            if config is None:
                raise ValueError("config is required on first call to get_instance")
            from src.server.services.workspace_manager import WorkspaceManager

            ComputerManager._instance = WorkspaceManager(config, **kwargs)
        return ComputerManager._instance

    @classmethod
    def current(cls) -> Optional["ComputerManager"]:
        return ComputerManager._instance

    @classmethod
    def reset_instance(cls) -> None:
        ComputerManager._instance = None

    async def shutdown(self) -> None:
        logger.info("Shutting down ComputerManager...")

        self._shutdown = True

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

        for task in list(self._mcp_discovery_tasks):
            task.cancel()
        self._mcp_discovery_tasks.clear()

        # Close before dropping: a classifier is a provider, cached precisely
        # to avoid opening an SDK HTTP client per failed teardown, so clearing
        # the dict alone leaks every client it was holding open.
        for classifier in self._error_classifiers.values():
            try:
                await classifier.close()
            except Exception as e:
                logger.debug(f"Error classifier close failed at shutdown: {e}")
        self._error_classifiers.clear()

        # Shutdown must leave machines running: this drops what this worker
        # knows about them, one record at a time, and nothing else.
        self._machines.clear()
        self._session_computer.clear()
        self._projects_attached.clear()

        logger.info("ComputerManager shutdown complete")

    def get_stats(self) -> Dict[str, Any]:
        cached = [
            computer_id
            for computer_id, machine in self._machines.items()
            if machine.session is not None
        ]
        return {
            "cached_sessions": len(cached),
            "idle_timeout": self.idle_timeout,
            "cleanup_interval": self.cleanup_interval,
            "cached_computer_ids": cached,
        }
