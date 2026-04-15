"""Background subagent orchestrator.

This module provides an orchestrator that wraps the agent and handles
re-invocation when background subagent tasks complete.
"""

from collections.abc import AsyncIterator
from typing import Any

import structlog
from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.utils import build_message_checker

logger = structlog.get_logger(__name__)


class BackgroundSubagentOrchestrator:
    """Orchestrator that handles re-invocation after background tasks complete.

    This orchestrator wraps the agent invocation and implements the
    notification pattern:

    1. First invocation: Agent runs normally, spawning background tasks
    2. After agent ends: Orchestrator waits for pending background tasks
    3. If tasks completed: Re-invoke agent with notification message
    4. Agent calls TaskOutput() to retrieve cached results

    Usage:
        middleware = BackgroundSubagentMiddleware(timeout=60.0)
        agent = create_deep_agent(
            model=...,
            tools=...,
            middleware=[middleware],
        )
        orchestrator = BackgroundSubagentOrchestrator(agent, middleware)

        # Use orchestrator instead of agent directly
        result = await orchestrator.ainvoke(input_state)
    """

    def __init__(
        self,
        agent: Any,
        middleware: BackgroundSubagentMiddleware,
        max_iterations: int = 3,
        auto_wait: bool = False,
    ) -> None:
        """Initialize the orchestrator.

        Args:
            agent: The deepagent instance to wrap
            middleware: The BackgroundSubagentMiddleware instance
            max_iterations: Maximum number of re-invocation iterations
            auto_wait: If True, wait for background tasks to complete before returning.
                      If False (default), return immediately and let CLI handle status.
        """
        self.agent = agent
        self.middleware = middleware
        self.max_iterations = max_iterations
        self.auto_wait = auto_wait

    async def ainvoke(
        self,
        input_state: dict[str, Any] | None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke agent with automatic re-invocation for background task completion.

        This method:
        1. Invokes the agent with the input state
        2. After agent ends, waits for any pending background tasks
        3. If tasks completed, injects notification via aupdate_state and resumes
        4. Returns the final result

        Args:
            input_state: Initial state for the agent (None to resume from checkpoint)
            config: Optional config dict for the agent

        Returns:
            Final agent result
        """
        config = config or {}
        iteration = 0
        current_state = input_state
        result: dict[str, Any] = {}

        while iteration < self.max_iterations:
            iteration += 1

            # Invoke the agent - agent turn ends here
            result = await self.agent.ainvoke(current_state, config)

            # After first iteration, strip checkpoint_id so subsequent
            # aupdate_state/ainvoke calls use the latest checkpoint.
            if iteration == 1:
                config.get("configurable", {}).pop("checkpoint_id", None)

            # Single source of truth for "agent awareness" is check_and_get_notification(),
            # which also syncs completion state from underlying asyncio tasks.
            notification = await self.check_and_get_notification()
            if notification:
                logger.info(
                    "Background tasks completed, notifying agent",
                    iteration=iteration,
                )

                notification_message = HumanMessage(
                    content=notification, name="orchestrator"
                )
                await self.agent.aupdate_state(
                    config,
                    {"messages": [notification_message]},
                    as_node="__start__",
                )
                current_state = None  # Resume from updated checkpoint
                continue

            # If there are still pending background tasks, wait for them.
            if self.middleware.registry.has_pending_tasks():
                thread_id = (config.get("configurable") or {}).get("thread_id")
                checker = await build_message_checker(thread_id)
                logger.info(
                    "Waiting for pending background tasks",
                    pending_count=self.middleware.registry.pending_count,
                    timeout=self.middleware.timeout,
                )
                await self.middleware.registry.wait_for_all(
                    timeout=self.middleware.timeout,
                    message_checker=checker,
                )

                notification = await self.check_and_get_notification()
                if notification:
                    logger.info(
                        "Background tasks completed, notifying agent",
                        iteration=iteration,
                    )

                    notification_message = HumanMessage(
                        content=notification, name="orchestrator"
                    )
                    await self.agent.aupdate_state(
                        config,
                        {"messages": [notification_message]},
                        as_node="__start__",
                    )
                    current_state = None  # Resume from updated checkpoint
                    continue

            if await self._reinvoke_for_steering(config, iteration):
                current_state = None
                continue

            return result

        logger.warning(
            "Orchestrator reached max iterations",
            max_iterations=self.max_iterations,
        )
        return result

    def invoke(
        self,
        input_state: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Synchronous invoke - no background task support.

        For sync execution, background tasks are not supported.
        Falls back to direct agent invocation.

        Args:
            input_state: Initial state for the agent
            config: Optional config dict for the agent

        Returns:
            Agent result
        """
        logger.warning(
            "Sync invoke called - background tasks not supported in sync mode"
        )
        return self.agent.invoke(input_state, config or {})

    async def astream(
        self,
        input_state: dict[str, Any] | None,
        config: dict[str, Any] | None = None,
        *,
        stream_mode: str | list[str] | None = None,
        subgraphs: bool = False,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Stream agent responses with background task handling.

        Streams the agent's responses, handling background tasks between
        invocations. Uses ``aupdate_state`` + resume (``astream(None)``) for
        re-invocation so that notification messages create ``source=update``
        checkpoints instead of ``source=input``, avoiding phantom user turns.

        Args:
            input_state: Initial state for the agent (None to resume from checkpoint)
            config: Optional config dict for the agent
            stream_mode: Stream mode(s) - "values", "updates", "messages", or list
            subgraphs: Whether to include subgraph events
            **kwargs: Additional arguments passed to underlying agent.astream()

        Yields:
            Agent events/updates (format depends on stream_mode)
        """
        config = config or {}
        iteration = 0
        current_state = input_state

        # Build kwargs for underlying astream call
        stream_kwargs: dict[str, Any] = {**kwargs}
        if stream_mode is not None:
            stream_kwargs["stream_mode"] = stream_mode
        if subgraphs:
            stream_kwargs["subgraphs"] = subgraphs

        while iteration < self.max_iterations:
            iteration += 1

            # Stream the agent with all parameters - agent turn ends after streaming
            async for event in self.agent.astream(
                current_state, config, **stream_kwargs
            ):
                yield event

            # After first iteration, strip checkpoint_id so subsequent
            # aupdate_state/astream calls use the latest checkpoint.
            if iteration == 1:
                config.get("configurable", {}).pop("checkpoint_id", None)

            # Single source of truth for "agent awareness" is check_and_get_notification().
            # This catches tasks that completed during the stream (i.e. no longer "pending").
            notification = await self.check_and_get_notification()
            if notification:
                logger.info(
                    "Background tasks completed after stream, notifying agent",
                    iteration=iteration,
                )

                notification_message = HumanMessage(
                    content=notification, name="orchestrator"
                )
                await self.agent.aupdate_state(
                    config,
                    {"messages": [notification_message]},
                    as_node="__start__",
                )
                current_state = None  # Resume from updated checkpoint
                continue

            # After streaming completes, check for pending background tasks
            if not self.middleware.registry.has_pending_tasks():
                if await self._reinvoke_for_steering(config, iteration):
                    current_state = None
                    continue
                return

            # If auto_wait is False, return immediately without waiting
            # CLI will handle displaying task status and collecting results later
            if not self.auto_wait:
                if await self._reinvoke_for_steering(config, iteration):
                    current_state = None
                    continue
                logger.info(
                    "Background tasks pending, returning immediately (auto_wait=False)",
                    pending_count=self.middleware.registry.pending_count,
                )
                return

            # Wait for all background tasks to complete
            thread_id = (config.get("configurable") or {}).get("thread_id")
            checker = await build_message_checker(thread_id)
            logger.info(
                "Waiting for pending background tasks after stream",
                pending_count=self.middleware.registry.pending_count,
            )
            await self.middleware.registry.wait_for_all(
                timeout=self.middleware.timeout,
                message_checker=checker,
            )

            notification = await self.check_and_get_notification()
            if not notification:
                if await self._reinvoke_for_steering(config, iteration):
                    current_state = None
                    continue
                return

            logger.info(
                "Background tasks completed after stream, notifying agent",
                iteration=iteration,
            )

            notification_message = HumanMessage(
                content=notification, name="orchestrator"
            )
            await self.agent.aupdate_state(
                config,
                {"messages": [notification_message]},
                as_node="__start__",
            )
            current_state = None  # Resume from updated checkpoint

            # NOTE: Do NOT clear registry here - agent needs to call TaskOutput()
            # to retrieve results in the next iteration.

    def _format_notification(self) -> str:
        """Format notification message for all completed background tasks.

        Returns:
            Notification string prompting agent to call TaskOutput()
        """
        completed_tasks = [
            task for task in self.middleware.registry._tasks.values() if task.completed
        ]
        return self._format_notification_for_tasks(completed_tasks)

    def _format_notification_for_tasks(self, tasks: list) -> str:
        """Format notification message for specific tasks.

        Args:
            tasks: List of BackgroundTask objects to include in notification

        Returns:
            Notification string prompting agent to call TaskOutput()
        """
        if not tasks:
            return ""

        # Sort by task_id for consistent ordering
        sorted_tasks = sorted(tasks, key=lambda t: t.task_id)

        # Build notification message
        if len(sorted_tasks) == 1:
            task = sorted_tasks[0]
            return (
                f"Your background subagent task has completed: **{task.display_id}**.\n\n"
                f"Call `TaskOutput(task_id=\"{task.task_id}\")` to see the result."
            )

        task_list = ", ".join(f"**{t.display_id}**" for t in sorted_tasks)
        return (
            f"Your background subagent tasks have completed: {task_list}.\n\n"
            f"Call `TaskOutput()` to see all results, or "
            f"`TaskOutput(task_id=\"...\")` for a specific task."
        )

    def get_pending_tasks_status(self) -> dict[str, Any]:
        """Get status of pending background tasks for CLI display.

        Returns:
            Dict with task counts and details for display
        """
        tasks = list(self.middleware.registry._tasks.values())
        pending = [t for t in tasks if not t.completed]
        completed = [t for t in tasks if t.completed]

        return {
            "total": len(tasks),
            "pending": len(pending),
            "completed": len(completed),
            "pending_tasks": [
                {
                    "id": t.display_id,
                    "type": t.subagent_type,
                    "description": t.description[:50],
                }
                for t in pending
            ],
            "completed_tasks": [
                {"id": t.display_id, "type": t.subagent_type} for t in completed
            ],
        }

    def has_pending_tasks(self) -> bool:
        """Check if there are any pending background tasks."""
        return self.middleware.registry.has_pending_tasks()

    async def _has_pending_steering(self, config: dict[str, Any]) -> bool:
        """Check if there are pending steering messages in Redis."""
        thread_id = (config.get("configurable") or {}).get("thread_id")
        checker = await build_message_checker(thread_id)
        if checker is None:
            return False
        try:
            return await checker()
        except Exception as e:
            logger.warning("Steering check failed, skipping re-invocation", error=str(e))
            return False

    async def _reinvoke_for_steering(
        self, config: dict[str, Any], iteration: int
    ) -> bool:
        """Check for pending steering and set up re-invocation if found.

        Returns True if re-invocation was set up (caller should ``continue``),
        False otherwise (caller should proceed to return).
        """
        if not await self._has_pending_steering(config):
            return False

        logger.info(
            "Pending steering detected, re-invoking agent",
            iteration=iteration,
        )

        # Inject a minimal trigger so the graph routes to the agent node.
        # SteeringMiddleware.abefore_model() will consume the actual content.
        trigger = HumanMessage(
            content="User sent additional instructions.",
            name="orchestrator",
        )
        await self.agent.aupdate_state(
            config,
            {"messages": [trigger]},
            as_node="__start__",
        )
        return True

    async def check_and_get_notification(self) -> str | None:
        """Check for newly completed tasks and return notification if any.

        This is called by CLI before processing a new query to inject
        notifications about completed background tasks.

        Returns:
            Notification string if tasks completed, None otherwise
        """
        # Sync completion status first
        for task in self.middleware.registry._tasks.values():
            if not task.completed and task.asyncio_task and task.asyncio_task.done():
                task.completed = True
                try:
                    task.result = task.asyncio_task.result()
                except Exception as e:
                    task.error = str(e)
                    task.result = {"success": False, "error": str(e)}

        # Check for completed tasks whose results haven't been seen yet
        all_tasks = list(self.middleware.registry._tasks.values())
        unseen_tasks = [t for t in all_tasks if t.completed and not t.result_seen]

        logger.debug(
            "check_and_get_notification",
            total_tasks=len(all_tasks),
            completed=[t.display_id for t in all_tasks if t.completed],
            unseen=[t.display_id for t in unseen_tasks],
        )

        if not unseen_tasks:
            return None

        # Mark tasks as seen (via notification)
        for task in unseen_tasks:
            task.result_seen = True

        # NOTE: Do NOT clear registry here - agent needs to call TaskOutput()
        # to retrieve results. Registry is only cleared when session ends.

        return self._format_notification_for_tasks(unseen_tasks)

    def with_config(self, config: dict[str, Any]) -> "BackgroundSubagentOrchestrator":
        """Return orchestrator with config applied to underlying agent.

        Args:
            config: Config to apply

        Returns:
            New orchestrator with configured agent
        """
        configured_agent = self.agent.with_config(config)
        return BackgroundSubagentOrchestrator(
            agent=configured_agent,
            middleware=self.middleware,
            max_iterations=self.max_iterations,
            auto_wait=self.auto_wait,
        )

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the underlying agent.

        This allows the orchestrator to be used as a drop-in replacement
        for the agent in most cases.
        """
        return getattr(self.agent, name)
