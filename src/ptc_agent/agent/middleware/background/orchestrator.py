"""Background subagent orchestrator.

This module provides an orchestrator that wraps the agent and handles
re-invocation when background subagent tasks complete.
"""

from collections.abc import AsyncIterator
from typing import Any

import structlog
from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.background.middleware import BackgroundSubagentMiddleware

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
        input_state: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Invoke agent with automatic re-invocation for background task completion.

        This method:
        1. Invokes the agent with the input state
        2. After agent ends, waits for any pending background tasks
        3. If tasks completed, re-invokes agent with notification
        4. Returns the final result

        Args:
            input_state: Initial state for the agent
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

            logger.info(
                "Orchestrator invoking agent",
                iteration=iteration,
                has_messages="messages" in current_state,
            )

            # Invoke the agent - agent turn ends here
            result = await self.agent.ainvoke(current_state, config)

            # Single source of truth for "agent awareness" is check_and_get_notification(),
            # which also syncs completion state from underlying asyncio tasks.
            notification = await self.check_and_get_notification()
            if notification:
                logger.info(
                    "Background tasks completed, notifying agent",
                    iteration=iteration,
                )

                messages = result.get("messages", [])
                notification_message = HumanMessage(content=notification)

                # Preserve all state keys from the previous run, not just messages.
                current_state = {**result, "messages": [*messages, notification_message]}
                continue

            # If there are still pending background tasks, wait for them.
            if self.middleware.registry.has_pending_tasks():
                logger.info(
                    "Waiting for pending background tasks",
                    pending_count=self.middleware.registry.pending_count,
                    timeout=self.middleware.timeout,
                )
                await self.middleware.registry.wait_for_all(timeout=self.middleware.timeout)

                notification = await self.check_and_get_notification()
                if notification:
                    logger.info(
                        "Background tasks completed, notifying agent",
                        iteration=iteration,
                    )

                    messages = result.get("messages", [])
                    notification_message = HumanMessage(content=notification)

                    # Preserve all state keys from the previous run, not just messages.
                    current_state = {**result, "messages": [*messages, notification_message]}
                    continue

            logger.debug(
                "No pending background tasks, returning",
                iteration=iteration,
            )
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
        input_state: dict[str, Any],
        config: dict[str, Any] | None = None,
        *,
        stream_mode: str | list[str] | None = None,
        subgraphs: bool = False,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Stream agent responses with background task handling.

        Streams the agent's responses, handling background tasks between
        invocations. Fully compatible with LangGraph's streaming API.

        Args:
            input_state: Initial state for the agent
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

            logger.info(
                "Orchestrator streaming agent",
                iteration=iteration,
                stream_mode=stream_mode,
                subgraphs=subgraphs,
            )

            # Stream the agent with all parameters - agent turn ends after streaming
            async for event in self.agent.astream(current_state, config, **stream_kwargs):
                yield event

            # Single source of truth for "agent awareness" is check_and_get_notification().
            # This catches tasks that completed during the stream (i.e. no longer "pending").
            notification = await self.check_and_get_notification()
            if notification:
                logger.info(
                    "Background tasks completed after stream, notifying agent",
                    iteration=iteration,
                )

                state_snapshot = await self.agent.aget_state(config)
                state_values = state_snapshot.values
                messages = state_values.get("messages", [])

                notification_message = HumanMessage(content=notification)
                current_state = {**state_values, "messages": [*messages, notification_message]}
                continue

            # After streaming completes, check for pending background tasks
            if not self.middleware.registry.has_pending_tasks():
                return

            # If auto_wait is False, return immediately without waiting
            # CLI will handle displaying task status and collecting results later
            if not self.auto_wait:
                logger.info(
                    "Background tasks pending, returning immediately (auto_wait=False)",
                    pending_count=self.middleware.registry.pending_count,
                )
                return

            # Wait for all background tasks to complete
            logger.info(
                "Waiting for pending background tasks after stream",
                pending_count=self.middleware.registry.pending_count,
            )
            await self.middleware.registry.wait_for_all(timeout=self.middleware.timeout)

            notification = await self.check_and_get_notification()
            if not notification:
                return

            logger.info(
                "Background tasks completed after stream, notifying agent",
                iteration=iteration,
            )

            state_snapshot = await self.agent.aget_state(config)
            state_values = state_snapshot.values
            messages = state_values.get("messages", [])

            notification_message = HumanMessage(content=notification)
            current_state = {**state_values, "messages": [*messages, notification_message]}

            # NOTE: Do NOT clear registry here - agent needs to call TaskOutput()
            # to retrieve results in the next iteration.

    def _format_notification(self) -> str:
        """Format notification message for all completed background tasks.

        Returns:
            Notification string prompting agent to call TaskOutput()
        """
        completed_tasks = [
            task for task in self.middleware.registry._tasks.values()
            if task.completed
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

        # Sort by task number for consistent ordering
        sorted_tasks = sorted(tasks, key=lambda t: t.task_number)

        # Build notification message
        if len(sorted_tasks) == 1:
            task = sorted_tasks[0]
            return (
                f"Your background subagent task has completed: **{task.display_id}**.\n\n"
                f"Call `TaskOutput(task_number={task.task_number})` to see the result."
            )

        task_list = ", ".join(f"**{t.display_id}**" for t in sorted_tasks)
        return (
            f"Your background subagent tasks have completed: {task_list}.\n\n"
            f"Call `TaskOutput()` to see all results, or "
            f"`TaskOutput(task_number=N)` for a specific task."
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
                {"id": t.display_id, "type": t.subagent_type, "description": t.description[:50]}
                for t in pending
            ],
            "completed_tasks": [
                {"id": t.display_id, "type": t.subagent_type}
                for t in completed
            ],
        }

    def has_pending_tasks(self) -> bool:
        """Check if there are any pending background tasks."""
        return self.middleware.registry.has_pending_tasks()

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
