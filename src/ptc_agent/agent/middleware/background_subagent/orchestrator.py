"""Background subagent orchestrator.

This module provides an orchestrator that wraps the agent and handles
re-invocation when background subagent tasks complete.
"""

from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import Any

import structlog
from langchain_core.messages import HumanMessage

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
# Module-attribute binding (not a direct symbol import) so a single patch of
# utils.build_message_checker governs every caller.
from ptc_agent.agent.middleware.background_subagent import utils
from ptc_agent.agent.middleware.background_subagent.utils import (
    MessageChecker,
    config_own_run_id,
)
from ptc_agent.agent.transcript.classify import ORCHESTRATOR_SOURCE, reentry_trigger

logger = structlog.get_logger(__name__)


def _is_interrupt(event: Any) -> bool:
    """Whether a streamed event carries an interrupt. The payload comes last
    in every shape ``stream_mode`` and ``subgraphs`` give an event."""
    payload = event[-1] if isinstance(event, tuple) and event else event
    return isinstance(payload, dict) and "__interrupt__" in payload


class BackgroundSubagentOrchestrator:
    """Orchestrator that handles re-invocation after background tasks complete.

    This orchestrator wraps the agent invocation and implements the
    notification pattern:

    1. First invocation: Agent runs normally, spawning background tasks
    2. After agent ends: Orchestrator waits for pending background tasks
    3. If tasks completed: Re-invoke agent with notification message
    4. Agent calls TaskOutput() to retrieve cached results

    Usage:
        middleware = BackgroundSubagentMiddleware(timeout=60.0, subagent_types=["research"])
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
        alert_senders: frozenset[str] = frozenset(),
    ) -> None:
        """Initialize the orchestrator.

        Args:
            agent: The deepagent instance to wrap
            middleware: The BackgroundSubagentMiddleware instance
            max_iterations: Maximum number of re-invocation iterations
            auto_wait: If True, wait for background tasks to complete before returning.
                      If False (default), return immediately and let CLI handle status.
            alert_senders: The notices nobody sent (``REENTRY_TRIGGERS``)
                      whose feature this run's owner has on, so one waiting
                      on the thread is a notice its steering takes in rather
                      than closes unread.
        """
        self.agent = agent
        self.middleware = middleware
        self.max_iterations = max_iterations
        self.auto_wait = auto_wait
        self.alert_senders = alert_senders

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

                await self._inject(config, notification)
                current_state = None  # Resume from updated checkpoint
                continue

            # If there are still pending background tasks, wait for them.
            if self.middleware.registry.has_pending_tasks():
                checker = await self._message_checker(
                    config, last_pass=iteration >= self.max_iterations
                )
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

                    await self._inject(config, notification)
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

        That update drops any interrupt the pass ended on, so re-invoking
        supersedes it. Interrupt events are therefore held to the end of each
        pass and only the last pass's go out: the ones the checkpoint still
        holds pending, which replay shows and a resume can answer.

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

            # Stream the agent with all parameters - agent turn ends after streaming.
            # Closing this stream must close the agent's too, and in this task:
            # a bare ``async for`` leaves it to the garbage collector, so its
            # model calls and tools would outlive the close the caller awaits.
            pauses: list[Any] = []
            async with aclosing(
                self.agent.astream(current_state, config, **stream_kwargs)
            ) as events:
                async for event in events:
                    if _is_interrupt(event):
                        pauses.append(event)
                    else:
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

                await self._inject(config, notification)
                current_state = None  # Resume from updated checkpoint
                continue

            # After streaming completes, check for pending background tasks
            if not self.middleware.registry.has_pending_tasks():
                if await self._reinvoke_for_steering(config, iteration):
                    current_state = None
                    continue
                break

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
                break

            # Wait for all background tasks to complete
            checker = await self._message_checker(
                config, last_pass=iteration >= self.max_iterations
            )
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
                break

            logger.info(
                "Background tasks completed after stream, notifying agent",
                iteration=iteration,
            )

            await self._inject(config, notification)
            current_state = None  # Resume from updated checkpoint

            # NOTE: Do NOT clear registry here - agent needs to call TaskOutput()
            # to retrieve results in the next iteration.
        else:
            # Every pass ended by injecting, the last included, so none of
            # their interrupts is still pending.
            return

        for event in pauses:
            yield event

    async def _inject(self, config: dict[str, Any], content: str) -> None:
        """Append an orchestrator-authored human turn as if from ``__start__``."""
        message = HumanMessage(
            content=content,
            name="orchestrator",
            additional_kwargs={"lc_source": ORCHESTRATOR_SOURCE},
        )
        await self.agent.aupdate_state(
            config,
            {"messages": ensure_message_ids([message])},
            as_node="__start__",
        )

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
            if task.terminal_status == "cancelled":
                # Announcing a stop as finished work invites the agent to go
                # fetch a result that does not exist, and to offer to redo work
                # the user just stopped.
                return (
                    f"Your background subagent task was stopped before it "
                    f"finished: **{task.display_id}**. No result was produced, "
                    f"and the stop was deliberate — report it and await "
                    f"instruction rather than re-dispatching."
                )
            return (
                f"Your background subagent task has completed: **{task.display_id}**.\n\n"
                f"Call `TaskOutput(task_id=\"{task.task_id}\")` to see the result."
            )

        task_list = ", ".join(
            f"**{t.display_id}**"
            + (" (stopped)" if t.terminal_status == "cancelled" else "")
            for t in sorted_tasks
        )
        return (
            f"Your background subagent tasks have completed: {task_list}.\n\n"
            f"Call `TaskOutput(task_id=\"...\")` for each task's result. A task "
            f"marked stopped produced no result; do not re-dispatch it unless asked."
        )

    def get_pending_tasks_status(self) -> dict[str, Any]:
        """Get status of pending background tasks for CLI display.

        Returns:
            Dict with task counts and details for display
        """
        tasks = list(self.middleware.registry._tasks.values())
        pending = [t for t in tasks if not t.completed]
        settled = [t for t in tasks if t.completed]

        return {
            "total": len(tasks),
            "pending": len(pending),
            "completed": len(settled),
            "pending_tasks": [
                {
                    "id": t.display_id,
                    "type": t.subagent_type,
                    "description": t.description[:50],
                }
                for t in pending
            ],
            "completed_tasks": [
                {
                    "id": t.display_id,
                    "type": t.subagent_type,
                    "status": t.terminal_status,
                }
                for t in settled
            ],
        }

    def has_pending_tasks(self) -> bool:
        """Check if there are any pending background tasks."""
        return self.middleware.registry.has_pending_tasks()

    async def _message_checker(
        self, config: dict[str, Any], *, last_pass: bool = False
    ) -> MessageChecker | None:
        """What ends a wait or re-invokes the agent.

        A market target's alert or a wakeup's notice counts only when this
        run's steering takes it in: the owner has the feature on and the run
        has subagents, as otherwise the re-run would hold nothing to read.
        Nor on the last pass, which re-invokes nothing: an alert ending that
        wait would end the turn with its tasks still running, and each result
        would come back as a turn of its own. Nor while the run waits on the
        user's answer: re-invoking supersedes the question for a notice
        nobody sent, and the turn that resumes it takes the alert in. A steer
        keeps ending it, as it always has.
        """
        thread_id = (config.get("configurable") or {}).get("thread_id")
        alert_senders = (
            self.alert_senders
            if self.middleware.enabled and not last_pass
            else frozenset()
        )
        if alert_senders and await self._awaits_answer(config):
            alert_senders = frozenset()
        return await utils.build_message_checker(
            thread_id,
            own_run_id=config_own_run_id(config),
            alert_senders=alert_senders,
        )

    async def _awaits_answer(self, config: dict[str, Any]) -> bool:
        """Whether the run stopped on an interrupt. A failed read counts as
        stopped, which only leaves an alert parked for the next turn."""
        try:
            state = await self.agent.aget_state(config)
        except Exception as e:
            logger.warning("Interrupt check failed, holding alerts", error=str(e))
            return True
        return bool(state.interrupts)

    async def _pending_steer_senders(self, config: dict[str, Any]) -> list[str]:
        """Who sent the steers waiting that this run would take in."""
        checker = await self._message_checker(config)
        if checker is None:
            return []
        try:
            return await checker()
        except Exception as e:
            logger.warning("Steering check failed, skipping re-invocation", error=str(e))
            return []

    async def _reinvoke_for_steering(
        self, config: dict[str, Any], iteration: int
    ) -> bool:
        """Check for pending steering and set up re-invocation if found.

        Returns True if re-invocation was set up (caller should ``continue``),
        False otherwise (caller should proceed to return).
        """
        # On the last pass there is no re-run to read it, and a trigger left in
        # the checkpoint would sit ahead of the turn the steer becomes.
        if iteration >= self.max_iterations:
            return False
        senders = await self._pending_steer_senders(config)
        if not senders:
            return False

        logger.info(
            "Pending steering detected, re-invoking agent",
            iteration=iteration,
        )

        # Inject a minimal trigger so the graph routes to the agent node.
        # SteeringMiddleware.abefore_model() will consume the actual content.
        await self._inject(config, reentry_trigger(senders))
        return True

    async def check_and_get_notification(self) -> str | None:
        """Announce settled LEDGER-LESS tasks the agent hasn't seen.

        Runs after every agent invoke/stream (server and library alike), but
        announces only tasks with no ledger identity — ledgered runs belong
        to the durable outbox notifier, whose claim-time arbitration this
        memory-gated check cannot participate in.

        Returns:
            Notification string if tasks completed, None otherwise
        """
        # A disabled middleware means this turn has no TaskOutput tool (e.g.
        # a disable_subagents notification turn): never point the agent at a
        # tool it doesn't have. The shared per-thread registry may still hold
        # completed tasks from other turns — they're not this turn's to
        # announce.
        if not self.middleware.enabled:
            return None

        # Sync completion status first
        for task in self.middleware.registry._tasks.values():
            if not task.completed and task.asyncio_task and task.asyncio_task.done():
                task.adopt_writer_outcome(task.asyncio_task)

        # Completed tasks whose results the agent hasn't seen. Ledgered runs
        # (task_run_id set at admission) are excluded: the durable outbox
        # notifier owns them, arbitrating at claim time against
        # result_delivered_at — a second, memory-gated announcer here can only
        # duplicate it. The local sweep announces exactly the ledger-less
        # tasks (direct library embedding, or a failed admission), where this
        # process is the only notifier there is. Workflow-owned children are
        # excluded too: their results flow to the driver, not the main agent —
        # only the run task itself notifies.
        all_tasks = list(self.middleware.registry._tasks.values())
        unseen_tasks = [
            t
            for t in all_tasks
            if t.completed
            and not t.result_seen
            and t.is_turn_visible
            and not t.task_run_id
        ]

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
            alert_senders=self.alert_senders,
        )

    def __getattr__(self, name: str) -> Any:
        """Proxy attribute access to the underlying agent.

        This allows the orchestrator to be used as a drop-in replacement
        for the agent in most cases.
        """
        return getattr(self.agent, name)
