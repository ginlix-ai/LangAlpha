"""CompactionMiddleware: two-tier context management for LangGraph agents.

Compaction is a view over the checkpoint, never a rewrite of it: each model
call is sent the history after the last summary with the recorded Tier 1 cuts
applied, so the checkpoint keeps every message and a later compaction builds
on the one before.

- Tier 1: at a turn start after a long pause, when the provider's prompt
  cache has expired, large tool arguments are cut to a pointer at the
  transcript file that keeps them and stale Read results are hidden.
- Tier 2: when the context reaches its threshold, the start of the view is
  summarized (see ``compact``).

Each step is reported as a ``context_window`` event, whose ``action`` values
are wire protocol: ``token_usage`` after each model call, ``summarize``
(start, complete or error) around a summary, and ``offload`` (complete) after
Tier 1.
"""

from __future__ import annotations

import copy
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from langchain_core.exceptions import ContextOverflowError
from langchain_core.messages import AIMessage, AnyMessage
from langgraph.config import get_config, get_stream_writer
from langgraph.types import Command
from typing_extensions import override

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
)

from src.llms.token_counter import extract_token_usage

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.types import CompactionState, OffloadSettings
from ptc_agent.agent.middleware.compaction.compact import Compaction, Summarizer
from ptc_agent.agent.middleware.compaction.utils import (
    find_group_safe_cutoff,
    get_effective_messages,
    measured_tokens,
)
from ptc_agent.agent.middleware.compaction.offloading import (
    is_idle,
    offload_view,
    record_offloads,
    recorded_offloads,
    select_offloads,
)
from ptc_agent.agent.transcript import TranscriptTarget
from ptc_agent.agent.transcript.pointer import aexport_transcript, transcript_target

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.config.agent import AgentConfig

logger = logging.getLogger(__name__)


class CompactionMiddleware(AgentMiddleware):
    """Keeps an agent's context under its threshold (see the module docstring).

    Every per-run value is read from graph state on each call, never kept on
    the instance, so one instance serves concurrent runs.
    """

    state_schema = CompactionState

    def __init__(
        self,
        summarizer: Summarizer,
        *,
        token_threshold: int,
        keep_messages: int,
        offload: OffloadSettings = OffloadSettings(),
        backend: SandboxBackend | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """
        Args:
            summarizer: Writes the summaries, and counts the tokens that
                trigger one.
            token_threshold: The context size that triggers a summary.
            keep_messages: The newest messages a summary leaves out.
            offload: Tier 1 settings.
            backend: The sandbox, which holds the transcript and the
                attachments a summary saves. None for Flash, which has
                neither.
            workspace_id: The workspace the turn runs in, whose folder must
                reach the transcript before anything points at it.
        """
        super().__init__()
        self._summarizer = summarizer
        self._token_threshold = token_threshold
        self._keep_messages = keep_messages
        self._offload = offload
        self._backend = backend
        self._workspace_id = workspace_id
        # The notes folder this agent's summaries name; see with_scratchpad_notes.
        self._notes_dir: str | None = None

    @classmethod
    def for_agent(
        cls,
        config: AgentConfig,
        *,
        backend: SandboxBackend | None = None,
        workspace_id: str | None = None,
    ) -> CompactionMiddleware | None:
        """The middleware ``config.compaction`` describes, or None when it is off."""
        settings = config.compaction
        if not settings.enabled:
            return None
        return cls(
            Summarizer.for_agent(config),
            token_threshold=settings.token_threshold,
            keep_messages=settings.keep_messages,
            offload=OffloadSettings.from_config(settings),
            backend=backend,
            workspace_id=workspace_id,
        )

    # =========================================================================
    # Tier 2: every model call, summarizing first at the threshold
    # =========================================================================

    @override
    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse | ExtendedModelResponse:
        """Send the model the view, summarizing its start first once the
        context reaches the threshold, or when the model refuses the view as
        too long."""
        ensure_message_ids(request.messages)
        transcript = self._transcript_target()
        view = offload_view(
            request.messages,
            request.state,
            max_length=self._offload.max_length,
            transcript=transcript,
        )

        tokens = self._context_tokens(request, view)
        if tokens < self._token_threshold:
            try:
                return self._answered(await handler(request.override(messages=view)))
            except ContextOverflowError:
                logger.warning(
                    "[Compaction] ContextOverflowError caught, triggering emergency summarization"
                )
        else:
            logger.info("[Compaction] Triggered: %d >= %d tokens", tokens, self._token_threshold)

        cutoff = (
            find_group_safe_cutoff(view, len(view) - self._keep_messages)
            if len(view) > self._keep_messages
            else 0
        )
        if cutoff <= 0:
            # Too few messages to summarize.
            return self._answered(await handler(request.override(messages=view)))

        compaction = await self._compact(request, view, cutoff, transcript)
        response = await handler(request.override(messages=compaction.messages))
        return self._answered(response, compaction.update(request.state))

    def _context_tokens(self, request: ModelRequest, view: list[AnyMessage]) -> int:
        """What the last call measured, else a count of ``view``."""
        measured = measured_tokens(request.state)
        if measured is not None:
            return measured
        system = [request.system_message] if request.system_message is not None else []
        return self._summarizer.counter([*system, *view])

    async def _compact(
        self,
        request: ModelRequest,
        view: list[AnyMessage],
        cutoff: int,
        transcript: TranscriptTarget | None,
    ) -> Compaction:
        """Summarize ``view`` before ``cutoff``, the turn's own model being
        the fallback.

        Start is outside the try, so a start that fails opened no window. A
        cancellation still emits error before it propagates: otherwise the
        stream would keep an orphan start and its compaction window open.
        """
        self._emit_context_signal("summarize", "start")
        try:
            compaction = await self._summarizer.compact(
                request.messages,
                view,
                cutoff,
                backend=self._backend,
                workspace_id=self._workspace_id,
                transcript=transcript,
                fallback=request.model,
                notes_dir=self._notes_dir,
            )
        except BaseException as e:
            self._emit_context_signal("summarize", "error", error=str(e))
            raise
        self._emit_context_signal(
            "summarize",
            "complete",
            summary_length=len(compaction.summary.text),
            original_message_count=compaction.original_count,
            summary_text=compaction.summary.text,
            source=compaction.summary.source,
        )
        return compaction

    def _answered(
        self, response: ModelResponse, update: dict[str, Any] | None = None
    ) -> ExtendedModelResponse:
        """``response``, with ``update`` and the state the next call reads:
        the context size this call measured, and when the model answered,
        which the next turn's Tier 1 measures its pause from."""
        input_tokens, output_tokens = self._extract_token_usage(response)
        return ExtendedModelResponse(
            model_response=response,
            command=Command(
                update={
                    **(update or {}),
                    "_cached_input_tokens": input_tokens,
                    "_cached_output_tokens": output_tokens,
                    "_last_model_response_at": time.time(),
                }
            ),
        )

    def _extract_token_usage(self, response: ModelResponse) -> tuple[int, int]:
        """The (input, output) tokens the newest AI message reports, emitted
        to the frontend, or (0, 0) when none does."""
        for msg in reversed(response.result or ()):
            if not isinstance(msg, AIMessage):
                continue
            usage = extract_token_usage(msg)
            input_tokens = usage.get("input_tokens", 0)
            output_tokens = usage.get("output_tokens", 0)
            if input_tokens > 0:
                logger.debug(
                    "[Compaction] Token usage: input=%d, output=%d", input_tokens, output_tokens
                )
                self._emit_context_signal(
                    "token_usage",
                    "complete",
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    total_tokens=input_tokens + output_tokens,
                )
                return (input_tokens, output_tokens)
        return (0, 0)

    # =========================================================================
    # Tier 1: trimming old tool args and Read results after a long pause
    # =========================================================================

    @override
    async def abefore_agent(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        """Tier 1, once per turn and only when the model last answered more
        than the idle threshold ago: by then the provider's prompt cache has
        expired, so hiding part of the prefix costs no cache hit, while doing
        it mid-turn would throw a warm one away. A resume after an interrupt
        does not pass through here, since the graph picks up where it stopped.
        An argument is hidden only once the transcript holding it is saved
        where the workspace can read it, since that is then its one copy."""
        if not is_idle(state, self._offload, time.time()):
            return None
        messages = state["messages"]
        effective = get_effective_messages(messages, state.get("_summarization_event"))
        args, reads = select_offloads(effective, self._offload, recorded_offloads(state))
        if args and await aexport_transcript(
            self._backend, self._transcript_target(), messages, workspace_id=self._workspace_id
        ) is None:
            args = set()
        if args:
            self._emit_context_signal("offload", "complete", kind="args", offloaded_args=len(args))
        if reads:
            self._emit_context_signal(
                "offload", "complete", kind="reads", offloaded_reads=len(reads)
            )
        return record_offloads(state, args, reads) or None

    def with_scratchpad_notes(self, notes_dir: str) -> CompactionMiddleware:
        """This middleware, with each summary naming ``notes_dir``'s files.

        For the main agent's stack only: a subagent compacts its own run,
        which keeps no notes, so its stack holds the plain instance. A copy
        is enough because the instance keeps nothing per invocation.
        """
        main = copy.copy(self)
        main._notes_dir = notes_dir
        return main

    def _transcript_target(self) -> TranscriptTarget | None:
        """This agent's transcript, or None where no mount serves one."""
        try:
            configurable = get_config().get("configurable", {})
        except RuntimeError:
            return None
        return transcript_target(
            self._backend,
            configurable.get("thread_id"),
            str(configurable.get("checkpoint_ns") or ""),
        )

    # =========================================================================
    # Events
    # =========================================================================

    def _emit_context_signal(self, action: str, signal: str, **kwargs: Any) -> None:
        """Emit a context_window event via the stream writer.

        Args:
            action: Action discriminator ("summarize", "offload", "token_usage")
            signal: Signal type ("start", "complete", or "error")
            **kwargs: Additional payload fields (summary_length, error, etc.)
        """
        try:
            stream_writer = get_stream_writer()
            payload: dict[str, Any] = {
                "type": "context_window",
                "action": action,
                "signal": signal,
            }
            # Include checkpoint_ns for agent identification by streaming handler
            try:
                checkpoint_ns = get_config().get("configurable", {}).get("checkpoint_ns", "")
                if checkpoint_ns:
                    payload["checkpoint_ns"] = checkpoint_ns
            except RuntimeError:
                pass
            payload.update(kwargs)
            stream_writer(payload)
            if signal == "error":
                logger.warning(
                    "[Compaction] Emitted %s error signal: %s", action, kwargs.get("error")
                )
            else:
                logger.debug("[Compaction] Emitted %s %s signal", action, signal)
        except Exception as e:
            logger.debug("Could not emit context_window %s/%s signal: %s", action, signal, e)
