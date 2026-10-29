"""CompactionMiddleware: two-tier context management for LangGraph agents.

Compaction is a view over the checkpoint, never a rewrite of it: each model
call is sent the history after the last summary with the recorded Tier 1 cuts
applied, so the checkpoint keeps every message and a later compaction builds
on the one before.

- Tier 1: at a turn start after a long pause, when the provider's prompt
  cache has expired, large tool arguments are cut to a pointer at the
  transcript file that keeps them and stale Read results are hidden, never
  ahead of thinking a later call is checked against (see ``select_offloads``).
  Off unless the operator turns it on (see ``OffloadSettings.idle_seconds``).
- Tier 2: when the context reaches its threshold, or the model refuses it
  as too long, the start of the view is summarized (see ``compact``), all of
  it at a turn start while the thread's system prompt is stale, so the call
  after it can take the new one (see ``rebuild_pending``), and
  the attachments in what it keeps are taken out until it fits; on Claude,
  whose kept turns are sent as they were, the summary takes them instead
  (see ``fitting.Fit``); and between turns when the user asks (see
  ``between_turns``).
- The window: on the main agent, at each pass through ``before_agent``, the
  runs a summary stands in for are trimmed from the checkpoint once the
  server holds them (see ``window``).

Each step is reported as a ``context_window`` event, whose ``action`` values
are wire protocol: ``token_usage`` after each model call, ``summarize``
(start, complete or error) around a summary, and ``offload`` (complete) after
Tier 1.
"""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import functools
import logging
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from langchain_core.messages import AIMessage, AnyMessage, HumanMessage
from langchain_core.runnables.config import var_child_runnable_config
from langgraph.config import get_config, get_stream_writer
from langgraph.constants import TAG_NOSTREAM
from langgraph.types import Command
from typing_extensions import override

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelRequest,
    ModelResponse,
)

from src.llms.error_classification import is_context_overflow
from src.llms.provider_compaction import (
    compaction_settings,
    is_opaque,
    kept_user_texts,
    native_answer,
    native_kind,
)

from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.middleware.compaction.between_turns import Ask, HandOff, asked
from ptc_agent.agent.middleware.compaction.types import CompactionState, OffloadSettings
from ptc_agent.agent.middleware.compaction.fitting import Fit, reported_usage
from ptc_agent.agent.middleware.compaction.compact import (
    NOTHING_TO_COMPACT,
    Compaction,
    NativeAnswer,
    Own,
    Summarizer,
)
from ptc_agent.agent.middleware.compaction.measure import (
    ContextReading,
    Estimate,
    context_tokens,
    estimate_for,
    keep_cutoff,
    keep_tokens_for,
    stamp_context_growth,
)
from ptc_agent.agent.middleware.compaction.summarize import (
    is_other_model,
    summary_text,
)
from ptc_agent.agent.middleware.compaction.summary_request import (
    finished_summary,
    keeps_turns,
    summary_prompt,
)
from ptc_agent.agent.middleware.compaction.utils import (
    get_effective_messages,
    handed_off,
    provider_cutoff,
    read_ahead,
    stamp_compaction_summary,
    stamp_context_threshold,
    target_cost,
    turn_start_cut,
    with_summary_text,
)
from ptc_agent.agent.middleware.runtime_context.frozen_prompt import rebuild_pending
from ptc_agent.agent.middleware.compaction.offloading import (
    is_idle,
    offload_view,
    record_offloads,
    recorded_offloads,
    select_offloads,
)
from ptc_agent.agent.middleware.compaction.window import (
    WindowCarry,
    WindowCoverage,
    trim,
    window_cut,
)
from ptc_agent.agent.middleware.model_resilience import as_sent
from ptc_agent.agent.middleware.reasoning_compat import ORIGIN_KEY
from ptc_agent.agent.transcript import TranscriptTarget, Window
from ptc_agent.agent.transcript.classify import human_kind, is_summary_message
from ptc_agent.agent.transcript.identity import DIGEST_VERSION
from ptc_agent.agent.transcript.pointer import (
    aexport_transcript,
    shell_reads_transcripts,
    transcript_target,
)

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.config.agent import AgentConfig

logger = logging.getLogger(__name__)

# The coverage read sits in front of a turn. It walks the thread's turn
# boundaries, which takes tens of milliseconds; past this the trim waits for
# the next pass rather than hold the turn.
_COVERAGE_TIMEOUT_S = 5.0


@contextmanager
def _unstreamed() -> Iterator[None]:
    """Tag the model calls made inside ``nostream``. The model node calls the
    model with the config it finds here, so the tag reaches the call, and the
    callbacks that bill and trace it still see it."""
    ambient = var_child_runnable_config.get() or {}
    token = var_child_runnable_config.set(
        {**ambient, "tags": [*(ambient.get("tags") or ()), TAG_NOSTREAM]}
    )
    try:
        yield
    finally:
        var_child_runnable_config.reset(token)


def _user_texts(messages: Sequence[AnyMessage]) -> list[str]:
    """What the user wrote in ``messages``, oldest first, with what an
    earlier OpenAI summary among them kept: the messages Codex keeps beside
    its item, none of the rows the runtime stamps."""
    texts: list[str] = []
    for message in messages:
        if not isinstance(message, HumanMessage):
            continue
        if is_summary_message(message):
            texts.extend(kept_user_texts(message))
        elif human_kind(message) in ("plain", "steering"):
            texts.append(message.text)
    return texts


def _last_ai(response: ModelResponse) -> AIMessage | None:
    return next((m for m in reversed(response.result or ()) if isinstance(m, AIMessage)), None)


def _log_usage(call: str, message: AIMessage | None) -> None:
    """Log what a call the turn's stream never shows read from the cache."""
    usage = (message.usage_metadata if message is not None else None) or {}
    details = usage.get("input_token_details") or {}
    logger.info(
        "[Compaction] %s: %s in, %s cached, %s out",
        call,
        usage.get("input_tokens"),
        details.get("cache_read"),
        usage.get("output_tokens"),
    )


def _summary_fields(compaction: Compaction) -> dict[str, Any]:
    """The summarize complete signal's payload, one shape for the live signal
    and the stamp replay reads it back from."""
    return {
        "summary_length": len(compaction.summary.text),
        "original_message_count": compaction.original_count,
        "summary_text": compaction.summary.text,
        "source": compaction.summary.source,
    }


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
        keep_tokens: int,
        claude_keep_tokens: int | None = None,
        offload: OffloadSettings = OffloadSettings(),
        backend: SandboxBackend | None = None,
        workspace_id: str | None = None,
        native: bool = True,
        self_summary: bool = True,
        receives: Callable[[Any], Callable[[Any], bool]] | None = None,
        page_tokens: Callable[[Any], int | None] | None = None,
    ) -> None:
        """
        Args:
            summarizer: Writes the summaries, and counts the tokens that
                trigger one.
            token_threshold: The context size that triggers a summary.
            keep_tokens: How much of the newest history a summary leaves
                out, word for word (see ``keep_cutoff``).
            claude_keep_tokens: The same for a Claude model, whose tokens
                hold less text; None keeps ``keep_tokens``.
            offload: Tier 1 settings.
            backend: The sandbox, which holds the transcript and the
                attachments a summary saves. None for a build without a
                sandbox, which has neither.
            workspace_id: The workspace the turn runs in, whose folder must
                reach the transcript before anything points at it.
            native: Whether a provider that can compact the conversation
                itself does.
            self_summary: Whether the agent's model writes a summary from its
                own request (see ``_writes_own``).
            receives: Which attachment blocks a call to a model is sent as
                they are (``MultimodalStripMiddleware.receives``). The strip
                sits inside this middleware and turns the rest into short
                placeholders, so only these count toward the trigger and the
                fit. None counts every attachment.
            page_tokens: What a call to a model is charged for a PDF page
                (``MultimodalStripMiddleware.pdf_page_tokens``), or None
                where that is not known, which counts ``PDF_PAGE_TOKENS``.
        """
        super().__init__()
        self._summarizer = summarizer
        self._token_threshold = token_threshold
        self._keep_tokens = keep_tokens
        self._claude_keep_tokens = (
            keep_tokens if claude_keep_tokens is None else claude_keep_tokens
        )
        self._offload = offload
        self._backend = backend
        self._workspace_id = workspace_id
        self._native = native
        self._self_summary_on = self_summary
        self._receives = receives
        self._page_tokens = page_tokens
        # The notes folder this agent's summaries name; see with_scratchpad_notes.
        self._notes_dir: str | None = None
        # The main agent's window; see with_window.
        self._coverage: WindowCoverage | None = None
        self._carries: tuple[WindowCarry, ...] = ()

    @classmethod
    def for_agent(
        cls,
        config: AgentConfig,
        *,
        backend: SandboxBackend | None = None,
        workspace_id: str | None = None,
        receives: Callable[[Any], Callable[[Any], bool]] | None = None,
        page_tokens: Callable[[Any], int | None] | None = None,
    ) -> CompactionMiddleware | None:
        """The middleware ``config.compaction`` describes, or None when it is off.

        The ``provider_compaction`` feature is read here rather than with the
        user's settings, so a build that resolves no user, a system insight,
        still honors an operator who turned it off.
        """
        settings = config.compaction
        if not settings.enabled:
            return None
        return cls(
            Summarizer.for_agent(config),
            token_threshold=settings.token_threshold,
            keep_tokens=settings.keep_tokens,
            claude_keep_tokens=settings.claude_keep_tokens,
            offload=OffloadSettings.from_config(settings),
            backend=backend,
            workspace_id=workspace_id,
            native=settings.native and config.feature_enabled("provider_compaction"),
            self_summary=settings.self_summary,
            receives=receives,
            page_tokens=page_tokens,
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
        too long, and taking attachments out of what is kept until it fits.
        A call made between turns (``between_turns``) is answered with what
        it asks instead."""
        ensure_message_ids(request.messages)
        view = await self.view(request.messages, request.state)
        await read_ahead(view)
        between = asked()
        if between is not None:
            between.answer = await self._between_turns(request, handler, view, between.ask)
            between.answered = True
            return ModelResponse(result=[])

        system = [request.system_message] if request.system_message is not None else []
        estimate = self._estimate(request.model)
        reading = await context_tokens(request.state, view, estimate, system)
        tokens = reading.tokens
        refusal: Exception | None = None
        if tokens < self._token_threshold:
            try:
                response = await handler(request.override(messages=view))
                return self._answered(response, reading=reading)
            except Exception as exc:
                # Matched across providers, most of which LangChain does not
                # classify as an overflow.
                if not is_context_overflow(exc):
                    raise
                logger.warning("[Compaction] Context overflow, summarizing: %s", exc)
                refusal = exc
        else:
            logger.info("[Compaction] Triggered: %d >= %d tokens", tokens, self._token_threshold)

        # A stale system prompt stays while any thinking it bound is in view,
        # so at a turn start the summary keeps none: its own request still
        # carries the old prompt and tools, which the thinking it summarizes
        # is checked against, and the call after it takes the new ones.
        rebuild = turn_start_cut(view) if rebuild_pending(request.state) else 0
        cutoff = rebuild or self._cutoff(view, request.model)
        fit = Fit(
            self._token_threshold,
            system,
            estimate,
            lossless=self._native and native_kind(request.model) == "anthropic",
        )
        if rebuild:
            logger.info("[Compaction] Rebuild: the summary keeps no turns")
        elif fit.lossless:
            cutoff = fit.cut(view, cutoff, tokens, refused=refusal is not None)
        if cutoff <= 0:
            # All of it is the kept tail: the attachments are all that can go.
            fitted = await fit.send(request, handler, view, refusal=refusal)
            return self._answered(
                fitted.response, fitted.update(request.state), spared=fitted.spared
            )

        compaction = await self._summarize(request, handler, view, cutoff, rebuild=bool(rebuild))
        note = compaction.note_row()
        # A refusal of the view says nothing of what the summary made room
        # for, so its messages are fitted by estimate like any others. The
        # note goes into history ahead of the answer produced behind it.
        fitted = await fit.send(
            request, handler, [*compaction.messages, note] if note is not None else compaction.messages
        )
        if note is not None:
            fitted = fitted._replace(
                response=dataclasses.replace(fitted.response, result=[note, *fitted.response.result])
            )
        self._stamp_summary(fitted.response, compaction)
        update = compaction.update(request.state)
        return self._answered(
            fitted.response, {**update, **fitted.update(update)}, spared=fitted.spared
        )

    async def _summarize(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        view: list[AnyMessage],
        cutoff: int,
        *,
        rebuild: bool = False,
    ) -> Compaction:
        """Summarize ``view`` before ``cutoff``, first by the agent's own
        model: its provider compacting the conversation where it can, else the
        model writing the summary itself (see ``_writes_own``). A provider
        that could compact but is told not to leaves it to the summarizer, as
        the setting says. ``rebuild`` is True for a summary that keeps no
        turn, whose cut is the user's new message: Claude's cut stays there
        rather than move back onto the answer before it. Whether turns stay
        after the summary is read from what it keeps (``keeps_turns``)."""
        own: Own | None = None
        kind = native_kind(request.model)
        # Claude reads its block only where the kept turns can follow it.
        cut = provider_cutoff(view, cutoff) if kind == "anthropic" and not rebuild else cutoff
        if kind is not None and self._native and cut > 0:
            cutoff = cut
            own = functools.partial(
                self._provider_summary, request, handler, notes=self._notes_dir is not None
            )
        elif kind is None and self._writes_own(request):
            own = functools.partial(
                self._self_summary, request, handler, notes=self._notes_dir is not None
            )
        return await self._compact(request, view, cutoff, self._transcript_target(), own)

    def _writes_own(self, request: ModelRequest) -> bool:
        """Whether the agent's model writes the summary from its own request.

        It reads the conversation from its provider's cache, with the system
        prompt, tools and reasoning the transcript summarizer leaves out, and
        the summarizer stays the fallback. Its request is about half again the
        transcript's size, so on a cache gone cold it costs that much more;
        that is taken for the warm case, the common one. A model picked for
        compaction is the user's choice, and writes it instead.
        """
        return self._self_summary_on and not is_other_model(
            self._summarizer.model, request.model
        )

    def _cost(self, model: Any) -> Callable[[Any], int] | None:
        """What an attachment costs on a call to ``model`` (``target_cost``)."""
        return target_cost(
            self._receives(model) if self._receives is not None else None,
            self._page_tokens(model) if self._page_tokens is not None else None,
        )

    def _estimate(self, model: Any) -> Estimate:
        """How what no call to ``model`` measured is counted (``counter_for``),
        its attachments as the model is charged for them."""
        return estimate_for(model, self._summarizer.counter, self._cost(model))

    def _cutoff(self, view: list[AnyMessage], model: Any) -> int:
        """Where a summary of ``view`` for ``model`` ends (``keep_cutoff``)."""
        tokens = keep_tokens_for(
            model, self._keep_tokens, self._claude_keep_tokens, self._token_threshold
        )
        return keep_cutoff(view, tokens=tokens, estimate=self._estimate(model))

    async def _between_turns(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        view: list[AnyMessage],
        ask: Ask,
    ) -> Compaction | HumanMessage | None:
        """What a call between turns asks for (see ``between_turns``)."""
        if isinstance(ask, HandOff):
            return await self._hand_off(request, handler, view, notes=self._notes_dir is not None)
        cutoff = self._cutoff(view, request.model)
        if cutoff <= 0:
            raise ValueError(NOTHING_TO_COMPACT)
        return await self._summarize(request, handler, view, cutoff)

    @staticmethod
    async def _hand_off(
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        view: list[AnyMessage],
        *,
        notes: bool,
    ) -> HumanMessage | None:
        """The view's opening summary written out by the agent's model, when
        it is one only that model's provider can read, else None.

        The summary goes out as the provider's own answer (``adopt``, inside
        ``handler``), from the provider's cache, and the model writes it out
        as text. Tools stay bound so the prefix is the one the cache holds;
        none may be called. Only this model reading that answer can write it,
        so a failure is never moved to a fallback model or retried without it.
        """
        head = view[0] if view and is_summary_message(view[0]) else None
        if not isinstance(head, HumanMessage) or not is_opaque(head) or handed_off(head):
            return None
        prompt = summary_prompt("handoff", keeps_turns(view[1:]), notes)
        with as_sent():
            response = await handler(
                request.override(
                    messages=[head, HumanMessage(prompt)],
                    tool_choice="none",
                )
            )
        message = _last_ai(response)
        _log_usage("hand-off", message)
        text = message.text.strip() if message is not None and not message.tool_calls else ""
        written = with_summary_text(head, text) if text else None
        if written is None:
            raise ValueError("the model wrote no hand-off summary")
        return written

    async def _compact(
        self,
        request: ModelRequest,
        view: list[AnyMessage],
        cutoff: int,
        transcript: TranscriptTarget | None,
        own: Own | None = None,
    ) -> Compaction:
        """Summarize ``view`` before ``cutoff``, by ``own`` when given,
        this agent's model being the fallback (see ``Summarizer.compact``).

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
                window=Window.of(request.state),
                # Its own action: a client built before the count reads any
                # summarize signal but start as the summary's end.
                progress=lambda tokens: self._emit_context_signal(
                    "summary_progress", "progress", summary_tokens=tokens
                ),
                own=own,
            )
        except BaseException as e:
            self._emit_context_signal("summarize", "error", error=str(e))
            raise
        self._emit_context_signal("summarize", "complete", **_summary_fields(compaction))
        return compaction

    @staticmethod
    async def _provider_summary(
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        to_summarize: list[AnyMessage],
        kept: bool,
        *,
        notes: bool,
    ) -> NativeAnswer:
        """The agent's provider compacting ``to_summarize`` itself, with the
        summary prompt in place of its own where it takes one (Claude), the
        one for a summary that keeps no turn when ``kept`` is False.

        Sent through ``handler`` so the request is the conversation's own:
        the same system prompt, tools and thinking, which its cache and the
        thinking in the kept turns are checked against. It goes to this model
        as sent: the kept thinking stays valid only through a compaction its
        own model writes, and the summarizer is the fallback here. It stays
        out of the turn's message stream, where its reply would read as the
        answer. OpenAI's goes after the user's own messages it summarized.
        """
        settings = compaction_settings(
            request.model,
            request.model_settings,
            summary_prompt("native", kept, notes),
        )
        with as_sent(), _unstreamed():
            response = await handler(
                request.override(messages=to_summarize, model_settings=settings)
            )
        message = _last_ai(response)
        _log_usage("provider summary", message)
        answer = native_answer(message, _user_texts(to_summarize))
        if message is None or answer is None:
            stop = (message.response_metadata or {}).get("stop_reason") if message else None
            raise ValueError(f"no compaction (stop reason {stop})")
        return NativeAnswer(answer, (message.response_metadata or {}).get(ORIGIN_KEY))

    @staticmethod
    async def _self_summary(
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
        to_summarize: list[AnyMessage],
        kept: bool,
        *,
        notes: bool,
    ) -> str:
        """The agent's model writing the summary of ``to_summarize`` as text,
        asked in the last user message of its own request.

        Sent through ``handler`` with the turn's tools and settings unchanged,
        so the request opens on the prefix the provider has cached; none may
        be called. It goes to this model as sent: a fallback model would read
        none of it from a cache, and the summarizer is the fallback here. It
        stays out of the turn's message stream. A summary that stops before
        its closing line was cut off, and counts as none.
        """
        with as_sent(), _unstreamed():
            response = await handler(
                request.override(
                    messages=[
                        *to_summarize,
                        HumanMessage(summary_prompt("self", kept, notes)),
                    ]
                )
            )
        message = _last_ai(response)
        _log_usage("self summary", message)
        if message is None or message.tool_calls:
            raise ValueError("no reply" if message is None else "called a tool")
        text = finished_summary(summary_text(message))
        if text is None:
            stop = (message.response_metadata or {}).get("stop_reason")
            raise ValueError(f"no closing line (stop reason {stop})")
        return text

    @staticmethod
    def _stamp_summary(response: ModelResponse, compaction: Compaction) -> None:
        """Stamp the summary on the response that answered from it, for
        replay to show where it fired: live, its signal comes right before
        this response streams."""
        for msg in reversed(response.result or ()):
            if isinstance(msg, AIMessage):
                stamp_compaction_summary(
                    msg, compaction.event["summary_message"].id, _summary_fields(compaction)
                )
                return

    def _answered(
        self,
        response: ModelResponse,
        update: dict[str, Any] | None = None,
        *,
        spared: int = 0,
        reading: ContextReading | None = None,
    ) -> ExtendedModelResponse:
        """``response``, with ``update`` and the state the next call reads:
        the context size this call measured, less the ``spared`` tokens of
        attachments ``update`` takes out of the next one, and when the model
        answered, which the next turn's Tier 1 measures its pause from.
        ``reading`` is the trigger's reading of the view this call was sent
        as it was, which its growth is stamped from; a call that was sent a
        summary or a fitted view has none."""
        input_tokens, output_tokens = self._extract_token_usage(response, reading)
        return ExtendedModelResponse(
            model_response=response,
            command=Command(
                update={
                    **(update or {}),
                    "_cached_input_tokens": max(input_tokens - spared, 0),
                    "_cached_output_tokens": output_tokens,
                    "_last_model_response_at": time.time(),
                }
            ),
        )

    def _extract_token_usage(
        self, response: ModelResponse, reading: ContextReading | None = None
    ) -> tuple[int, int]:
        """The (input, output) tokens the newest AI message reports, emitted
        to the frontend with the threshold they fill and stamped with it for
        replay, and with its growth from ``reading``, or (0, 0) when none
        does."""
        usage = reported_usage(response)
        if usage is None:
            return (0, 0)
        msg, input_tokens, output_tokens = usage
        logger.debug("[Compaction] Token usage: input=%d, output=%d", input_tokens, output_tokens)
        stamp_context_threshold(msg, self._token_threshold)
        if reading is not None:
            stamp_context_growth(msg, reading, input_tokens)
        self._emit_context_signal(
            "token_usage",
            "complete",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
            threshold=self._token_threshold,
        )
        return (input_tokens, output_tokens)

    # =========================================================================
    # Tier 1: trimming old tool args and Read results after a long pause
    # =========================================================================

    @override
    async def abefore_agent(self, state: Any, runtime: Any) -> dict[str, Any] | None:
        """Tier 1 when the turn starts after a long pause, then the window's
        trim. A resume after an interrupt does not pass through here, since
        the graph picks up where it stopped; an orchestrator re-entry does."""
        update = await self._offload_idle(state)
        trimmed = await self._trim({**state, **update})
        return {**update, **(trimmed or {})} or None

    async def _offload_idle(self, state: Mapping[str, Any]) -> dict[str, Any]:
        """Tier 1, once per turn and only when the model last answered more
        than the idle threshold ago: by then the provider's prompt cache has
        expired, so hiding part of the prefix costs no cache hit, while doing
        it mid-turn would throw a warm one away. An argument is hidden only
        once the transcript holding it is saved where the workspace can read
        it, since that is then its one copy."""
        if not is_idle(state, self._offload, time.time()):
            return {}
        messages = state["messages"]
        effective = get_effective_messages(messages, state.get("_summarization_event"))
        args, reads = select_offloads(effective, self._offload, recorded_offloads(state))
        if args and await aexport_transcript(
            self._backend,
            self._transcript_target(),
            messages,
            workspace_id=self._workspace_id,
            window=Window.of(state),
        ) is None:
            args = set()
        if args:
            self._emit_context_signal("offload", "complete", kind="args", offloaded_args=len(args))
        if reads:
            self._emit_context_signal(
                "offload", "complete", kind="reads", offloaded_reads=len(reads)
            )
        return record_offloads(state, args, reads)

    async def _trim(self, state: Mapping[str, Any]) -> dict[str, Any] | None:
        """The window's trim (see ``window``), or None while it may not:
        without the server to ask (any stack but the main agent's, see
        ``with_window``), before a summary with an anchor, or while the
        server's slices do not hold exactly the messages it would drop, in
        which case the update keeps the refusal.

        A refusal is kept in the checkpoint with the anchor and cut it was
        for, and the digest it was checked under, and not asked again until
        one of them moves: a thread whose slices do not match pays for the
        check once per summary, not per turn, and one backfilled since trims
        after its next summary.
        """
        if self._coverage is None:
            return None
        messages = state["messages"]
        event = state.get("_summarization_event")
        cut = window_cut(messages, event)
        if cut <= 0:
            return None
        asked = {"anchor": event["anchor_message_id"], "cut": cut, "digest": DIGEST_VERSION}
        if state.get("_window_refused") == asked:
            return None
        try:
            async with asyncio.timeout(_COVERAGE_TIMEOUT_S):
                head = await self._coverage(Window.of(state), messages[:cut])
        except Exception:  # noqa: BLE001 - an unanswered check only delays the trim
            logger.warning("[Compaction] window coverage unread", exc_info=True)
            return None
        if head is None:
            logger.info(
                "[Compaction] window waits for the next summary: slices do not "
                "hold the %d messages before the cut",
                cut,
            )
            return {"_window_refused": asked}
        logger.info(
            "[Compaction] window trimmed %d messages, runs up to %d", cut, head.runs
        )
        return trim(state, cut, self._carries, head=head)

    def with_window(
        self, coverage: WindowCoverage, carries: Sequence[WindowCarry] = ()
    ) -> CompactionMiddleware:
        """This middleware, trimming the main agent's checkpoint to its window.

        ``coverage`` asks the server whether turn slices hold the runs a trim
        would drop; ``carries`` keep what other readers counted in them. For
        the main agent's stack only: a subagent's run is bounded, and its
        transcript reads its namespace's whole history.
        """
        main = copy.copy(self)
        main._coverage = coverage
        main._carries = tuple(carries)
        return main

    def with_scratchpad_notes(self, notes_dir: str) -> CompactionMiddleware:
        """This middleware, with each summary naming ``notes_dir``'s files.

        For the main agent's stack only: a subagent compacts its own run,
        which keeps no notes, so its stack holds the plain instance. A copy
        is enough because the instance keeps nothing per invocation.
        """
        main = copy.copy(self)
        main._notes_dir = notes_dir
        return main

    async def view(self, messages: list[AnyMessage], state: Mapping[str, Any]) -> list[AnyMessage]:
        """What this agent's model is sent of ``messages``: what the summary
        trigger counts, and so what the scratchpad's fill counts too."""
        return await offload_view(
            messages,
            state,
            max_length=self._offload.max_length,
            transcript=self._transcript_target(),
            shell=shell_reads_transcripts(self._backend),
        )

    def _transcript_target(self) -> TranscriptTarget | None:
        """This agent's transcript, or None where nothing serves one."""
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
            action: Action discriminator ("summarize", "summary_progress",
                "offload", "token_usage")
            signal: Signal type ("start", "progress", "complete", or "error")
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
