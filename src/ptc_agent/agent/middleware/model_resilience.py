"""Model resilience middleware: retry with backoff, model fallback, SSE signals.

Replaces the stock langchain ``ModelRetryMiddleware`` + ``ModelFallbackMiddleware``
pair so that:

- non-retryable errors (bad request / auth / unknown model, or a crash in
  parsing the response) skip straight to the next fallback model instead of
  burning retries on a call that cannot succeed,
- a context overflow moves on only to a fallback with a larger window, and
  otherwise goes up to compaction, the one thing that makes the request
  smaller, when the stack has compaction,
- every retry and fallback switch is emitted as a custom stream event
  (``model_retry`` / ``model_fallback``) for the client to render, and
- on total exhaustion the PRIMARY model's exception is re-raised with the full
  attempt trace attached, so the user sees the error for the model they
  configured rather than the last fallback's.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

import structlog
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelCallResult,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import AIMessage, AnyMessage
from langgraph.types import Command

from ptc_agent.agent.middleware.thinking_binding import (
    UNSENT_THINKING_KEY,
    UnsentThinkingState,
    unsent_thinking,
    without_thinking,
)

from src.llms.error_classification import (
    extract_status_code,
    is_context_overflow,
    is_mid_stream_error,
    is_retryable_error,
    is_stalled_stream,
)
from src.llms.input_transformations import INPUT_TRANSFORMATIONS, REPEATED, thinking_drops
from src.llms.provider_compaction import is_compaction_request
from src.llms.reasoning_payload import strip_all_reasoning
from src.observability.langsmith_run import stamp_run

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = structlog.get_logger(__name__)

# Attribute attached to the primary exception on total exhaustion. Read by the
# server's ``_build_error_event`` to enrich the SSE error payload with the
# primary model name and the per-model attempt trace.
RESILIENCE_TRACE_ATTR = "__model_resilience__"

_ERROR_SUMMARY_MAX_CHARS = 300

#: The ``response_metadata`` key naming the fallback that wrote an answer. Its
#: usage counts that model's tokens, so compaction measures nothing for the
#: primary by it.
FALLBACK_KEY = "model_fallback"

_as_sent: ContextVar[bool] = ContextVar("model_resilience_as_sent", default=False)

#: Drop reasons that follow the thread rather than an edit of ours: a model
#: switch, or a thread moved between accounts.
_BY_DESIGN_DROPS = frozenset({"model_binding_mismatch", "organization_binding_mismatch"})

_STRIP_ERROR_MAX_CHARS = 1000


@contextmanager
def as_sent() -> Iterator[None]:
    """Model calls made inside go to the primary as sent, retried but never
    moved to a fallback or stripped of reasoning: for an answer only that
    model reading that request can give, such as a summary of what only its
    provider can read."""
    token = _as_sent.set(True)
    try:
        yield
    finally:
        _as_sent.reset(token)


def _last_message_id(request: ModelRequest) -> str | None:
    """The newest message of the state the call was made from."""
    state = getattr(request, "state", None)
    messages = state.get("messages") if isinstance(state, dict) else None
    return getattr(messages[-1], "id", None) if messages else None


def _thread_id() -> str | None:
    try:
        from langgraph.config import get_config

        return (get_config().get("configurable") or {}).get("thread_id")
    except Exception:
        return None


def _stripped_ids(sent: list[AnyMessage], stripped: list[AnyMessage]) -> list[str]:
    """The ids of the messages in ``sent`` the strip changed or removed."""
    kept = {id(message) for message in stripped}
    return [m.id for m in sent if id(m) not in kept and getattr(m, "id", None)]


def _keep_unsent(request: ModelRequest) -> ModelRequest:
    """``request`` with the messages sent without their reasoning sent so
    again, where no compaction view already did it."""
    messages = without_thinking(request.messages, unsent_thinking(getattr(request, "state", None)))
    return request if messages is request.messages else request.override(messages=messages)


def _report_drops(response: ModelResponse, model: str, sent: list[AnyMessage]) -> None:
    """Log and stamp the earlier thinking the API dropped before answering.

    One line per answer with any, so a thread's count is a sum over its own
    log lines and runs, whichever worker served each call. A block dropped
    again because the request still sends it counts as ``repeated``, judged
    against the newest answer in ``sent`` that carries the list, so the sum
    counts each loss once.
    """
    earlier = next(
        (
            m
            for m in reversed(sent)
            if isinstance(m, AIMessage)
            and INPUT_TRANSFORMATIONS in (getattr(m, "response_metadata", None) or {})
        ),
        None,
    )
    for message in getattr(response, "result", None) or ():
        counts = thinking_drops(message, earlier)
        if not counts:
            continue
        log = logger.info if set(counts) <= _BY_DESIGN_DROPS | {REPEATED} else logger.warning
        log(
            "[ModelResilience] earlier thinking dropped by the provider",
            model=model,
            thread_id=_thread_id(),
            **counts,
        )
        stamp_run(thinking_dropped=counts)


def _summarize_error(exc: BaseException) -> str:
    text = str(exc) or type(exc).__name__
    return text[:_ERROR_SUMMARY_MAX_CHARS]


@dataclass(slots=True)
class _Recovery:
    """One more attempt on the same candidate: back off, or send a fixed request.

    ``request`` set means the payload was repaired rather than merely retried,
    which the caller records so it only escalates once per candidate.
    """

    delay: float = 0.0
    request: ModelRequest | None = None


def _model_display_name(model: Any) -> str:
    for attr in ("model", "model_name", "model_id"):
        value = getattr(model, attr, None)
        if isinstance(value, str) and value:
            return value
    return type(model).__name__


# The ``billing_type`` of a call the user's own credential pays for.
_OWN_BILLING = frozenset({"byok", "oauth"})


def _stamped(model: Any, key: str) -> str | None:
    """A fact ``LLM.get_llm()`` stamped in a client's metadata: its models.json
    name (``manifest_model``), the namespace fallback names are in, or who
    pays (``billing_type``). None for an unstamped client."""
    metadata = getattr(model, "metadata", None)
    value = metadata.get(key) if isinstance(metadata, dict) else None
    return value if isinstance(value, str) and value else None


def _window(model: Any) -> int | None:
    """The context window a client's profile declares, None when it declares none."""
    profile = getattr(model, "profile", None)
    window = profile.get("max_input_tokens") if isinstance(profile, dict) else None
    return window if isinstance(window, int) and window > 0 else None


def _mark_fallback(response: ModelResponse, name: str) -> ModelResponse:
    for msg in getattr(response, "result", None) or ():
        if isinstance(msg, AIMessage):
            msg.response_metadata = {**(msg.response_metadata or {}), FALLBACK_KEY: name}
    return response


def _distinct(fallbacks: list[tuple[str, Any]], primary: str | None) -> list[tuple[str, Any]]:
    """``fallbacks`` without ``primary`` and without a name listed twice.

    Moving to the model that just failed re-sends the same request to it, so
    the "fallback" is one more identical attempt that fails the same way.
    """
    seen = {primary} if primary else set()
    distinct = []
    for name, client in fallbacks:
        if name not in seen:
            seen.add(name)
            distinct.append((name, client))
    return distinct


def build_fallback_pairs(config: Any) -> list[tuple[str, Any]]:
    """Resolve ``(display_name, client)`` pairs for the configured fallbacks.

    Prefers the pre-resolved OAuth/BYOK-aware clients (with the parallel names
    list kept aligned by ``_resolve_fallback_clients``); falls back to
    resolving names via the platform manifest. A name with no usable key is
    skipped, as the chat path does: a fallback that cannot run must not stop
    the graph from building for the primary.
    """
    clients = getattr(config, "fallback_llm_clients", None)
    if clients is not None:
        names = getattr(config, "fallback_llm_names", None)
        if names and len(names) == len(clients):
            return list(zip(names, clients))
        return [(_model_display_name(client), client) for client in clients]

    fallback_names = getattr(config.llm, "fallback", None) or []
    if not fallback_names:
        return []
    from src.llms import get_llm_by_type

    pairs = []
    for name in fallback_names:
        try:
            pairs.append((name, get_llm_by_type(name)))
        except Exception as e:
            logger.warning(
                "[ModelResilience] Skipping unusable fallback model",
                model=name,
                error=str(e),
            )
    return pairs


@dataclass
class _AttemptRecord:
    model: str
    exc: Exception
    status_code: int | None
    attempts: int


class ModelResilienceMiddleware(AgentMiddleware):
    """Retry + fallback across models with client-visible progress events.

    One instance is shared across the main and subagent stacks, so all loop
    state is call-local.
    """

    state_schema = UnsentThinkingState

    def __init__(
        self,
        *,
        primary_name: str,
        primary_client: Any | None = None,
        fallbacks: list[tuple[str, Any]] | None = None,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True,
        compacts_overflow: bool = False,
        users_fallbacks: bool = False,
    ) -> None:
        super().__init__()
        self.primary_name = primary_name
        self.primary_client = primary_client
        # Repeats only; the model a request targets is dropped per request,
        # as the instance is shared into subagents on other models.
        self.fallbacks = _distinct(list(fallbacks or []), None)
        # Whether the fallbacks are a list the user saved (see _on_its_payer).
        self.users_fallbacks = users_fallbacks
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.jitter = jitter
        # Whether a compaction middleware outside this one takes an overflow.
        # Without one, nothing would make the request smaller, so an overflow
        # moves down the chain like any other refusal, where a fallback with a
        # longer window may take it.
        self.compacts_overflow = compacts_overflow

    # ------------------------------------------------------------------
    # Helpers

    def _primary_display_name(self, request: ModelRequest) -> str:
        # The instance is shared into subagent stacks where request.model is a
        # subagent-specific client — only claim the configured name when the
        # request actually targets the primary client.
        if self.primary_client is not None and request.model is self.primary_client:
            return self.primary_name
        if self.primary_client is None:
            return self.primary_name
        return _model_display_name(request.model)

    def _calculate_delay(self, retry_number: int) -> float:
        # Local copy of langchain.agents.middleware._retry.calculate_delay
        # (private module — not imported to survive library refactors).
        if self.backoff_factor == 0.0:
            delay = self.initial_delay
        else:
            delay = self.initial_delay * (self.backoff_factor**retry_number)
        delay = min(delay, self.max_delay)
        if self.jitter and delay > 0:
            jitter_amount = delay * 0.25
            delay += random.uniform(-jitter_amount, jitter_amount)
            delay = max(0, delay)
        return delay

    @staticmethod
    def _emit(payload: dict[str, Any]) -> None:
        try:
            from langgraph.config import get_stream_writer

            writer = get_stream_writer()
            if writer is not None:
                writer(payload)
        except Exception:
            # Stream writer is unavailable outside a streaming graph run
            # (tests, sync invocations); resilience must not depend on it.
            logger.debug("[ModelResilience] stream_writer unavailable, skipping event")

    def _emit_retry(
        self,
        model: str,
        attempt: int,
        exc: Exception,
        status_code: int | None,
        delay: float,
    ) -> None:
        # ``attempt`` counts calls that have failed so far; the retry about to
        # happen is attempt + 1 of max_retries + 1 total.
        self._emit(
            {
                "type": "model_retry",
                "model": model,
                "attempt": attempt,
                "max_retries": self.max_retries,
                "error": _summarize_error(exc),
                "status_code": status_code,
                "delay_seconds": round(delay, 2),
            }
        )

    def _emit_fallback(
        self,
        record: _AttemptRecord,
        to_model: str,
        to_client: Any,
        request: ModelRequest,
        *,
        from_is_primary: bool,
    ) -> None:
        # Unlike retries, fallbacks must survive replay: push_ui_message
        # dual-writes the record to the custom stream (live SSE) and the
        # checkpointed ``ui`` channel, which replay projects per turn. The
        # turn's records keep no step, so the record names the message the
        # call followed, which replay places the notice after.
        props = {
            "from_model": record.model,
            "to_model": to_model,
            "from_is_primary": from_is_primary,
            "error": _summarize_error(record.exc),
            "status_code": record.status_code,
            "attempts_on_from": record.attempts,
        }
        # A switch can change who pays, so the notice says who pays for the
        # model it moved to.
        if (paid_by := _stamped(to_client, "billing_type")) is not None:
            props["to_paid_by"] = paid_by
            # The provider the call went through. A model the user added on a
            # sign-in may share its name with another provider's model, so the
            # name alone does not say which plan pays.
            if (provider := _stamped(to_client, "pricing_provider")) is not None:
                props["to_provider"] = provider
        try:
            from langgraph.graph.ui import push_ui_message

            after = _last_message_id(request)
            push_ui_message(
                name="model_fallback",
                props=props,
                metadata={"after_message_id": after} if after else None,
            )
        except Exception:
            # Runtime context is unavailable outside a graph run (tests, sync
            # invocations); resilience must not depend on it.
            logger.debug("[ModelResilience] ui emit unavailable, skipping fallback event")

    def _raise_exhausted(
        self, records: list[_AttemptRecord], raised: Exception | None = None
    ) -> NoReturn:
        """Raise ``raised``, else the primary's error, with the attempt trace."""
        primary = records[0]
        exc = raised or primary.exc
        trace = {
            "model": primary.model,
            "attempted_models": [
                {
                    "model": r.model,
                    "error": _summarize_error(r.exc),
                    "status_code": r.status_code,
                    "attempts": r.attempts,
                }
                for r in records
            ],
        }
        try:
            setattr(exc, RESILIENCE_TRACE_ATTR, trace)
        except Exception:
            # Exceptions with __slots__ reject arbitrary attributes; the
            # primary error still surfaces, just without the trace.
            pass
        logger.warning(
            "[ModelResilience] All models exhausted"
            if raised is None
            else "[ModelResilience] Context overflow, left to compaction",
            primary_model=primary.model,
            attempted=[r.model for r in records],
        )
        raise exc

    def _hands_up(self, exc: Exception) -> bool:
        return self.compacts_overflow and is_context_overflow(exc)

    def _raise_overflow(self, records: list[_AttemptRecord], exc: Exception) -> NoReturn:
        """Hand an overflow up to compaction, which sits outside this middleware.

        Reached once no fallback with a larger window is left: one no larger
        is sent the same history and refuses it the same way, so the compacted
        request is the one worth sending, and it comes back through here with
        the whole chain. The overflow is what is raised even when another
        candidate failed otherwise, since it is the failure the compaction can
        act on.
        """
        self._raise_exhausted(records, exc)

    @staticmethod
    def _worth_trying(
        request: ModelRequest, candidate: tuple[str, Any | None], refused_at: float | None
    ) -> bool:
        """Whether ``candidate`` may take a request a window of ``refused_at``
        refused as too long; every candidate may before any overflow. A window
        that is not declared is not known to be larger."""
        if refused_at is None:
            return True
        window = _window(request.model if candidate[1] is None else candidate[1])
        return window is not None and window > refused_at

    def _next_tried(
        self,
        request: ModelRequest,
        candidates: list[tuple[str, Any | None]],
        index: int,
        refused_at: float | None,
    ) -> int | None:
        return next(
            (
                j
                for j in range(index + 1, len(candidates))
                if self._worth_trying(request, candidates[j], refused_at)
            ),
            None,
        )

    def _candidates(self, request: ModelRequest) -> list[tuple[str, Any | None]]:
        primary = (self._primary_display_name(request), None)
        # A compaction request asks the primary's provider for its own
        # summary; another model would answer it as a turn, and the caller
        # has a summarizer of its own to fall back to.
        if is_compaction_request(request.model_settings) or _as_sent.get():
            return [primary]
        if self.primary_client is None or request.model is self.primary_client:
            target = self.primary_name
        else:
            # A subagent on its own model, which may be one of the fallbacks.
            target = _stamped(request.model, "manifest_model")
        return [primary, *self._on_its_payer(request.model, _distinct(self.fallbacks, target))]

    def _on_its_payer(
        self, model: Any, fallbacks: list[tuple[str, Any]]
    ) -> list[tuple[str, Any]]:
        """A call the user's own key or subscription pays for moves onto the
        platform's key only through fallbacks the user saved.

        The turn's fallbacks were built for the main model, so on a turn the
        platform pays for they may run on its key; a subagent on a model of the
        user's own would otherwise fail over onto credits.
        """
        if self.users_fallbacks or _stamped(model, "billing_type") not in _OWN_BILLING:
            return fallbacks
        return [(name, client) for name, client in fallbacks
                if _stamped(client, "billing_type") != "platform"]

    def _classify(self, exc: Exception) -> tuple[int | None, bool]:
        status = extract_status_code(exc)
        return status, is_retryable_error(exc, status)

    @staticmethod
    def _escalate_reasoning(request: ModelRequest) -> ModelRequest | None:
        """Rebuild the request without any reasoning payload.

        ``None`` when there is nothing to strip, so the caller never spends a
        call re-sending an identical request. Retrying in place is safe: a
        request-validation 400 is raised while creating the stream, before any
        chunk is emitted or any token billed.

        Deliberately does *not* also disable thinking. Measured against
        api.anthropic.com and the claude-oauth route across enabled, adaptive and
        interleaved modes: replaying an assistant turn whose thinking block was
        stripped is accepted while thinking is on. Disabling it would buy nothing
        and would mask genuine config 400s — ``max_tokens`` below
        ``thinking.budget_tokens`` succeeds once thinking is off, turning a
        surfaced error into a silently thinking-free answer.
        """
        messages = strip_all_reasoning(request.messages)
        if messages is request.messages:
            return None
        return request.override(messages=messages)

    def _decide(
        self, exc: Exception, attempts: int, req: ModelRequest, *, escalated: bool
    ) -> tuple[int | None, _Recovery | None]:
        """What to do about a failed attempt, shared by the sync and async loops.

        ``None`` means give up on this candidate. A reasoning-payload 400 is the
        one non-retryable status worth another call: the provider rejected the
        history's reasoning blocks, and the same request without them can
        succeed. Detected structurally, a 400 plus "we did send reasoning",
        rather than by matching provider error prose, which drifts between API
        versions and fails silently when it does. The cost is one wasted call
        when an unrelated 400 coincides with reasoning in history, on a turn that
        is already failing anyway.

        A mid-stream failure is excluded because that repair cannot apply to it.
        Stripping reasoning answers a request rejected during validation, before
        it ran; a provider that accepted the request, opened the stream and only
        then reported failure has already judged the payload fine, and re-sending
        bills the tokens it emitted a second time. Content rejected by an input
        filter is the case that made this concrete: every attempt is refused
        identically, so the call is pure latency before the fallback that works.

        A stream that went silent past its chunk bound gets one retry, not the
        full count, whether or not its first chunk had arrived: each attempt
        waits out the whole bound before failing, so the full count held one
        candidate for most of an hour before its fallback, and stalls seen in
        prod repeated at the same point on every attempt.

        A context overflow gets nothing, with compaction or without: the same
        request is over the window on every attempt, with or without its
        reasoning, and a provider's summary read as its text instead is no
        smaller for certain. Whether the chain then moves on or hands it to
        compaction is the loop's call.
        """
        status, retryable = self._classify(exc)
        if is_context_overflow(exc):
            return status, None
        if retryable and is_stalled_stream(exc) and attempts > 1:
            retryable = False
        if retryable and attempts <= self.max_retries:
            return status, _Recovery(delay=self._calculate_delay(attempts - 1))
        if (
            not escalated
            and status == 400
            and not is_mid_stream_error(exc)
            and not _as_sent.get()
        ):
            stripped = self._escalate_reasoning(req)
            if stripped is not None:
                logger.warning(
                    "[ModelResilience] retrying without reasoning after a 400",
                    model=_model_display_name(req.model),
                    thread_id=_thread_id(),
                    error=(str(exc) or type(exc).__name__)[:_STRIP_ERROR_MAX_CHARS],
                )
                return status, _Recovery(request=stripped)
        return status, None

    @staticmethod
    def _answered(
        response: ModelResponse,
        name: str,
        index: int,
        request: ModelRequest,
        stripped: list[str],
    ) -> ModelCallResult:
        """The answer, with the strip it took kept for the thread's later
        requests (``UnsentThinkingState``). Only the request's own model
        keeps one: a fallback refusing another model's reasoning says nothing
        about what the primary reads."""
        _report_drops(response, name, request.messages)
        if index:
            return _mark_fallback(response, name)
        if not stripped:
            return response
        return ExtendedModelResponse(
            model_response=response, command=Command(update={UNSENT_THINKING_KEY: set(stripped)})
        )

    # ------------------------------------------------------------------
    # Hooks

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        # Sync twin of awrap_model_call. Unused in production (the graph is
        # only driven async) — time.sleep here would stall an event loop.
        request = _keep_unsent(request)
        candidates = self._candidates(request)
        records: list[_AttemptRecord] = []
        # The largest window that refused the request as too long (infinite
        # when it declared none), and the refusal compaction is handed.
        refused_at: float | None = None
        overflow: Exception | None = None

        for index, (name, client) in enumerate(candidates):
            if not self._worth_trying(request, candidates[index], refused_at):
                continue
            req = request if client is None else request.override(model=client)
            attempts = 0
            escalated = False
            stripped: list[str] = []
            while True:
                attempts += 1
                try:
                    response = handler(req)
                    return self._answered(response, name, index, request, stripped)
                except Exception as exc:
                    status, recovery = self._decide(
                        exc, attempts, req, escalated=escalated
                    )
                    if recovery is None:
                        records.append(_AttemptRecord(name, exc, status, attempts))
                        if self._hands_up(exc):
                            overflow = exc
                            refused_at = max(refused_at or 0, _window(req.model) or math.inf)
                        break
                    self._emit_retry(name, attempts, exc, status, recovery.delay)
                    if recovery.request is not None:
                        stripped = _stripped_ids(req.messages, recovery.request.messages)
                        req, escalated = recovery.request, True
                    elif recovery.delay > 0:
                        time.sleep(recovery.delay)
                    continue
            following = self._next_tried(request, candidates, index, refused_at)
            if following is not None:
                to_model, to_client = candidates[following]
                self._emit_fallback(
                    records[-1],
                    to_model,
                    to_client,
                    request,
                    from_is_primary=index == 0,
                )

        if overflow is not None:
            self._raise_overflow(records, overflow)
        return self._raise_exhausted(records)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        request = _keep_unsent(request)
        candidates = self._candidates(request)
        records: list[_AttemptRecord] = []
        # The largest window that refused the request as too long (infinite
        # when it declared none), and the refusal compaction is handed.
        refused_at: float | None = None
        overflow: Exception | None = None

        for index, (name, client) in enumerate(candidates):
            if not self._worth_trying(request, candidates[index], refused_at):
                continue
            req = request if client is None else request.override(model=client)
            attempts = 0
            escalated = False
            stripped: list[str] = []
            while True:
                attempts += 1
                try:
                    response = await handler(req)
                    return self._answered(response, name, index, request, stripped)
                except Exception as exc:
                    status, recovery = self._decide(
                        exc, attempts, req, escalated=escalated
                    )
                    if recovery is None:
                        records.append(_AttemptRecord(name, exc, status, attempts))
                        if self._hands_up(exc):
                            overflow = exc
                            refused_at = max(refused_at or 0, _window(req.model) or math.inf)
                        break
                    self._emit_retry(name, attempts, exc, status, recovery.delay)
                    if recovery.request is not None:
                        stripped = _stripped_ids(req.messages, recovery.request.messages)
                        req, escalated = recovery.request, True
                    elif recovery.delay > 0:
                        await asyncio.sleep(recovery.delay)
                    continue
            following = self._next_tried(request, candidates, index, refused_at)
            if following is not None:
                to_model, to_client = candidates[following]
                self._emit_fallback(
                    records[-1],
                    to_model,
                    to_client,
                    request,
                    from_is_primary=index == 0,
                )

        if overflow is not None:
            self._raise_overflow(records, overflow)
        return self._raise_exhausted(records)
