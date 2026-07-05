"""Model resilience middleware: retry with backoff, model fallback, SSE signals.

Replaces the stock langchain ``ModelRetryMiddleware`` + ``ModelFallbackMiddleware``
pair so that:

- non-retryable errors (bad request / auth / unknown model) skip straight to
  the next fallback model instead of burning retries on a call that cannot
  succeed,
- every retry and fallback switch is emitted as a custom stream event
  (``model_retry`` / ``model_fallback``) for the client to render, and
- on total exhaustion the PRIMARY model's exception is re-raised with the full
  attempt trace attached, so the user sees the error for the model they
  configured rather than the last fallback's.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

import structlog
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ModelRequest,
    ModelResponse,
)

from src.llms.error_classification import extract_status_code, is_retryable_error

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from langchain_core.messages import AIMessage

logger = structlog.get_logger(__name__)

# Attribute attached to the primary exception on total exhaustion. Read by the
# server's ``format_error_event`` to enrich the SSE error payload with the
# primary model name and the per-model attempt trace.
RESILIENCE_TRACE_ATTR = "__model_resilience__"

_ERROR_SUMMARY_MAX_CHARS = 300


def _summarize_error(exc: BaseException) -> str:
    text = str(exc) or type(exc).__name__
    return text[:_ERROR_SUMMARY_MAX_CHARS]


def _model_display_name(model: Any) -> str:
    for attr in ("model", "model_name", "model_id"):
        value = getattr(model, attr, None)
        if isinstance(value, str) and value:
            return value
    return type(model).__name__


def build_fallback_pairs(config: Any) -> list[tuple[str, Any]]:
    """Resolve ``(display_name, client)`` pairs for the configured fallbacks.

    Prefers the pre-resolved OAuth/BYOK-aware clients (with the parallel names
    list kept aligned by ``_resolve_fallback_clients``); falls back to
    resolving names via the platform manifest.
    """
    clients = getattr(config, "fallback_llm_clients", None)
    if clients:
        names = getattr(config, "fallback_llm_names", None)
        if names and len(names) == len(clients):
            return list(zip(names, clients))
        return [(_model_display_name(client), client) for client in clients]

    fallback_names = getattr(config.llm, "fallback", None) or []
    if not fallback_names:
        return []
    from src.llms import get_llm_by_type

    return [(name, get_llm_by_type(name)) for name in fallback_names]


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
    ) -> None:
        super().__init__()
        self.primary_name = primary_name
        self.primary_client = primary_client
        self.fallbacks = list(fallbacks or [])
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.jitter = jitter

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
        self, record: _AttemptRecord, to_model: str, *, from_is_primary: bool
    ) -> None:
        self._emit(
            {
                "type": "model_fallback",
                "from_model": record.model,
                "to_model": to_model,
                "from_is_primary": from_is_primary,
                "error": _summarize_error(record.exc),
                "status_code": record.status_code,
                "attempts_on_from": record.attempts,
            }
        )

    def _raise_exhausted(self, records: list[_AttemptRecord]) -> NoReturn:
        primary = records[0]
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
            setattr(primary.exc, RESILIENCE_TRACE_ATTR, trace)
        except Exception:
            # Exceptions with __slots__ reject arbitrary attributes; the
            # primary error still surfaces, just without the trace.
            pass
        logger.warning(
            "[ModelResilience] All models exhausted",
            primary_model=primary.model,
            attempted=[r.model for r in records],
        )
        raise primary.exc

    def _candidates(self, request: ModelRequest) -> list[tuple[str, Any | None]]:
        return [(self._primary_display_name(request), None), *self.fallbacks]

    def _classify(self, exc: Exception) -> tuple[int | None, bool]:
        status = extract_status_code(exc)
        return status, is_retryable_error(exc, status)

    # ------------------------------------------------------------------
    # Hooks

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelResponse | AIMessage:
        # Sync twin of awrap_model_call. Unused in production (the graph is
        # only driven async) — time.sleep here would stall an event loop.
        candidates = self._candidates(request)
        records: list[_AttemptRecord] = []

        for index, (name, client) in enumerate(candidates):
            req = request if client is None else request.override(model=client)
            attempts = 0
            while True:
                attempts += 1
                try:
                    return handler(req)
                except Exception as exc:
                    status, retryable = self._classify(exc)
                    if retryable and attempts <= self.max_retries:
                        delay = self._calculate_delay(attempts - 1)
                        self._emit_retry(name, attempts, exc, status, delay)
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    records.append(_AttemptRecord(name, exc, status, attempts))
                    break
            if index + 1 < len(candidates):
                self._emit_fallback(
                    records[-1], candidates[index + 1][0], from_is_primary=index == 0
                )

        return self._raise_exhausted(records)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse | AIMessage:
        candidates = self._candidates(request)
        records: list[_AttemptRecord] = []

        for index, (name, client) in enumerate(candidates):
            req = request if client is None else request.override(model=client)
            attempts = 0
            while True:
                attempts += 1
                try:
                    return await handler(req)
                except Exception as exc:
                    status, retryable = self._classify(exc)
                    if retryable and attempts <= self.max_retries:
                        delay = self._calculate_delay(attempts - 1)
                        self._emit_retry(name, attempts, exc, status, delay)
                        if delay > 0:
                            await asyncio.sleep(delay)
                        continue
                    records.append(_AttemptRecord(name, exc, status, attempts))
                    break
            if index + 1 < len(candidates):
                self._emit_fallback(
                    records[-1], candidates[index + 1][0], from_is_primary=index == 0
                )

        return self._raise_exhausted(records)
