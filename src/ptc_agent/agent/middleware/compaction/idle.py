"""Idle compaction on the agent's side (see ``rebuild.TurnStartRebuilds.idle``):
the clock a main-agent Claude turn's final answer leaves in the checkpoint,
and where the summary of the thread idle after it cuts, read off that clock.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, AnyMessage
from langchain_core.utils.function_calling import convert_to_openai_tool

from src.config.features import is_feature_enabled_system
from src.llms.cache_markers import written_ttl_seconds
from src.llms.provider_compaction import native_kind

from ptc_agent.agent.middleware.compaction.between_turns import IdleSkipped
from ptc_agent.agent.middleware.compaction.measure import is_claude
from ptc_agent.agent.middleware.compaction.rebuild import IDLE_TTL_SECONDS, RebuildPlan, TurnStartRebuilds, cache_ttl
from ptc_agent.agent.middleware.compaction.types import IdleClock
from ptc_agent.agent.middleware.model_resilience import FALLBACK_KEY

#: The deployment switch (``src.config.features``), off by default: off, no
#: answer is stamped or leaves a clock, and nothing arms or fires.
FEATURE = "idle_compaction"


def enabled() -> bool:
    return is_feature_enabled_system(FEATURE)


#: The ``response_metadata`` key holding when the request that produced a
#: message started, epoch seconds: the life of a cache entry it wrote or read
#: counts from there, not from the answer's end.
REQUEST_STARTED_KEY = "request_started_at"


def stamp_request_start(message: AIMessage, started: float) -> None:
    message.response_metadata = {**(message.response_metadata or {}), REQUEST_STARTED_KEY: started}


def request_started_at(message: Any) -> float | None:
    """When the request that produced ``message`` started, or None for one
    that is not an answer or was saved before the stamp."""
    value = (getattr(message, "response_metadata", None) or {}).get(REQUEST_STARTED_KEY)
    return float(value) if isinstance(value, (int, float)) else None


def stamping(
    handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
) -> Callable[[ModelRequest], Awaitable[ModelResponse]]:
    """``handler``, stamping each answer with when its request started, with
    the feature on."""
    if not enabled():
        return handler

    async def call(request: ModelRequest) -> ModelResponse:
        started = time.time()
        response = await handler(request)
        answer = next((m for m in reversed(response.result or ()) if isinstance(m, AIMessage)), None)
        if answer is not None:
            stamp_request_start(answer, started)
        return response

    return call


#: How long before the entry an idle compaction reads expires it is given up
#: rather than started: the summary may take the whole compaction timeout,
#: and on an expired entry it would write the history at the write rate.
IDLE_DEADLINE_MARGIN_S = 300

#: The model fields that key a request's prompt cache, beside its tools.
_CACHE_KEYED_FIELDS = (
    "model",
    "betas",
    "thinking",
    "output_config",
    "reasoning_effort",
    "model_kwargs",
    "context_management",
    "inference_geo",
)


def request_digest(request: ModelRequest) -> str:
    """sha256 of what keys a request's prompt cache besides its system and
    messages: its tools, which the API caches ahead of the system, and the
    model and settings it is sent with (thinking, effort, fast mode, betas).
    A call between turns that differs from the turn's in any of them reads
    none of the prefix the turn cached."""
    model = getattr(request.model, "bound", request.model)
    keyed = {
        "tools": [tool if isinstance(tool, dict) else convert_to_openai_tool(tool) for tool in request.tools],
        "settings": request.model_settings,
        "model": {name: getattr(model, name, None) for name in _CACHE_KEYED_FIELDS},
        "bound": getattr(request.model, "kwargs", None),
    }
    return hashlib.sha256(json.dumps(keyed, sort_keys=True, default=str).encode()).hexdigest()


def idle_clock(
    request: ModelRequest,
    answer: AIMessage,
    view: list[AnyMessage] | None,
    tokens: int,
    *,
    rebuilds: TurnStartRebuilds,
    native: bool,
    tail_cut: Callable[[list[AnyMessage]], int],
) -> IdleClock | None:
    """What a turn's final answer leaves for an idle compaction after it, or
    None: only the main agent's, from Claude on an hour's cache, where its
    provider compacts natively. The entry a summary keeping the tail would
    read (the one at ``tail_cut``) is placed only from ``idle_tokens``, where such
    a summary is due by ``tokens``, the context the answer leaves, and only
    for a call sent the view as it was (``view``). A turn whose send asked to
    keep the full context places none: the user chose the whole context over
    that summary, which would otherwise run before their next send could
    choose again. A deploy's rebuild, which a send never skips, still arms."""
    requested_at = request_started_at(answer)
    if (
        not native
        or requested_at is None
        or answer.tool_calls
        or native_kind(request.model) != "anthropic"
        or not is_claude(request.model)
        or FALLBACK_KEY in (answer.response_metadata or {})
        or (written_ttl_seconds(answer.usage_metadata) or cache_ttl(view or [], request.model)) != IDLE_TTL_SECONDS
    ):
        return None
    tail = None
    if view is not None and not rebuilds.keep_full_context and 0 < rebuilds.idle_tokens <= tokens:
        whole = [*view, answer]
        cut = tail_cut(whole)
        tail = request_started_at(whole[cut]) if cut > 0 else None
    return {
        "requested_at": requested_at,
        "tail_requested_at": tail,
        "digest": request_digest(request),
    }


def idle_cut(request: ModelRequest, view: Sequence[AnyMessage], plan: RebuildPlan, tail_cut: int, now: float) -> int:
    """Where the summary of an idle thread cuts ``view``: all of it for a
    rebuild (``plan.cut``), else at ``tail_cut``. Only on the request the turn
    cached, and only while the entry the summary reads lives, with the margin
    it needs: a rebuild reads the one the last answer's request wrote, a
    summary keeping the tail the one at its cut.

    Raises:
        IdleSkipped: Naming why nothing is summarized.
    """
    if (request.state.get("_idle_clock") or {}).get("digest") != request_digest(request):
        raise IdleSkipped("request_changed")
    cut = plan.cut or tail_cut
    if cut <= 0:
        raise IdleSkipped("no_cut")
    read = view[cut] if not plan.cut else next(m for m in reversed(view) if isinstance(m, AIMessage))
    entry = request_started_at(read)
    if entry is None or now > entry + IDLE_TTL_SECONDS - IDLE_DEADLINE_MARGIN_S:
        raise IdleSkipped("expired")
    return cut
