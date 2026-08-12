"""Keep reasoning payloads inside the provider lineage that produced them.

Reasoning blocks are opaque and provider-bound, but the message history is not:
a mid-thread model switch, a ``/retry`` override, or a
``ModelResilienceMiddleware`` fallback replays one provider's blocks to another
and the turn dies with a non-retryable 400. langchain cannot prevent it —
``ChatAnthropic`` stamps ``model_provider="anthropic"`` for every
Anthropic-compatible shim, so its own provenance field is not trustworthy.
Everything here is read-side: the checkpoint keeps whatever it holds and each
request is sanitized on the way out, so already-broken threads heal on their
next turn without a migration.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ExtendedModelResponse,
    ModelCallResult,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import AIMessage, AnyMessage

from src.llms.reasoning_lineage import (
    ANTHROPIC_LINEAGE,
    UNKNOWN_LINEAGE,
    lineage_for_model_id,
    lineage_for_route,
)
from src.llms.reasoning_payload import drop_if_empty, strip_reasoning

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = structlog.get_logger(__name__)

# Set on outgoing AIMessages: the provider key that actually produced them.
# Resolved through ``lineage_for_route`` on read so regrouping the manifest
# retroactively re-attributes old messages.
ORIGIN_KEY = "reasoning_origin"


@dataclass(slots=True)
class SanitizeStats:
    """Counters for one sanitization pass, at the granularity their names say."""

    msgs_stripped: int = 0
    msgs_removed: int = 0
    blocks_repaired: int = 0
    blocks_dropped: int = 0

    def __bool__(self) -> bool:
        return any(dataclasses.astuple(self))

    def as_log_fields(self) -> dict[str, int]:
        return dataclasses.asdict(self)


def _target_route(model: Any) -> str | None:
    # ``ModelRequest.model`` is a plain BaseChatModel — tools ride on
    # ``request.tools``, so there is no binding to unwrap here.
    metadata = getattr(model, "metadata", None)
    route = metadata.get("provider_route") if isinstance(metadata, dict) else None
    return route if isinstance(route, str) and route else None


def _message_lineage(msg: AIMessage) -> str:
    meta = msg.response_metadata or {}
    origin = meta.get(ORIGIN_KEY)
    if isinstance(origin, str) and origin:
        return lineage_for_route(origin)
    # Pre-stamping history: fall back to the API model id. Ambiguous ids
    # resolve to UNKNOWN, which never matches a target — i.e. strip.
    model_id = meta.get("model_name") or meta.get("model")
    return lineage_for_model_id(model_id if isinstance(model_id, str) else None)


def _origin_gate(msg: AIMessage, stats: SanitizeStats) -> AIMessage | None:
    """Gate A — drop reasoning minted by a lineage other than the target's.

    The caller establishes that the lineages differ; unattributable history
    resolves to ``UNKNOWN_LINEAGE``, which matches nothing and so fails closed.
    """
    stripped = strip_reasoning(msg)
    if stripped is not msg:
        stats.msgs_stripped += 1
    return stripped


def _structure_gate(
    msg: AIMessage, stats: SanitizeStats, *, drop_unsigned: bool
) -> AIMessage | None:
    """Gate B — repair and (optionally) drop Anthropic thinking blocks by shape.

    Repair is unconditional: langchain-anthropic streams ``signature_delta``
    into a block with no ``thinking`` field, and the API rejects that on replay.
    Dropping unsigned blocks is Anthropic-specific — a shim's own unsigned or
    UUID-signed blocks are valid to replay to that same shim. ``redacted_thinking``
    is deliberately untouched: it carries ``data``, never a ``signature``.
    """
    content = msg.content
    if not isinstance(content, list):
        return msg

    new_blocks: list[Any] = []
    repaired = dropped = 0

    for block in content:
        if not isinstance(block, dict):
            new_blocks.append(block)
            continue

        btype = block.get("type")
        if btype == "thinking":
            signature = block.get("signature")
            has_signature = isinstance(signature, str) and bool(signature)
            if has_signature and not isinstance(block.get("thinking"), str):
                new_blocks.append({**block, "thinking": ""})
                repaired += 1
                continue
            if drop_unsigned and not has_signature:
                dropped += 1
                continue
        elif btype == "reasoning" and drop_unsigned:
            # Standardized v1 form: langchain converts this back into a
            # ``thinking`` block, carrying the signature in ``extras``.
            extras = block.get("extras")
            signature = extras.get("signature") if isinstance(extras, dict) else None
            if not (isinstance(signature, str) and signature):
                dropped += 1
                continue

        new_blocks.append(block)

    if not (repaired or dropped):
        return msg
    stats.blocks_repaired += repaired
    stats.blocks_dropped += dropped
    return drop_if_empty(msg.model_copy(update={"content": new_blocks}))


def _sanitize(
    messages: list[AnyMessage], target_lineage: str
) -> tuple[list[AnyMessage], SanitizeStats]:
    """Run both gates over a history — origin first, then structure.

    Order matters only in that gate A already removes every reasoning block on a
    cross-lineage message, so gate B sees nothing left to judge there.
    """
    stats = SanitizeStats()
    # A target goes UNKNOWN only when its client skipped LLM.get_llm() — today
    # that is a subagent whose config declares a bare model string, which
    # deepagents resolves via init_chat_model. Gate A would then strip that
    # subagent's whole history every turn on no evidence, and skipping it is
    # safe: such a target only sees its own output, and a resilience fallback
    # moves it onto a stamped candidate that re-arms both gates. Repair stays.
    origin_gate = target_lineage != UNKNOWN_LINEAGE
    drop_unsigned = target_lineage == ANTHROPIC_LINEAGE

    result: list[AnyMessage] = []
    changed = False

    for msg in messages:
        if not isinstance(msg, AIMessage):
            result.append(msg)
            continue

        new_msg: AIMessage | None = msg
        if origin_gate and _message_lineage(msg) != target_lineage:
            new_msg = _origin_gate(msg, stats)
        if new_msg is not None:
            new_msg = _structure_gate(new_msg, stats, drop_unsigned=drop_unsigned)

        if new_msg is None:
            stats.msgs_removed += 1
            changed = True
            continue
        if new_msg is not msg:
            changed = True
        result.append(new_msg)

    return (result, stats) if changed else (messages, stats)


def _stamp_messages(messages: list[Any], route: str) -> list[Any]:
    return [
        m.model_copy(
            update={
                "response_metadata": {
                    **(m.response_metadata or {}),
                    ORIGIN_KEY: route,
                }
            }
        )
        if isinstance(m, AIMessage)
        else m
        for m in messages
    ]


def _stamp_origin(result: Any, route: str | None) -> Any:
    """Record the true producing provider on the returned AIMessages.

    Handles every member of langchain's ``ModelCallResult`` union. Rebuilt with
    ``dataclasses.replace`` rather than mutated, so the response object stays
    copy-on-write like the request side.
    """
    if not route:
        return result
    if isinstance(result, AIMessage):
        return _stamp_messages([result], route)[0]
    if isinstance(result, ExtendedModelResponse):
        return dataclasses.replace(
            result, model_response=_stamp_origin(result.model_response, route)
        )
    if isinstance(result, ModelResponse):
        return dataclasses.replace(
            result, result=_stamp_messages(result.result, route)
        )
    logger.warning(
        "[ReasoningCompat] unrecognized model result; origin not stamped",
        result_type=type(result).__name__,
    )
    return result


class ReasoningCompatibilityMiddleware(AgentMiddleware):
    """Sanitize inbound reasoning by lineage and shape; stamp outbound origin.

    Mounted innermost so it runs inside ``ModelResilienceMiddleware`` and sees
    the post-fallback model rather than the originally requested one.
    """

    def _prepare(self, request: ModelRequest) -> tuple[ModelRequest, str | None]:
        route = _target_route(request.model)
        target_lineage = lineage_for_route(route)
        sanitized, stats = _sanitize(request.messages, target_lineage)
        if stats:
            # Sanitizing is read-side and the checkpoint keeps the foreign
            # blocks, so one provider switch re-sanitizes on every later call
            # for the life of the thread. Only losing a whole message is an
            # anomaly; stripping reasoning off messages that survive intact is
            # the expected steady state after a switch.
            emit = logger.warning if stats.msgs_removed else logger.info
            emit(
                "[ReasoningCompat] sanitized reasoning payloads",
                target_route=route,
                target_lineage=target_lineage,
                **stats.as_log_fields(),
            )
            request = request.override(messages=sanitized)
        return request, route

    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        request, route = self._prepare(request)
        return _stamp_origin(handler(request), route)

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        request, route = self._prepare(request)
        return _stamp_origin(await handler(request), route)
