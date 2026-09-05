"""ChatOpenAI subclass for DashScope (Qwen) Responses backends.

DashScope speaks the Responses API but uses two event shapes langchain-openai's
converter has no branch for, so both fall through its final ``else`` and vanish:
``response.reasoning_text.*`` carries the thinking tokens, and
``response.failed`` carries the reason a stream died. The bridge below rescues
both, which is the difference between a Qwen turn that shows its reasoning and
names its own failures, and one that silently returns nothing.
"""

import logging

from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)

# The provider writes this text, and it lands in a log line and in an exception
# a user can see, so a newline in it would forge a second log entry.
_MAX_PROVIDER_TEXT = 500


def _one_line(value) -> str | None:
    if value is None:
        return None
    return str(value)[:_MAX_PROVIDER_TEXT].replace("\r", "\\r").replace("\n", "\\n")


class ChatDashScope(ChatOpenAI):
    """ChatOpenAI under a DashScope name, carrying no request-side behavior.

    DashScope accepts the standard Responses payload, and replaying a prior
    turn's reasoning item back to it round-trips, so there is nothing to
    override yet. The subclass earns its place by giving the route a name: the
    manifest's ``sdk`` says DashScope, the factory hands back a class that says
    DashScope, and anything provider-specific that shows up later has a home
    that is not an ``if`` in the shared OpenAI path. It does not scope the
    bridge below, which is process-wide.
    """


class ResponsesStreamFailedError(RuntimeError):
    """A Responses stream ended in ``response.failed``, quoting the provider.

    ``status_code`` is what the adapter concluded the failure is, and it is
    read ahead of the message: ``src.llms.error_classification`` prefers this
    attribute and only falls back to regex-matching a bare 4xx/5xx out of the
    text. Leaving it ``None`` is therefore how an adapter says "I do not know",
    which restores exactly that older guess rather than asserting a wrong one.
    """

    # Reported inside an established stream, so the model-resilience middleware
    # skips the reasoning-strip escalation: that repair answers a request being
    # rejected before it runs, which by definition is not what happened here.
    mid_stream = True

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


# Message tokens that make a coarse ``server_error`` permanent. DashScope
# reports an input-filter block with the same code it uses for real transient
# faults, so the code alone would send a turn through the full retry ladder for
# a verdict that will never change.
_TERMINAL_SERVER_ERROR_TOKENS = ("DataInspectionFailed",)


def _status_for(code: str | None, message: str) -> int | None:
    """What a DashScope failure code means, as an HTTP status, or None if unknown.

    Neither field decides this alone: the code says ``server_error`` for a
    permanent content block, and the message carries no status at all for an
    unsupported model. Returning None leaves the older message-regex guess in
    place, so an unrecognised code behaves exactly as it did before.
    """
    if code == "InvalidParameter":
        return 400
    if code == "server_error":
        if any(token in message for token in _TERMINAL_SERVER_ERROR_TOKENS):
            return 400
        return None
    # Not observed from this provider, but a rate limit must never be decided by
    # a number that happens to sit in its own message ("quota is 400 tokens").
    if code == "rate_limit_exceeded":
        return 429
    return None


def _install_responses_stream_bridge() -> None:
    """Rescue the two event families langchain-openai's Responses converter drops.

    The hook is a module global, so this is process-wide and keyed on event
    type rather than on provider: any Responses backend that emits these two
    frames gets the same treatment, which is what makes it a fix to the
    converter's blind spot rather than a DashScope special case. One installer
    wrapping it once, not two, since a second would stack in import order and
    collide on the idempotence flag.
    """
    try:
        import langchain_openai.chat_models.base as base
        from openai.types.responses import ResponseReasoningSummaryTextDeltaEvent

        original_fn = base._convert_responses_chunk_to_generation_chunk
    except (ImportError, AttributeError):
        # langchain_openai internals moved. Degrade to no bridge rather than
        # breaking module import, which would take down all of DashScope.
        logger.warning(
            "Responses stream bridge not installed: "
            "langchain_openai._convert_responses_chunk_to_generation_chunk is gone; "
            "Qwen turns will stream without reasoning and fail without a reason"
        )
        return

    if getattr(original_fn, "_is_patched", False):
        return

    _warned_unbridgeable = False

    def _as_summary_delta(chunk):
        """Rewrite a raw-reasoning delta as the summary event upstream understands.

        Translating the event rather than the output is what keeps this from
        rotting: block shape, index bookkeeping and the v0 downgrade all stay
        upstream's. ``content_index`` becomes ``summary_index`` because both
        number the thought section, which the SSE layer reads to space sections
        apart. The ``done`` event is deliberately still dropped, since the
        deltas already aggregate to the same text.
        """
        try:
            return ResponseReasoningSummaryTextDeltaEvent(
                type="response.reasoning_summary_text.delta",
                delta=chunk.delta,
                item_id=chunk.item_id,
                output_index=chunk.output_index,
                summary_index=chunk.content_index,
                sequence_number=chunk.sequence_number,
            )
        except Exception as e:
            # A shape we cannot translate is still better delivered as the
            # original chunk, which upstream drops, than as a dead turn. Warn
            # once: this fires per reasoning token, so a drifted shape would
            # otherwise write one record per token of every thought.
            nonlocal _warned_unbridgeable
            if not _warned_unbridgeable:
                _warned_unbridgeable = True
                logger.warning("Could not bridge a reasoning delta: %s", e)
            return chunk

    def _raise_if_failed(chunk) -> None:
        """Surface a mid-stream failure that both layers below us let through.

        The OpenAI SDK only raises for a top-level ``error`` key, and
        ``response.failed`` nests its own at ``response.error``, so the frame
        arrives here intact and langchain then discards it. The event type is
        the whole signal and a missing ``error`` payload does not soften it:
        returning here would hand back whatever the stream had already
        accumulated as a successful generation, which for an agent turn can be
        a tool call it then executes.
        """
        error = getattr(getattr(chunk, "response", None), "error", None)
        code = _one_line(getattr(error, "code", None))
        message = _one_line(getattr(error, "message", None)) or "no reason given"
        logger.warning("Responses stream failed: %s: %s", code, message)
        raise ResponsesStreamFailedError(
            f"Responses stream failed ({code}): {message}",
            code=code,
            status_code=_status_for(code, message),
        )

    def _patched(chunk, *args, **kwargs):
        chunk_type = getattr(chunk, "type", None)
        if chunk_type == "response.reasoning_text.delta":
            chunk = _as_summary_delta(chunk)
        elif chunk_type == "response.failed":
            _raise_if_failed(chunk)

        return original_fn(chunk, *args, **kwargs)

    _patched._is_patched = True
    base._convert_responses_chunk_to_generation_chunk = _patched
    logger.debug("Bridged the raw-reasoning and response.failed events upstream drops")
