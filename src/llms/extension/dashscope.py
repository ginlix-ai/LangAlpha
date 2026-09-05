"""ChatOpenAI subclass for DashScope (Qwen) Responses backends.

DashScope speaks the Responses API but uses two event shapes langchain-openai's
converter has no branch for, so both fall through its final ``else`` and vanish:
``response.reasoning_text.*`` carries the thinking tokens, and
``response.failed`` carries the reason a stream died. The bridge below rescues
both, which is the difference between a Qwen turn that shows its reasoning and
names its own failures, and one that silently returns nothing.

The two halves reach different distances, because the evidence for them does.
``response.failed`` is rescued for every Responses backend: the event type is
self-describing, no provider means anything else by it, and letting it through
returns a half-finished stream as a success. The raw-reasoning rewrite is scoped
to this client, because it guesses how one provider numbers its thought sections
and a backend emitting both reasoning families would collide on
``summary_index``. Scope is carried by a ContextVar the streaming overrides set
while upstream's body runs; widening it to another provider later is one more
read in ``_patched``.
"""

import contextvars
import logging

from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)

# The provider writes this text, and it lands in a log line and in an exception
# a user can see, so a newline in it would forge a second log entry.
_MAX_PROVIDER_TEXT = 500

# True only while this client's own Responses stream is being advanced, which is
# exactly the window in which upstream calls the converter the bridge replaces.
_in_dashscope_stream: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "in_dashscope_stream", default=False
)


def _one_line(value) -> str | None:
    if value is None:
        return None
    return str(value)[:_MAX_PROVIDER_TEXT].replace("\r", "\\r").replace("\n", "\\n")


class ChatDashScope(ChatOpenAI):
    """ChatOpenAI under a DashScope name, marking its own stream while it runs.

    DashScope accepts the standard Responses payload and replaying a prior
    turn's reasoning item back to it round-trips, so nothing about the request
    is overridden. The two overrides exist only to raise a flag while upstream's
    body runs: the converter the bridge replaces is a module global taking no
    provider argument, so this is the only handle it has for telling a Qwen
    stream apart from any other Responses stream in the process.

    They hook ``_stream``/``_astream`` and not the ``*_responses`` pair those
    dispatch to, which would read as the tighter choice and is in fact dead
    code: ``ChatOpenAI._stream`` reaches the Responses body through ``super()``,
    so an override of it on a subclass is never consulted. Nothing reports that,
    the flag simply never rises, which is why a test drives a real stream and
    asserts the converter sees the flag rather than calling the overrides here.
    """

    def _stream(self, *args, **kwargs):
        stream = iter(super()._stream(*args, **kwargs))
        try:
            while True:
                # Set around the resume and never held across the ``yield``: a
                # generator body runs in its caller's context, so a flag still
                # set at the yield would stay set for the caller, and the next
                # plain OpenAI stream opened in that same context would be read
                # as a Qwen one.
                token = _in_dashscope_stream.set(True)
                try:
                    chunk = next(stream)
                except StopIteration:
                    return
                finally:
                    _in_dashscope_stream.reset(token)
                yield chunk
        finally:
            # Reached on an abandoned stream too, where it is what propagates
            # the close into upstream's ``with`` and releases the response. Looked
            # up rather than called, because upstream returns this as an
            # ``Iterator``, and only the generator it happens to be has a close.
            close = getattr(stream, "close", None)
            if close is not None:
                close()

    async def _astream(self, *args, **kwargs):
        stream = super()._astream(*args, **kwargs).__aiter__()
        try:
            while True:
                # Same window as the sync twin above, and for the same reason.
                token = _in_dashscope_stream.set(True)
                try:
                    chunk = await stream.__anext__()
                except StopAsyncIteration:
                    return
                finally:
                    _in_dashscope_stream.reset(token)
                yield chunk
        finally:
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()


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


def _status_for_failure_code(code: str | None, message: str) -> int | None:
    """What a failed stream's error code means as an HTTP status, or None if unknown.

    The codes are the ones observed from DashScope, but this decides for every
    Responses backend, because the failure it feeds is not scoped to one. That
    holds up rather than merely not having broken yet: ``InvalidParameter`` is
    DashScope's own spelling and is not in the OpenAI SDK's error vocabulary at
    all, ``server_error`` turns permanent only on prose no other provider
    writes, and ``rate_limit_exceeded`` means 429 wherever it appears.

    Anything unrecognised returns None, which leaves the older message-regex
    guess in ``src.llms.error_classification`` exactly as it was.
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

    The hook is a module global taking no provider argument, so the wrapper is
    process-wide and each half picks its own reach inside it: ``response.failed``
    for every backend, the raw-reasoning rewrite only for a stream
    ``ChatDashScope`` has marked. One installer wrapping it once, not two, since
    a second would stack in import order and collide on the idempotence flag.
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
            "Qwen turns will stream without reasoning, and every Responses stream "
            "will report a killed stream as a truncated success"
        )
        return

    if getattr(original_fn, "_is_patched", False):
        return

    _warned_unbridgeable = False
    _warned_off_scope = False

    def _as_summary_delta(chunk):
        """Rewrite a raw-reasoning delta as the summary event upstream understands.

        Translating the event rather than the output is what keeps this from
        rotting: block shape, index bookkeeping and the v0 downgrade all stay
        upstream's. ``content_index`` becomes ``summary_index`` because both
        number the thought section, which the SSE layer reads to space sections
        apart. Reusing that number is also why the caller scopes this to one
        provider: a backend emitting both reasoning families would have the two
        share a section. The ``done`` event is deliberately still dropped, since
        the deltas already aggregate to the same text.
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
            status_code=_status_for_failure_code(code, message),
        )

    def _note_off_scope_reasoning() -> None:
        """Say once that a backend outside the scope emits raw reasoning we drop.

        The scope above is a claim about which providers emit this family, and
        upstream drops what it does not recognise in silence, so the day the
        claim is wrong the only symptom is a model that stopped showing its
        thinking. One line per process, since this fires per reasoning token.
        """
        nonlocal _warned_off_scope
        if _warned_off_scope:
            return
        _warned_off_scope = True
        logger.info(
            "Dropping raw reasoning from a Responses backend outside the bridge's "
            "scope. If this model should show its thinking, its client has to mark "
            "its own stream the way ChatDashScope does."
        )

    def _patched(chunk, *args, **kwargs):
        chunk_type = getattr(chunk, "type", None)
        if chunk_type == "response.reasoning_text.delta":
            if _in_dashscope_stream.get():
                chunk = _as_summary_delta(chunk)
            else:
                _note_off_scope_reasoning()
        elif chunk_type == "response.failed":
            _raise_if_failed(chunk)

        return original_fn(chunk, *args, **kwargs)

    _patched._is_patched = True
    base._convert_responses_chunk_to_generation_chunk = _patched
    logger.debug("Bridged the raw-reasoning and response.failed events upstream drops")
