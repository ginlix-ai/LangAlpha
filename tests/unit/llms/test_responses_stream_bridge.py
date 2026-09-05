"""The Responses converter bridge: what it rescues, and for whom.

Every test drives ``stream``/``astream``, the entry point production uses, over
a stubbed SDK client. Reaching in at ``_stream_responses`` instead would pass
just as happily while the scoping was dead: ``ChatOpenAI._stream`` dispatches to
the Responses body through ``super()``, so a subclass override of that method is
never consulted, and only a test that starts where callers start can tell.
"""

import asyncio
import typing

import pytest
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from langchain_openai.chat_models import base
from openai.types.responses import (
    ResponseFailedEvent,
    ResponseReasoningTextDeltaEvent,
    ResponseTextDeltaEvent,
)
from openai.types.responses.response import Response
from openai.types.responses.response_error import ResponseError

import src.llms  # noqa: F401  installs the bridge
from src.llms.extension.dashscope import (
    ChatDashScope,
    ResponsesStreamFailedError,
    _in_dashscope_stream,
    _status_for_failure_code,
)


def _reasoning_delta(text="thinking out loud", *, sequence_number=1):
    return ResponseReasoningTextDeltaEvent(
        type="response.reasoning_text.delta",
        delta=text,
        item_id="rs_1",
        output_index=0,
        content_index=0,
        sequence_number=sequence_number,
    )


def _text_delta(text="ok"):
    return ResponseTextDeltaEvent(
        type="response.output_text.delta",
        delta=text,
        item_id="msg_1",
        output_index=1,
        content_index=0,
        sequence_number=99,
        logprobs=[],
    )


def _failed(code=None, message=None):
    response = Response(
        id="resp_1",
        created_at=0,
        model="test-model",
        object="response",
        output=[],
        parallel_tool_calls=False,
        tool_choice="auto",
        tools=[],
        status="failed",
        error={"code": code, "message": message} if code else None,
    )
    return ResponseFailedEvent(
        type="response.failed", response=response, sequence_number=1
    )


class _FakeStream:
    """Stands in for the SDK's streaming context manager, sync and async."""

    def __init__(self, events):
        self._events = events
        self.exited = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.exited = True
        return False

    def __iter__(self):
        return iter(self._events)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.exited = True
        return False

    def __aiter__(self):
        return self._aiter()

    async def _aiter(self):
        for event in self._events:
            yield event


def _client(cls, events, *, is_async=False):
    """A client of ``cls`` whose SDK stream replays ``events``.

    A text delta is appended to every stream because langchain-core raises on a
    run that yields no chunk at all, and a plain client is supposed to drop the
    reasoning event that is often the only other one here.
    """
    stream = _FakeStream([*events, _text_delta()])

    class _Responses:
        def create(self, **kwargs):
            return stream

    class _AsyncResponses:
        async def create(self, **kwargs):
            return stream

    class _Root:
        responses = _AsyncResponses() if is_async else _Responses()

    llm = cls(
        model="test-model",
        api_key="fake",
        use_responses_api=True,
        output_version="responses/v1",
    )
    object.__setattr__(llm, "root_async_client" if is_async else "root_client", _Root())
    return llm, stream


def _drain(llm):
    return list(llm.stream([HumanMessage(content="hi")]))


async def _adrain(llm):
    return [c async for c in llm.astream([HumanMessage(content="hi")])]


def _reasoning_text(chunks):
    out = []
    for chunk in chunks:
        content = chunk.content if hasattr(chunk, "content") else []
        for block in content if isinstance(content, list) else []:
            if isinstance(block, dict) and block.get("type") == "reasoning":
                out += [s["text"] for s in block.get("summary", [])]
    return "".join(out)


def _openai_error_codes():
    """The SDK's declared `response.failed` codes, unwrapped from their Literal.

    Read through ``get_args`` rather than off ``.__args__`` so an SDK that wraps
    the annotation fails one parametrized case instead of the module import,
    which would take the scope tests down with it.
    """
    annotation = ResponseError.model_fields["code"].annotation
    args = typing.get_args(annotation)
    while args and not all(isinstance(a, str) for a in args):
        args = tuple(x for a in args for x in (typing.get_args(a) or ()))
    return sorted(a for a in args if isinstance(a, str))


@pytest.fixture(autouse=True)
def _no_tracing(monkeypatch):
    """Keep the tracer out of a hermetic suite.

    These are the only unit tests that drive a real ``stream``/``astream``, so
    they are the only ones that wake LangSmith if the developer's ``.env`` has
    it on. It then posts these fake-model runs to a real tenant from a
    background flush, after the socket block is lifted at teardown.
    """
    for flag in ("LANGCHAIN_TRACING_V2", "LANGSMITH_TRACING", "LANGCHAIN_TRACING"):
        monkeypatch.setenv(flag, "false")
    for key in ("LANGCHAIN_API_KEY", "LANGSMITH_API_KEY"):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture
def converter_spy(monkeypatch):
    """Records the scope flag as each raw event reaches the patched converter."""
    installed = base._convert_responses_chunk_to_generation_chunk
    seen = {}

    def _spy(chunk, *args, **kwargs):
        event = getattr(chunk, "type", None)
        seen.setdefault(event, set()).add(_in_dashscope_stream.get())
        return installed(chunk, *args, **kwargs)

    monkeypatch.setattr(base, "_convert_responses_chunk_to_generation_chunk", _spy)
    return seen


class TestScopeReachesTheConverter:
    """The flag has to be raised at the one moment the bridge reads it.

    This is the regression guard for the attachment point. Hook the wrong
    method and everything else still behaves: the client constructs, the stream
    runs, the events arrive. Only the flag stays down, and only here does that
    show up as a failure.
    """

    def test_dashscope_stream_raises_the_flag_at_the_converter(self, converter_spy):
        llm, _ = _client(ChatDashScope, [_reasoning_delta()])
        _drain(llm)
        assert converter_spy["response.reasoning_text.delta"] == {True}

    def test_plain_openai_stream_leaves_it_down(self, converter_spy):
        llm, _ = _client(ChatOpenAI, [_reasoning_delta()])
        _drain(llm)
        assert converter_spy["response.reasoning_text.delta"] == {False}

    @pytest.mark.asyncio
    async def test_async_dashscope_stream_raises_the_flag(self, converter_spy):
        llm, _ = _client(ChatDashScope, [_reasoning_delta()], is_async=True)
        await _adrain(llm)
        assert converter_spy["response.reasoning_text.delta"] == {True}


class TestReasoningScope:
    """The rewrite reaches a DashScope stream and nothing else."""

    def test_dashscope_stream_surfaces_reasoning(self):
        llm, _ = _client(ChatDashScope, [_reasoning_delta("thinking out loud")])
        assert _reasoning_text(_drain(llm)) == "thinking out loud"

    def test_plain_openai_stream_still_drops_it(self):
        # Upstream's own behavior, deliberately left alone: this event family is
        # not one OpenAI emits, so rewriting it there would only risk the
        # summary_index collision for a frame that never arrives.
        llm, _ = _client(ChatOpenAI, [_reasoning_delta()])
        assert _reasoning_text(_drain(llm)) == ""

    @pytest.mark.asyncio
    async def test_async_dashscope_stream_surfaces_reasoning(self):
        llm, _ = _client(ChatDashScope, [_reasoning_delta("async thought")], is_async=True)
        assert _reasoning_text(await _adrain(llm)) == "async thought"

    @pytest.mark.asyncio
    async def test_async_plain_openai_stream_still_drops_it(self):
        llm, _ = _client(ChatOpenAI, [_reasoning_delta()], is_async=True)
        assert _reasoning_text(await _adrain(llm)) == ""


class TestScopeDoesNotLeak:
    """The flag is held around the resume only, never across the yield.

    Setting it once for the life of the generator would be the obvious way to
    write this and would be wrong: a generator body runs in its caller's
    context, so the flag would still be raised in the consumer, and the next
    plain stream opened there would be read as a DashScope one.
    """

    def test_caller_never_observes_the_flag(self):
        llm, _ = _client(ChatDashScope, [_reasoning_delta() for _ in range(3)])
        seen = [_in_dashscope_stream.get() for _ in llm.stream([HumanMessage(content="hi")])]
        assert seen and set(seen) == {False}

    def test_a_plain_stream_interleaved_with_a_live_one_stays_unscoped(self, converter_spy):
        qwen, _ = _client(ChatDashScope, [_reasoning_delta() for _ in range(3)])
        live = qwen.stream([HumanMessage(content="hi")])
        next(live)  # left open, mid-flight

        plain, _ = _client(ChatOpenAI, [_reasoning_delta()])
        plain_chunks = _drain(plain)
        assert converter_spy["response.reasoning_text.delta"] == {True, False}
        assert _reasoning_text(plain_chunks) == ""

        live.close()

    @pytest.mark.asyncio
    async def test_concurrent_tasks_do_not_share_scope(self):
        qwen, _ = _client(ChatDashScope, [_reasoning_delta("qwen")], is_async=True)
        plain, _ = _client(ChatOpenAI, [_reasoning_delta("openai")], is_async=True)

        qwen_chunks, plain_chunks = await asyncio.gather(_adrain(qwen), _adrain(plain))
        assert _reasoning_text(qwen_chunks) == "qwen"
        assert _reasoning_text(plain_chunks) == ""

    def test_abandoning_the_stream_closes_the_response(self):
        llm, stream = _client(ChatDashScope, [_reasoning_delta() for _ in range(3)])
        live = llm.stream([HumanMessage(content="hi")])
        next(live)
        assert stream.exited is False
        live.close()
        assert stream.exited is True


class TestFailureIsRescuedEverywhere:
    """``response.failed`` is not scoped, and the OpenAI path is the reason to check.

    The event type is self-describing and every Responses backend can send it,
    so a plain client raises on it too. Left unrescued it is worse than silent:
    upstream discards the frame and the turn returns whatever the killed stream
    had already accumulated, which for an agent turn can be a half-formed tool
    call that then executes.
    """

    def test_openai_shaped_failure_raises_on_a_plain_client(self):
        llm, _ = _client(ChatOpenAI, [_failed("rate_limit_exceeded", "slow down")])
        with pytest.raises(ResponsesStreamFailedError) as exc_info:
            _drain(llm)
        assert exc_info.value.code == "rate_limit_exceeded"
        assert exc_info.value.status_code == 429
        assert "slow down" in str(exc_info.value)

    def test_an_openai_code_we_do_not_map_defers_to_the_message(self):
        llm, _ = _client(ChatOpenAI, [_failed("invalid_prompt", "rejected by policy")])
        with pytest.raises(ResponsesStreamFailedError) as exc_info:
            _drain(llm)
        assert exc_info.value.status_code is None

    @pytest.mark.asyncio
    async def test_async_plain_client_raises_too(self):
        llm, _ = _client(ChatOpenAI, [_failed("server_error", "boom")], is_async=True)
        with pytest.raises(ResponsesStreamFailedError):
            await _adrain(llm)

    def test_failure_without_an_error_payload_still_raises(self):
        llm, _ = _client(ChatDashScope, [_failed()])
        with pytest.raises(ResponsesStreamFailedError) as exc_info:
            _drain(llm)
        assert exc_info.value.code is None
        assert "no reason given" in str(exc_info.value)

    def test_partial_output_is_not_returned_as_success(self):
        llm, _ = _client(
            ChatDashScope,
            [_reasoning_delta("half a thought"), _failed("server_error", "killed")],
        )
        with pytest.raises(ResponsesStreamFailedError):
            _drain(llm)


class TestStatusMappingAssertsNothingItCannotKnow:
    """The mapper runs process-wide, so it is pinned against OpenAI's own codes.

    Every code the SDK declares must come back either unmapped, which restores
    the older message-regex guess, or with the status that code means anywhere.
    Unmapped is the honest answer for a code this provider never emits, not a
    safe one: an unknown status reads as retryable, so an OpenAI policy verdict
    arriving as ``response.failed`` spends the retry ladder before falling back.
    That is the behaviour these codes already had, and giving them a status is a
    change to the OpenAI route rather than to this one.
    """

    OPENAI_CODES = _openai_error_codes()

    @pytest.mark.parametrize("code", OPENAI_CODES)
    def test_no_openai_code_gets_a_wrong_status(self, code):
        expected = 429 if code == "rate_limit_exceeded" else None
        assert _status_for_failure_code(code, "an ordinary failure message") == expected

    def test_dashscopes_own_code_is_absent_from_that_vocabulary(self):
        # Which is why mapping it to 400 process-wide cannot mislead OpenAI.
        assert "InvalidParameter" not in self.OPENAI_CODES

    def test_the_permanent_server_error_turns_on_prose_not_on_the_code(self):
        assert _status_for_failure_code("server_error", "upstream unavailable") is None
        assert (
            _status_for_failure_code(
                "server_error", "<400> InternalError.Algo.DataInspectionFailed: blocked"
            )
            == 400
        )
