"""Tests for ModelResilienceMiddleware — retry, fallback, events, error trace."""

import pytest

from ptc_agent.agent.middleware.model_resilience import (
    RESILIENCE_TRACE_ATTR,
    ModelResilienceMiddleware,
    build_fallback_pairs,
)


class _FakeModel:
    def __init__(self, name, thinking=None):
        self.model = name
        self.thinking = thinking

    def model_copy(self, *, update):
        return _FakeModel(self.model, **update)


class _FakeRequest:
    """Duck-typed stand-in for langchain's ModelRequest."""

    def __init__(self, model, messages=None):
        self.model = model
        self.messages = messages if messages is not None else []

    def override(self, *, model=None, messages=None):
        return _FakeRequest(
            model if model is not None else self.model,
            messages if messages is not None else self.messages,
        )


def _status_error(message, status=None):
    exc = Exception(message)
    if status is not None:
        exc.status_code = status
    return exc


def _make_middleware(primary_client, fallbacks=(), **kwargs):
    kwargs.setdefault("initial_delay", 0.0)  # no real sleeps in tests
    kwargs.setdefault("jitter", False)
    return ModelResilienceMiddleware(
        primary_name="primary-model",
        primary_client=primary_client,
        fallbacks=list(fallbacks),
        **kwargs,
    )


@pytest.fixture
def events(monkeypatch):
    """Capture custom stream events emitted via get_stream_writer().

    Retry events go through the raw writer; fallback events go through
    ``push_ui_message``, which resolves the writer + config from
    ``langgraph.graph.ui``'s namespace and needs ``CONFIG_KEY_SEND`` for its
    state write — patch both so ui records land in the same capture list.
    """
    from langgraph._internal._constants import CONFIG_KEY_SEND
    from langgraph.constants import CONF

    captured = []
    monkeypatch.setattr(
        "langgraph.config.get_stream_writer", lambda: captured.append
    )
    monkeypatch.setattr(
        "langgraph.graph.ui.get_stream_writer", lambda: captured.append
    )
    monkeypatch.setattr(
        "langgraph.graph.ui.get_config",
        lambda: {CONF: {CONFIG_KEY_SEND: lambda writes: None}},
    )
    return captured


def _fallback_props(events):
    """Props of captured model_fallback ui records, in emission order."""
    return [
        e["props"]
        for e in events
        if e.get("type") == "ui" and e.get("name") == "model_fallback"
    ]


class TestSuccessPath:
    @pytest.mark.asyncio
    async def test_success_first_try_no_events(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            return "ok"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok"
        assert len(calls) == 1
        assert calls[0].model is client
        assert events == []


class TestRetryBehavior:
    @pytest.mark.asyncio
    async def test_transient_error_retries_then_succeeds(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        attempts = []

        async def handler(req):
            attempts.append(req)
            if len(attempts) < 3:
                raise _status_error("upstream 500", status=500)
            return "ok"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok"
        assert len(attempts) == 3
        assert [e["type"] for e in events] == ["model_retry", "model_retry"]
        assert events[0]["model"] == "primary-model"
        assert events[0]["attempt"] == 1
        assert events[0]["max_retries"] == 3
        assert events[0]["status_code"] == 500
        assert events[1]["attempt"] == 2

    @pytest.mark.asyncio
    async def test_transient_error_max_attempts_per_model(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client, max_retries=2)
        calls = []

        async def handler(req):
            calls.append(req)
            raise _status_error("upstream 503", status=503)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(client), handler)
        # 1 initial + 2 retries
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_non_retryable_error_skips_retries(self, events):
        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback_client)])
        calls = []

        async def handler(req):
            calls.append(req.model)
            if req.model is client:
                raise _status_error("Error code: 404 - model not found", status=404)
            return "ok-from-fallback"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok-from-fallback"
        # Exactly ONE attempt on the primary (no retries on a 404), then fallback
        assert calls == [client, fallback_client]
        fallbacks = _fallback_props(events)
        assert len(fallbacks) == 1 and len(events) == 1
        assert fallbacks[0]["from_model"] == "primary-model"
        assert fallbacks[0]["to_model"] == "fallback-a"
        assert fallbacks[0]["from_is_primary"] is True
        assert fallbacks[0]["attempts_on_from"] == 1
        assert fallbacks[0]["status_code"] == 404


class TestFallbackChain:
    @pytest.mark.asyncio
    async def test_second_fallback_emits_non_primary_switch(self, events):
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        fb_b = _FakeModel("fallback-b")
        mw = _make_middleware(
            client, fallbacks=[("fallback-a", fb_a), ("fallback-b", fb_b)]
        )

        async def handler(req):
            if req.model is fb_b:
                return "ok"
            raise _status_error("bad request", status=400)

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok"
        switches = _fallback_props(events)
        assert [(s["from_model"], s["to_model"], s["from_is_primary"]) for s in switches] == [
            ("primary-model", "fallback-a", True),
            ("fallback-a", "fallback-b", False),
        ]

    @pytest.mark.asyncio
    async def test_total_exhaustion_raises_primary_exception_with_trace(self, events):
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb_a)])
        primary_exc = _status_error("Error calling model 'primary-model': 404", status=404)
        fallback_exc = _status_error("fallback param mismatch", status=400)

        async def handler(req):
            raise primary_exc if req.model is client else fallback_exc

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)

        # The PRIMARY model's exception surfaces, not the last fallback's
        assert exc_info.value is primary_exc
        trace = getattr(exc_info.value, RESILIENCE_TRACE_ATTR)
        assert trace["model"] == "primary-model"
        assert [a["model"] for a in trace["attempted_models"]] == [
            "primary-model",
            "fallback-a",
        ]
        assert trace["attempted_models"][0]["status_code"] == 404
        assert trace["attempted_models"][0]["attempts"] == 1
        assert trace["attempted_models"][1]["error"] == "fallback param mismatch"

    @pytest.mark.asyncio
    async def test_no_fallbacks_configured_raises_primary(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        exc = _status_error("unauthorized", status=401)

        async def handler(req):
            raise exc

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert exc_info.value is exc
        trace = getattr(exc_info.value, RESILIENCE_TRACE_ATTR)
        assert len(trace["attempted_models"]) == 1
        # No fallback switch events without fallbacks
        assert [e["type"] for e in events] == []


class TestRobustness:
    @pytest.mark.asyncio
    async def test_stream_writer_failure_does_not_break_resilience(self, monkeypatch):
        def _broken_writer():
            raise RuntimeError("no streaming context")

        monkeypatch.setattr("langgraph.config.get_stream_writer", _broken_writer)
        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb)])

        async def handler(req):
            if req.model is client:
                raise _status_error("nope", status=400)
            return "ok"

        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "ok"

    @pytest.mark.asyncio
    async def test_subagent_request_uses_derived_model_name(self, events):
        # Shared instance: a subagent stack routes a different model through
        # the same middleware — its display name must come from the request.
        primary_client = _FakeModel("primary-model")
        sub_client = _FakeModel("subagent-model")
        mw = _make_middleware(primary_client)
        calls = []

        async def handler(req):
            calls.append(req)
            if len(calls) == 1:
                raise _status_error("upstream 500", status=500)
            return "ok"

        await mw.awrap_model_call(_FakeRequest(sub_client), handler)
        assert events[0]["model"] == "subagent-model"

    def test_sync_wrap_model_call_parity(self, events):
        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb)])

        def handler(req):
            if req.model is client:
                raise _status_error("bad request", status=400)
            return "ok"

        assert mw.wrap_model_call(_FakeRequest(client), handler) == "ok"
        assert [p["to_model"] for p in _fallback_props(events)] == ["fallback-a"]

    def test_sync_retry_loop_parity(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        attempts = []

        def handler(req):
            attempts.append(req)
            if len(attempts) < 3:
                raise _status_error("upstream 500", status=500)
            return "ok"

        assert mw.wrap_model_call(_FakeRequest(client), handler) == "ok"
        assert len(attempts) == 3
        assert [e["type"] for e in events] == ["model_retry", "model_retry"]
        assert events[0]["attempt"] == 1
        assert events[1]["attempt"] == 2

    def test_calculate_delay_backoff(self):
        mw = ModelResilienceMiddleware(
            primary_name="p",
            initial_delay=1.0,
            backoff_factor=2.0,
            max_delay=3.0,
            jitter=False,
        )
        assert mw._calculate_delay(0) == 1.0
        assert mw._calculate_delay(1) == 2.0
        assert mw._calculate_delay(2) == 3.0  # capped at max_delay


class TestBuildFallbackPairs:
    class _Cfg:
        def __init__(self, clients=None, names=None, fallback=None):
            self.fallback_llm_clients = clients
            self.fallback_llm_names = names
            self.llm = type("_LLM", (), {"fallback": fallback})()

    def test_prefers_aligned_names(self):
        clients = [_FakeModel("sdk-id-a"), _FakeModel("sdk-id-b")]
        cfg = self._Cfg(clients=clients, names=["alias-a", "alias-b"])
        assert build_fallback_pairs(cfg) == [
            ("alias-a", clients[0]),
            ("alias-b", clients[1]),
        ]

    def test_misaligned_names_fall_back_to_derived(self):
        clients = [_FakeModel("sdk-id-a"), _FakeModel("sdk-id-b")]
        cfg = self._Cfg(clients=clients, names=["only-one"])
        assert build_fallback_pairs(cfg) == [
            ("sdk-id-a", clients[0]),
            ("sdk-id-b", clients[1]),
        ]

    def test_empty_config_returns_empty(self):
        assert build_fallback_pairs(self._Cfg()) == []


class TestReasoningPayloadEscalation:
    """The one non-retryable status the middleware retries in place.

    A reasoning-payload 400 is a request-validation rejection raised while
    creating the stream — before any chunk is emitted or any token billed — so
    retrying it with the reasoning stripped cannot double-emit a response.
    Detection is structural (a 400 on a request that carried reasoning) rather
    than a match against provider error prose, so it cannot silently stop
    working when an API rewords its message.
    """

    THINKING = {"type": "thinking", "thinking": "reasoned", "signature": "foreign"}
    TEXT = {"type": "text", "text": "answer"}
    ERROR = "messages.55.content.0.thinking.signature: Field required"

    # Rejections captured from live APIs while settling this bug, with item ids
    # replaced by placeholders. They are fixtures, not patterns: the code never
    # reads them, so this list is evidence that the structural check covers the
    # real failures.
    REAL_REJECTIONS = [
        # api.anthropic.com — the production traceback this exists for, and the
        # foreign-signature variant, which uses a different field path.
        "messages.55.content.0.thinking.signature: Field required",
        "messages.1.content.0: Invalid `signature` in `thinking` block",
        "messages.12.content.0.thinking: Field required",
        # api.anthropic.com rejecting an OpenAI reasoning item by tag.
        "messages.1.content.0: Input tag 'reasoning' found using 'type' does not "
        "match any of the expected tags: 'redacted_thinking', 'text', 'thinking'",
        # api.moonshot.cn — same rejection, its own tag list.
        "messages.1.content.0: Input tag 'reasoning' found using 'type' does not "
        "match any of the expected tags: 'image', 'server_tool_use'",
        # api.openai.com — an rs_ item minted by a different route.
        "The encrypted content for item rs_abc123 could not be "
        "verified. Reason: Encrypted content could not be decrypted.",
        # api.openai.com — an Anthropic-shaped block sent to OpenAI.
        "Invalid value: 'thinking'. Supported values are: 'agent_message'",
        # OpenAI Responses replaying an orphaned reasoning item.
        "Item 'rs_abc' of type 'reasoning' was provided without its required "
        "following item.",
    ]

    def _history(self):
        from langchain_core.messages import AIMessage, HumanMessage

        return [HumanMessage("q"), AIMessage(content=[self.THINKING, self.TEXT])]

    @staticmethod
    def _block_types(req):
        from langchain_core.messages import AIMessage

        return [
            b["type"]
            for m in req.messages
            if isinstance(m, AIMessage) and isinstance(m.content, list)
            for b in m.content
        ]

    @pytest.mark.asyncio
    async def test_retries_once_with_reasoning_stripped(self, events):
        """The retry drops the history's blocks but keeps thinking enabled.

        A stripped assistant turn is accepted with thinking on — verified live
        on Anthropic and claude-oauth — so the model still reasons on the retry.
        """
        client = _FakeModel("primary-model", thinking={"type": "enabled"})
        mw = _make_middleware(client)
        seen = []

        async def handler(req):
            seen.append((self._block_types(req), getattr(req.model, "thinking", None)))
            if len(seen) == 1:
                raise _status_error(self.ERROR, status=400)
            return "ok"

        result = await mw.awrap_model_call(
            _FakeRequest(client, self._history()), handler
        )
        assert result == "ok"
        assert seen == [
            (["thinking", "text"], {"type": "enabled"}),
            (["text"], {"type": "enabled"}),
        ]
        assert [e["type"] for e in events] == ["model_retry"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("message", REAL_REJECTIONS)
    async def test_every_observed_rejection_recovers(self, message, events):
        """Each string is a real 400 seen from a live API during this fix."""
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        seen = []

        async def handler(req):
            seen.append(self._block_types(req))
            if len(seen) == 1:
                raise _status_error(message, status=400)
            return "ok"

        result = await mw.awrap_model_call(
            _FakeRequest(client, self._history()), handler
        )
        assert result == "ok"
        assert seen[1] == ["text"]

    @pytest.mark.asyncio
    async def test_mid_stream_400_never_escalates(self, events):
        """A failure reported inside an open stream skips the reasoning repair.

        The repair answers a request rejected during validation. A provider that
        accepted the request, opened the stream and only then reported failure
        has already judged the payload fine, so the extra call is pure latency
        before the fallback, and re-sending bills whatever the stream emitted a
        second time. DashScope reporting an input-filter block as
        ``response.failed`` over HTTP 200 is the case this was measured on.
        """
        from src.llms.extension import ResponsesStreamFailedError

        # Verbatim from the live provider: the structured code says
        # ``server_error`` even though the block is permanent, and only the
        # message names the real cause. Do not "correct" the code to match the
        # message.
        exc = ResponsesStreamFailedError(
            "Responses stream failed (server_error): <400> "
            "InternalError.Algo.DataInspectionFailed: Input text data may "
            "contain inappropriate content.",
            code="server_error",
        )
        client = _FakeModel("primary-model", thinking={"type": "enabled"})
        mw = _make_middleware(client, fallbacks=[("fallback-model", _FakeModel("fallback-model"))])
        calls = []

        async def handler(req):
            calls.append(self._block_types(req))
            if len(calls) == 1:
                raise exc
            return "ok"

        result = await mw.awrap_model_call(
            _FakeRequest(client, self._history()), handler
        )
        assert result == "ok"
        # Two calls either way, so the count alone proves nothing: an escalation
        # would also answer on its second call. What separates them is which
        # call it is. The fallback re-sends the history intact; a stripped retry
        # would arrive here with the thinking block removed.
        assert len(calls) == 2, calls
        assert "thinking" in calls[0]
        assert "thinking" in calls[1], calls
        assert [p["to_model"] for p in _fallback_props(events)] == ["fallback-model"]
        assert [e["type"] for e in events if e["type"] == "model_retry"] == []

    @pytest.mark.asyncio
    async def test_escalates_at_most_once_per_candidate(self, events):
        client = _FakeModel("primary-model")
        fallback = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback)])
        seen = []

        async def handler(req):
            seen.append(self._block_types(req))
            raise _status_error(self.ERROR, status=400)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(client, self._history()), handler)

        # Each candidate gets one full-history attempt and one stripped retry;
        # the fallback starts fresh rather than inheriting the escalation.
        assert seen == [
            ["thinking", "text"],
            ["text"],
            ["thinking", "text"],
            ["text"],
        ]

    @pytest.mark.asyncio
    async def test_400_on_a_clean_history_costs_no_extra_call(self, events):
        """Nothing to strip, so re-sending would be byte-identical."""
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            raise _status_error("max_tokens: must be <= 64000", status=400)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(client, []), handler)
        assert len(calls) == 1
        assert events == []

    @pytest.mark.asyncio
    async def test_unrelated_400_with_reasoning_costs_one_extra_call(self, events):
        """The accepted price of structural detection over prose matching.

        The turn is already failing, so one wasted call buys immunity from
        provider error strings drifting out from under us. The real error is
        still what surfaces.
        """
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            raise _status_error("prompt is too long: 250000 tokens", status=400)

        with pytest.raises(Exception, match="prompt is too long"):
            await mw.awrap_model_call(_FakeRequest(client, self._history()), handler)
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_a_config_400_is_surfaced_rather_than_masked(self, events):
        """Why escalation strips reasoning without also disabling thinking.

        ``max_tokens`` below ``thinking.budget_tokens`` is a genuine config bug,
        and it *starts succeeding* the moment thinking is off — so a disable
        would answer the turn with no reasoning instead of reporting the error.
        Escalation still runs here (the history has blocks to strip), and
        thinking is still enabled on the retry, so the 400 reaches the caller.
        """
        client = _FakeModel("primary-model", thinking={"type": "enabled"})
        mw = _make_middleware(client)
        seen = []

        async def handler(req):
            seen.append((self._block_types(req), getattr(req.model, "thinking", None)))
            raise _status_error(
                "`max_tokens` must be greater than `thinking.budget_tokens`",
                status=400,
            )

        with pytest.raises(Exception, match="max_tokens"):
            await mw.awrap_model_call(_FakeRequest(client, self._history()), handler)
        assert seen == [
            (["thinking", "text"], {"type": "enabled"}),
            (["text"], {"type": "enabled"}),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [401, 403, 404, 422])
    async def test_other_non_retryable_statuses_never_escalate(self, status, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            raise _status_error("nope", status=status)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(client, self._history()), handler)
        assert len(calls) == 1
