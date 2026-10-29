"""Tests for ModelResilienceMiddleware — retry, fallback, events, error trace."""

import pytest
from langchain.agents.middleware.types import ModelResponse
from langchain_core.messages import AIMessage

from ptc_agent.agent.middleware.model_resilience import (
    FALLBACK_KEY,
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

    def __init__(self, model, messages=None, state=None, model_settings=None):
        self.model = model
        self.messages = messages if messages is not None else []
        self.state = state if state is not None else {"messages": []}
        self.model_settings = model_settings if model_settings is not None else {}

    def override(self, *, model=None, messages=None):
        return _FakeRequest(
            model if model is not None else self.model,
            messages if messages is not None else self.messages,
            self.state,
            self.model_settings,
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

    @pytest.mark.asyncio
    async def test_an_answer_a_fallback_wrote_names_it(self, events):
        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback_client)])
        down = True

        async def handler(req):
            if down and req.model is client:
                raise _status_error("Error code: 404 - model not found", status=404)
            return ModelResponse(result=[AIMessage("ok", response_metadata={"model_name": "x"})])

        by_fallback = await mw.awrap_model_call(_FakeRequest(client), handler)
        down = False
        by_primary = await mw.awrap_model_call(_FakeRequest(client), handler)

        assert by_fallback.result[0].response_metadata == {"model_name": "x", FALLBACK_KEY: "fallback-a"}
        assert by_primary.result[0].response_metadata == {"model_name": "x"}


class TestProgrammingErrors:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exc",
        [
            AttributeError("'dict' object has no attribute 'model_dump'"),
            TypeError("'NoneType' object is not iterable"),
            KeyError("choices"),
        ],
    )
    async def test_a_parsing_crash_goes_straight_to_the_fallback(self, exc, events):
        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback_client)])
        calls = []

        async def handler(req):
            calls.append(req.model)
            if req.model is client:
                raise exc
            return "ok-from-fallback"

        result = await mw.awrap_model_call(_FakeRequest(client), handler)
        assert result == "ok-from-fallback"
        assert calls == [client, fallback_client]
        assert _fallback_props(events)[0]["attempts_on_from"] == 1

    @pytest.mark.asyncio
    async def test_a_crash_with_no_fallback_surfaces_after_one_attempt(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            raise KeyError("choices")

        with pytest.raises(KeyError):
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_a_connection_error_keeps_its_retries(self, events):
        import httpx

        client = _FakeModel("primary-model")
        mw = _make_middleware(client, max_retries=2)
        calls = []

        async def handler(req):
            calls.append(req)
            raise httpx.ConnectError("refused")

        with pytest.raises(httpx.ConnectError):
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert len(calls) == 3


class TestStalledStream:
    @pytest.mark.asyncio
    async def test_a_stall_after_the_stream_started_is_retried_once(self, events):
        from langchain_openai.chat_models._client_utils import StreamChunkTimeoutError

        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback_client)])
        calls = []

        async def handler(req):
            calls.append(req.model)
            if req.model is client:
                raise StreamChunkTimeoutError(600.0, chunks_received=231)
            return "ok-from-fallback"

        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "ok-from-fallback"
        assert calls == [client, client, fallback_client]

    @pytest.mark.asyncio
    async def test_a_stall_before_any_chunk_is_retried_once(self, events):
        # Four attempts at the full bound each held a candidate 40 minutes
        # before its fallback.
        from langchain_openai.chat_models._client_utils import StreamChunkTimeoutError

        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(
            client, fallbacks=[("fallback-a", fallback_client)], max_retries=3
        )
        calls = []

        async def handler(req):
            calls.append(req.model)
            if req.model is client:
                raise StreamChunkTimeoutError(600.0, chunks_received=0)
            return "ok-from-fallback"

        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "ok-from-fallback"
        assert calls == [client, client, fallback_client]

    @pytest.mark.asyncio
    async def test_another_timeout_keeps_the_full_retries(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client, max_retries=3)
        calls = []

        async def handler(req):
            calls.append(req)
            raise TimeoutError("read timed out")

        with pytest.raises(TimeoutError):
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert len(calls) == 4


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
    async def test_a_switch_names_who_pays_for_the_model_it_moved_to(self, events):
        # A switch can move the turn onto another payer, so the notice says
        # whose key the next model runs on; an unstamped client says nothing.
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        fb_a.metadata = {"billing_type": "oauth", "pricing_provider": "codex-oauth"}
        fb_b = _FakeModel("fallback-b")
        mw = _make_middleware(
            client, fallbacks=[("fallback-a", fb_a), ("fallback-b", fb_b)]
        )

        async def handler(req):
            if req.model is fb_b:
                return "ok"
            raise _status_error("bad request", status=400)

        await mw.awrap_model_call(_FakeRequest(client), handler)
        switches = _fallback_props(events)
        # The provider names the plan: a model added on a sign-in can share
        # its name with another provider's.
        assert (switches[0]["to_paid_by"], switches[0]["to_provider"]) == ("oauth", "codex-oauth")
        assert "to_paid_by" not in switches[1] and "to_provider" not in switches[1]

    @pytest.mark.asyncio
    async def test_a_fallback_names_the_message_its_call_followed(self, events):
        # The turn's records keep no step, so replay places the notice by it.
        from langchain_core.messages import HumanMessage, ToolMessage

        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb)])
        state = {
            "messages": [
                HumanMessage(content="go", id="h-1"),
                ToolMessage(content="out", tool_call_id="tc-1", id="tm-1"),
            ]
        }

        async def handler(req):
            if req.model is fb:
                return "ok"
            raise _status_error("bad request", status=400)

        await mw.awrap_model_call(_FakeRequest(client, state=state), handler)
        [record] = [e for e in events if e.get("name") == "model_fallback"]
        assert record["metadata"]["after_message_id"] == "tm-1"
        assert "after_message_id" not in record["props"]

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
    async def test_each_distinct_model_is_tried_once(self, events):
        # A fallback naming the primary, or a name listed twice, would send the
        # request that just failed to the model that refused it.
        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(
            client,
            fallbacks=[("primary-model", client), ("fallback-a", fb), ("fallback-a", fb)],
        )
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            raise _status_error("bad request", status=400)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert tried == ["primary-model", "fallback-a"]

    @pytest.mark.asyncio
    async def test_a_subagent_on_a_fallback_model_skips_that_fallback(self, events):
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        fb_b = _FakeModel("fallback-b")
        sub = _FakeModel("fallback-a-id")
        sub.metadata = {"manifest_model": "fallback-a"}
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb_a), ("fallback-b", fb_b)])
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            raise _status_error("bad request", status=400)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(sub), handler)
        assert tried == ["fallback-a-id", "fallback-b"]

    @pytest.mark.asyncio
    async def test_a_subagent_on_its_own_model_can_fall_back_to_the_turns_model(self, events):
        # The turn's model is listed as a fallback: useless to the main agent,
        # but a real second model for a subagent on another one.
        client = _FakeModel("primary-model")
        sub = _FakeModel("sub-model-id")
        sub.metadata = {"manifest_model": "sub-model"}
        mw = _make_middleware(client, fallbacks=[("primary-model", client)])
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            raise _status_error("bad request", status=400)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(sub), handler)
        assert tried == ["sub-model-id", "primary-model"]

    @pytest.mark.parametrize(
        ("billing", "users_fallbacks", "expected"),
        [
            # On the user's own key, the deployment's list may not move it
            # onto the platform's key, only onto another key of the user's.
            ("byok", False, ["sub-model-id", "fallback-b"]),
            ("oauth", False, ["sub-model-id", "fallback-b"]),
            # A list the user saved is billed as they picked it.
            ("byok", True, ["sub-model-id", "fallback-a", "fallback-b"]),
            # On credits already, credits is no move at all.
            ("platform", False, ["sub-model-id", "fallback-a", "fallback-b"]),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_subagent_falls_back_only_onto_a_payer_the_user_chose(
        self, events, billing, users_fallbacks, expected
    ):
        # The turn's list was built for a main model on credits.
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        fb_a.metadata = {"billing_type": "platform"}
        fb_b = _FakeModel("fallback-b")
        fb_b.metadata = {"billing_type": "byok"}
        sub = _FakeModel("sub-model-id")
        sub.metadata = {"manifest_model": "sub-model", "billing_type": billing}
        mw = _make_middleware(
            client,
            fallbacks=[("fallback-a", fb_a), ("fallback-b", fb_b)],
            users_fallbacks=users_fallbacks,
        )
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            raise _status_error("bad request", status=400)

        with pytest.raises(Exception):
            await mw.awrap_model_call(_FakeRequest(sub), handler)
        assert tried == expected

    @pytest.mark.asyncio
    async def test_an_overflow_is_neither_retried_nor_moved_to_a_fallback(self, events):
        # Every candidate is sent the same history; compaction, outside this
        # middleware, is what makes it smaller.
        client = _FakeModel("primary-model")
        fb = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb)], compacts_overflow=True)
        overflow = _status_error("prompt is too long: 250000 tokens", status=400)
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            raise overflow

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert exc_info.value is overflow
        assert tried == ["primary-model"]
        assert getattr(overflow, RESILIENCE_TRACE_ATTR)["attempted_models"][0]["attempts"] == 1
        assert _fallback_props(events) == []

    @pytest.mark.asyncio
    async def test_a_fallback_that_overflows_hands_its_overflow_up(self, events):
        client = _FakeModel("primary-model")
        fb_a = _FakeModel("fallback-a")
        fb_b = _FakeModel("fallback-b")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fb_a), ("fallback-b", fb_b)], compacts_overflow=True)
        overflow = _status_error("maximum context length is 131072 tokens", status=400)
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            if req.model is client:
                raise _status_error("model not found", status=404)
            raise overflow

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert exc_info.value is overflow
        assert tried == ["primary-model", "fallback-a"]

    @staticmethod
    def _windowed(name, window):
        model = _FakeModel(name)
        model.profile = {"max_input_tokens": window}
        return model

    @pytest.mark.asyncio
    async def test_an_overflow_moves_on_to_a_fallback_with_a_larger_window(self, events):
        # The request fits a longer window as it is, so no summary is needed.
        client = self._windowed("primary-model", 200_000)
        small = self._windowed("fallback-small", 128_000)
        large = self._windowed("fallback-large", 1_000_000)
        mw = _make_middleware(
            client,
            fallbacks=[("fallback-small", small), ("fallback-large", large)],
            compacts_overflow=True,
        )
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            if req.model is large:
                return "answered"
            raise _status_error("prompt is too long: 250000 tokens", status=400)

        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "answered"
        assert tried == ["primary-model", "fallback-large"]
        assert [p["to_model"] for p in _fallback_props(events)] == ["fallback-large"]

    @pytest.mark.asyncio
    async def test_an_overflow_on_a_smaller_fallback_moves_on_to_a_larger_one(self, events):
        client = self._windowed("primary-model", 1_000_000)
        small = self._windowed("fallback-small", 128_000)
        large = self._windowed("fallback-large", 1_000_000)
        mw = _make_middleware(
            client,
            fallbacks=[("fallback-small", small), ("fallback-large", large)],
            compacts_overflow=True,
        )
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            if req.model is client:
                raise _status_error("model not found", status=404)
            if req.model is small:
                raise _status_error("maximum context length is 131072 tokens", status=400)
            return "answered"

        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "answered"
        assert tried == ["primary-model", "fallback-small", "fallback-large"]

    @pytest.mark.asyncio
    async def test_an_overflow_with_no_larger_window_left_goes_to_compaction(self, events):
        # A later failure of another kind does not hide it: the overflow is
        # what compaction can act on.
        client = self._windowed("primary-model", 200_000)
        large = self._windowed("fallback-large", 1_000_000)
        same = self._windowed("fallback-same", 200_000)
        mw = _make_middleware(
            client,
            fallbacks=[("fallback-same", same), ("fallback-large", large)],
            compacts_overflow=True,
            max_retries=0,
        )
        overflow = _status_error("prompt is too long: 250000 tokens", status=400)
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            if req.model is large:
                raise _status_error("overloaded", status=529)
            raise overflow

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)
        assert exc_info.value is overflow
        assert tried == ["primary-model", "fallback-large"]

    def test_a_sync_overflow_moves_on_to_a_larger_window_too(self, events):
        client = self._windowed("primary-model", 200_000)
        large = self._windowed("fallback-large", 1_000_000)
        mw = _make_middleware(client, fallbacks=[("fallback-large", large)], compacts_overflow=True)
        tried = []

        def handler(req):
            tried.append(req.model.model)
            if req.model is large:
                return "answered"
            raise _status_error("prompt is too long", status=400)

        assert mw.wrap_model_call(_FakeRequest(client), handler) == "answered"
        assert tried == ["primary-model", "fallback-large"]

    def test_a_sync_overflow_is_not_retried_either(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client, fallbacks=[("fallback-a", _FakeModel("fallback-a"))], compacts_overflow=True)
        tried = []

        def handler(req):
            tried.append(req.model.model)
            raise _status_error("prompt is too long", status=400)

        with pytest.raises(Exception, match="prompt is too long"):
            mw.wrap_model_call(_FakeRequest(client), handler)
        assert tried == ["primary-model"]

    @staticmethod
    def _built(compaction_enabled: bool, client, fallback):
        from types import SimpleNamespace

        from ptc_agent.agent.turn import TurnModel, build_model_resilience_middleware

        cfg = SimpleNamespace(
            fallback_llm_clients=[fallback],
            fallback_llm_names=["fallback-a"],
            fallback_users_choice=False,
            llm=SimpleNamespace(fallback=None),
            compaction=SimpleNamespace(enabled=compaction_enabled),
        )
        turn = TurnModel(client=client, name="primary-model", guidance="full")
        return build_model_resilience_middleware(cfg, turn)

    @pytest.mark.asyncio
    async def test_without_compaction_an_overflow_still_moves_to_a_fallback(self, events):
        # Nothing outside would make the request smaller, and a fallback with
        # a longer window may take it as it is.
        client, fb = _FakeModel("primary-model"), _FakeModel("fallback-a")
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            if req.model is client:
                raise _status_error("prompt is too long: 250000 tokens", status=400)
            return "answered"

        for enabled, expected in ((False, ["primary-model", "fallback-a"]), (True, ["primary-model"])):
            tried.clear()
            mw = self._built(enabled, client, fb)
            if enabled:
                with pytest.raises(Exception, match="prompt is too long"):
                    await mw.awrap_model_call(_FakeRequest(client), handler)
            else:
                assert await mw.awrap_model_call(_FakeRequest(client), handler) == "answered"
            assert tried == expected

    @pytest.mark.asyncio
    async def test_without_compaction_a_statusless_overflow_is_not_retried(self, events):
        # LangChain raises its overflow class with no status, which would
        # otherwise read as retryable and repeat the same request.
        from langchain_core.exceptions import ContextOverflowError

        client, fb = _FakeModel("primary-model"), _FakeModel("fallback-a")
        tried = []

        async def handler(req):
            tried.append(req.model.model)
            if req.model is client:
                raise ContextOverflowError("prompt is too long")
            return "answered"

        mw = self._built(False, client, fb)
        assert await mw.awrap_model_call(_FakeRequest(client), handler) == "answered"
        assert tried == ["primary-model", "fallback-a"]

    def test_without_compaction_a_sync_overflow_still_moves_to_a_fallback(self, events):
        client, fb = _FakeModel("primary-model"), _FakeModel("fallback-a")
        tried = []

        def handler(req):
            tried.append(req.model.model)
            if req.model is client:
                raise _status_error("prompt is too long", status=400)
            return "answered"

        mw = self._built(False, client, fb)
        assert mw.wrap_model_call(_FakeRequest(client), handler) == "answered"
        assert tried == ["primary-model", "fallback-a"]

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


class TestCompactionRequest:
    """A compaction request asks the primary's provider for its own summary:
    another model would answer it as a turn, and compaction has a summarizer
    of its own to fall back to."""

    _SETTINGS = {"extra_body": {"compaction": {"type": "summarize"}}}

    @pytest.mark.asyncio
    async def test_only_the_primary_is_tried(self, events):
        client = _FakeModel("primary-model")
        fallback_client = _FakeModel("fallback-a")
        mw = _make_middleware(client, fallbacks=[("fallback-a", fallback_client)])
        exc = _status_error("Error code: 404 - model not found", status=404)
        calls = []

        async def handler(req):
            calls.append(req.model)
            raise exc

        with pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client, model_settings=self._SETTINGS), handler)

        assert exc_info.value is exc
        assert calls == [client]
        assert _fallback_props(events) == []

    def test_candidates(self):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client, fallbacks=[("fallback-a", _FakeModel("fallback-a"))])

        compaction = mw._candidates(_FakeRequest(client, model_settings=self._SETTINGS))
        turn = mw._candidates(_FakeRequest(client, model_settings={"extra_body": {"x": 1}}))

        assert [name for name, _ in compaction] == ["primary-model"]
        assert [name for name, _ in turn] == ["primary-model", "fallback-a"]


class TestAsSent:
    """A call only the primary reading that request can answer (the hand-off
    of a summary only its provider can read) is retried, but never moved to a
    fallback or sent again without its reasoning."""

    @pytest.mark.asyncio
    async def test_only_the_primary_is_tried(self, events):
        from ptc_agent.agent.middleware.model_resilience import as_sent

        client = _FakeModel("primary-model")
        mw = _make_middleware(client, fallbacks=[("fallback-a", _FakeModel("fallback-a"))])
        exc = _status_error("Error code: 404 - model not found", status=404)
        calls = []

        async def handler(req):
            calls.append(req.model)
            raise exc

        with as_sent(), pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client), handler)

        assert exc_info.value is exc
        assert calls == [client]
        assert _fallback_props(events) == []

    @pytest.mark.asyncio
    async def test_a_transient_error_is_still_retried(self, events):
        from ptc_agent.agent.middleware.model_resilience import as_sent

        client = _FakeModel("primary-model")
        mw = _make_middleware(client, fallbacks=[("fallback-a", _FakeModel("fallback-a"))])
        attempts = []

        async def handler(req):
            attempts.append(req.model)
            if len(attempts) < 2:
                raise _status_error("upstream 500", status=500)
            return "ok"

        with as_sent():
            assert await mw.awrap_model_call(_FakeRequest(client), handler) == "ok"

        assert attempts == [client, client]

    @pytest.mark.asyncio
    async def test_a_400_is_not_sent_again_without_reasoning(self, events):
        from langchain_core.messages import AIMessage, HumanMessage

        from ptc_agent.agent.middleware.model_resilience import as_sent

        client = _FakeModel("primary-model", thinking={"type": "enabled"})
        mw = _make_middleware(client)
        thinking = {"type": "thinking", "thinking": "reasoned", "signature": "foreign"}
        history = [HumanMessage("q"), AIMessage(content=[thinking, {"type": "text", "text": "a"}])]
        exc = _status_error(TestReasoningPayloadEscalation.ERROR, status=400)
        sent = []

        async def handler(req):
            sent.append(req.messages)
            raise exc

        with as_sent(), pytest.raises(Exception) as exc_info:
            await mw.awrap_model_call(_FakeRequest(client, history), handler)

        assert exc_info.value is exc
        assert sent == [history]
        assert events == []

    def test_the_fallbacks_are_back_after_the_block(self):
        from ptc_agent.agent.middleware.model_resilience import as_sent

        client = _FakeModel("primary-model")
        mw = _make_middleware(client, fallbacks=[("fallback-a", _FakeModel("fallback-a"))])

        with as_sent():
            inside = mw._candidates(_FakeRequest(client))
        after = mw._candidates(_FakeRequest(client))

        assert [name for name, _ in inside] == ["primary-model"]
        assert [name for name, _ in after] == ["primary-model", "fallback-a"]


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

    def test_resolved_empty_does_not_retry_rejected_names(self):
        cfg = self._Cfg(clients=[], names=[], fallback=["deepseek-v4-pro"])
        assert build_fallback_pairs(cfg) == []

    def test_unresolved_config_uses_name_factory(self, monkeypatch):
        client = _FakeModel("standalone")
        calls = []

        def factory(name):
            calls.append(name)
            return client

        monkeypatch.setattr("src.llms.get_llm_by_type", factory)
        assert build_fallback_pairs(self._Cfg(fallback=["standalone"])) == [("standalone", client)]
        assert calls == ["standalone"]

    def test_empty_config_returns_empty(self):
        assert build_fallback_pairs(self._Cfg()) == []

    def test_unresolvable_name_is_skipped(self, monkeypatch):
        # A fallback is insurance for the primary: a stale or unkeyed name in
        # the list is dropped, never what takes the turn down.
        good = _FakeModel("good")

        def factory(name):
            if name == "gone-model":
                raise ValueError("Model gone-model not found in models.json")
            return good

        monkeypatch.setattr("src.llms.get_llm_by_type", factory)
        cfg = self._Cfg(fallback=["gone-model", "good"])
        assert build_fallback_pairs(cfg) == [("good", good)]


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
            raise _status_error("tools.0.input_schema: unsupported keyword", status=400)

        with pytest.raises(Exception, match="unsupported keyword"):
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


class TestAStripIsKept:
    """A stripped retry the primary answered stays stripped for the thread.

    The answer's thinking is bound to the history it was sent with, so putting
    the stripped reasoning back on a later request would edit that history.
    """

    THINKING = {"type": "thinking", "thinking": "", "signature": "sig-1"}

    def _history(self):
        from langchain_core.messages import AIMessage, HumanMessage

        return [
            HumanMessage("q", id="h-1"),
            AIMessage(content=[self.THINKING, {"type": "text", "text": "a"}], id="a-1"),
            HumanMessage("again", id="h-2"),
        ]

    @pytest.mark.asyncio
    async def test_the_answer_records_what_was_stripped(self, events):
        from langchain.agents.middleware.types import ExtendedModelResponse

        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        answer = ModelResponse(result=[AIMessage("ok", id="a-2")])
        calls = []

        async def handler(req):
            calls.append(req)
            if len(calls) == 1:
                raise _status_error("messages.1.content.0: Invalid signature", status=400)
            return answer

        result = await mw.awrap_model_call(_FakeRequest(client, self._history()), handler)

        assert isinstance(result, ExtendedModelResponse)
        assert result.model_response is answer
        assert result.command.update == {"_unsent_thinking_ids": {"a-1"}}

    @pytest.mark.asyncio
    async def test_a_later_request_sends_the_stripped_messages_stripped(self, events):
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        seen = []

        async def handler(req):
            seen.append([m.content for m in req.messages])
            return ModelResponse(result=[AIMessage("ok")])

        newer = AIMessage(content=[{**self.THINKING, "signature": "sig-2"}], id="a-2")
        request = _FakeRequest(
            client,
            [*self._history(), newer],
            state={"messages": [], "_unsent_thinking_ids": {"a-1"}},
        )
        result = await mw.awrap_model_call(request, handler)

        assert isinstance(result, ModelResponse)
        assert seen == [["q", [{"type": "text", "text": "a"}], "again", newer.content]]

    @pytest.mark.asyncio
    async def test_a_fallback_keeps_nothing(self, events):
        primary = _FakeModel("primary-model")
        backup = _FakeModel("backup-model")
        mw = _make_middleware(primary, fallbacks=[("backup-model", backup)], max_retries=0)

        async def handler(req):
            if req.model is primary:
                raise _status_error("overloaded", status=400)
            if any(isinstance(m.content, list) and self.THINKING in m.content for m in req.messages):
                raise _status_error("messages.1.content.0: Invalid signature", status=400)
            return ModelResponse(result=[AIMessage("ok")])

        result = await mw.awrap_model_call(_FakeRequest(primary, self._history()), handler)

        assert isinstance(result, ModelResponse)
        assert result.result[0].response_metadata[FALLBACK_KEY] == "backup-model"

    @pytest.mark.asyncio
    async def test_the_strip_is_logged_with_the_providers_text(self, events):
        from structlog.testing import capture_logs

        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        calls = []

        async def handler(req):
            calls.append(req)
            if len(calls) == 1:
                raise _status_error("messages.1.content.0: Invalid signature in thinking block", status=400)
            return ModelResponse(result=[AIMessage("ok")])

        with capture_logs() as logs:
            await mw.awrap_model_call(_FakeRequest(client, self._history()), handler)

        strips = [e for e in logs if e["event"].startswith("[ModelResilience] retrying without reasoning")]
        assert len(strips) == 1
        assert strips[0]["model"] == "primary-model"
        assert strips[0]["error"] == "messages.1.content.0: Invalid signature in thinking block"
        assert strips[0]["log_level"] == "warning"


class TestDroppedThinkingReport:
    """Every answer that says the provider dropped earlier thinking is logged
    and stamped on its run, by reason, so a thread's count needs no state."""

    @staticmethod
    def _answer(entries):
        return ModelResponse(
            result=[AIMessage("ok", response_metadata={"input_transformations": entries})]
        )

    @pytest.mark.asyncio
    async def test_drops_are_logged_and_stamped_by_reason(self, events, monkeypatch):
        from structlog.testing import capture_logs

        stamps = []
        monkeypatch.setattr(
            "ptc_agent.agent.middleware.model_resilience.stamp_run",
            lambda **fields: stamps.append(fields),
        )
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        entries = [
            {"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"},
            {"type": "thinking_dropped", "path": "messages.3.content.0", "reason": "prefix_binding_mismatch"},
            {"type": "thinking_dropped", "path": "messages.5.content.0", "reason": "model_binding_mismatch"},
        ]

        async def handler(req):
            return self._answer(entries)

        with capture_logs() as logs:
            await mw.awrap_model_call(_FakeRequest(client), handler)

        drops = [e for e in logs if "dropped" in e["event"]]
        assert len(drops) == 1
        assert drops[0]["log_level"] == "warning"
        assert drops[0]["prefix_binding_mismatch"] == 2
        assert drops[0]["model_binding_mismatch"] == 1
        assert stamps == [{"thinking_dropped": {"prefix_binding_mismatch": 2, "model_binding_mismatch": 1}}]

    @pytest.mark.asyncio
    async def test_a_block_dropped_again_is_stamped_as_repeated(self, events, monkeypatch):
        from structlog.testing import capture_logs

        stamps = []
        monkeypatch.setattr(
            "ptc_agent.agent.middleware.model_resilience.stamp_run",
            lambda **fields: stamps.append(fields),
        )
        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        entries = [{"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "prefix_binding_mismatch"}]
        earlier = self._answer(entries).result[0]

        async def handler(req):
            return self._answer(entries)

        with capture_logs() as logs:
            await mw.awrap_model_call(_FakeRequest(client, messages=[earlier]), handler)

        assert [e["log_level"] for e in logs if "dropped" in e["event"]] == ["info"]
        assert stamps == [{"thinking_dropped": {"repeated": 1}}]

    @pytest.mark.asyncio
    async def test_a_model_switch_alone_is_logged_as_info(self, events):
        from structlog.testing import capture_logs

        client = _FakeModel("primary-model")
        mw = _make_middleware(client)
        entries = [{"type": "thinking_dropped", "path": "messages.1.content.0", "reason": "model_binding_mismatch"}]

        async def handler(req):
            return self._answer(entries)

        with capture_logs() as logs:
            await mw.awrap_model_call(_FakeRequest(client), handler)

        assert [e["log_level"] for e in logs if "dropped" in e["event"]] == ["info"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("entries", [[], None])
    async def test_an_answer_with_nothing_dropped_says_nothing(self, entries, events):
        from structlog.testing import capture_logs

        client = _FakeModel("primary-model")
        mw = _make_middleware(client)

        async def handler(req):
            return self._answer(entries)

        with capture_logs() as logs:
            await mw.awrap_model_call(_FakeRequest(client), handler)

        assert not [e for e in logs if "dropped" in e["event"]]
