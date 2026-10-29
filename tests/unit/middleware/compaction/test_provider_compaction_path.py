"""A thread past its threshold, on a model whose provider compacts for it.

The compaction request is the conversation's own model call with one more
setting, sent through the same handler the turn's call goes through; its
answer rides the summary message beside the readable text. When the provider
writes no compaction the summarizer writes the summary as it always did.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage, SystemMessage, ToolMessage
from langchain_core.runnables.config import var_child_runnable_config
from langchain_openai import ChatOpenAI
from langgraph.constants import TAG_NOSTREAM

from ptc_agent.agent.middleware.compaction import compact as compact_module
from ptc_agent.agent.middleware.compaction import middleware as mw_module
from ptc_agent.agent.middleware.compaction.compact import Summarizer
from ptc_agent.agent.middleware.compaction.measure import Estimate
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.summarize import PROVIDER_LEAD
from ptc_agent.agent.middleware.compaction.summary_request import summary_prompt
from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.middleware.compaction.utils import parse_summary_message, summary_source
from src.llms.provider_compaction import COMPACTION_BETA, SIDECAR_KEY, is_compaction_request

pytestmark = pytest.mark.asyncio

_BLOCK = {"type": "compaction", "content": "the provider's summary", "signature": "sig-1"}
def _user_item(text: str) -> dict:
    return {"type": "message", "role": "user", "content": [{"type": "input_text", "text": text}]}


_COMPACTION_ITEM = {"type": "compaction", "encrypted_content": "opaque"}


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    for var in ("OPENAI_BASE_URL", "OPENAI_API_BASE", "ANTHROPIC_BASE_URL", "ANTHROPIC_API_URL"):
        monkeypatch.delenv(var, raising=False)

    async def _passthrough(backend, messages, **kwargs):
        return messages

    monkeypatch.setattr(compact_module, "aoffload_base64_content", _passthrough)
    monkeypatch.setattr(mw_module, "get_stream_writer", lambda: lambda payload: None)


def _claude() -> ChatAnthropic:
    return ChatAnthropic(model="claude-test", api_key="x", betas=[COMPACTION_BETA])


def _gpt() -> ChatOpenAI:
    return ChatOpenAI(model="gpt-test", api_key="x", output_version="responses/v1")


def _summary_model(reply: str = "the summarizer's summary") -> MagicMock:
    model = MagicMock()
    model.model_name = "summary-model"
    model.sent = []

    async def astream(request, config=None):
        model.sent.append(request)
        yield AIMessageChunk(content=reply)

    model.astream = astream
    return model


def _history() -> list:
    """Eight messages; the summarizer's cutoff with two kept lands on h2, the
    latest point Claude can cut at is a2, right after a tool result."""
    return [
        HumanMessage("q0", id="h0"),
        AIMessage("r0", id="a0"),
        HumanMessage("q1", id="h1"),
        AIMessage("", id="a1", tool_calls=[{"name": "Bash", "id": "c1", "args": {}}]),
        ToolMessage("out", tool_call_id="c1", id="t1"),
        AIMessage("r1", id="a2"),
        HumanMessage("q2", id="h2"),
        AIMessage("r2", id="a3"),
    ]


class _Handler:
    """The rest of the stack: answers a compaction request with ``answer``
    (or raises it) and anything else as the turn, recording each request and
    the tags its call ran under."""

    def __init__(self, answer):
        self.answer = answer
        self.requests: list[ModelRequest] = []
        self.tags: list[list[str]] = []

    async def __call__(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        self.tags.append(list((var_child_runnable_config.get() or {}).get("tags") or ()))
        if is_compaction_request(request.model_settings):
            if isinstance(self.answer, BaseException):
                raise self.answer
            return ModelResponse(result=[self.answer])
        return ModelResponse(result=[AIMessage("done", id="turn-answer")])

    @property
    def compaction_requests(self) -> list[ModelRequest]:
        return [r for r in self.requests if is_compaction_request(r.model_settings)]


async def _run(model, answer, *, native=True, summary_model=None, history=None):
    summary_model = summary_model or _summary_model()
    mw = CompactionMiddleware(
        Summarizer(summary_model, limit=100_000, counter=len),
        token_threshold=6,
        keep_tokens=2,
        native=native,
    )
    # A token a message for Claude too, so the kept part is the newest two.
    mw._estimate = lambda model: Estimate(len)
    handler = _Handler(answer)
    history = history or _history()
    request = ModelRequest(
        model=model,
        messages=history,
        system_message=SystemMessage("the agent's system prompt"),
        tools=[],
        state={"messages": history},
        runtime=None,
        model_settings={"max_tokens": 1000},
    )
    response = await mw.awrap_model_call(request, handler)
    event = response.command.update["_summarization_event"]
    return SimpleNamespace(
        handler=handler,
        summary=event["summary_message"],
        summary_model=summary_model,
        turn=handler.requests[-1],
    )


def _claude_answer(**metadata) -> AIMessage:
    return AIMessage(
        content=[{**_BLOCK, "index": 0}],
        response_metadata={"stop_reason": "compaction", **metadata},
    )


class TestClaude:
    async def test_the_compaction_request_is_the_conversations_own_call(self):
        run = await _run(_claude(), _claude_answer(reasoning_origin="anthropic"))

        (sent,) = run.handler.compaction_requests
        assert sent.model_settings["extra_body"]["compaction"] == {
            "type": "summarize",
            "instructions": summary_prompt("native", True),
        }
        assert sent.model_settings["max_tokens"] == 1000
        assert sent.system_message.content == "the agent's system prompt"
        # Exactly the summarized part: Claude cuts where the kept part opens
        # on an assistant turn, a2 here rather than the summarizer's h2.
        assert [m.id for m in sent.messages] == ["h0", "a0", "h1", "a1", "t1"]
        assert run.summary_model.sent == []

    async def test_the_compaction_call_stays_out_of_the_stream(self):
        run = await _run(_claude(), _claude_answer(reasoning_origin="anthropic"))

        compaction_tags, turn_tags = run.handler.tags[0], run.handler.tags[-1]
        assert TAG_NOSTREAM in compaction_tags
        assert TAG_NOSTREAM not in turn_tags

    async def test_the_summary_carries_the_block(self):
        run = await _run(_claude(), _claude_answer(reasoning_origin="anthropic"))

        assert parse_summary_message(run.summary) == "the provider's summary"
        assert summary_source(run.summary) == "provider"
        carried = run.summary.additional_kwargs[SIDECAR_KEY]
        assert carried["block"] == _BLOCK
        assert carried["origin"] == "anthropic"

    async def test_the_turn_is_sent_the_summary_then_the_kept_part(self):
        run = await _run(_claude(), _claude_answer(reasoning_origin="anthropic"))

        assert [m.id for m in run.turn.messages] == [run.summary.id, "a2", "h2", "a3"]
        assert not is_compaction_request(run.turn.model_settings)

    @pytest.mark.parametrize(
        "answer",
        [
            AIMessage("I would rather answer", response_metadata={"stop_reason": "end_turn"}),
            AIMessage(content=[{"type": "compaction", "content": "unsigned"}]),
            AIMessage(content=[_BLOCK], response_metadata={"stop_reason": "max_tokens"}),
            RuntimeError("upstream down"),
        ],
        ids=["no-block", "unsigned", "cut-off", "error"],
    )
    async def test_no_compaction_falls_back_to_the_summarizer(self, answer):
        run = await _run(_claude(), answer)

        assert len(run.handler.compaction_requests) == 1
        assert len(run.summary_model.sent) == 1
        assert summary_source(run.summary) == "model"
        assert parse_summary_message(run.summary) == "the summarizer's summary"
        assert SIDECAR_KEY not in run.summary.additional_kwargs

    async def test_turned_off_no_compaction_request_is_sent(self):
        run = await _run(_claude(), _claude_answer(), native=False)

        assert run.handler.compaction_requests == []
        assert summary_source(run.summary) == "model"
        # The summarizer's own cutoff, h2.
        assert [m.id for m in run.turn.messages] == [run.summary.id, "h2", "a3"]

    async def test_a_client_without_the_beta_sends_none(self):
        client = ChatAnthropic(model="claude-test", api_key="x")
        run = await _run(client, _claude_answer())

        assert run.handler.compaction_requests == []


class TestOpenAI:
    def _answer(self) -> AIMessage:
        return AIMessage(
            content=[{**_COMPACTION_ITEM, "id": "cmp_1", "index": 0}],
            response_metadata={"reasoning_origin": "openai"},
        )

    async def test_turned_off_no_compaction_request_is_sent(self):
        run = await _run(_gpt(), self._answer(), native=False)

        assert run.handler.compaction_requests == []
        assert summary_source(run.summary) == "model"

    async def test_the_summary_is_a_digest_beside_the_items(self):
        run = await _run(_gpt(), self._answer())

        (sent,) = run.handler.compaction_requests
        # No assistant-turn cut for OpenAI: the summarizer's own cutoff.
        assert [m.id for m in sent.messages] == ["h0", "a0", "h1", "a1", "t1", "a2"]
        assert run.summary_model.sent == []
        text = parse_summary_message(run.summary)
        assert text.startswith(PROVIDER_LEAD)
        assert "q0" in text
        assert summary_source(run.summary) == "provider"
        carried = run.summary.additional_kwargs[SIDECAR_KEY]
        assert carried["items"] == [_user_item("q0"), _user_item("q1"), _COMPACTION_ITEM]
        assert carried["origin"] == "openai"
        assert run.summary.content.startswith(CONTEXT_SUMMARY_PREFIX)

    async def test_the_item_follows_only_what_the_user_wrote(self):
        """Runtime rows are left to the item, and the messages an earlier
        item was sent after are kept again, ahead of the newer ones."""
        earlier = HumanMessage(
            "[Context Summary] digest",
            id="s0",
            additional_kwargs={
                "lc_source": "summarization",
                SIDECAR_KEY: {
                    "items": [_user_item("from before"), _COMPACTION_ITEM],
                    "origin": "openai",
                    "text": "",
                },
            },
        )
        row = HumanMessage("12:49 PM EDT", id="r0", additional_kwargs={"lc_source": "runtime_update"})
        history = [earlier, row, *_history()]

        run = await _run(_gpt(), self._answer(), history=history)

        (sent,) = run.handler.compaction_requests
        assert sent.messages[0].id == "s0"
        items = run.summary.additional_kwargs[SIDECAR_KEY]["items"]
        assert items == [
            _user_item("from before"),
            _user_item("q0"),
            _user_item("q1"),
            _COMPACTION_ITEM,
        ]


class TestTheFeature:
    """Read when the middleware is built, so a build that resolves no user
    still honors an operator who turned it off."""

    @staticmethod
    def _built(features, native=True):
        from unittest.mock import patch

        from ptc_agent.config import AgentConfig, LLMConfig
        from ptc_agent.config.core import (
            DaytonaConfig,
            FilesystemConfig,
            LoggingConfig,
            MCPConfig,
            SandboxConfig,
            SecurityConfig,
        )

        config = AgentConfig(
            llm=LLMConfig(name="main-model"),
            security=SecurityConfig(),
            logging=LoggingConfig(),
            sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
            mcp=MCPConfig(),
            filesystem=FilesystemConfig(),
        )
        config.features = features
        config.compaction.native = native
        with patch.object(compact_module, "create_llm", return_value=MagicMock()):
            return CompactionMiddleware.for_agent(config)

    @pytest.mark.parametrize(
        ("features", "native", "expected"),
        [
            ({"provider_compaction": True}, True, True),
            ({"provider_compaction": True}, False, False),
            ({"provider_compaction": False}, True, False),
        ],
    )
    async def test_the_feature_over_the_users_choice(self, features, native, expected):
        assert self._built(features, native)._native is expected

    async def test_a_build_with_no_user_reads_the_operators_default(self, monkeypatch):
        import src.config.features as features_module

        monkeypatch.setattr(features_module, "default_feature_enabled", lambda key: False)
        assert self._built(None)._native is False
