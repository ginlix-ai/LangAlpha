"""A provider's own compaction: which models take it, the request that asks
for it, the answer kept beside a summary, and where that answer is sent back.

Clients are real langchain clients built offline, since ``native_kind`` reads
their fields; the env is cleared of base-url overrides so the official host is
what an unset ``base_url`` resolves to.
"""

from __future__ import annotations

import pytest
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI

from src.llms.provider_compaction import (
    COMPACTION_BETA,
    SIDECAR_KEY,
    adopt,
    carried_text,
    compaction_settings,
    is_compaction_request,
    is_opaque,
    kept_user_items,
    kept_user_texts,
    native_answer,
    native_compaction_kind,
    native_kind,
    replays,
    without_blocks,
)
from src.llms.reasoning_lineage import ROUTE_ENDPOINT_SEP


@pytest.fixture(autouse=True)
def _official_hosts(monkeypatch):
    for var in ("OPENAI_BASE_URL", "OPENAI_API_BASE", "ANTHROPIC_BASE_URL", "ANTHROPIC_API_URL"):
        monkeypatch.delenv(var, raising=False)


def _claude(**kwargs) -> ChatAnthropic:
    kwargs.setdefault("betas", [COMPACTION_BETA])
    return ChatAnthropic(model="claude-test", api_key="x", **kwargs)


def _gpt(**kwargs) -> ChatOpenAI:
    kwargs.setdefault("output_version", "responses/v1")
    return ChatOpenAI(model="gpt-test", api_key="x", **kwargs)


_ANTHROPIC = {"sdk": "anthropic", "access_type": "api_key"}
_OPENAI = {"sdk": "openai", "access_type": "api_key", "use_response_api": True}
_CODEX = {
    "sdk": "codex",
    "access_type": "oauth",
    "use_response_api": True,
    "base_url": "https://chatgpt.com/backend-api/codex",
}
_WITH_BETA = {"additional_betas": ["some-other-beta", COMPACTION_BETA]}


class TestNativeCompactionKind:
    """What the settings offer per manifest model: Claude's where the entry
    sends the beta, on its key or its subscription, OpenAI's on the official
    Responses endpoint and on Codex, and nothing on a repointed route."""

    @pytest.mark.parametrize(
        ("entry", "provider", "expected"),
        [
            (_WITH_BETA, _ANTHROPIC, "anthropic"),
            ({"additional_betas": ["some-other-beta"]}, _ANTHROPIC, None),
            ({}, _ANTHROPIC, None),
            (_WITH_BETA, {**_ANTHROPIC, "base_url": "https://api.anthropic.com"}, "anthropic"),
            (_WITH_BETA, {**_ANTHROPIC, "base_url": "https://proxy.example"}, None),
            (_WITH_BETA, {**_ANTHROPIC, "access_type": "oauth"}, "anthropic"),
            (
                _WITH_BETA,
                {**_ANTHROPIC, "access_type": "oauth", "base_url": "https://api.anthropic.com"},
                "anthropic",
            ),
            ({}, {**_ANTHROPIC, "access_type": "oauth"}, None),
            ({"model_id": "gpt-test"}, _OPENAI, "openai"),
            ({"model_id": "gpt-test"}, {**_OPENAI, "base_url": "https://api.openai.com/v1"}, "openai"),
            ({"model_id": "gpt-test"}, {**_OPENAI, "base_url": "https://proxy.example/v1"}, None),
            ({"model_id": "gpt-test"}, {**_OPENAI, "use_response_api": False}, None),
            ({"model_id": "gpt-test"}, {**_OPENAI, "access_type": "oauth"}, None),
            ({"model_id": "gpt-test"}, {**_OPENAI, "sdk": "codex"}, None),
            ({"model_id": "gpt-test"}, _CODEX, "openai"),
            ({"model_id": "gpt-test"}, {**_CODEX, "use_response_api": False}, None),
            (None, _ANTHROPIC, None),
            (_WITH_BETA, None, None),
        ],
    )
    def test_kind(self, entry, provider, expected):
        assert native_compaction_kind(entry, provider) == expected

    def test_openai_reads_the_env_base_url_when_the_entry_sets_none(self, monkeypatch):
        monkeypatch.setenv("OPENAI_BASE_URL", "https://proxy.example/v1")
        assert native_compaction_kind({"model_id": "gpt-test"}, _OPENAI) is None

    @pytest.mark.parametrize("var", ["ANTHROPIC_API_URL", "ANTHROPIC_BASE_URL"])
    def test_claude_reads_the_env_base_url_when_the_entry_sets_none(self, monkeypatch, var):
        monkeypatch.setenv(var, "https://proxy.example")
        assert native_compaction_kind(_WITH_BETA, _ANTHROPIC) is None


class TestNativeKind:
    """Read off the client: only the official hosts, and for Claude only a
    client that sends the beta on every request."""

    def test_claude_with_the_beta_in_betas(self):
        assert native_kind(_claude()) == "anthropic"

    @pytest.mark.parametrize("key", ["anthropic-beta", "Anthropic-Beta"])
    def test_claude_with_the_beta_in_the_header(self, key):
        client = _claude(betas=None, default_headers={key: f"other-beta, {COMPACTION_BETA}"})
        assert native_kind(client) == "anthropic"

    def test_claude_without_the_beta(self):
        assert native_kind(_claude(betas=["other-beta"])) is None

    def test_claude_on_the_official_host_named_explicitly(self):
        assert native_kind(_claude(anthropic_api_url="https://api.anthropic.com")) == "anthropic"

    def test_claude_on_another_host(self):
        assert native_kind(_claude(anthropic_api_url="https://proxy.example")) is None

    def test_gpt_on_the_official_responses_endpoint(self):
        assert native_kind(_gpt()) == "openai"
        assert native_kind(_gpt(base_url="https://api.openai.com/v1")) == "openai"

    def test_gpt_on_another_host(self):
        assert native_kind(_gpt(base_url="https://proxy.example/v1")) is None

    def test_gpt_without_responses_output(self):
        assert native_kind(_gpt(output_version=None)) is None

    def test_codex_on_its_backend(self):
        from src.llms.extension.codex import ChatCodexOpenAI

        def codex(**kwargs):
            kwargs.setdefault("output_version", "responses/v1")
            return ChatCodexOpenAI(model="gpt-test", api_key="x", **kwargs)

        assert native_kind(codex(base_url="https://chatgpt.com/backend-api/codex")) == "openai"
        assert native_kind(codex(base_url="https://proxy.example/v1")) is None
        assert native_kind(codex()) is None
        assert native_kind(codex(base_url="https://chatgpt.com/backend-api/codex", output_version=None)) is None

    def test_dashscope_is_not_openai(self):
        from src.llms.extension.dashscope import ChatDashScope

        dashscope = ChatDashScope(
            model="qwen-test",
            api_key="x",
            output_version="responses/v1",
            base_url="https://api.openai.com/v1",
        )
        assert native_kind(dashscope) is None

    @pytest.mark.parametrize("client", [_claude, _gpt])
    def test_a_repointed_route_is_off(self, client):
        route = f"anthropic{ROUTE_ENDPOINT_SEP}https://proxy.example"
        assert native_kind(client(metadata={"provider_route": route})) is None

    @pytest.mark.parametrize("client", [_claude, _gpt])
    def test_a_manifest_route_stays_on(self, client):
        assert native_kind(client(metadata={"provider_route": "anthropic"})) is not None

    def test_anything_else(self):
        assert native_kind(object()) is None
        assert native_kind(None) is None


class TestCompactionSettings:
    def test_extra_body_is_merged_from_the_client_and_the_call(self):
        client = _claude(model_kwargs={"extra_body": {"thinking": {"type": "adaptive"}, "a": 1}})
        settings = {"max_tokens": 10, "extra_body": {"a": 2, "b": 3}}

        result = compaction_settings(client, settings, "summarize this")

        assert result == {
            "max_tokens": 10,
            # The client's own header joins these on the way out (patches).
            "betas": ["inline-tools-2026-09-15"],
            "extra_body": {
                "thinking": {"type": "adaptive"},
                "a": 2,
                "b": 3,
                "compaction": {"type": "summarize", "instructions": "summarize this"},
            },
        }
        assert settings == {"max_tokens": 10, "extra_body": {"a": 2, "b": 3}}
        assert is_compaction_request(result)

    def test_claudes_request_keeps_the_betas_the_call_already_sends(self):
        settings = {"betas": ["context-management-2025-06-27"]}

        result = compaction_settings(_claude(), settings, "p")

        assert result["betas"] == ["context-management-2025-06-27", "inline-tools-2026-09-15"]

    def test_extra_body_an_openai_client_was_built_with_is_kept(self):
        client = _gpt(extra_body={"a": 1, "b": 1})

        result = compaction_settings(client, {"extra_body": {"b": 2}}, "p")

        assert result["extra_body"] == {
            "a": 1,
            "b": 2,
            "compaction": {"type": "summarize", "instructions": "p"},
        }

    def test_a_client_without_extra_body(self):
        result = compaction_settings(object(), {}, "p")
        assert result == {"extra_body": {"compaction": {"type": "summarize", "instructions": "p"}}}

    @pytest.mark.parametrize(
        "settings",
        [{}, {"extra_body": None}, {"extra_body": {"thinking": {}}}, {"compaction": {}}],
    )
    def test_other_settings_are_not_a_compaction_request(self, settings):
        assert not is_compaction_request(settings)


_BLOCK = {"type": "compaction", "content": "what happened", "signature": "sig-1"}
_USER_ITEM = {
    "type": "message",
    "role": "user",
    "id": "msg_1",
    "content": [{"type": "input_text", "text": "the first request"}],
}
_COMPACTION_ITEM = {"type": "compaction", "encrypted_content": "opaque"}


def _claude_reply(*content, stop_reason="compaction") -> AIMessage:
    return AIMessage(content=list(content), response_metadata={"stop_reason": stop_reason})


class TestNativeAnswer:
    def test_a_signed_block_reads_as_claude_even_with_encrypted_content(self):
        message = _claude_reply({**_BLOCK, "encrypted_content": "e"})
        assert native_answer(message) == {"block": _BLOCK}

    def test_a_claude_block_is_cleaned_to_what_the_api_takes_back(self):
        message = _claude_reply({**_BLOCK, "index": 0, "extra": None})
        assert native_answer(message) == {"block": _BLOCK}

    def test_a_claude_block_after_other_blocks(self):
        message = _claude_reply({"type": "text", "text": "x"}, {**_BLOCK, "index": 1})
        assert native_answer(message) == {"block": _BLOCK}

    def test_a_claude_block_without_a_signature_is_no_answer(self):
        message = _claude_reply({"type": "compaction", "content": "what happened"})
        assert native_answer(message) is None

    @pytest.mark.parametrize("stop_reason", ["max_tokens", "refusal", "end_turn", None])
    def test_a_claude_block_that_did_not_finish_is_no_answer(self, stop_reason):
        """Only a ``compaction`` stop says the summary was written to its end."""
        assert native_answer(_claude_reply(_BLOCK, stop_reason=stop_reason)) is None

    def test_openai_answers_with_the_item_alone_without_its_id(self):
        streamed = {**_COMPACTION_ITEM, "id": "cmp_1", "index": 1}
        reasoning = {"type": "reasoning", "id": "rs_1", "summary": []}
        message = AIMessage(content=[reasoning, streamed])
        assert native_answer(message) == {"items": [_COMPACTION_ITEM]}

    @pytest.mark.parametrize(
        "message",
        [
            None,
            AIMessage(content="a plain answer"),
            AIMessage(content=[{"type": "text", "text": "a plain answer"}]),
            AIMessage(content=[_USER_ITEM]),
        ],
    )
    def test_no_compaction_is_no_answer(self, message):
        assert native_answer(message) is None


def _summary(sidecar: dict, *, content: str = "[Context Summary] readable") -> HumanMessage:
    return HumanMessage(
        content=content,
        id="summary",
        additional_kwargs={SIDECAR_KEY: sidecar, "lc_source": "summarization"},
    )


class TestAdopt:
    """A summary is sent as its provider's answer only to a client that can
    take it, inside the lineage that wrote it; anywhere else it reads its
    text, and nothing in the checkpoint changes."""

    def _history(self, summary: HumanMessage) -> list:
        return [summary, AIMessage("kept reply", id="a1"), HumanMessage("next", id="h1")]

    def test_claude_block_in_the_same_lineage(self):
        summary = _summary(
            {"block": _BLOCK, "origin": "anthropic", "text": "notes after it", "alone": True}
        )
        history = self._history(summary)

        adopted = adopt(history, _claude(), "anthropic")

        assert adopted is not None
        # The kept turns follow the block with nothing between: the notes
        # are for a message after them.
        assert adopted[0].content == [_BLOCK]
        assert carried_text(summary) == "notes after it"
        assert adopted[0].id == "summary"
        assert adopted[1:] == history[1:]
        assert summary.content == "[Context Summary] readable"

    def test_a_summary_written_before_the_block_went_alone_keeps_its_text(self):
        """The thinking after such a summary was produced behind the block and
        its notes in one message, so they are sent that way still."""
        summary = _summary({"block": _BLOCK, "origin": "anthropic", "text": "notes after it"})

        adopted = adopt(self._history(summary), _claude(), "anthropic")

        assert adopted[0].content == [_BLOCK, {"type": "text", "text": "notes after it"}]
        assert isinstance(adopted[0], HumanMessage)

    def test_claude_block_without_notes_sends_the_block_alone(self):
        summary = _summary({"block": _BLOCK, "origin": "anthropic", "text": ""})
        assert adopt([summary], _claude(), "anthropic")[0].content == [_BLOCK]

    def test_claude_block_written_on_another_route_of_the_lineage(self):
        summary = _summary({"block": _BLOCK, "origin": "claude-oauth", "text": ""})
        assert adopt([summary], _claude(), "anthropic")[0].content == [_BLOCK]

    @pytest.mark.parametrize(
        "origin", ["openai", None, f"anthropic{ROUTE_ENDPOINT_SEP}https://proxy.example"]
    )
    def test_another_lineage_reads_the_text(self, origin):
        summary = _summary({"block": _BLOCK, "origin": origin, "text": ""})
        assert adopt(self._history(summary), _claude(), "anthropic") is None

    def test_a_client_that_cannot_take_it_reads_the_text(self):
        summary = _summary({"block": _BLOCK, "origin": "anthropic", "text": ""})
        assert adopt([summary], _claude(betas=None), "anthropic") is None
        assert adopt([summary], _gpt(), "anthropic") is None

    def test_openai_items_are_expanded(self):
        developer = {"type": "message", "role": "developer", "content": [{"type": "input_text", "text": "sys"}]}
        summary = _summary(
            {
                "items": [developer, _USER_ITEM, _COMPACTION_ITEM],
                "origin": "openai",
                "text": "notes after it",
            }
        )
        history = self._history(summary)

        adopted = adopt(history, _gpt(), "openai")

        assert adopted is not None
        user, compaction, notes, *rest = adopted
        assert isinstance(user, HumanMessage)
        assert user.content == _USER_ITEM["content"]
        assert isinstance(compaction, AIMessage)
        assert compaction.content == [{"type": "compaction", "encrypted_content": "opaque"}]
        assert isinstance(notes, HumanMessage) and notes.content == "notes after it"
        assert rest == history[1:]

    def test_only_user_messages_and_the_item_are_sent(self):
        assistant = {
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "earlier answer"}],
        }
        output = {"type": "function_call_output", "call_id": "call_1", "output": "42"}
        summary = _summary(
            {"items": [assistant, output, _USER_ITEM, _COMPACTION_ITEM], "origin": "openai", "text": ""}
        )

        user, compaction = adopt([summary], _gpt(), "openai")

        assert user.content == _USER_ITEM["content"]
        assert compaction.content == [_COMPACTION_ITEM]

    def test_openai_items_without_notes(self):
        summary = _summary({"items": [_USER_ITEM, _COMPACTION_ITEM], "origin": "openai", "text": ""})

        adopted = adopt([summary], _gpt(), "openai")

        assert [type(m) for m in adopted] == [HumanMessage, AIMessage]

    def test_a_claude_block_is_not_sent_to_openai(self):
        summary = _summary({"block": _BLOCK, "origin": "openai", "text": ""})
        assert adopt([summary], _gpt(), "openai") is None

    @pytest.mark.parametrize(("origin", "route"), [("openai", "codex-oauth"), ("codex-oauth", "openai")])
    def test_openai_items_cross_between_the_api_and_codex(self, origin, route):
        """Each route keeps its reasoning to itself, but an item from either
        decrypts on the other, so a switch between them keeps it."""
        summary = _summary({"items": [_COMPACTION_ITEM], "origin": origin, "text": ""})
        assert adopt([summary], _gpt(), route)[0].content == [_COMPACTION_ITEM]

    def test_openai_items_are_not_sent_to_claude(self):
        summary = _summary({"items": [_COMPACTION_ITEM], "origin": "anthropic", "text": ""})
        assert adopt([summary], _claude(), "anthropic") is None

    def test_only_summary_messages_are_touched(self):
        sidecar = {SIDECAR_KEY: {"block": _BLOCK, "origin": "anthropic", "text": ""}}
        history = [
            SystemMessage("sys", additional_kwargs=sidecar),
            AIMessage("reply", additional_kwargs=sidecar),
            ToolMessage("result", tool_call_id="c1"),
            HumanMessage("plain"),
        ]
        assert adopt(history, _claude(), "anthropic") is None

    def test_a_summary_built_with_a_native_answer_round_trips(self):
        from ptc_agent.agent.middleware.compaction.utils import build_summary_message

        summary = build_summary_message(
            "what happened", None, source="provider", native={"block": _BLOCK}, origin="anthropic"
        )

        assert summary.additional_kwargs[SIDECAR_KEY]["origin"] == "anthropic"
        assert adopt([summary], _claude(), "anthropic")[0].content[0] == _BLOCK
        assert adopt([summary], _claude(), "openai") is None


class TestWithoutBlocks:
    def test_strips_every_sidecar(self):
        summary = _summary({"block": _BLOCK, "origin": "anthropic", "text": ""})
        history = [summary, AIMessage("reply"), HumanMessage("plain")]

        readable = without_blocks(history)

        assert readable is not None
        assert SIDECAR_KEY not in readable[0].additional_kwargs
        assert readable[0].additional_kwargs["lc_source"] == "summarization"
        assert readable[0].content == summary.content
        assert readable[1:] == history[1:]
        assert SIDECAR_KEY in summary.additional_kwargs

    def test_none_when_nothing_carried_one(self):
        assert without_blocks([HumanMessage("plain"), AIMessage("reply")]) is None

    @pytest.mark.parametrize(
        "carried",
        [
            {"block": _BLOCK, "origin": "anthropic", "text": ""},
            # OpenAI answers an item it cannot decrypt with a 400
            # (invalid_encrypted_content) before the stream opens.
            {"items": [_USER_ITEM, _COMPACTION_ITEM], "origin": "openai", "text": ""},
        ],
        ids=["claude", "openai"],
    )
    def test_the_reasoning_retry_drops_them(self, carried):
        """The retry after a provider refused the request is made only when
        stripping changed something, and a sidecar counts."""
        from src.llms.reasoning_payload import strip_all_reasoning

        history = [_summary(carried), AIMessage("reply")]
        plain = [HumanMessage("plain"), AIMessage("reply")]

        stripped = strip_all_reasoning(history)

        assert stripped is not history
        assert SIDECAR_KEY not in stripped[0].additional_kwargs
        assert strip_all_reasoning(plain) is plain


class TestIsOpaque:
    def test_openai_items_are_opaque(self):
        assert is_opaque(_summary({"items": [_COMPACTION_ITEM], "origin": "openai", "text": ""}))

    def test_a_claude_block_is_readable(self):
        assert not is_opaque(_summary({"block": _BLOCK, "origin": "anthropic", "text": ""}))

    def test_a_plain_message(self):
        assert not is_opaque(HumanMessage("plain"))


class TestReplays:
    """Whether a model a thread moves to is sent the summary's item, as
    ``adopt`` would send it, or reads its digest: only an OpenAI client in the
    lineage that wrote the item, its route read off the client."""

    _ITEMS = {"items": [_USER_ITEM, _COMPACTION_ITEM], "origin": "openai", "text": ""}

    @pytest.mark.parametrize("route", ["openai", "codex-oauth"])
    def test_an_openai_client_in_the_items_lineage(self, route):
        assert replays(_summary(self._ITEMS), _gpt(metadata={"provider_route": route}))

    @pytest.mark.parametrize(
        "client",
        [
            pytest.param(lambda: _gpt(metadata={"provider_route": "anthropic"}), id="other-route"),
            pytest.param(lambda: _gpt(), id="no-route"),
            pytest.param(
                lambda: _gpt(metadata={"provider_route": "openai"}, output_version=None),
                id="no-responses",
            ),
            pytest.param(
                lambda: _gpt(
                    metadata={"provider_route": "openai"}, base_url="https://proxy.example/v1"
                ),
                id="other-host",
            ),
            pytest.param(lambda: _claude(metadata={"provider_route": "anthropic"}), id="claude"),
            pytest.param(lambda: object(), id="not-a-client"),
        ],
    )
    def test_any_other_model_reads_the_digest(self, client):
        assert not replays(_summary(self._ITEMS), client())

    def test_a_message_carrying_no_answer(self):
        client = _gpt(metadata={"provider_route": "openai"})
        assert not replays(HumanMessage("[Context Summary] readable", id="s"), client)

    def test_a_claude_block_in_its_lineage_replays_to_claude(self):
        summary = _summary({"block": _BLOCK, "origin": "anthropic", "text": ""})
        assert replays(summary, _claude(metadata={"provider_route": "anthropic"}))
        assert not replays(summary, _gpt(metadata={"provider_route": "openai"}))


def _texts(items: list[dict]) -> list[str]:
    return [item["content"][0]["text"] for item in items]


class TestKeptUserItems:
    """The user messages OpenAI's item goes after, kept as Codex keeps them:
    newest first up to a budget, the one crossing it cut, older dropped."""

    def test_all_kept_in_order_under_the_budget(self):
        items = kept_user_items(["first", "", "second"])
        assert items == [
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "first"}]},
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "second"}]},
        ]

    def test_the_newest_win_and_the_one_crossing_the_budget_is_cut(self, monkeypatch):
        from src.llms import provider_compaction

        monkeypatch.setattr(provider_compaction, "KEPT_USER_TOKENS", 3)  # 12 bytes

        assert _texts(kept_user_items(["oldest", "abcdefgh", "newest"])) == ["abcdef", "newest"]

    def test_a_cut_never_splits_a_character(self, monkeypatch):
        from src.llms import provider_compaction

        monkeypatch.setattr(provider_compaction, "KEPT_USER_TOKENS", 1)  # 4 bytes

        assert _texts(kept_user_items(["€€"])) == ["€"]

    def test_none(self):
        assert kept_user_items([]) == []

    def test_an_earlier_summary_hands_its_kept_messages_on(self):
        summary = _summary({"items": [_USER_ITEM, _COMPACTION_ITEM], "origin": "openai", "text": ""})
        assert kept_user_texts(summary) == ["the first request"]

    @pytest.mark.parametrize(
        "message",
        [
            _summary({"block": _BLOCK, "origin": "anthropic", "text": ""}),
            HumanMessage("plain"),
        ],
    )
    def test_nothing_to_hand_on(self, message):
        assert kept_user_texts(message) == []
