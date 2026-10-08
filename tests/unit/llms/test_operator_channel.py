"""Tests for the per-provider operator channel.

Three things are locked here:

- The resolution matrix. Config can only turn a channel off; the client in hand
  decides whether it can carry one at all, so a repointed or wrong-family model
  resolves to None no matter what its provider entry says.
- The carrier messages, whose ``additional_kwargs`` are the whole contract with
  langchain-openai (``__openai_role__``) and with our own Anthropic bridge.
- The wire payload inside and outside the Anthropic opt-in window. Section 5 of the
  runtime-context build note is explicit that a middleware-level assertion
  proves nothing here: an untagged SystemMessage is hoisted back into the
  top-level ``system`` without raising, which is the exact bug being fixed.
  These tests assert on the payload the SDK would send.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI

from src.llms.extension import ChatAnthropicOAuth, ChatCodexOpenAI
from src.llms.llm import LLM
from src.llms.operator_channel import (
    anthropic_serves_midturn_system,
    build_operator_message,
    is_operator_system_message,
    operator_request,
    resolve_operator_channel,
)
from src.llms.vendor.langchain_zai import ChatZai


@contextmanager
def provider_entry(name: str, entry: dict):
    """Add or override one provider_config entry for the duration of a test."""
    flat = LLM.get_model_config().flat_providers
    missing = object()
    previous = flat.get(name, missing)
    flat[name] = entry
    try:
        yield
    finally:
        if previous is missing:
            flat.pop(name, None)
        else:
            flat[name] = previous


def _stamp(client, provider: str):
    """Mimic the metadata ``LLM.get_llm`` stamps onto every client it builds."""
    client.metadata = {**(client.metadata or {}), "pricing_provider": provider}
    return client


def _openai(provider: str = "openai", **kwargs):
    return _stamp(ChatOpenAI(model="gpt-5.6", api_key="test-key", **kwargs), provider)


def _anthropic(provider: str = "anthropic", model: str = "claude-opus-5", **kwargs):
    return _stamp(
        ChatAnthropic(model=model, api_key="test-key", max_tokens=1024, **kwargs),
        provider,
    )


def _zai(provider: str = "z-ai-test", model: str = "glm-5.3"):
    return _stamp(ChatZai(model=model, api_key="test-key"), provider)


# ---------------------------------------------------------------------------
# Resolution matrix
# ---------------------------------------------------------------------------


def test_openai_resolves_developer_when_configured():
    with provider_entry("openai-test", {"sdk": "openai", "operator_channel": "developer"}):
        assert resolve_operator_channel(_openai("openai-test")) == "developer"


def test_openai_off_when_key_absent():
    with provider_entry("openai-test", {"sdk": "openai"}):
        assert resolve_operator_channel(_openai("openai-test")) is None


@pytest.mark.parametrize(
    ("client", "channel"),
    [
        (_openai, "developer"),
        (_anthropic, "system"),
    ],
)
def test_a_repointed_client_carries_no_channel(client, channel):
    """A BYOK base_url override keeps the provider key but not its proof.

    ``LLM.get_llm`` marks the move as ``provider_route`` ``<key>@<url>``; the
    role was probed on the declared host, not on the gateway in hand.
    """
    model = client("route-test")
    model.metadata["provider_route"] = "route-test@https://gateway.example/v1"
    entry = {"sdk": "openai" if channel == "developer" else "anthropic", "operator_channel": channel}
    with provider_entry("route-test", entry):
        assert resolve_operator_channel(model) is None


@pytest.mark.parametrize("variable", ["OPENAI_BASE_URL", "OPENAI_API_BASE"])
def test_an_env_rerouted_openai_client_carries_no_channel(monkeypatch, variable):
    """The SDK reads the override after the factory stamped the route, so the
    built client is the only place the move shows."""
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.setenv(variable, "https://gateway.example/v1")
    with provider_entry("openai-test", {"sdk": "openai", "operator_channel": "developer"}):
        assert resolve_operator_channel(_openai("openai-test")) is None


def test_a_custom_base_url_off_the_declared_host_carries_no_channel(monkeypatch):
    """A custom model's ``parameters.base_url`` reaches the constructor after
    the route is stamped, so the route stays clean while the client moves."""
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    entry = {
        "sdk": "openai",
        "operator_channel": "developer",
        "base_url": "https://api.openai.com/v1",
    }
    with provider_entry("openai-test", entry):
        moved = _openai("openai-test", base_url="https://gateway.example/v1")
        assert resolve_operator_channel(moved) is None
        assert resolve_operator_channel(_openai("openai-test")) == "developer"


def test_a_client_on_its_declared_host_keeps_the_channel(monkeypatch):
    monkeypatch.setenv("OPENAI_BASE_URL", "https://gateway.example/v1")
    entry = {
        "sdk": "openai",
        "operator_channel": "developer",
        "base_url": "https://gateway.example/v1",
    }
    with provider_entry("openai-test", entry):
        model = _openai("openai-test", base_url="https://gateway.example/v1")
        assert resolve_operator_channel(model) == "developer"


def test_anthropic_system_is_off_without_the_bridge(monkeypatch):
    """The warning in the installer promises the reminder fallback; this is it."""
    from src.llms import operator_channel

    monkeypatch.setattr(operator_channel, "_bridge_installed", False)
    with provider_entry("anthropic-test", {"sdk": "anthropic", "operator_channel": "system"}):
        assert resolve_operator_channel(_anthropic("anthropic-test")) is None


def test_manifest_operator_channels_are_valid():
    """Locks the providers.json side of the contract.

    Enabling a channel is a one-key edit, so the guard is on the value rather
    than on the count: a typo, or "system" on an endpoint that cannot serve it,
    would otherwise cost a 400 on every turn.
    """
    from src.llms.operator_channel import _ANTHROPIC_SYSTEM_HOSTS, _resolved_host

    for name, entry in LLM.get_model_config().flat_providers.items():
        channel = entry.get("operator_channel")
        if channel is None:
            continue
        assert channel in ("developer", "system"), f"{name}: bad operator_channel {channel!r}"
        if channel == "system":
            sdk = entry.get("sdk")
            assert sdk in ("anthropic", "glm"), f"{name}: 'system' needs sdk anthropic or glm"
            if sdk == "anthropic":
                assert _resolved_host(entry.get("base_url")) in _ANTHROPIC_SYSTEM_HOSTS, (
                    f"{name}: 'system' needs a host proven to serve the role"
                )


def test_codex_resolves_developer():
    """The codex path strips role "system" from the input array, not "developer"."""
    client = _stamp(
        ChatCodexOpenAI(model="gpt-5.6", api_key="test-key", output_version="responses/v1"),
        "codex-test",
    )
    with provider_entry("codex-test", {"sdk": "codex", "operator_channel": "developer"}):
        assert resolve_operator_channel(client) == "developer"


def test_anthropic_resolves_system_on_official_endpoint():
    with provider_entry("anthropic-test", {"sdk": "anthropic", "operator_channel": "system"}):
        assert resolve_operator_channel(_anthropic("anthropic-test")) == "system"


def test_anthropic_off_when_key_absent():
    with provider_entry("anthropic-test", {"sdk": "anthropic"}):
        assert resolve_operator_channel(_anthropic("anthropic-test")) is None


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.deepseek.com/anthropic",
        "https://api.minimax.io/anthropic",
        "https://api.minimaxi.com/anthropic",
        "https://api.minimax.cn/anthropic",
        "https://api.moonshot.ai/anthropic",
        "https://api.moonshot.cn/anthropic",
    ],
)
def test_anthropic_compatible_hosts_resolve_system_on_any_model(base_url):
    """Proven live on the production client: the role lands at its own position.

    These hosts carry no model split of their own, so an id the official host
    would refuse still resolves here.
    """
    client = _anthropic("compat-test", model="claude-sonnet-4-6", base_url=base_url)
    with provider_entry("compat-test", {"sdk": "anthropic", "operator_channel": "system"}):
        assert resolve_operator_channel(client) == "system"


@pytest.mark.parametrize(
    "base_url",
    [
        "https://api.z.ai/api/anthropic",
        "https://open.bigmodel.cn/api/anthropic",
        "https://ark.cn-beijing.volces.com/api/coding",
        "https://api.kimi.com/coding",
    ],
)
def test_unproven_anthropic_hosts_stay_off(base_url):
    """An Anthropic-protocol host with no live probe behind it fails closed."""
    client = _anthropic("compat-test", base_url=base_url)
    with provider_entry("compat-test", {"sdk": "anthropic", "operator_channel": "system"}):
        assert resolve_operator_channel(client) is None


@pytest.mark.parametrize(
    "model",
    [
        "claude-opus-4-8",
        "claude-sonnet-5-5-20261001",
        "claude-opus-4-10",
        "claude-haiku-9",
    ],
)
def test_official_anthropic_models_from_opus_4_8_on_take_the_role(model):
    """The cutoff is a rule, so a release nobody listed (``claude-haiku-9``) needs no edit.

    ``claude-opus-4-10`` pins the comparison as numeric rather than lexical.
    """
    client = _anthropic("anthropic-test", model=model)
    with provider_entry("anthropic-test", {"sdk": "anthropic", "operator_channel": "system"}):
        assert resolve_operator_channel(client) == "system"


@pytest.mark.parametrize(
    "model",
    [
        "claude-opus-4-7",
        "claude-sonnet-4-6",
        "claude-haiku-4-5-20251001",
        "claude-opus-4-20250514",
        "claude-3-5-haiku-latest",
    ],
)
def test_anthropic_models_without_midturn_system_forced_off(model):
    """A provider entry is coarser than the feature: these 400 on the role.

    A custom model can name any of them on the official host.
    """
    client = _anthropic("anthropic-test", model=model)
    with provider_entry("anthropic-test", {"sdk": "anthropic", "operator_channel": "system"}):
        assert resolve_operator_channel(client) is None


@pytest.mark.parametrize(
    "model_id",
    [None, "", "claude-opus", "claude-opus-5-5-fast", "Claude-Opus-5-5", "gpt-5.6"],
)
def test_an_id_that_names_no_claude_generation_fails_closed(model_id):
    assert anthropic_serves_midturn_system(None, model_id) is False


def test_zai_resolves_system_on_the_model_that_reads_it():
    """GLM's mid-list system entry needs no patch, but it is per model.

    ``glm-5.3-flash`` answers 200 and reads the same entry as a user message,
    which is worse than a refusal, so it fails closed.
    """
    with provider_entry("z-ai-test", {"sdk": "glm", "operator_channel": "system"}):
        assert resolve_operator_channel(_zai(model="glm-5.3")) == "system"
        assert resolve_operator_channel(_zai(model="glm-5.3-flash")) is None
        assert resolve_operator_channel(_zai(model="glm-4.6")) is None


def test_zai_still_takes_developer_when_that_is_what_the_entry_says():
    """ChatZai is an OpenAI-shaped client, so the developer rule reaches it too."""
    with provider_entry("z-ai-test", {"sdk": "glm", "operator_channel": "developer"}):
        assert resolve_operator_channel(_zai()) == "developer"


def test_zai_system_message_reaches_the_wire_as_role_system():
    """The OpenAI serializer emits role system for a SystemMessage without __openai_role__."""
    payload = ChatZai(model="glm-5.3", api_key="test-key")._get_request_payload(
        [
            SystemMessage("base prompt"),
            HumanMessage("hi"),
            build_operator_message("envelope body", "system"),
        ],
        stop=None,
    )
    assert [m["role"] for m in payload["messages"]] == ["system", "user", "system"]
    assert payload["messages"][-1] == {"role": "system", "content": "envelope body"}


def test_gemini_forced_off():
    """Gemini's converter never emits a mid-conversation system message."""
    genai = pytest.importorskip("langchain_google_genai")
    client = _stamp(
        genai.ChatGoogleGenerativeAI(model="gemini-3-pro", google_api_key="test-key"),
        "gemini-test",
    )
    for channel in ("developer", "system"):
        with provider_entry("gemini-test", {"sdk": "gemini", "operator_channel": channel}):
            assert resolve_operator_channel(client) is None


def test_channel_mismatched_client_forced_off():
    """An OpenAI client cannot carry "system"; an Anthropic one cannot carry "developer"."""
    with provider_entry("openai-test", {"sdk": "openai", "operator_channel": "system"}):
        assert resolve_operator_channel(_openai("openai-test")) is None
    with provider_entry("anthropic-test", {"sdk": "anthropic", "operator_channel": "developer"}):
        assert resolve_operator_channel(_anthropic("anthropic-test")) is None


def test_unknown_value_and_unknown_provider_are_off():
    with provider_entry("openai-test", {"sdk": "openai", "operator_channel": "operator"}):
        assert resolve_operator_channel(_openai("openai-test")) is None
    assert resolve_operator_channel(_openai("not-in-the-manifest")) is None


def test_unstamped_model_is_off():
    """A client built outside LLM.get_llm carries no provider identity."""
    assert resolve_operator_channel(ChatOpenAI(model="gpt-5.6", api_key="test-key")) is None
    assert resolve_operator_channel(object()) is None


# ---------------------------------------------------------------------------
# Message building
# ---------------------------------------------------------------------------


def test_build_developer_message():
    msg = build_operator_message("envelope", "developer")
    assert isinstance(msg, SystemMessage)
    assert msg.content == "envelope"
    assert msg.additional_kwargs == {
        "__openai_role__": "developer",
        "lc_source": "runtime_context",
    }
    assert not is_operator_system_message(msg)


def test_build_system_message():
    msg = build_operator_message("envelope", "system")
    assert isinstance(msg, SystemMessage)
    assert msg.additional_kwargs == {
        "lc_operator_channel": "system",
        "lc_source": "runtime_context",
    }
    assert is_operator_system_message(msg)


def test_build_tags_the_writer_that_asked_for_the_carrier():
    """A durable row and the envelope share the channel, not the provenance."""
    row = build_operator_message("a row", "system", source="runtime_update")
    assert row.additional_kwargs == {
        "lc_operator_channel": "system",
        "lc_source": "runtime_update",
    }
    # The tag is provenance only: the channel still routes the same way.
    assert is_operator_system_message(row)
    assert build_operator_message("a row", "developer", source="runtime_update").additional_kwargs[
        "__openai_role__"
    ] == "developer"


def test_build_rejects_unknown_channel():
    with pytest.raises(ValueError):
        build_operator_message("envelope", "operator")  # type: ignore[arg-type]


def test_developer_message_reaches_the_wire_as_developer_role():
    """langchain-openai's own mapping, on both request layouts."""
    completions = ChatOpenAI(model="gpt-5.6", api_key="test-key")
    payload = completions._get_request_payload(
        [HumanMessage("hi"), build_operator_message("envelope", "developer")], stop=None
    )
    assert [m["role"] for m in payload["messages"]] == ["user", "developer"]

    codex = ChatCodexOpenAI(model="gpt-5.6", api_key="test-key", output_version="responses/v1")
    codex_payload = codex._get_request_payload(
        [
            SystemMessage("base prompt"),
            HumanMessage("hi"),
            build_operator_message("envelope", "developer"),
        ],
        stop=None,
    )
    # The base system prompt is promoted to `instructions`; the developer item
    # survives in the input array because the codex filter keys on role
    # "system" alone.
    assert codex_payload["instructions"] == "base prompt"
    assert [item["role"] for item in codex_payload["input"]] == ["user", "developer"]


# ---------------------------------------------------------------------------
# Anthropic wire payload: the opt-in window
# ---------------------------------------------------------------------------


def _client():
    return ChatAnthropic(model="claude-opus-5", api_key="test-key", max_tokens=1024)


def _payload(messages, model=None):
    """The payload the stock client builds inside the window the tail opens."""
    with operator_request("system"):
        return (model or _client())._get_request_payload(messages, stop=None)


def _conversation():
    return [
        SystemMessage("base prompt"),
        HumanMessage("first question"),
        AIMessage("first answer"),
        HumanMessage("second question"),
        build_operator_message("envelope body", "system"),
    ]


@pytest.mark.parametrize("shape", [None, "reminder", "developer"], ids=str)
def test_outside_the_window_the_tagged_message_is_refused(shape):
    """The stock hoist is what every call gets until the tail opens the window
    for a request it composed in the system shape."""
    with pytest.raises(ValueError, match="multiple non-consecutive system messages"):
        with operator_request(shape):
            _client()._get_request_payload(_conversation(), stop=None)


def test_window_closes_with_the_call():
    from src.llms.operator_channel import _in_operator_request

    with operator_request("system"):
        assert _in_operator_request.get() is True
    assert _in_operator_request.get() is False
    with pytest.raises(ValueError, match="multiple non-consecutive system messages"):
        _client()._get_request_payload(_conversation(), stop=None)


def test_windowed_payload_carries_the_system_message_in_messages():
    payload = _payload(_conversation())

    assert payload["system"] == "base prompt"
    assert payload["messages"] == [
        {"role": "user", "content": "first question"},
        {"role": "assistant", "content": "first answer"},
        {"role": "user", "content": "second question"},
        {"role": "system", "content": [{"type": "text", "text": "envelope body"}]},
    ]


def test_oauth_client_splices_under_its_identity_block():
    client = ChatAnthropicOAuth(model="claude-opus-5", api_key="test-key", max_tokens=1024)
    payload = _payload(_conversation(), model=client)
    assert [block["text"] for block in payload["system"]][-1] == "base prompt"
    assert payload["messages"][-1]["role"] == "system"


def test_windowed_payload_keeps_position_before_an_assistant_turn():
    payload = _payload(
        [
            SystemMessage("base prompt"),
            HumanMessage("first question"),
            build_operator_message("envelope body", "system"),
            AIMessage("first answer"),
        ]
    )
    assert [m["role"] for m in payload["messages"]] == ["user", "system", "assistant"]


def test_window_leaves_untagged_conversations_byte_identical():
    plain = [SystemMessage("base prompt"), HumanMessage("first question")]
    assert _payload(plain) == {
        "model": "claude-opus-5",
        "max_tokens": 1024,
        "system": "base prompt",
        "messages": [{"role": "user", "content": "first question"}],
    }


def test_window_still_refuses_a_second_untagged_system_message():
    with pytest.raises(ValueError, match="multiple non-consecutive system messages"):
        _payload(
            [
                SystemMessage("base prompt"),
                HumanMessage("first question"),
                SystemMessage("a plain second system message"),
            ]
        )


def test_tagged_message_at_index_zero_is_still_hoisted():
    """messages[0] cannot be role system on the wire, so the hoist is correct there."""
    payload = _payload([build_operator_message("envelope body", "system"), HumanMessage("hi")])
    assert payload["system"] == "envelope body"
    assert payload["messages"] == [{"role": "user", "content": "hi"}]


def test_empty_carrier_is_dropped_rather_than_sent():
    payload = _payload(
        [SystemMessage("base prompt"), HumanMessage("hi"), build_operator_message("   ", "system")]
    )
    assert payload["messages"] == [{"role": "user", "content": "hi"}]


def test_carrier_after_a_tool_result_run():
    """The real tail shape. The merged tool_result run stays one user message.

    Anthropic requires a mid-conversation system message to follow a user turn,
    and a run of tool results merges into exactly that, so the splice must not
    break the merge that produces it.
    """
    payload = _payload(
        [
            SystemMessage("base prompt"),
            HumanMessage("question"),
            AIMessage(
                content="",
                tool_calls=[
                    {"name": "a", "args": {}, "id": "call_a"},
                    {"name": "b", "args": {}, "id": "call_b"},
                ],
            ),
            ToolMessage(content="result a", tool_call_id="call_a"),
            ToolMessage(content="result b", tool_call_id="call_b"),
            build_operator_message("envelope body", "system"),
        ]
    )
    roles = [m["role"] for m in payload["messages"]]
    assert roles == ["user", "assistant", "user", "system"]
    tool_results = payload["messages"][2]["content"]
    assert [block["tool_use_id"] for block in tool_results] == ["call_a", "call_b"]


def test_multiple_carriers_each_get_their_own_entry():
    payload = _payload(
        [
            SystemMessage("base prompt"),
            HumanMessage("first question"),
            build_operator_message("first envelope", "system"),
            AIMessage("first answer"),
            HumanMessage("second question"),
            build_operator_message("second envelope", "system"),
        ]
    )
    assert [m["role"] for m in payload["messages"]] == [
        "user",
        "system",
        "assistant",
        "user",
        "system",
    ]
    assert payload["messages"][1]["content"] == [{"type": "text", "text": "first envelope"}]
    assert payload["messages"][4]["content"] == [{"type": "text", "text": "second envelope"}]


def test_bridge_is_installed_once_and_idle_outside_the_window():
    """The wrapped hoist is the one module global; outside the window it is
    the stock function's answer, so every other Anthropic call is untouched."""
    import langchain_anthropic.chat_models as chat_models

    from src.llms import operator_channel

    bridged = chat_models._format_messages
    assert getattr(bridged, "_operator_bridge", False)
    operator_channel._install_format_bridge()
    assert chat_models._format_messages is bridged
    with pytest.raises(ValueError, match="multiple non-consecutive system messages"):
        bridged(_conversation())
