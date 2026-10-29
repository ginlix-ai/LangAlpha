"""``sdk: "openrouter"`` builds ``ChatOpenRouter`` with the shared config shape
moved onto the fields that send it.

Manifest entries and saved custom models were written for the OpenAI-family
clients. ``ChatOpenRouter`` forwards any key it does not know to the SDK's
``chat.send``, which takes only the keys it names, so a key left in place is a
TypeError on every call.
"""

import asyncio

import pytest
from langchain_core.messages import AIMessageChunk
from langchain_core.outputs import ChatGenerationChunk
from langchain_openrouter import ChatOpenRouter

from src.llms.error_classification import (
    extract_status_code,
    is_mid_stream_error,
    is_stalled_stream,
)
from src.llms.extension import OpenRouterStreamError
from src.llms.llm import create_llm_from_custom

# OpenRouter carries no manifest models: a user saves the ones they route to.
_GROQ_ROUTED = {
    "name": "or-gpt-oss",
    "model_id": "openai/gpt-oss-120b",
    "provider": "openrouter",
    "parameters": {"extra_body": {"provider": {"order": ["Groq"], "allow_fallbacks": True}}},
    "reasoning": {"efforts": ["low", "medium", "high"], "default": "low", "write": "parameters.reasoning.effort"},
}


def _routed(**kwargs):
    return create_llm_from_custom(_GROQ_ROUTED, api_key="sk-or-test", **kwargs)


def test_custom_model_builds_chat_openrouter():
    client = _routed()

    assert isinstance(client, ChatOpenRouter)
    assert client.model_name == "openai/gpt-oss-120b"
    # extra_body.provider is OpenRouter's routing preference.
    assert client.openrouter_provider == {"order": ["Groq"], "allow_fallbacks": True}
    assert client.reasoning == {"effort": "low"}
    assert client.streaming and client.stream_usage
    # Milliseconds on this client; seconds everywhere else in the factory.
    assert client.request_timeout == 600_000
    assert client.app_title == "LangAlpha"
    assert client.metadata["provider_route"] == "openrouter"


def test_a_field_set_both_ways_keeps_its_explicit_value():
    # The client refuses a field given as itself and in model_kwargs.
    client = create_llm_from_custom(
        {**_GROQ_ROUTED, "parameters": {"temperature": 0.5, "extra_body": {"temperature": 0.7}}},
        api_key="sk-or-test",
    )

    assert client.temperature == 0.5
    assert "temperature" not in client.model_kwargs


def test_a_requested_effort_reaches_reasoning():
    client = _routed(reasoning_effort="high")

    assert client.reasoning == {"effort": "high"}


def test_openai_family_spellings_are_moved_or_dropped():
    client = create_llm_from_custom(
        {
            "name": "or-claude",
            "model_id": "anthropic/claude-haiku-5.5",
            "provider": "openrouter",
            "parameters": {
                "reasoning_effort": "low",
                "use_previous_response_id": False,
                "service_tier": "auto",
                "extra_body": {"provider": {"order": ["Anthropic"]}, "transforms": ["x"]},
            },
            "reasoning": {"efforts": ["low", "high"]},
        },
        api_key="sk-or-test",
        reasoning_effort="high",
    )

    assert client.reasoning == {"effort": "high"}
    assert client.openrouter_provider == {"order": ["Anthropic"]}
    # Only keys the SDK's send() names stay in model_kwargs.
    assert client.model_kwargs == {"service_tier": "auto"}
    sent = client._default_params
    assert "reasoning_effort" not in sent and "use_previous_response_id" not in sent


def test_parameters_cannot_move_the_endpoint():
    # The SDK's send() takes a server_url, and the egress policy reads only the
    # client's endpoint: a user's parameters must not reach it from any level.
    internal = "http://10.0.0.5:8080/x"
    client = create_llm_from_custom(
        {
            "name": "or-moved",
            "model_id": "openai/gpt-oss-120b",
            "provider": "openrouter",
            "parameters": {
                "server_url": internal,
                "http_headers": {"Metadata-Flavor": "Google"},
                "model_kwargs": {"server_url": internal, "base_url": internal},
                "extra_body": {"server_url": internal, "base_url": internal, "temperature": 0.2},
            },
        },
        api_key="sk-or-test",
    )

    sent = client._default_params
    assert "server_url" not in sent and "http_headers" not in sent
    assert internal not in str(client.openrouter_api_base)
    assert client.temperature == 0.2


def test_clients_share_one_connection_pool():
    a = _routed()
    b = _routed()

    pool = a.client.sdk_configuration.async_client._transport
    assert pool is b.client.sdk_configuration.async_client._transport


def test_held_to_the_egress_transport():
    class _Held:
        def handle_request(self, request): ...
        async def handle_async_request(self, request): ...

    transport = _Held()
    families: list[str] = []

    def egress(family):
        families.append(family)
        return transport

    client = _routed(egress_transport=egress)

    config = client.client.sdk_configuration
    assert families == ["openrouter"]
    assert config.client._transport is transport
    assert config.async_client._transport is transport
    assert not config.async_client.follow_redirects


async def _drain(client):
    async for _ in client._astream([]):
        pass


def _patched_upstream(monkeypatch, gen):
    async def upstream(self, messages, stop=None, run_manager=None, **kwargs):
        async for chunk in gen():
            yield chunk

    monkeypatch.setattr(ChatOpenRouter, "_astream", upstream)


def _chunk(text="", **metadata):
    return ChatGenerationChunk(message=AIMessageChunk(content=text, response_metadata=metadata))


@pytest.mark.asyncio
async def test_an_error_event_carries_its_status(monkeypatch):
    async def gen():
        yield _chunk("partial")
        raise ValueError(
            "OpenRouter API returned an error during streaming: rate limited (code: 429)"
        )

    _patched_upstream(monkeypatch, gen)
    client = _routed()
    with pytest.raises(Exception) as caught:
        await _drain(client)

    assert extract_status_code(caught.value) == 429
    assert is_mid_stream_error(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("first,mid_stream", [("partial", True), (None, False), ("", False)])
async def test_a_finish_reason_of_error_is_a_failure_not_output(monkeypatch, first, mid_stream):
    # Mid-stream only once output went out: before that the reasoning-strip
    # retry may still answer the refusal. A role-only delta is no output.
    async def gen():
        if first is not None:
            yield _chunk(first)
        yield _chunk("", finish_reason="error")

    _patched_upstream(monkeypatch, gen)
    client = _routed()
    with pytest.raises(OpenRouterStreamError) as caught:
        await _drain(client)
    assert is_mid_stream_error(caught.value) is mid_stream


@pytest.mark.asyncio
async def test_a_silent_stream_is_a_stall(monkeypatch):
    async def gen():
        yield _chunk("partial")
        await asyncio.sleep(5)

    _patched_upstream(monkeypatch, gen)
    client = _routed()
    client = client.model_copy(update={"stream_chunk_timeout": 0.05})
    with pytest.raises(TimeoutError) as caught:
        await _drain(client)

    assert is_stalled_stream(caught.value)
    assert caught.value.chunks_received == 1


@pytest.mark.parametrize("provider, tuned", [("openrouter", True), ("openai", False)])
def test_a_known_pick_saved_under_openrouters_id_is_tuned_as_its_match(provider, tuned):
    # The list calls ``anthropic/claude-sonnet-5.5`` known as ``claude-sonnet-5-5``
    # (``manifest_slug_index``); the entry saved from it keeps OpenRouter's id
    # as its name, and still runs with that model's prompting and compaction.
    from src.llms.llm import LLM
    from src.llms.model_spec import shadowed_model

    mc = LLM.get_model_config()
    entry = {"name": "anthropic/claude-sonnet-5.5", "model_id": "anthropic/claude-sonnet-5.5", "provider": provider}
    shadow = shadowed_model(entry, mc)
    assert (shadow == mc.get_model_config("claude-sonnet-5-5")) is tuned
    assert (shadow is None) is not tuned
