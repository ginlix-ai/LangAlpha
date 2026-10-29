"""The pure layer of the provider model lists: normalizing each family's list,
the chat-model filter, OpenRouter name matching and the fallback rule.

Payloads are trimmed copies of what each provider returned live (2026-10-24).
"""

from datetime import datetime, timezone

import httpx
import pytest

from src.llms.llm import ModelConfig
from src.server.services.llm import provider_models as pm
from src.server.services.oauth_protocol import TokenSet
from src.server.services.oauth_providers import SUBSCRIPTIONS


@pytest.fixture(scope="module")
def mc():
    return ModelConfig()


class TestNormalize:
    def test_openai_shape_reads_the_optional_fields_vendors_add(self):
        payload = {"object": "list", "data": [
            {"id": "gpt-6.1-sol", "object": "model", "owned_by": "system"},
            {"id": "deepseek-flash", "name": "DeepSeek-V4.1-Flash", "context_window": 1048576,
             "input_modalities": ["text", "image"]},
            {"id": "kimi-k3.1-pro", "context_length": 1048576,
             "modalities": {"input": ["text", "image", "video"], "output": ["text"]}},
            {"id": "doubao-seed-2-1-pro-260915", "name": "doubao-seed-2-1-pro", "domain": "VLM",
             "token_limits": {"context_window": 1048576},
             "modalities": {"input_modalities": ["text", "image", "video"]}},
        ]}
        assert pm.normalize(pm.OPENAI, payload) == [
            {"id": "gpt-6.1-sol", "display_name": "gpt-6.1-sol"},
            {"id": "deepseek-flash", "display_name": "DeepSeek-V4.1-Flash", "context_window": 1048576,
             "input_modalities": ["text", "image"]},
            {"id": "kimi-k3.1-pro", "display_name": "kimi-k3.1-pro", "context_window": 1048576,
             "input_modalities": ["text", "image", "video"]},
            {"id": "doubao-seed-2-1-pro-260915", "display_name": "doubao-seed-2-1-pro",
             "context_window": 1048576, "input_modalities": ["text", "image", "video"]},
        ]

    def test_openai_shape_drops_shut_down_and_non_chat_domains(self):
        payload = {"data": [
            {"id": "doubao-lite-128k-240428", "domain": "LLM", "status": "Shutdown"},
            {"id": "doubao-embedding-text-240715", "domain": "Embedding"},
            {"id": "doubao-seed3d-2-0-260328", "domain": "3DGeneration"},
            {"id": "glm-5-2-260617", "domain": "LLM", "status": "Retiring"},
        ]}
        assert [m["id"] for m in pm.normalize(pm.OPENAI, payload)] == ["glm-5-2-260617"]

    def test_anthropic_shape(self):
        payload = {"data": [{
            "type": "model", "id": "claude-haiku-5-5", "display_name": "Claude Haiku 5.5",
            "max_input_tokens": 1000000,
            "capabilities": {"image_input": {"supported": True}, "pdf_input": {"supported": True}},
        }], "has_more": False}
        assert pm.normalize(pm.ANTHROPIC, payload) == [{
            "id": "claude-haiku-5-5", "display_name": "Claude Haiku 5.5", "context_window": 1000000,
            "input_modalities": ["text", "image", "pdf"],
        }]

    def test_gemini_shape_keeps_only_generate_content(self):
        payload = {"models": [
            {"name": "models/gemini-4-argon", "displayName": "Gemini 4 Argon", "inputTokenLimit": 1048576,
             "supportedGenerationMethods": ["generateContent", "countTokens"]},
            {"name": "models/gemini-embedding-2", "supportedGenerationMethods": ["embedContent"]},
        ]}
        assert pm.normalize(pm.GEMINI, payload) == [
            {"id": "gemini-4-argon", "display_name": "Gemini 4 Argon", "context_window": 1048576},
        ]

    def test_codex_shape_keeps_listed_api_models(self):
        payload = {"models": [
            {"slug": "gpt-6.1-sol", "display_name": "GPT-6.1-Sol", "visibility": "list",
             "supported_in_api": True, "context_window": 500000, "input_modalities": ["text", "image"]},
            {"slug": "codex-auto-review", "visibility": "hide", "supported_in_api": True},
            {"slug": "gpt-internal", "visibility": "list", "supported_in_api": False},
        ]}
        assert pm.normalize(pm.CODEX, payload) == [{
            "id": "gpt-6.1-sol", "display_name": "GPT-6.1-Sol", "context_window": 500000,
            "input_modalities": ["text", "image"],
        }]

    def test_openrouter_shape_prices_per_million_and_drops_duplicate_skus(self):
        model = {
            "id": "anthropic/claude-sonnet-5.5", "name": "Anthropic: Claude Sonnet 5.5",
            "context_length": 1000000,
            "architecture": {"input_modalities": ["text", "image"]},
            "pricing": {"prompt": "0.000002", "completion": "0.00001", "input_cache_read": "0.0000001",
                        "request": "0"},
            "supported_parameters": ["tools", "reasoning"],
        }
        payload = {"data": [
            model,
            {**model, "id": "anthropic/claude-sonnet-5.5:batch"},
            {"id": "liquid/lfm-2.5-2.6b:free", "name": "LFM"},
        ]}
        assert pm.normalize(pm.OPENROUTER, payload) == [
            {"id": "anthropic/claude-sonnet-5.5", "display_name": "Anthropic: Claude Sonnet 5.5",
             "context_window": 1000000, "input_modalities": ["text", "image"],
             "pricing": {"input": 2.0, "output": 10.0, "cache_read": 0.1},
             "supported_parameters": ["tools", "reasoning"]},
            # A :free SKU with no base model is the only way to that model.
            {"id": "liquid/lfm-2.5-2.6b:free", "display_name": "LFM"},
        ]

    def test_openrouter_reasoning_becomes_a_ladder_written_to_its_effort_field(self):
        payload = {"data": [
            {"id": "deepseek/v4", "reasoning": {"mandatory": False, "supported_efforts": ["max", "high", "low", "none"],
                                                "default_effort": "high"}},
            # Thinking it cannot turn off: the listed off rung would be refused.
            {"id": "google/gemini", "reasoning": {"mandatory": True, "supported_efforts": ["none", "high", "low"],
                                                  "default_effort": "max"}},
            # Reasoning by token budget only, and a non-reasoning model.
            {"id": "qwen/flash", "reasoning": {"supports_max_tokens": True}},
            {"id": "plain/chat"},
        ]}
        ladders = {m["id"]: m.get("reasoning") for m in pm.normalize(pm.OPENROUTER, payload)}
        write = "parameters.reasoning.effort"
        assert ladders == {
            "deepseek/v4": {"efforts": ["none", "low", "high", "max"], "default": "high", "write": write},
            "google/gemini": {"efforts": ["low", "high"], "write": write},
            "qwen/flash": None,
            "plain/chat": None,
        }

    @pytest.mark.parametrize("shape,payload", [
        (pm.OPENAI, {"error": "nope"}),
        (pm.GEMINI, {"models": "nope"}),
        (pm.CODEX, []),
    ])
    def test_a_body_without_its_list_is_a_bad_response(self, shape, payload):
        with pytest.raises(pm.ListFetchError) as e:
            pm.normalize(shape, payload)
        assert e.value.reason == pm.BAD_RESPONSE


class TestChatFilter:
    @pytest.mark.parametrize("model_id", [
        "text-embedding-3-large", "omni-moderation-latest", "tts-1-hd", "gpt-4o-mini-tts",
        "gpt-realtime-2.1-mini", "gpt-4o-transcribe", "whisper-1", "gpt-audio", "gpt-image-2",
        "chatgpt-image-latest", "sora-2-pro", "dall-e-3", "babbage-002", "gpt-3.5-turbo-instruct",
        "gpt-4o-search-preview", "gpt-5-search-api", "gpt-live-1",
        "gemini-3.8-flash-tts", "gemini-3.1-flash-image-preview", "nano-banana-pro-preview",
        "lyria-3.5", "gemini-2.5-computer-use-preview-10-2025", "deep-research-preview-04-2026",
        "qwen3.7-text-embedding", "qwen3.7-text-rerank", "qwen-mt-flash", "qwen3-asr-flash-realtime",
        "qwen3-livetranslate-flash", "qwen-vl-ocr", "wan2.7-image-pro", "MiniMax/speech-2.8-hd",
        "doubao-smart-router-250928",
    ])
    def test_non_chat_models_are_dropped(self, model_id):
        assert not pm.is_chat_model(model_id)

    @pytest.mark.parametrize("model_id", [
        "gpt-6.1-sol", "gpt-5-mini", "o4-mini", "gpt-5.1-codex", "chat-latest", "claude-haiku-5-5",
        "gemini-4-argon", "gemma-4-31b-it", "qwen3.8-max", "qwen3.5-omni-plus", "qwen3-vl-plus",
        "glm-5.3", "kimi-k3", "MiniMax-M3", "deepseek-flash", "qwen-flash-character",
        "moonshotai/kimi-k3.1-pro", "openrouter/auto",
    ])
    def test_chat_models_are_kept(self, model_id):
        assert pm.is_chat_model(model_id)


class TestOpenRouterMatching:
    @pytest.mark.parametrize("listed,manifest_id", [
        ("anthropic/claude-sonnet-5.5", "claude-sonnet-5-5"),
        ("openai/gpt-6.1-sol", "gpt-6.1-sol"),
        ("openai/gpt-6.1-sol:batch", "gpt-6.1-sol"),
        ("minimax/minimax-m3", "MiniMax-M3"),
        ("qwen/qwen3.8-max", "qwen3.8-max"),
    ])
    def test_vendor_dots_case_and_suffix_are_ignored(self, listed, manifest_id):
        assert pm.gateway_slug(listed) == pm.gateway_slug(manifest_id)

    def test_known_names_the_vendors_own_entry(self, mc):
        index = pm.manifest_slug_index(mc)
        assert index[pm.gateway_slug("anthropic/claude-sonnet-5.5")] == "claude-sonnet-5-5"
        # Not the subscription copy, nor the regional variant.
        assert index[pm.gateway_slug("openai/gpt-6.1-sol")] == "gpt-6.1-sol"
        assert index[pm.gateway_slug("z-ai/glm-5.3")] == "glm-5.3"

    def test_results_carry_known_and_the_manifest_key(self, mc):
        models = [
            {"id": "anthropic/claude-sonnet-5.5", "display_name": "Claude Sonnet 5.5"},
            {"id": "anthropic/claude-sonnet-4.6", "display_name": "Claude Sonnet 4.6"},
        ]
        result = pm.build_result("openrouter", pm.OPENROUTER, models, mc)
        assert result["status"] == "ok"
        assert result["others"] == [
            {**models[0], "known": True, "manifest_key": "claude-sonnet-5-5"},
            {**models[1], "known": False},
        ]

    def test_a_known_model_keeps_only_the_levels_it_is_tuned_for(self, mc):
        tuned = mc.get_model_config("claude-sonnet-5-5")["reasoning"]
        offered = ["none", "low", "medium", "high", "xhigh", "max"]
        ladder = {"efforts": offered, "default": "low", "write": "parameters.reasoning.effort"}
        models = [
            {"id": "anthropic/claude-sonnet-5.5", "reasoning": ladder},
            {"id": "acme/house", "reasoning": ladder},
        ]
        known, unknown = pm.build_result("openrouter", pm.OPENROUTER, models, mc)["others"]
        assert known["reasoning"] == {
            "efforts": [e for e in offered if e in tuned["efforts"]],
            "default": tuned["default"],
            "write": "parameters.reasoning.effort",
        }
        assert unknown["reasoning"] == ladder


class TestFallbackRule:
    def test_listed_manifest_models_are_ready_and_the_rest_others(self, mc):
        models = [{"id": "gpt-6.1-sol", "display_name": "x"}, {"id": "gpt-4o", "display_name": "y"}]
        result = pm.build_result("openai", pm.OPENAI, models, mc, fetched_at="t")
        assert result["status"] == "ok"
        assert result["ready"] == ["gpt-6.1-sol"]
        assert "gpt-6-luna" in result["missing"]
        assert [m["id"] for m in result["others"]] == ["gpt-4o"]
        assert result["total"] == 2

    def test_a_failed_list_falls_back_to_the_manifest(self, mc):
        result = pm.build_result("openai", pm.OPENAI, None, mc, reason=pm.UNAUTHORIZED)
        assert result["status"] == "fallback"
        assert result["reason"] == pm.UNAUTHORIZED
        assert set(result["ready"]) >= {"gpt-6.1-sol", "gpt-6-luna"}
        assert result["others"] == []

    def test_a_list_naming_no_manifest_model_falls_back_but_keeps_its_models(self, mc):
        models = [{"id": "my-proxy-model", "display_name": "m"}]
        result = pm.build_result("openai", pm.OPENAI, models, mc, fetched_at="t")
        assert result["status"] == "fallback"
        assert result["reason"] == pm.NO_MATCH
        assert "gpt-6.1-sol" in result["ready"]
        assert result["others"] == models

    def test_a_provider_the_manifest_tunes_nothing_for_is_ok_with_others(self, mc):
        models = [{"id": "gpt-oss-120b", "display_name": "gpt-oss-120b"}]
        result = pm.build_result("cerebras", pm.OPENAI, models, mc, fetched_at="t")
        assert result["status"] == "ok"
        assert result["ready"] == []
        assert result["others"] == models

    def test_an_empty_openrouter_list_falls_back(self, mc):
        result = pm.build_result("openrouter", pm.OPENROUTER, [], mc, fetched_at="t")
        assert result["status"] == "fallback"
        assert result["reason"] == pm.EMPTY_LIST

    def test_a_subscription_matches_its_own_entries_and_reports_live_context(self, mc):
        models = [{"id": "gpt-6.1-sol", "display_name": "GPT-6.1-Sol", "context_window": 500000}]
        result = pm.build_result("codex-oauth", pm.CODEX, models, mc, fetched_at="t")
        assert result["ready"] == ["gpt-6.1-sol-oauth"]
        assert result["context_windows"] == {"gpt-6.1-sol-oauth": 500000}

    def test_a_coding_plan_with_its_own_key_lists_its_brands_models_as_ready(self, mc):
        # Its key cannot reach the brand's rows, but the picker offers them,
        # each saved as an entry naming the plan, which runs on that key.
        brand = {k: e for k, e in mc.llm_config.items() if e and e.get("visible") and e.get("provider") == "minimax"}
        assert brand and mc.get_provider_info("minimax-coding").get("own_api_key")
        models = [{"id": e.get("model_id") or k} for k, e in brand.items()]
        result = pm.build_result("minimax-coding", pm.ANTHROPIC, models, mc, fetched_at="t")
        assert result["status"] == "ok"
        assert sorted(result["ready"]) == sorted(brand)
        assert result["others"] == []

    def test_q_narrows_others_by_every_word(self):
        result = {"others": [
            {"id": "anthropic/claude-sonnet-5.5", "display_name": "Anthropic: Claude Sonnet 5.5"},
            {"id": "openai/gpt-6-sol", "display_name": "OpenAI: GPT-6 Sol"},
        ]}
        assert [m["id"] for m in pm.filter_others(result, "Claude sonnet")["others"]] == [
            "anthropic/claude-sonnet-5.5"
        ]


class TestListUrl:
    def test_a_declared_list_url_follows_a_saved_base_to_its_origin(self, mc):
        info = mc.get_provider_info("moonshot")
        assert pm.list_url(info, pm.ANTHROPIC, None) == ("https://api.moonshot.ai/v1/models", pm.OPENAI)
        assert pm.list_url(info, pm.ANTHROPIC, "https://api.moonshot.cn/anthropic") == (
            "https://api.moonshot.cn/v1/models", pm.OPENAI,
        )
        # A base on another path is listed the way its SDK family lists.
        assert pm.list_url(info, pm.ANTHROPIC, "https://gw.example.com/kimi") == (
            "https://gw.example.com/kimi/v1/models?limit=1000", pm.ANTHROPIC,
        )

    def test_the_coding_variant_lists_at_its_own_endpoint(self, mc):
        info = mc.get_provider_info("moonshot-coding")
        assert pm.list_url(info, pm.OPENAI, None) == (
            "https://api.kimi.ai/coding/v1/models", pm.OPENAI,
        )

    def test_codex_sends_the_configured_client_version(self, mc):
        info = mc.get_provider_info("codex-oauth")
        token = TokenSet("tok", "rt", datetime.now(timezone.utc), account_id="acct")
        req = pm.build_list_request(
            info, api_key="tok",
            token_headers=SUBSCRIPTIONS["codex-oauth"].request_headers(token),
        )
        version = info["client_version"]
        assert req.url.endswith(f"/models?client_version={version}")
        assert req.headers["User-Agent"] == f"codex_cli_rs/{version}"
        assert req.headers["originator"] == "codex_cli_rs"
        assert req.headers["ChatGPT-Account-Id"] == "acct"


class TestEndpointModels:
    def test_tuned_models_keep_the_manifest_name_and_the_endpoint_id(self, mc):
        models = [
            {"id": "anthropic/claude-sonnet-5.5", "display_name": "Claude Sonnet 5.5"},
            {"id": "anthropic/claude-sonnet-5.5:beta", "display_name": "Claude Sonnet 5.5 (beta)"},
            {"id": "acme/house-model-7b", "display_name": "House 7B", "context_window": 32768},
        ]
        result = pm.match_endpoint_models(models, mc, route_api="openai-chat")
        assert [(m["name"], m["model_id"]) for m in result["recognized"]] == [
            ("claude-sonnet-5-5", "anthropic/claude-sonnet-5.5"),
        ]
        assert result["others"] == [models[2]]

    def test_a_window_is_set_only_where_the_endpoint_serves_less(self, mc):
        declared = mc.get_model_config("claude-sonnet-5-5")["context"]
        smaller = pm.match_endpoint_models([{"id": "claude-sonnet-5-5", "context_window": 200_000}], mc, route_api="anthropic")
        larger = pm.match_endpoint_models([{"id": "claude-sonnet-5-5", "context_window": declared * 2}], mc, route_api="anthropic")
        assert smaller["recognized"][0]["context"] == 200_000
        assert "context" not in larger["recognized"][0]

    @pytest.mark.parametrize("listed,expected", [
        # An endpoint serving a vision model as text only is not sent images.
        (["text"], ["text"]),
        (["text", "image"], ["text", "image"]),
        # OpenRouter's word for a PDF is `file`.
        (["text", "image", "file"], None),
        (["text", "image", "pdf"], None),
        # A list that names no inputs says nothing either way.
        (None, None),
    ])
    def test_inputs_are_set_only_where_the_endpoint_takes_fewer(self, mc, listed, expected):
        if mc.get_model_config("claude-sonnet-5-5").get("input_modalities") != ["text", "image", "pdf"]:
            pytest.skip("the manifest declares other inputs for this model now")
        model = {"id": "claude-sonnet-5-5", **({"input_modalities": listed} if listed else {})}
        result = pm.match_endpoint_models([model], mc, route_api="anthropic")
        assert result["recognized"][0].get("input_modalities") == expected

    @pytest.mark.parametrize("route_api,ladder", [
        ("anthropic", None),
        # The effort control is an Anthropic client keyword, which another API cannot take.
        ("openai-chat", {"efforts": []}),
        ("openai-responses", {"efforts": []}),
    ])
    def test_a_model_on_another_api_declares_no_levels(self, mc, route_api, ladder):
        result = pm.match_endpoint_models([{"id": "claude-sonnet-5-5"}], mc, route_api=route_api)
        assert result["recognized"][0].get("reasoning") == ladder

    def test_a_control_written_into_the_body_travels_to_any_api(self, mc):
        result = pm.match_endpoint_models([{"id": "z-ai/glm-5.3"}], mc, route_api="anthropic")
        assert "reasoning" not in result["recognized"][0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status,body,reason", [
        # z.ai's Anthropic route answers its list path with this, as a 200.
        (200, {"code": 500, "msg": "Internal service error", "success": False}, pm.BAD_RESPONSE),
        (401, {"error": {"message": "Incorrect API key provided"}}, pm.UNAUTHORIZED),
    ])
    async def test_a_list_that_cannot_be_read_is_unavailable_not_an_error(self, status, body, reason):
        transport = httpx.MockTransport(lambda request: httpx.Response(status, json=body))
        result = await pm.endpoint_models("anthropic", "https://gateway.example/api/anthropic", "k", transport=transport)
        assert result == {"status": "unavailable", "reason": reason, "recognized": [], "others": []}


class TestSubscriptionListRefusal:
    """xAI answers both a plan without API access and a stale token with a
    403; only the body tells them apart."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("error,reason,expired", [
        # A stale token is spent, so the next list or turn refreshes it.
        ("The provided token could not be validated.", pm.UNAUTHORIZED, True),
        ("Your plan does not include API access.", pm.NO_API_ACCESS, False),
    ])
    async def test_a_grok_403_is_read_by_its_message(self, error, reason, expired):
        from unittest.mock import AsyncMock, patch

        cred = pm.service.Credential(api_key="token-example", holder="grok-oauth", account_id="account-example")
        transport = httpx.MockTransport(lambda request: httpx.Response(403, json={"error": error}))
        expire = AsyncMock(return_value=True)
        with (
            patch.object(pm.service, "_resolve_credential", AsyncMock(return_value=cred)),
            patch.object(pm.service, "_cache_read", AsyncMock(return_value=None)),
            patch("src.server.services.llm.egress.user_endpoint_transport", AsyncMock(return_value=transport)),
            patch("src.server.database.oauth_tokens.expire_oauth_access_token", expire),
        ):
            result = await pm.available_models("u1", "grok-oauth")
        assert result["reason"] == reason
        assert expire.await_count == (1 if expired else 0)
