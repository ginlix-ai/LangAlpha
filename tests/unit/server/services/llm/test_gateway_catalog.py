"""A gateway resolved at runtime (OpenCode Go): its catalog sends each listed
model to the route for its API with what that API needs, a route is one API of
the gateway's connection, and a call goes to the route the model was saved
under on the connection's key.
"""

import pytest

from src.llms.llm import ModelConfig
from src.llms.model_spec import shadowed_model
from src.server.services.llm import provider_models as pm
from src.server.services.llm.byok import call_endpoint, key_fits
from src.server.services.llm.provider_models.catalog import GatewayCatalog

GATEWAY = "opencode-go"
ROUTES = ["opencode-go-chat", "opencode-go-responses", "opencode-go-google"]

# A trimmed models.dev section: one model per API, one the manifest tunes.
SECTION = {
    "npm": "@ai-sdk/openai-compatible",
    "models": {
        "glm-5.3": {
            "name": "GLM-5.3",
            "reasoning": True,
            "reasoning_options": [{"type": "effort", "values": ["low", "medium", "high"]}],
            "limit": {"context": 200000, "output": 32000},
            "modalities": {"input": ["text"]},
            "cost": {"input": 1, "output": 2},
        },
        "vendor-claude": {
            "name": "Vendor Claude",
            "provider": {"npm": "@ai-sdk/anthropic"},
            "reasoning": True,
            "reasoning_options": [{"type": "effort", "values": ["low", "high"]}],
            "limit": {"context": 100000, "output": 8000},
        },
        "vendor-gpt": {"name": "Vendor GPT", "provider": {"npm": "@ai-sdk/openai"}, "reasoning": True},
        "vendor-gemini": {"name": "Vendor Gemini", "provider": {"npm": "@ai-sdk/google"}, "reasoning": True},
    },
}


@pytest.fixture(scope="module")
def mc():
    return ModelConfig()


@pytest.fixture(scope="module")
def catalog(mc):
    return GatewayCatalog(mc.get_provider_info(GATEWAY)["model_catalog"], SECTION)


def _by_id(models):
    return {m["id"]: m for m in models}


class TestConnections:
    @pytest.mark.parametrize("route", ROUTES)
    def test_every_route_is_one_api_of_the_gateways_connection(self, mc, route):
        assert mc.connection_of(route) == GATEWAY

    @pytest.mark.parametrize("provider", [GATEWAY, "openai", "moonshot-cn-coding", "not-a-provider"])
    def test_any_other_provider_is_its_own_connection(self, mc, provider):
        assert mc.connection_of(provider) == provider


class TestDescribe:
    def test_each_model_goes_to_the_route_for_its_api(self, mc, catalog):
        out = _by_id(catalog.describe([{"id": m} for m in SECTION["models"]], mc))
        assert {k: m["route"] for k, m in out.items()} == {
            "glm-5.3": "opencode-go-chat",
            "vendor-claude": "opencode-go",
            "vendor-gpt": "opencode-go-responses",
            "vendor-gemini": "opencode-go-google",
        }

    def test_each_api_gets_its_own_effort_field_and_reasoning_settings(self, mc, catalog):
        out = _by_id(catalog.describe([{"id": m} for m in SECTION["models"]], mc))
        assert out["glm-5.3"]["reasoning"]["write"] == "parameters.reasoning_effort"
        claude = out["vendor-claude"]
        assert claude["reasoning"] == {
            "efforts": ["low", "high"],
            "default": claude["reasoning"]["default"],
            "write": "parameters.output_config.effort",
        }
        assert claude["parameters"] == {"max_tokens": 8000, "thinking": {"type": "adaptive"}}
        assert out["vendor-gpt"]["parameters"]["store"] is False
        assert out["vendor-gemini"]["parameters"] == {"include_thoughts": True}

    def test_a_model_the_manifest_tunes_is_known_by_its_key(self, mc, catalog):
        out = _by_id(catalog.describe([{"id": "glm-5.3"}, {"id": "vendor-gpt"}], mc))
        assert out["glm-5.3"]["known"] is True
        assert mc.get_model_config(out["glm-5.3"]["manifest_key"])
        assert "known" not in out["vendor-gpt"]
        # The catalog's own facts, not the manifest's, describe the model.
        assert out["glm-5.3"]["display_name"] == "GLM-5.3"
        assert out["glm-5.3"]["context_window"] == 200000

    def test_a_model_the_catalog_does_not_describe_runs_untuned_on_the_fallback(self, mc, catalog):
        (out,) = catalog.describe([{"id": "brand-new", "display_name": "brand-new"}], mc)
        assert out == {"id": "brand-new", "display_name": "brand-new", "route": "opencode-go-chat"}

    def test_the_catalog_stands_in_for_an_unreadable_list(self, mc, catalog):
        assert {m["id"] for m in catalog.describe(None, mc)} == set(SECTION["models"])
        assert GatewayCatalog(catalog.config, None).describe(None, mc) is None


class TestBuildResult:
    def test_listed_models_leave_described(self, mc, catalog):
        result = pm.build_result(GATEWAY, pm.OPENAI, [{"id": "vendor-gpt"}], mc, fetched_at="t", catalog=catalog)
        (model,) = result["others"]
        assert model["route"] == "opencode-go-responses" and model["described"] is True

    def test_an_unreachable_list_falls_back_to_the_catalogs(self, mc, catalog):
        result = pm.build_result(GATEWAY, pm.OPENAI, None, mc, reason=pm.UNREACHABLE, catalog=catalog)
        assert result["status"] == "fallback"
        assert {m["id"] for m in result["others"]} == set(SECTION["models"])


class TestCallEndpoint:
    @pytest.mark.parametrize("custom", [True, False])
    def test_a_route_of_the_keys_connection_is_called_as_saved(self, mc, custom):
        assert call_endpoint("opencode-go-chat", GATEWAY, mc, custom=custom) == "opencode-go-chat"

    def test_a_key_under_one_route_serves_a_sibling_route_on_its_own_api(self, mc):
        assert call_endpoint("opencode-go-google", "opencode-go-chat", mc, custom=True) == "opencode-go-google"

    @pytest.mark.parametrize("custom", [True, False])
    def test_a_region_of_the_models_plan_is_called_where_the_key_was_issued(self, mc, custom):
        assert call_endpoint("moonshot-coding", "moonshot-cn-coding", mc, custom=custom) == "moonshot-cn-coding"

    def test_a_manifest_model_keeps_its_own_endpoint_on_a_parents_key(self, mc):
        assert call_endpoint("z-ai-coding", "z-ai", mc, custom=False) == "z-ai-coding"

    def test_a_brands_model_takes_a_plans_own_key_only_with_the_base_url_saved_with_it(self, mc):
        # The plan issues its keys apart from the brand's, so the walk takes one
        # for the brand's model only where a saved base URL says where it goes,
        # and that URL wins over whichever endpoint call_endpoint names.
        assert not key_fits("z-ai", "z-ai-coding", has_base_url=False, mc=mc)
        assert key_fits("z-ai", "z-ai-coding", has_base_url=True, mc=mc)

    def test_otherwise_a_custom_model_goes_where_the_key_is(self, mc):
        assert call_endpoint("moonshot", "moonshot-coding", mc, custom=True) == "moonshot-coding"
        assert call_endpoint("moonshot", "moonshot-coding", mc, custom=False) == "moonshot"


@pytest.mark.parametrize("route", [GATEWAY, *ROUTES])
def test_a_gateway_pick_is_tuned_as_the_manifest_model_it_matched(mc, route):
    entry = {"name": "opencode-go/glm-5.3", "model_id": "glm-5.3", "provider": route}
    shadow = shadowed_model(entry, mc)
    assert shadow is not None and shadow.get("model_id", "glm-5.3") == "glm-5.3"



def test_the_web_matches_a_pick_to_the_manifest_model_a_turn_is_tuned_as(mc):
    from src.llms.gateway_names import gateway_index, gateway_slug

    index = gateway_index(mc)
    assert {GATEWAY, *ROUTES, "openrouter"} <= set(index["routes"])
    entry = {"name": "opencode-go/glm-5.3", "model_id": "glm-5.3", "provider": GATEWAY}
    assert mc.get_model_config(index["models"][gateway_slug("glm-5.3")]) is shadowed_model(entry, mc)


@pytest.mark.parametrize(
    "odd",
    [
        {"provider": "x"},
        {"provider": {"npm": ["@ai-sdk/anthropic"]}},
        {"limit": [1], "modalities": "text"},
        {"reasoning": True, "reasoning_options": 5},
        {},
    ],
)
def test_a_models_odd_field_leaves_it_listed_rather_than_failing_the_list(mc, catalog, odd):
    section = {"models": {"odd": {"name": "Odd", **odd} if odd else "oops"}}
    (out,) = GatewayCatalog(catalog.config, section).describe([{"id": "odd"}], mc)
    assert out["route"] in {GATEWAY, *ROUTES}


@pytest.mark.parametrize("route", [GATEWAY, *ROUTES])
@pytest.mark.parametrize("cache_key", [None, "thread-1"])
def test_every_call_names_a_session_which_opencode_refuses_to_route_without(route, cache_key):
    from src.llms.llm import create_llm_from_custom

    llm = create_llm_from_custom({"name": "x", "model_id": "glm-5.3", "provider": route}, api_key="k", cache_key=cache_key)
    headers = getattr(llm, "default_headers", None) or getattr(llm, "additional_headers", None) or {}
    assert headers.get("x-opencode-session")


def test_the_chat_route_keeps_a_models_reasoning_to_send_back():
    # OpenCode's chat models return ``reasoning_content`` and read it back
    # between tool calls; a plain OpenAI client drops it both ways.
    from src.llms.llm import create_llm_from_custom
    from src.llms.vendor.langchain_zai import ChatZai

    llm = create_llm_from_custom({"name": "x", "model_id": "kimi-k3", "provider": "opencode-go-chat"}, api_key="k")
    assert isinstance(llm, ChatZai) and llm.preserve_reasoning


def test_a_deployment_key_connects_no_gateway_and_lists_it_once_not_each_api(monkeypatch):
    # A turn takes a deployment key for a manifest model only, and a gateway's
    # are all picks, so its key alone would list it connected and fail on use.
    import src.server.app.api_keys as api_keys

    # Standalone, where a deployment key is the user's to run turns on.
    monkeypatch.setattr("src.config.settings.HOST_MODE", "oss")
    monkeypatch.setenv("OPENCODE_API_KEY", "sk-placeholder")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-placeholder")
    monkeypatch.setenv("ZAI_CODING_API_KEY", "sk-placeholder")
    monkeypatch.setenv("MOONSHOT_CODING_API_KEY", "sk-placeholder")
    monkeypatch.setattr(api_keys, "_BYOK_PROVIDERS_CACHE", None)
    providers = api_keys._format_response(False, {}, {}, [])["providers"]
    rows = [r for r in providers if r["provider"].startswith(GATEWAY)]
    assert [(r["provider"], r["has_key"], r["masked_key"]) for r in rows] == [(GATEWAY, False, None)]
    masked = {r["provider"]: r["masked_key"] for r in providers}
    # A provider whose own manifest rows run on its key still lists it; a plan
    # offering its brand's rows does not, since those read the brand's key.
    assert masked["deepseek"] == masked["moonshot-coding"] == "(server)"
    assert masked["z-ai-coding"] is None
    # Nor does a key go under one of its APIs, where no row would show it.
    assert GATEWAY in api_keys._get_allowed_providers([])
    assert not set(ROUTES) & api_keys._get_allowed_providers([])
