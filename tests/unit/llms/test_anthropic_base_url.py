"""A saved ``.../v1`` base URL must not reach an Anthropic-SDK client as is.

The SDK appends ``/v1/messages``, so the OpenAI-style shape 404s at
``/v1/v1/messages``. The fix is applied where the client is built and the saved
value is left alone, because it also keys the route's reasoning lineage.
"""

import httpx
import pytest

from src.llms.endpoints import anthropic_sdk_base_url
from src.llms.llm import LLM


class TestAnthropicSdkBaseUrl:
    @pytest.mark.parametrize(
        "saved,expected",
        [
            ("https://example.test/anthropic/v1", "https://example.test/anthropic"),
            ("https://example.test/anthropic/v1/", "https://example.test/anthropic"),
            ("https://example.test/anthropic/v1//", "https://example.test/anthropic"),
            ("https://example.test/v1", "https://example.test"),
            ("https://example.test/anthropic", "https://example.test/anthropic"),
            ("https://example.test/anthropic/", "https://example.test/anthropic"),
            ("https://example.test", "https://example.test"),
            ("https://example.test/", "https://example.test"),
            ("  https://example.test/anthropic/v1 ", "https://example.test/anthropic"),
            ("http://localhost:8080/v1", "http://localhost:8080"),
        ],
    )
    def test_strips_one_trailing_v1_and_slashes(self, saved, expected):
        assert anthropic_sdk_base_url(saved) == expected

    @pytest.mark.parametrize(
        "saved",
        [
            # A /v1 or /v3 in the middle belongs to the upstream's own path.
            "https://example.test/compatible-mode/v1/anthropic",
            "https://example.test/api/v3/anthropic",
            # Only a whole trailing segment counts.
            "https://example.test/anthropic/v10",
            "https://example.test/anthropic/rev1",
            # A host that happens to be called v1.
            "http://v1",
        ],
    )
    def test_leaves_other_paths_alone(self, saved):
        assert anthropic_sdk_base_url(saved) == saved

    def test_strips_only_one_v1(self):
        assert anthropic_sdk_base_url("https://example.test/v1/v1") == "https://example.test/v1"

    def test_keeps_a_query_string(self):
        assert (
            anthropic_sdk_base_url("https://example.test/anthropic/v1?key=1")
            == "https://example.test/anthropic?key=1"
        )

    @pytest.mark.parametrize("empty", [None, ""])
    def test_empty_passes_through(self, empty):
        assert anthropic_sdk_base_url(empty) == empty


class TestClientBuilder:
    def _llm(self, base_url):
        return LLM("deepseek-flash", api_key="test-key", base_url_override=base_url)

    @pytest.mark.parametrize(
        "saved",
        ["https://example.test/anthropic/v1", "https://example.test/anthropic/v1/"],
    )
    def test_client_is_built_on_the_normalized_base(self, saved):
        client = self._llm(saved).get_llm()
        assert client.anthropic_api_url == "https://example.test/anthropic"
        # What the SDK joins its /v1/messages onto.
        assert client._client.base_url == httpx.URL("https://example.test/anthropic/")

    def test_stored_value_and_route_identity_are_untouched(self):
        saved = "https://example.test/anthropic/v1"
        llm = self._llm(saved)
        client = llm.get_llm()
        assert llm.base_url == saved
        assert client.metadata["provider_route"].endswith(saved)

    def test_manifest_base_is_unchanged(self):
        client = LLM("deepseek-flash", api_key="test-key").get_llm()
        assert client.anthropic_api_url == LLM("deepseek-flash", api_key="k").provider_info["base_url"]

    def test_a_parameters_alias_is_normalized_too(self):
        llm = self._llm("https://example.test/anthropic")
        llm.parameters["anthropic_api_url"] = "https://other.test/anthropic/v1"
        assert llm.get_llm().anthropic_api_url == "https://other.test/anthropic"

    def test_no_base_url_leaves_the_sdk_default(self, monkeypatch):
        for name in ("ANTHROPIC_API_URL", "ANTHROPIC_BASE_URL"):
            monkeypatch.delenv(name, raising=False)
        client = LLM("claude-sonnet-5-5", api_key="test-key").get_llm()
        assert client.anthropic_api_url == "https://api.anthropic.com"


class TestKeyTest:
    """The key test calls the host the way the SDK would."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "saved",
        ["https://example.test/anthropic/v1", "https://example.test/anthropic/v1/"],
    )
    async def test_probe_does_not_double_the_version(self, saved):
        from src.server.services.llm.key_probe import _test_anthropic_key

        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            return httpx.Response(200, json={"data": [{"id": "m1"}]})

        result = await _test_anthropic_key(
            "test-key", "m1", {"sdk": "anthropic"}, saved, 5.0, transport=httpx.MockTransport(handler)
        )

        assert seen == ["/anthropic/v1/models"]
        assert result == {"models_found": 1, "model": "m1"}

    @pytest.mark.asyncio
    async def test_inference_fallback_posts_to_the_messages_path(self):
        from src.server.services.llm.key_probe import _test_anthropic_key

        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append((request.method, request.url.path))
            return httpx.Response(404 if request.method == "GET" else 200, json={})

        await _test_anthropic_key(
            "test-key",
            "m1",
            {"sdk": "anthropic"},
            "https://example.test/anthropic/v1",
            5.0,
            transport=httpx.MockTransport(handler),
        )

        assert seen == [
            ("GET", "/anthropic/v1/models"),
            ("POST", "/anthropic/v1/messages"),
        ]
