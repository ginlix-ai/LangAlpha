"""Endpoint gate for the ``prompt_cache_options`` explicit-caching opt-in.

The manifest attaches ``prompt_cache_options`` to eligible OpenAI models, but
the param (and the breakpoint marker it gates) is an api.openai.com-only
surface. ``_get_openai_llm`` must strip it whenever the effective base_url
points anywhere else (platform proxy, groq/cerebras/local endpoints, env
redirects), and ``_get_codex_llm`` must never forward it (the codex backend
400s on it).
"""

from __future__ import annotations

import pytest

from src.llms.endpoints import is_official_openai_endpoint
from src.llms.llm import LLM

_OPTIONS = {"mode": "implicit"}


@pytest.fixture(autouse=True)
def _clean_openai_base_env(monkeypatch):
    monkeypatch.delenv("OPENAI_API_BASE", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)


def _build_llm(sdk: str, base_url: str | None, parameters: dict | None = None) -> LLM:
    llm = LLM.__new__(LLM)
    llm.sdk = sdk
    llm.provider = f"test-{sdk}"
    llm.provider_info = {"access_type": "api_key"}
    llm.env_key = None
    llm.base_url = base_url
    llm.default_headers = None
    llm.use_response_api = sdk == "codex"
    llm.use_previous_response_id = False
    llm.parameters = dict(parameters or {})
    llm.extra_body = {}
    llm.model = "gpt-5.6-sol"
    llm.api_key_override = "dummy-key"
    llm.prompt_cache_key_enabled = False
    return llm


class TestOpenAIEndpointGate:
    def test_official_base_url_keeps_options(self):
        llm = _build_llm(
            "openai",
            "https://api.openai.com/v1",
            {"prompt_cache_options": dict(_OPTIONS)},
        )
        client = llm.get_llm()
        assert client.prompt_cache_options == _OPTIONS

    def test_default_base_url_keeps_options(self):
        llm = _build_llm("openai", None, {"prompt_cache_options": dict(_OPTIONS)})
        client = llm.get_llm()
        assert client.prompt_cache_options == _OPTIONS

    def test_non_official_base_url_strips_options(self):
        llm = _build_llm(
            "openai",
            "https://proxy.example.com/v1",
            {"prompt_cache_options": dict(_OPTIONS)},
        )
        client = llm.get_llm()
        assert client.prompt_cache_options is None

    def test_env_redirect_strips_options(self, monkeypatch):
        monkeypatch.setenv("OPENAI_BASE_URL", "http://localhost:9000/v1")
        llm = _build_llm("openai", None, {"prompt_cache_options": dict(_OPTIONS)})
        client = llm.get_llm()
        assert client.prompt_cache_options is None

    def test_strip_leaves_other_parameters_intact(self):
        llm = _build_llm(
            "openai",
            "https://proxy.example.com/v1",
            {"prompt_cache_options": dict(_OPTIONS), "temperature": 0.3},
        )
        # gpt-5.x is a reasoning family: langchain-openai would normalize
        # temperature away on its own, masking what this test asserts.
        llm.model = "gpt-4o-mini"
        client = llm.get_llm()
        assert client.prompt_cache_options is None
        assert client.temperature == 0.3

    def test_codex_never_forwards_options(self):
        llm = _build_llm("codex", None, {"prompt_cache_options": dict(_OPTIONS)})
        client = llm.get_llm()
        assert client.prompt_cache_options is None

    def test_openai_api_base_alias_strips_options(self):
        # ChatOpenAI accepts base_url under its openai_api_base alias; the
        # gate must see through both spellings.
        llm = _build_llm(
            "openai",
            None,
            {
                "prompt_cache_options": dict(_OPTIONS),
                "openai_api_base": "https://proxy.example.com/v1",
            },
        )
        client = llm.get_llm()
        assert client.prompt_cache_options is None


class TestIsOfficialOpenAIEndpoint:
    def test_none_defaults_to_official(self):
        assert is_official_openai_endpoint(None) is True

    def test_official_url(self):
        assert is_official_openai_endpoint("https://api.openai.com/v1") is True

    def test_other_host(self):
        assert is_official_openai_endpoint("https://api.groq.com/openai/v1") is False

    def test_lookalike_host_rejected(self):
        assert is_official_openai_endpoint("https://api.openai.com.evil.example/v1") is False

    def test_env_api_base_fallback(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_BASE", "http://localhost:1234/v1")
        assert is_official_openai_endpoint(None) is False

    def test_schemeless_official_host(self):
        assert is_official_openai_endpoint("api.openai.com/v1") is True

    def test_schemeless_lookalike_rejected(self):
        assert is_official_openai_endpoint("api.openai.com.evil.example/v1") is False
        assert is_official_openai_endpoint("api.openai.com@evil.example/v1") is False
