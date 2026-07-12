"""First-party client headers on codex requests.

The codex backend serves some models (e.g. GPT-5.6 Luna) only to recognized
first-party clients: a request must carry an ``originator`` naming a known
client AND a ``User-Agent`` with the matching ``<originator>/`` prefix, or it
404s "Model not found". ``_get_codex_llm`` must always inject the pair while
still honoring per-instance headers (ChatGPT-Account-Id, explicit overrides).
"""

from __future__ import annotations

from src.llms.llm import LLM


def _build_codex_llm(default_headers: dict | None = None) -> LLM:
    llm = LLM.__new__(LLM)
    llm.sdk = "codex"
    llm.provider = "test-codex"
    llm.provider_info = {"access_type": "oauth"}
    llm.env_key = None
    llm.base_url = None
    llm.default_headers = default_headers
    llm.use_response_api = True
    llm.use_previous_response_id = False
    llm.parameters = {}
    llm.extra_body = {}
    llm.model = "gpt-5.6-luna"
    llm.api_key_override = "dummy-token"
    llm.prompt_cache_key_enabled = False
    return llm


def test_first_party_headers_always_present():
    client = _build_codex_llm().get_llm()
    assert client.default_headers["originator"] == "codex_cli_rs"
    assert client.default_headers["User-Agent"].startswith("codex_cli_rs/")


def test_instance_headers_merged_and_override():
    client = _build_codex_llm(
        {"ChatGPT-Account-Id": "acct-123", "originator": "custom"}
    ).get_llm()
    assert client.default_headers["ChatGPT-Account-Id"] == "acct-123"
    assert client.default_headers["originator"] == "custom"
    assert client.default_headers["User-Agent"].startswith("codex_cli_rs/")
