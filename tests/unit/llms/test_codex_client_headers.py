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


def _build_openai_llm(
    default_headers: dict | None = None, parameters: dict | None = None
) -> LLM:
    llm = LLM.__new__(LLM)
    llm.sdk = "openai"
    llm.provider = "test-openai"
    llm.provider_info = {"access_type": "platform"}
    llm.env_key = None
    llm.base_url = None
    llm.default_headers = default_headers
    llm.use_response_api = False
    llm.use_previous_response_id = False
    llm.parameters = dict(parameters or {})
    llm.extra_body = {}
    llm.model = "gpt-4o-mini"
    llm.api_key_override = "dummy-token"
    llm.prompt_cache_key_enabled = False
    return llm


def _build_anthropic_llm(
    default_headers: dict | None = None, parameters: dict | None = None
) -> LLM:
    llm = LLM.__new__(LLM)
    llm.sdk = "anthropic"
    llm.provider = "test-anthropic"
    llm.provider_info = {"access_type": "platform"}
    llm.env_key = None
    llm.base_url = None
    llm.default_headers = default_headers
    llm.use_response_api = False
    llm.use_previous_response_id = False
    llm.parameters = dict(parameters or {})
    llm.extra_body = {}
    llm.model = "claude-test-model"
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


class TestSessionAffinityHeaders:
    """The codex backend routes its prompt cache by session headers, not by the
    ``prompt_cache_key`` body param — without them, warm-chain reads scatter
    across replicas (random total misses / stale hits)."""

    HEADERS = ("session-id", "thread-id", "session_id")

    def test_uuid_cache_key_passes_through(self):
        tid = "00000000-0000-4000-8000-000000000001"
        client = _build_codex_llm().get_llm(cache_key=tid)
        for h in self.HEADERS:
            assert client.default_headers[h] == tid

    def test_non_uuid_cache_key_derives_stable_uuid(self):
        import uuid as _uuid

        a = _build_codex_llm().get_llm(cache_key="thread-abc:compact")
        b = _build_codex_llm().get_llm(cache_key="thread-abc:compact")
        val = a.default_headers["session-id"]
        _uuid.UUID(val)  # must be a valid UUID string
        assert all(b.default_headers[h] == val for h in self.HEADERS)

    def test_distinct_cache_keys_derive_distinct_affinity(self):
        a = _build_codex_llm().get_llm(cache_key="thread-a:compact")
        b = _build_codex_llm().get_llm(cache_key="thread-b:compact")
        assert a.default_headers["session-id"] != b.default_headers["session-id"]

    def test_non_canonical_uuid_is_normalized(self):
        import uuid as _uuid

        raw = "0000000000004000800000000000000A"  # dashless, uppercase
        client = _build_codex_llm().get_llm(cache_key=raw)
        val = client.default_headers["session-id"]
        assert val == str(_uuid.UUID(raw))
        assert val != raw

    def test_no_cache_key_sends_no_session_headers(self):
        client = _build_codex_llm().get_llm()
        for h in self.HEADERS:
            assert h not in client.default_headers

    def test_instance_headers_win_over_derived(self):
        client = _build_codex_llm({"session-id": "pinned"}).get_llm(
            cache_key="00000000-0000-4000-8000-000000000001"
        )
        assert client.default_headers["session-id"] == "pinned"

    def test_partial_instance_pin_still_derives_others(self):
        tid = "00000000-0000-4000-8000-000000000001"
        client = _build_codex_llm({"session-id": "pinned"}).get_llm(cache_key=tid)
        assert client.default_headers["session-id"] == "pinned"
        assert client.default_headers["thread-id"] == tid
        assert client.default_headers["session_id"] == tid

    def test_differently_cased_pin_blocks_derived_duplicate(self):
        tid = "00000000-0000-4000-8000-000000000001"
        client = _build_codex_llm({"Session-Id": "pinned"}).get_llm(cache_key=tid)
        assert client.default_headers["Session-Id"] == "pinned"
        assert "session-id" not in client.default_headers
        assert client.default_headers["thread-id"] == tid


class TestNarrowedAffinityHeaders:
    """Narrowing must NOT touch session headers: they pin a replica (not a
    cache scope), and ``model_copy`` shares the parent's built HTTP clients so
    a field-level header rewrite would be wire-inert anyway (verified against
    the live backend — the copied client still sends the parent's headers)."""

    TID = "00000000-0000-4000-8000-000000000001"

    def test_narrow_keeps_parent_session_lane(self):
        from src.llms.llm import narrow_prompt_cache_key

        client = _build_codex_llm().get_llm(cache_key=self.TID)
        narrowed = narrow_prompt_cache_key(client, "general-purpose")
        assert (
            narrowed.model_kwargs["prompt_cache_key"] == f"{self.TID}:general-purpose"
        )
        for h in TestSessionAffinityHeaders.HEADERS:
            assert narrowed.default_headers[h] == self.TID


class TestParametersDefaultHeadersMerge:
    """A ``parameters['default_headers']`` entry is merged AFTER
    ``params.update(self.parameters)`` on both factories, so it augments the
    provider-level (and, for codex, the first-party) headers instead of
    replacing the whole mapping the way the pre-merge assignment did."""

    def test_codex_parameters_headers_augment_provider(self):
        llm = _build_codex_llm({"ChatGPT-Account-Id": "acct-1"})
        llm.parameters = {"default_headers": {"X-Extra": "from-params"}}
        client = llm.get_llm()
        # provider-level header survives the parameters merge
        assert client.default_headers["ChatGPT-Account-Id"] == "acct-1"
        # parameters-level header is added
        assert client.default_headers["X-Extra"] == "from-params"
        # first-party gating headers still present
        assert client.default_headers["originator"] == "codex_cli_rs"
        assert client.default_headers["User-Agent"].startswith("codex_cli_rs/")

    def test_codex_parameters_header_overrides_provider_same_key(self):
        llm = _build_codex_llm({"X-Dup": "provider"})
        llm.parameters = {"default_headers": {"X-Dup": "params"}}
        client = llm.get_llm()
        assert client.default_headers["X-Dup"] == "params"

    def test_openai_parameters_headers_augment_provider(self):
        llm = _build_openai_llm(
            default_headers={"X-Provider": "p"},
            parameters={"default_headers": {"X-Param": "q"}},
        )
        client = llm.get_llm()
        assert client.default_headers["X-Provider"] == "p"
        assert client.default_headers["X-Param"] == "q"

    def test_openai_provider_headers_land_on_client(self):
        llm = _build_openai_llm(default_headers={"X-Provider": "p"})
        client = llm.get_llm()
        assert client.default_headers["X-Provider"] == "p"

    def test_openai_no_headers_leaves_default_headers_unset(self):
        client = _build_openai_llm().get_llm()
        assert not client.default_headers

    def test_anthropic_parameters_headers_augment_provider(self):
        llm = _build_anthropic_llm(
            default_headers={"X-Provider": "p"},
            parameters={"default_headers": {"X-Param": "q"}},
        )
        client = llm.get_llm()
        assert client.default_headers["X-Provider"] == "p"
        assert client.default_headers["X-Param"] == "q"
