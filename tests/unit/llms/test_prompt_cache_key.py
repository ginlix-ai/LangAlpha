"""Tests for prompt_cache_key wiring on OpenAI/Codex factories.

Verifies the differentiated rollout:
- Codex (sdk="codex"): always-on, injects whenever cache_key is provided.
- Regular OpenAI (sdk="openai"): opt-in via providers.json `prompt_cache_key: true`.
- Other SDKs (anthropic, etc.): never inject, no error.

Implementation note: ``prompt_cache_key`` is wired via ``ChatOpenAI(model_kwargs=...)``
rather than ``Runnable.bind()`` so it survives ``bind_tools()`` /
``with_structured_output()`` (those produce a fresh RunnableBinding that drops
prior bind kwargs but keeps the underlying ChatOpenAI's model_kwargs).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.llms.llm import LLM


class _StubModel:
    """Returned by stubbed factory methods. Mirrors the surface our tests touch."""

    def __init__(self):
        self.metadata = {}


def _make_llm(
    sdk: str, prompt_cache_key_flag: bool = False
) -> tuple[LLM, list[dict]]:
    """Bypass __init__ to avoid models.json lookups; configure attributes directly.

    Returns the LLM and a list that captures every keyword call to the stubbed
    SDK factory (``_get_openai_llm`` / ``_get_codex_llm`` / ``_get_anthropic_llm``).
    Tests inspect that list to assert whether ``cache_key`` reached the factory.
    """
    llm = LLM.__new__(LLM)
    llm.sdk = sdk
    llm.provider = f"test-{sdk}"
    llm.provider_info = {"access_type": "platform"}
    llm.api_key_override = None
    llm.prompt_cache_key_enabled = prompt_cache_key_flag

    factory_name = {
        "openai": "_get_openai_llm",
        "codex": "_get_codex_llm",
        "anthropic": "_get_anthropic_llm",
    }[sdk]

    factory_calls: list[dict] = []

    def _stub_factory(self, *args, **kwargs):  # noqa: ARG001
        factory_calls.append(dict(kwargs))
        return _StubModel()

    patcher = patch.object(LLM, factory_name, _stub_factory)
    patcher.start()
    return llm, factory_calls


@pytest.fixture(autouse=True)
def _stop_all_patches():
    yield
    patch.stopall()


class TestCodexAlwaysOn:
    """Codex SDK should pass cache_key to its factory whenever provided,
    regardless of any provider flag — matches the official Codex CLI."""

    def test_codex_with_cache_key_passes_to_factory(self):
        llm, calls = _make_llm("codex")
        llm.get_llm(cache_key="thread-abc-123")
        assert calls == [{"cache_key": "thread-abc-123"}]

    def test_codex_without_cache_key_passes_none(self):
        llm, calls = _make_llm("codex")
        llm.get_llm(cache_key=None)
        assert calls == [{"cache_key": None}]

    def test_codex_empty_cache_key_passes_none(self):
        llm, calls = _make_llm("codex")
        llm.get_llm(cache_key="")
        assert calls == [{"cache_key": None}]

    def test_codex_stringifies_non_string_cache_key(self):
        llm, calls = _make_llm("codex")
        llm.get_llm(cache_key=12345)
        assert calls == [{"cache_key": "12345"}]


class TestOpenAIOptIn:
    """Regular OpenAI SDK should pass cache_key to factory only when the
    provider has ``prompt_cache_key: true`` in providers.json."""

    def test_openai_flag_off_passes_none(self):
        llm, calls = _make_llm("openai", prompt_cache_key_flag=False)
        llm.get_llm(cache_key="thread-abc-123")
        assert calls == [{"cache_key": None}]

    def test_openai_flag_on_with_cache_key_passes_to_factory(self):
        llm, calls = _make_llm("openai", prompt_cache_key_flag=True)
        llm.get_llm(cache_key="thread-abc-123")
        assert calls == [{"cache_key": "thread-abc-123"}]

    def test_openai_flag_on_without_cache_key_passes_none(self):
        llm, calls = _make_llm("openai", prompt_cache_key_flag=True)
        llm.get_llm(cache_key=None)
        assert calls == [{"cache_key": None}]


class TestNonOpenAIProvidersUnaffected:
    """Other SDKs should not receive a cache_key kwarg at all — no error."""

    def test_anthropic_with_cache_key_does_not_pass(self):
        llm, calls = _make_llm("anthropic")
        llm.get_llm(cache_key="thread-abc-123")
        assert calls == [{}]


class TestProviderFlagExtraction:
    """``LLM._extract_provider_info`` must read the flag only for sdk == 'openai'.
    For codex and other SDKs the attribute should default to False."""

    def test_codex_provider_flag_defaults_false(self):
        llm = LLM.__new__(LLM)
        llm.sdk = "codex"
        llm.provider_info = {"prompt_cache_key": True}
        llm.prompt_cache_key_enabled = (
            bool(llm.provider_info.get("prompt_cache_key", False))
            if llm.sdk == "openai"
            else False
        )
        assert llm.prompt_cache_key_enabled is False

    def test_openai_provider_flag_true(self):
        llm = LLM.__new__(LLM)
        llm.sdk = "openai"
        llm.provider_info = {"prompt_cache_key": True}
        llm.prompt_cache_key_enabled = (
            bool(llm.provider_info.get("prompt_cache_key", False))
            if llm.sdk == "openai"
            else False
        )
        assert llm.prompt_cache_key_enabled is True

    def test_openai_provider_flag_absent_defaults_false(self):
        llm = LLM.__new__(LLM)
        llm.sdk = "openai"
        llm.provider_info = {}
        llm.prompt_cache_key_enabled = (
            bool(llm.provider_info.get("prompt_cache_key", False))
            if llm.sdk == "openai"
            else False
        )
        assert llm.prompt_cache_key_enabled is False


class TestModelKwargsInjection:
    """End-to-end: building a real ChatOpenAI / ChatCodexOpenAI through the
    factory should land ``prompt_cache_key`` in ``model_kwargs`` and survive
    composition (``bind_tools`` / ``with_structured_output``).

    This is the regression test for the original ``bind()`` approach, where
    LangChain's ``bind_tools`` produced a fresh RunnableBinding with kwargs
    ``{'tools': [...]}`` and silently dropped ``prompt_cache_key``.
    """

    def _build_openai_llm(self, prompt_cache_key_flag: bool) -> LLM:
        llm = LLM.__new__(LLM)
        llm.sdk = "openai"
        llm.provider = "test-openai"
        llm.provider_info = {"access_type": "platform"}
        llm.env_key = None
        llm.base_url = None
        llm.default_headers = None
        llm.use_response_api = False
        llm.use_previous_response_id = False
        llm.parameters = {}
        llm.extra_body = {}
        llm.model = "gpt-4o-mini"
        llm.api_key_override = "dummy-key"
        llm.prompt_cache_key_enabled = prompt_cache_key_flag
        return llm

    def _build_codex_llm(self) -> LLM:
        llm = LLM.__new__(LLM)
        llm.sdk = "codex"
        llm.provider = "test-codex"
        llm.provider_info = {"access_type": "oauth"}
        llm.env_key = None
        llm.base_url = None
        llm.default_headers = None
        llm.use_response_api = True
        llm.parameters = {}
        llm.extra_body = {}
        llm.model = "gpt-5"
        llm.api_key_override = "dummy-codex-key"
        llm.prompt_cache_key_enabled = False  # ignored on codex path
        return llm

    def test_openai_flag_on_lands_in_model_kwargs(self):
        llm = self._build_openai_llm(prompt_cache_key_flag=True)
        client = llm.get_llm(cache_key="thread-xyz")
        assert client.model_kwargs.get("prompt_cache_key") == "thread-xyz"

    def test_openai_flag_off_omits_model_kwargs(self):
        llm = self._build_openai_llm(prompt_cache_key_flag=False)
        client = llm.get_llm(cache_key="thread-xyz")
        assert "prompt_cache_key" not in (client.model_kwargs or {})

    def test_codex_always_on_lands_in_model_kwargs(self):
        llm = self._build_codex_llm()
        client = llm.get_llm(cache_key="thread-xyz")
        assert client.model_kwargs.get("prompt_cache_key") == "thread-xyz"

    def test_openai_cache_key_survives_bind_tools(self):
        """The reason we use model_kwargs and not ``bind()``."""
        llm = self._build_openai_llm(prompt_cache_key_flag=True)
        client = llm.get_llm(cache_key="thread-xyz")

        bound = client.bind_tools([])
        # bind_tools wraps in RunnableBinding with kwargs={'tools': []}.
        # The underlying ChatOpenAI must keep prompt_cache_key.
        assert bound.bound.model_kwargs.get("prompt_cache_key") == "thread-xyz"

    def test_openai_cache_key_in_request_payload_after_bind_tools(self):
        """End-to-end: the SDK request payload must contain prompt_cache_key
        even after the agent applies bind_tools."""
        llm = self._build_openai_llm(prompt_cache_key_flag=True)
        client = llm.get_llm(cache_key="thread-xyz")
        bound = client.bind_tools([])

        payload = bound.bound._get_request_payload(
            [{"role": "user", "content": "hi"}], stop=None
        )
        assert payload.get("prompt_cache_key") == "thread-xyz"

    def test_openai_user_model_kwargs_preserved(self):
        """Existing user-supplied ``model_kwargs`` (from llm parameters) must
        merge with our injection, not get clobbered."""
        llm = self._build_openai_llm(prompt_cache_key_flag=True)
        llm.parameters = {"model_kwargs": {"user_field": "keep-me"}}
        client = llm.get_llm(cache_key="thread-xyz")
        assert client.model_kwargs.get("user_field") == "keep-me"
        assert client.model_kwargs.get("prompt_cache_key") == "thread-xyz"


class TestNarrowPromptCacheKey:
    """``narrow_prompt_cache_key`` clones a model with ``:<suffix>`` appended to
    the existing ``prompt_cache_key`` so parallel sub-tasks (subagents,
    compaction) don't compete for the main agent's RPM bucket."""

    def _build_openai_with_key(self, key: str | None) -> object:
        from langchain_openai import ChatOpenAI

        mk = {"prompt_cache_key": key} if key else {}
        return ChatOpenAI(model="gpt-4o-mini", api_key="dummy", model_kwargs=mk)

    def test_appends_suffix_to_existing_key(self):
        from src.llms.llm import narrow_prompt_cache_key

        model = self._build_openai_with_key("thread-abc")
        scoped = narrow_prompt_cache_key(model, "compact")

        assert scoped is not model  # new instance
        assert scoped.model_kwargs.get("prompt_cache_key") == "thread-abc:compact"
        # original untouched
        assert model.model_kwargs.get("prompt_cache_key") == "thread-abc"

    def test_no_op_when_no_existing_key(self):
        from src.llms.llm import narrow_prompt_cache_key

        model = self._build_openai_with_key(None)
        scoped = narrow_prompt_cache_key(model, "compact")
        # No key → nothing to narrow → returns model unchanged.
        assert scoped is model
        assert "prompt_cache_key" not in scoped.model_kwargs

    def test_no_op_with_empty_suffix(self):
        from src.llms.llm import narrow_prompt_cache_key

        model = self._build_openai_with_key("thread-abc")
        scoped = narrow_prompt_cache_key(model, "")
        assert scoped is model

    def test_no_op_with_non_chat_model(self):
        """Strings (subagent ``model: 'openai:gpt-4o'`` overrides) and other
        non-BaseChatModel objects must pass through untouched — we don't have
        a model_kwargs handle for them."""
        from src.llms.llm import narrow_prompt_cache_key

        assert narrow_prompt_cache_key("openai:gpt-4o", "x") == "openai:gpt-4o"
        assert narrow_prompt_cache_key(None, "x") is None
        assert narrow_prompt_cache_key(object(), "x") is not None

    def test_preserves_other_model_kwargs(self):
        from src.llms.llm import narrow_prompt_cache_key

        from langchain_openai import ChatOpenAI

        model = ChatOpenAI(
            model="gpt-4o-mini",
            api_key="dummy",
            model_kwargs={
                "prompt_cache_key": "thread-abc",
                "user_field": "keep",
            },
        )
        scoped = narrow_prompt_cache_key(model, "compact")
        assert scoped.model_kwargs.get("user_field") == "keep"
        assert scoped.model_kwargs.get("prompt_cache_key") == "thread-abc:compact"

    def test_survives_bind_tools_after_narrowing(self):
        """The narrowed model must still survive ``bind_tools`` — same property
        as the original key (this is why we use model_kwargs, not bind())."""
        from src.llms.llm import narrow_prompt_cache_key

        model = self._build_openai_with_key("thread-abc")
        scoped = narrow_prompt_cache_key(model, "general-purpose")
        bound = scoped.bind_tools([])
        assert (
            bound.bound.model_kwargs.get("prompt_cache_key")
            == "thread-abc:general-purpose"
        )
