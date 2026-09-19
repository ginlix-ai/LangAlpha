"""Tests for #221: custom providers must inherit their parent's SDK.

A user-defined ``custom_providers`` slug is absent from the manifest, so
``from_custom_config`` derives ``sdk`` from an empty ``provider_info`` and
defaults to ``"openai"``. For an Anthropic-parented custom provider that
produces an OpenAI client pointed at an Anthropic endpoint → 404 on
``/chat/completions``. ``_resolve_custom_model_byok`` must rewrite the
provider to the manifest parent so the SDK resolves correctly — while leaving
OpenAI-compatible gateways untouched (no forced Responses API).
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.llm.clients import _resolve_custom_model_byok

H = "src.server.services.llm.config"
USER_MODELS = "src.server.services.llm.user_models"
CLIENTS = "src.server.services.llm.clients"
DBK = "src.server.database.api_keys"


@pytest.fixture(autouse=True)
def _real_manifest():
    """``LLM._model_config`` is a class-level cache, so a mock another module
    installed outlives its own ``patch`` block and answers every SDK lookup
    here. Drop it on both sides so these tests always read the real manifest."""
    from src.llms.llm import LLM

    LLM._model_config = None
    try:
        yield
    finally:
        LLM._model_config = None


def _mock_mc(parent_sdk, *, parent_extra=None):
    """ModelConfig whose only manifest provider is the parent."""
    parents = {"vendor-parent": {"sdk": parent_sdk, **(parent_extra or {})}}
    mc = MagicMock()
    mc.get_provider_info.side_effect = lambda p: parents.get(p, {})
    return mc


def _patches(provider_def, key=None):
    """Stub custom-provider lookup + BYOK key store for one custom provider."""
    key = key or {"api_key": "user-key", "base_url": "https://gw.example/anthropic"}

    async def get_cp(user_id, name, _pref_cache=None):
        return provider_def if name == provider_def["name"] else None

    return (
        patch(f"{USER_MODELS}.get_custom_provider_config", new_callable=AsyncMock, side_effect=get_cp),
        patch(f"{DBK}.get_byok_config_for_provider", new_callable=AsyncMock, return_value=key),
        patch(f"{DBK}.get_byok_configs_for_providers", new_callable=AsyncMock, return_value={}),
    )


@pytest.mark.asyncio
async def test_path2_anthropic_parent_rewrites_provider_to_parent():
    """Model's ``provider`` is a custom slug with an anthropic parent → the
    returned config provider is rewritten to the parent so SDK = anthropic."""
    provider_def = {"name": "my-anthropic-gw", "parent_provider": "vendor-parent"}
    custom_model = {"name": "eval-model", "model_id": "some-model", "provider": "my-anthropic-gw"}
    mc = _mock_mc("anthropic")

    p1, p2, p3 = _patches(provider_def)
    with p1, p2, p3:
        byok, base_url, out = await _resolve_custom_model_byok("u", "eval-model", dict(custom_model), mc)

    # SDK now resolves via the parent's manifest entry, not the (absent) slug.
    assert out["provider"] == "vendor-parent"
    sdk = mc.get_provider_info(out["provider"]).get("sdk") or "openai"
    assert sdk == "anthropic"
    assert base_url == "https://gw.example/anthropic"


@pytest.mark.asyncio
async def test_path1_anthropic_parent_rewrites_provider_to_parent():
    """Model name itself is the custom provider slug (Path 1) → same rewrite."""
    provider_def = {"name": "my-anthropic-gw", "parent_provider": "vendor-parent"}
    custom_model = {"name": "my-anthropic-gw", "model_id": "some-model", "provider": "my-anthropic-gw"}
    mc = _mock_mc("anthropic")

    p1, p2, p3 = _patches(provider_def)
    with p1, p2, p3:
        byok, base_url, out = await _resolve_custom_model_byok("u", "my-anthropic-gw", dict(custom_model), mc)

    assert out["provider"] == "vendor-parent"


@pytest.mark.asyncio
async def test_openai_parent_does_not_rewrite_or_force_response_api():
    """OpenAI-compatible gateway: provider must stay the custom slug so the
    default sdk="openai" applies WITHOUT inheriting the manifest openai entry's
    use_response_api (which would break /chat/completions-only gateways)."""
    provider_def = {"name": "my-openai-gw", "parent_provider": "vendor-parent"}
    custom_model = {"name": "eval-model", "model_id": "some-model", "provider": "my-openai-gw"}
    # Parent openai entry carries use_response_api=True — must NOT leak through.
    mc = _mock_mc("openai", parent_extra={"use_response_api": True})

    p1, p2, p3 = _patches(provider_def)
    with p1, p2, p3:
        byok, base_url, out = await _resolve_custom_model_byok("u", "eval-model", dict(custom_model), mc)

    # Provider unchanged → from_custom_config sees {} → sdk defaults to openai,
    # use_response_api stays False (the safe gateway default).
    assert out["provider"] == "my-openai-gw"
    assert "_use_response_api" not in out


@pytest.mark.asyncio
async def test_dashscope_parent_is_matched_in_full():
    """A parent with its own SDK is a dialect, so naming it inherits all of it.

    DashScope declares ``sdk: "dashscope"`` because it has a client of its own,
    which is also what rescues its raw reasoning. A gateway that names it as a
    parent is saying it is that thing, so it gets the client, the headers and
    the manifest's ``use_response_api`` without opting in separately. Contrast
    the ``openai`` parent above, which is a wire shape rather than a vendor.
    """
    provider_def = {"name": "my-qwen-gw", "parent_provider": "vendor-parent"}
    custom_model = {"name": "eval-model", "model_id": "some-model", "provider": "my-qwen-gw"}
    mc = _mock_mc("dashscope", parent_extra={"use_response_api": True})

    p1, p2, p3 = _patches(provider_def)
    with p1, p2, p3:
        byok, base_url, out = await _resolve_custom_model_byok("u", "eval-model", dict(custom_model), mc)

    assert out["provider"] == "vendor-parent"


@pytest.mark.asyncio
async def test_custom_provider_explicit_use_response_api_opt_in_preserved():
    """A custom provider that explicitly opts into the Responses API still gets
    the ``_use_response_api`` flag, regardless of parent SDK."""
    provider_def = {
        "name": "my-openai-gw", "parent_provider": "vendor-parent", "use_response_api": True,
    }
    custom_model = {"name": "eval-model", "model_id": "some-model", "provider": "my-openai-gw"}
    mc = _mock_mc("openai")

    p1, p2, p3 = _patches(provider_def)
    with p1, p2, p3:
        byok, base_url, out = await _resolve_custom_model_byok("u", "eval-model", dict(custom_model), mc)

    assert out.get("_use_response_api") is True


@pytest.mark.asyncio
async def test_an_explicit_responses_opt_out_survives_the_inheritance():
    """Inheriting a parent's default is not the same as ignoring what the child said.

    ``users.py`` validates this field as a boolean, so ``false`` is a value a
    gateway can actually store, and it means it speaks only
    ``/chat/completions``. A truthiness check here forwards only ``true`` and
    leaves ``false`` indistinguishable from absent, which hands the gateway the
    parent's manifest default and points every call at ``/responses``.
    """
    provider_def = {
        "name": "my-qwen-gw",
        "parent_provider": "vendor-parent",
        "use_response_api": False,
    }
    custom_model = {"name": "eval-model", "model_id": "some-model", "provider": "my-qwen-gw"}
    mc = _mock_mc("dashscope", parent_extra={"use_response_api": True})

    p1, p2, p3 = _patches(provider_def)
    with p1, p2, p3:
        byok, base_url, out = await _resolve_custom_model_byok("u", "eval-model", dict(custom_model), mc)

    # Still the parent's dialect: the opt-out is about the route, not the client.
    assert out["provider"] == "vendor-parent"
    assert out["_use_response_api"] is False


def test_an_explicit_opt_out_reaches_the_client_as_chat_completions():
    """The opt-out has to survive to the built client, not just the config.

    ``False`` and absent take different paths through ``ModelSpec`` only if the
    key is present, so this is the assertion that a truthiness check breaks.
    """
    from src.llms.extension import ChatDashScope
    from src.llms.llm import LLM

    def build(**extra):
        return LLM.from_custom_config(
            {"name": "eval-model", "model_id": "qwen3.8-max", "provider": "dashscope", **extra},
            api_key="sk-test",
            base_url_override="https://gw.example.com/v1",
        )

    opted_out = build(_use_response_api=False)
    assert isinstance(opted_out, ChatDashScope)  # the bridge still applies
    assert opted_out.output_version != "responses/v1"

    # Absent still inherits the parent's default, which is the rule above.
    assert build().output_version == "responses/v1"


def test_an_inherited_dashscope_provider_actually_builds_the_bridged_client():
    """The rewrite above is only worth anything if it changes the client class.

    Asserting on the config alone would keep passing if the factory stopped
    honouring ``provider``, which is the failure the rewrite exists to prevent.
    """
    from langchain_openai import ChatOpenAI

    from src.llms.extension import ChatDashScope
    from src.llms.llm import LLM

    # No ``_use_response_api``: the inherited parent is what supplies it, which
    # is the half of "matched in full" a config assertion cannot see.
    def build(provider):
        return LLM.from_custom_config(
            {"name": "eval-model", "model_id": "qwen3.8-max", "provider": provider},
            api_key="sk-test",
            base_url_override="https://gw.example.com/v1",
        )

    not_inherited = build("my-qwen-gw")
    assert isinstance(not_inherited, ChatOpenAI)
    assert not isinstance(not_inherited, ChatDashScope)

    inherited = build("dashscope")
    assert isinstance(inherited, ChatDashScope)
    # The Responses opt-in rides as ``output_version``; see llm.py:698.
    assert inherited.output_version == "responses/v1"
    # The gateway's own endpoint outranks the manifest's, both ways.
    assert inherited.openai_api_base == "https://gw.example.com/v1"
