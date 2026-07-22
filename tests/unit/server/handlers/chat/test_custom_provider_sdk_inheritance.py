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

from src.server.services.llm.config import _resolve_custom_model_byok

H = "src.server.services.llm.config"
DBK = "src.server.database.api_keys"


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
        patch(f"{H}.get_custom_provider_config", new_callable=AsyncMock, side_effect=get_cp),
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
