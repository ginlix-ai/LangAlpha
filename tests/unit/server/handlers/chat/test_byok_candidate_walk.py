"""Tests for _walk_byok_candidates() and the system-branch coding-variant fix.

A neutral placeholder model ``vendor-x-coding`` (anthropic-style coding
endpoint) whose parent ``vendor-x`` is openai-style reproduces the SDK-trap:
the key may live under the variant slug OR the parent, but the client must
always build against the MODEL'S OWN endpoint. Also covers the _byok_cache
tri-state miss-safety contract.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

HANDLER = "src.server.handlers.chat.llm_config"
DB_KEYS = "src.server.database.api_keys"

CODING_URL = "https://coding.example/anthropic"
PARENT_URL = "https://api.example/v1"


def _mock_mc():
    """vendor-x-coding (own endpoint CODING_URL) is a child variant of
    vendor-x (parent endpoint PARENT_URL)."""
    providers = {
        "vendor-x-coding": {"parent": "vendor-x", "base_url": CODING_URL},
        "vendor-x": {"base_url": PARENT_URL},
    }
    system_models = {
        "vendor-x-coding": {"provider": "vendor-x-coding"},
        # A system model tagged with the PARENT slug — used by the
        # sibling-held-key mirror case.
        "vendor-x": {"provider": "vendor-x"},
    }

    mc = MagicMock()
    mc.get_model_config.side_effect = lambda n: system_models.get(n)
    mc.get_provider_info.side_effect = lambda n: providers.get(n, {})
    mc.get_parent_provider.side_effect = lambda n: providers.get(n, {}).get("parent", n)
    mc.get_child_variants.side_effect = lambda n: [
        k for k, info in providers.items() if info.get("parent") == n and k != n
    ]
    return mc


def _no_custom_model():
    """Force SYSTEM classification by stubbing out the custom lookup."""
    return patch(
        f"{HANDLER}.get_custom_model_config",
        new_callable=AsyncMock,
        return_value=None,
    )


# ---------------------------------------------------------------------------
# System-branch coding-variant fix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_key_under_variant_slug_uses_own_coding_endpoint():
    """Key stored only under the variant slug → resolves, builds against the
    model's own coding endpoint."""
    from src.server.handlers.chat.llm_config import resolve_byok_llm_client

    mc = _mock_mc()
    mock_llm = MagicMock(name="llm")
    with (
        _no_custom_model(),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch(
            f"{DB_KEYS}.get_byok_configs_for_providers",
            new_callable=AsyncMock,
            return_value={"vendor-x-coding": {"api_key": "k", "base_url": None}},
        ),
        patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
    ):
        result = await resolve_byok_llm_client("u", "vendor-x-coding", True)

    assert result is mock_llm
    assert mock_create.call_args.kwargs["base_url"] == CODING_URL


@pytest.mark.asyncio
async def test_key_under_parent_still_uses_own_coding_endpoint():
    """Key stored only under the PARENT slug → walk finds it, but base_url is
    the model's OWN coding endpoint, NOT the parent's openai endpoint."""
    from src.server.handlers.chat.llm_config import resolve_byok_llm_client

    mc = _mock_mc()
    mock_llm = MagicMock(name="llm")
    with (
        _no_custom_model(),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch(
            f"{DB_KEYS}.get_byok_configs_for_providers",
            new_callable=AsyncMock,
            return_value={"vendor-x": {"api_key": "k", "base_url": None}},
        ),
        patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
    ):
        result = await resolve_byok_llm_client("u", "vendor-x-coding", True)

    assert result is mock_llm
    # SDK-trap guard: own coding endpoint, never the parent's openai one.
    assert mock_create.call_args.kwargs["base_url"] == CODING_URL
    assert mock_create.call_args.kwargs["base_url"] != PARENT_URL


@pytest.mark.asyncio
async def test_key_under_sibling_resolves_with_own_parent_endpoint():
    """Mirror case (the walk's reason for existing): a system model tagged with
    the PARENT slug ``vendor-x`` whose key is stored ONLY under a sibling
    variant ``vendor-x-coding``. Resolves via the sibling, but base_url is
    pinned to the model's OWN provider (vendor-x), not the sibling's."""
    from src.server.handlers.chat.llm_config import (
        _walk_byok_candidates,
        resolve_byok_llm_client,
    )

    mc = _mock_mc()
    # Sibling reachable from the parent.
    assert mc.get_child_variants("vendor-x") == ["vendor-x-coding"]

    sibling_cfg = {"api_key": "k", "base_url": None}
    mock_llm = MagicMock(name="llm")
    with (
        _no_custom_model(),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch(
            f"{DB_KEYS}.get_byok_configs_for_providers",
            new_callable=AsyncMock,
            return_value={"vendor-x-coding": sibling_cfg},
        ),
        patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
    ):
        # Direct helper: confirms provider → parent → sibling priority lands on
        # the sibling and reports it as the holder.
        byok_config, holding = await _walk_byok_candidates("u", "vendor-x", mc)
        assert byok_config == sibling_cfg
        assert holding == "vendor-x-coding"

        result = await resolve_byok_llm_client("u", "vendor-x", True)

    assert result is mock_llm
    # Own-endpoint pin: the model's own provider (vendor-x), NOT the sibling's.
    assert mock_create.call_args.kwargs["base_url"] == PARENT_URL
    assert mock_create.call_args.kwargs["base_url"] != CODING_URL


# ---------------------------------------------------------------------------
# _walk_byok_candidates — cache tri-state contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_miss_falls_back_to_direct_lookup():
    """A candidate slug absent from a provided _byok_cache still resolves via
    the direct-lookup fallback (a cache miss is never a false 'no key')."""
    from src.server.handlers.chat.llm_config import _walk_byok_candidates

    mc = _mock_mc()
    # Cache has the parent recorded-absent but NOT the variant slug.
    cache = {"vendor-x": None}
    direct = AsyncMock(
        return_value={"vendor-x-coding": {"api_key": "k", "base_url": None}},
    )
    with patch(f"{DB_KEYS}.get_byok_configs_for_providers", direct):
        byok_config, holding = await _walk_byok_candidates(
            "u", "vendor-x-coding", mc, _byok_cache=cache,
        )

    assert byok_config == {"api_key": "k", "base_url": None}
    assert holding == "vendor-x-coding"
    # The missing slug must have been the one fetched directly.
    direct.assert_awaited_once()
    assert direct.await_args.args[1] == ["vendor-x-coding"]
    # Cache is updated with the fetched result.
    assert cache["vendor-x-coding"] == {"api_key": "k", "base_url": None}


@pytest.mark.asyncio
async def test_cache_none_value_treated_as_absent_without_requery():
    """A slug present with value None is a confirmed absence — no direct
    re-query for that slug."""
    from src.server.handlers.chat.llm_config import _walk_byok_candidates

    mc = _mock_mc()
    # Both candidates recorded-absent; parent holds nothing either.
    cache = {"vendor-x-coding": None, "vendor-x": None}
    direct = AsyncMock(return_value={})
    with patch(f"{DB_KEYS}.get_byok_configs_for_providers", direct):
        byok_config, holding = await _walk_byok_candidates(
            "u", "vendor-x-coding", mc, _byok_cache=cache,
        )

    assert byok_config is None
    assert holding is None
    # Everything was cached (as None) → no direct lookup at all.
    direct.assert_not_awaited()
