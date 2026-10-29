"""Tests for walk_byok_candidates() and the system-branch coding-variant fix.

A neutral placeholder model ``vendor-x-coding`` (anthropic-style coding
endpoint) whose parent ``vendor-x`` is openai-style reproduces the SDK-trap:
the key may live under the variant slug OR the parent, but the client must
always build against the MODEL'S OWN endpoint. Also covers the _byok_cache
tri-state miss-safety contract.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

HANDLER = "src.server.services.llm.config"
USER_MODELS = "src.server.services.llm.user_models"
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
        f"{USER_MODELS}.get_custom_model_config",
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
    from src.server.services.llm.clients import resolve_byok_llm_client

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
    from src.server.services.llm.clients import resolve_byok_llm_client

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
    from src.server.services.llm.clients import walk_byok_candidates, resolve_byok_llm_client

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
        byok_config, holding = await walk_byok_candidates("u", "vendor-x", mc)
        assert byok_config == sibling_cfg
        assert holding == "vendor-x-coding"

        result = await resolve_byok_llm_client("u", "vendor-x", True)

    assert result is mock_llm
    # Own-endpoint pin: the model's own provider (vendor-x), NOT the sibling's.
    assert mock_create.call_args.kwargs["base_url"] == PARENT_URL
    assert mock_create.call_args.kwargs["base_url"] != CODING_URL


# ---------------------------------------------------------------------------
# walk_byok_candidates — cache tri-state contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cache_miss_falls_back_to_direct_lookup():
    """A candidate slug absent from a provided _byok_cache still resolves via
    the direct-lookup fallback (a cache miss is never a false 'no key')."""
    from src.server.services.llm.byok import walk_byok_candidates

    mc = _mock_mc()
    # Cache has the parent recorded-absent but NOT the variant slug.
    cache = {"vendor-x": None}
    direct = AsyncMock(
        return_value={"vendor-x-coding": {"api_key": "k", "base_url": None}},
    )
    with patch(f"{DB_KEYS}.get_byok_configs_for_providers", direct):
        byok_config, holding = await walk_byok_candidates(
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
    from src.server.services.llm.byok import walk_byok_candidates

    mc = _mock_mc()
    # Both candidates recorded-absent; parent holds nothing either.
    cache = {"vendor-x-coding": None, "vendor-x": None}
    direct = AsyncMock(return_value={})
    with patch(f"{DB_KEYS}.get_byok_configs_for_providers", direct):
        byok_config, holding = await walk_byok_candidates(
            "u", "vendor-x-coding", mc, _byok_cache=cache,
        )

    assert byok_config is None
    assert holding is None
    # Everything was cached (as None) → no direct lookup at all.
    direct.assert_not_awaited()


# ---------------------------------------------------------------------------
# Nested-variant discovery (byok_candidates)
# ---------------------------------------------------------------------------


def testbyok_candidates_include_nested_child_variant():
    """A two-level variant (child of a non-root provider) is walked from its
    parent, before the root family."""
    from src.server.services.llm.byok import byok_candidates

    providers = {
        "vendor-x": {},
        "vendor-x-coding": {"parent": "vendor-x"},
        "vendor-x-cn": {"parent": "vendor-x"},
        "vendor-x-cn-coding": {"parent": "vendor-x-cn"},
    }
    mc = MagicMock()
    mc.get_parent_provider.side_effect = lambda n: providers.get(n, {}).get("parent")
    mc.get_child_variants.side_effect = lambda n: [
        k for k, info in providers.items() if info.get("parent") == n
    ]

    assert byok_candidates("vendor-x-cn", mc) == [
        "vendor-x-cn",
        "vendor-x-cn-coding",
        "vendor-x",
        "vendor-x-coding",
    ]
    # Root providers keep their original walk order
    assert byok_candidates("vendor-x", mc) == [
        "vendor-x",
        "vendor-x-coding",
        "vendor-x-cn",
    ]


# ---------------------------------------------------------------------------
# Variants whose keys are a separate credential (``own_api_key``)
# ---------------------------------------------------------------------------


def _config_over(flat: dict):
    """A real ``ModelConfig`` whose provider table is ``flat`` (already flattened).

    Real, not a mock, so the child-variant map and ``shares_api_key`` are the
    shipped ones. Model entries come from the caller via ``llm_config``.
    """
    from src.llms.llm import ModelConfig

    with patch.object(ModelConfig, "_flatten_providers", return_value=flat):
        return ModelConfig()


# vendor-y's coding-plan keys are issued apart from its platform keys; vendor-z
# is the control family whose variants share one key.
SEPARATE_FAMILY = {
    "vendor-y": {"base_url": "https://api.y.example/v1"},
    "vendor-y-coding": {
        "parent_provider": "vendor-y",
        "base_url": "https://coding.y.example",
        "own_api_key": True,
    },
    "vendor-z": {"base_url": "https://api.z.example/v1"},
    "vendor-z-coding": {
        "parent_provider": "vendor-z",
        "base_url": "https://coding.z.example",
    },
}
GATEWAY_URL = "https://gateway.example/v1"


def _key_walk(provider, mc):
    """The slugs whose key, saved without a base URL, may serve ``provider``."""
    from src.server.services.llm.byok import byok_candidates, key_fits

    return [c for c in byok_candidates(provider, mc) if key_fits(provider, c, has_base_url=False, mc=mc)]


def test_separate_credential_variant_is_walked_alone_in_both_directions():
    mc = _config_over(SEPARATE_FAMILY)

    assert _key_walk("vendor-y", mc) == ["vendor-y"]
    assert _key_walk("vendor-y-coding", mc) == ["vendor-y-coding"]


def test_variants_that_share_a_key_keep_the_family_walk():
    mc = _config_over(SEPARATE_FAMILY)

    assert _key_walk("vendor-z", mc) == ["vendor-z", "vendor-z-coding"]
    assert _key_walk("vendor-z-coding", mc) == ["vendor-z-coding", "vendor-z"]


def test_descendants_of_a_separate_credential_stay_with_it():
    """A nested variant belongs to the nearest ancestor that declares its own
    key, so marking the middle of a chain splits it there and nowhere else."""
    mc = _config_over({
        "vendor-x": {},
        "vendor-x-coding": {"parent_provider": "vendor-x"},
        "vendor-x-cn": {"parent_provider": "vendor-x", "own_api_key": True},
        "vendor-x-cn-coding": {"parent_provider": "vendor-x-cn"},
    })

    assert _key_walk("vendor-x-cn", mc) == ["vendor-x-cn", "vendor-x-cn-coding"]
    assert _key_walk("vendor-x-cn-coding", mc) == ["vendor-x-cn-coding", "vendor-x-cn"]
    assert _key_walk("vendor-x", mc) == ["vendor-x", "vendor-x-coding"]


@pytest.mark.parametrize(
    ("model_provider", "held_under", "saved_url", "resolves"),
    [
        # Only the other credential on file: refused, not sent to the wrong endpoint.
        ("vendor-y", "vendor-y-coding", None, False),
        ("vendor-y-coding", "vendor-y", None, False),
        # Its own key still resolves, against its own endpoint.
        ("vendor-y", "vendor-y", None, True),
        ("vendor-y-coding", "vendor-y-coding", None, True),
        # A family that shares one key keeps borrowing across variants.
        ("vendor-z", "vendor-z-coding", None, True),
        ("vendor-z-coding", "vendor-z", None, True),
        # A key saved with its own base URL goes there, so it crosses the line.
        ("vendor-y-coding", "vendor-y", GATEWAY_URL, True),
        ("vendor-y", "vendor-y-coding", GATEWAY_URL, True),
    ],
)
@pytest.mark.asyncio
async def test_resolver_pairs_a_key_only_with_the_credential_that_issued_it(
    model_provider, held_under, saved_url, resolves,
):
    from src.server.services.llm.clients import resolve_byok_llm_client

    mc = _config_over(SEPARATE_FAMILY)
    mc.llm_config = {model_provider: {"provider": model_provider}}

    async def fetch(user_id, providers):
        return {
            p: {"api_key": f"key-{p}", "base_url": saved_url}
            for p in providers if p == held_under
        }

    mock_llm = MagicMock(name="llm")
    with (
        _no_custom_model(),
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch(f"{DB_KEYS}.get_byok_configs_for_providers", side_effect=fetch),
        patch("src.llms.llm.create_llm", return_value=mock_llm) as mock_create,
    ):
        result = await resolve_byok_llm_client("u", model_provider, True)

    if not resolves:
        assert result is None
        mock_create.assert_not_called()
        return
    assert result is mock_llm
    assert mock_create.call_args.kwargs["api_key"] == f"key-{held_under}"
    # The endpoint is the URL saved with the key, else the model's own, never
    # the holder's manifest endpoint.
    expected = saved_url or SEPARATE_FAMILY[model_provider]["base_url"]
    assert mock_create.call_args.kwargs["base_url"] == expected


@pytest.mark.asyncio
async def test_a_custom_model_follows_a_separate_credential_to_its_own_endpoint():
    """A custom model is built against the endpoint of whichever slug held the
    key, so borrowing across a separate credential still pairs the key with
    the service that issued it."""
    from src.server.services.llm import clients

    mc = _config_over(SEPARATE_FAMILY)

    async def fetch(user_id, providers):
        return {
            p: {"api_key": "key-coding", "base_url": None}
            for p in providers if p == "vendor-y-coding"
        }

    with (
        patch.object(
            clients.user_models, "get_custom_provider_config",
            new=AsyncMock(return_value=None),
        ),
        patch(f"{DB_KEYS}.get_byok_configs_for_providers", side_effect=fetch),
    ):
        byok, base_url, custom = await clients._resolve_custom_model_byok(
            "u", "my-model", {"name": "my-model", "provider": "vendor-y"}, mc,
        )

    assert byok["api_key"] == "key-coding"
    assert base_url == "https://coding.y.example"
    assert custom["provider"] == "vendor-y-coding"


@pytest.mark.asyncio
async def test_prefetch_reads_what_the_walk_inspects():
    """STEP-0 prefetch builds its slug set from the same walk, separate
    credentials included, since only the stored config says whether a key
    there fits. The walk then needs no lookup of its own."""
    from src.server.services.llm import clients

    mc = _config_over(SEPARATE_FAMILY)
    asked: list[list[str]] = []

    async def fetch(user_id, providers):
        asked.append(list(providers))
        return {}

    config = MagicMock()
    config.llm.fallback = []
    with (
        patch("src.llms.llm.LLM.get_model_config", return_value=mc),
        patch.object(
            clients.user_models, "classify_model",
            new=AsyncMock(return_value=(clients.ModelSource.SYSTEM, {"provider": "vendor-y"})),
        ),
        patch(f"{DB_KEYS}.get_byok_configs_for_providers", side_effect=fetch),
    ):
        cache = await clients._prefetch_byok_cache("u", config, "vendor-y", {}, [])
        found = await clients.walk_byok_candidates("u", "vendor-y", mc, _byok_cache=cache)

    assert [sorted(a) for a in asked] == [["vendor-y", "vendor-y-coding"]]
    assert cache == {"vendor-y": None, "vendor-y-coding": None}
    assert found == (None, None)


# The verdicts in providers.json, pinned against the shipped manifest so a family
# cannot drift into or out of sharing without a deliberate edit here.
@pytest.mark.parametrize(
    "provider",
    [
        "moonshot",
        "minimax",
        "minimax-coding",
        "minimax-cn-coding",
        "z-ai-coding",
        "z-ai-cn-coding",
        "dashscope",
        "dashscope-intl",
    ],
)
def test_shipped_manifest_walks_separate_credential_providers_alone(provider):
    from src.llms.llm import LLM

    assert _key_walk(provider, LLM.get_model_config()) == [provider]


@pytest.mark.parametrize(
    ("provider", "walk"),
    [
        ("z-ai", ["z-ai", "z-ai-cn"]),
        # Kimi Code issues one key for both its hosts.
        ("moonshot-coding", ["moonshot-coding", "moonshot-cn-coding"]),
        ("moonshot-cn-coding", ["moonshot-cn-coding", "moonshot-coding"]),
        # OpenCode Go: one key, a route per wire format.
        ("opencode-go", ["opencode-go", "opencode-go-chat", "opencode-go-responses", "opencode-go-google"]),
        ("opencode-go-chat", ["opencode-go-chat", "opencode-go", "opencode-go-responses", "opencode-go-google"]),
        ("openrouter", ["openrouter"]),
    ],
)
def test_shipped_manifest_keeps_the_walk_where_the_vendor_shares_a_key(provider, walk):
    from src.llms.llm import LLM

    assert _key_walk(provider, LLM.get_model_config()) == walk


def test_shipped_separate_credentials_name_their_own_env_key():
    """A variant inherits its parent's ``env_key`` unless it names one, and a
    separate credential that inherits it is sent the parent's key from the
    environment, which its endpoint rejects."""
    from src.llms.llm import LLM

    mc = LLM.get_model_config()
    separate = [p for p, info in mc.flat_providers.items() if info.get("own_api_key") is True]

    assert separate
    for provider in separate:
        parent = mc.get_parent_provider(provider)
        assert mc.get_provider_info(provider).get("env_key") != (
            mc.get_provider_info(parent).get("env_key")
        ), provider
