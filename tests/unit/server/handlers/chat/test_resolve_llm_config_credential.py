"""Tests for resolve_llm_config() credential-source wiring + role registry.

Covers the credential_source matrix written onto AgentConfig (OAUTH / BYOK /
PLATFORM / NONE), the BYOK-pure write-time materialization gate (Codex #2:
a PLATFORM client must NOT be copied into subsidiary roles), the role_registry
builder, and the is_byok self-resolving net.

The network/DB surface is fully mocked: resolve_oauth_llm_client,
resolve_byok_llm_client, classify_model, get_byok_configs_for_providers,
is_byok_active, create_llm, and SubagentRegistry.
"""

import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.agent import (
    AgentConfig,
    CredentialSource,
    LLMConfig,
)

HANDLER = "src.server.services.llm.config"
USER_MODELS = "src.server.services.llm.user_models"
CLIENTS = "src.server.services.llm.clients"


def _make_config(*, compaction=None, fetch=None, fallback=None, subagents_enabled=None):
    """Build a minimal real AgentConfig via the create() factory."""
    cfg = AgentConfig.create(llm=MagicMock(name="placeholder-llm"))
    cfg.llm = LLMConfig(name="main-model", compaction=compaction, fetch=fetch, fallback=fallback)
    cfg.llm_client = None
    cfg.subsidiary_llm_clients = {}
    cfg.fallback_llm_clients = None
    if subagents_enabled is not None:
        cfg.subagents.enabled = subagents_enabled
    return cfg


class _FakeClient:
    """A stand-in LLM client with a model_copy() that returns a distinct copy."""

    def __init__(self, label):
        self.label = label
        self.metadata: dict | None = None

    def model_copy(self):
        c = _FakeClient(self.label)
        c._copied_from = self
        return c


def _common_patches(
    *,
    oauth=None,
    byok=None,
    classify_source=None,
    platform_client=None,
    is_byok_active_val=False,
):
    """Bundle the standard patch set. ``classify_source`` is a ModelSource."""
    from src.server.services.llm.user_models import ModelSource

    src = classify_source if classify_source is not None else ModelSource.SYSTEM

    patches = [
        patch(
            f"{USER_MODELS}.classify_model",
            new_callable=AsyncMock,
            return_value=(src, {}),
        ),
        patch(
            f"{USER_MODELS}.get_custom_provider_config",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{USER_MODELS}.get_model_preference",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch(
            f"{CLIENTS}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=oauth,
        ),
        patch(
            f"{CLIENTS}.resolve_byok_llm_client",
            new_callable=AsyncMock,
            return_value=byok,
        ),
        patch(
            "src.server.database.api_keys.is_byok_active",
            new_callable=AsyncMock,
            return_value=is_byok_active_val,
        ),
        patch(
            "src.server.database.api_keys.get_byok_configs_for_providers",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch("src.llms.llm.create_llm", return_value=platform_client),
    ]
    return patches


@contextlib.contextmanager
def _entered(patches):
    """Enter every patch in ``patches`` (a list of patch objects)."""
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        yield


# ---------------------------------------------------------------------------
# credential_source matrix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oauth_user_sets_oauth_source():
    from src.server.services.llm.config import resolve_llm_config

    oauth_client = _FakeClient("oauth")
    with _entered(_common_patches(oauth=oauth_client)):
        cfg = await resolve_llm_config(_make_config(), "u", "main-model", True)

    assert cfg.credential_source == CredentialSource.OAUTH
    assert cfg.llm_client is oauth_client


@pytest.mark.asyncio
async def test_byok_user_sets_byok_source():
    from src.server.services.llm.config import resolve_llm_config

    byok_client = _FakeClient("byok")
    with _entered(_common_patches(byok=byok_client)):
        cfg = await resolve_llm_config(_make_config(), "u", "main-model", True)

    assert cfg.credential_source == CredentialSource.BYOK
    assert cfg.llm_client is byok_client


@pytest.mark.asyncio
async def test_non_byok_reasoning_uses_platform():
    from src.server.services.llm.config import resolve_llm_config

    platform_client = _FakeClient("platform")
    with _entered(_common_patches(platform_client=platform_client)):
        cfg = await resolve_llm_config(
            _make_config(), "u", "main-model", False, reasoning_effort="high"
        )

    assert cfg.credential_source == CredentialSource.PLATFORM
    assert cfg.llm_client is platform_client


@pytest.mark.asyncio
async def test_non_byok_no_reasoning_is_none():
    from src.server.services.llm.config import resolve_llm_config

    with _entered(_common_patches()):
        cfg = await resolve_llm_config(_make_config(), "u", "main-model", False)

    assert cfg.credential_source == CredentialSource.NONE
    assert cfg.llm_client is None


@pytest.mark.asyncio
async def test_byok_on_system_model_is_byok_source():
    """Orthogonality: BYOK key on a SYSTEM-catalog model → cred=BYOK."""
    from src.server.services.llm.config import resolve_llm_config
    from src.server.services.llm.user_models import ModelSource

    byok_client = _FakeClient("byok")
    with _entered(_common_patches(byok=byok_client, classify_source=ModelSource.SYSTEM)):
        cfg = await resolve_llm_config(_make_config(), "u", "main-model", True)

    assert cfg.credential_source == CredentialSource.BYOK


# ---------------------------------------------------------------------------
# materialization gate (Codex #2 regression)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_platform_client_not_copied_into_roles():
    """Non-BYOK+reasoning (PLATFORM) main client must NOT seed subsidiary roles."""
    from src.server.services.llm.config import resolve_llm_config

    platform_client = _FakeClient("platform")
    # compaction model resolves to no client (no key, no platform fallback for roles)
    with _entered(_common_patches(platform_client=platform_client)):
        cfg = await resolve_llm_config(
            _make_config(compaction="compaction-model"),
            "u",
            "main-model",
            False,
            reasoning_effort="high",
        )

    assert cfg.credential_source == CredentialSource.PLATFORM
    assert "compaction" not in cfg.subsidiary_llm_clients


@pytest.mark.asyncio
async def test_byok_keyless_compaction_runs_main_model_at_summary_level():
    """BYOK user with a compaction model they have no key for → the summary
    runs the main model on their key at the summary level, not a copy of the
    main client at the turn's level."""
    from src.server.services.llm.config import resolve_llm_config

    async def _byok(user_id, model_name, is_byok, reasoning_effort=None, *a, **k):
        return _FakeClient(f"main@{reasoning_effort}") if model_name == "main-model" else None

    with (
        patch(
            f"{USER_MODELS}.classify_model",
            new_callable=AsyncMock,
            return_value=(__import__(USER_MODELS, fromlist=["ModelSource"]).ModelSource.SYSTEM, {}),
        ),
        patch(f"{USER_MODELS}.get_custom_provider_config", new_callable=AsyncMock, return_value=None),
        patch(f"{USER_MODELS}.get_model_preference", new_callable=AsyncMock, return_value={}),
        patch(f"{CLIENTS}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
        patch(f"{CLIENTS}.resolve_byok_llm_client", side_effect=_byok),
        patch(
            f"{CLIENTS}._summary_effort",
            side_effect=lambda pref, model: "medium" if model == "main-model" else None,
        ),
        patch("src.server.database.api_keys.is_byok_active", new_callable=AsyncMock, return_value=True),
        patch("src.server.database.api_keys.get_byok_configs_for_providers", new_callable=AsyncMock, return_value={}),
        patch("src.llms.llm.create_llm", return_value=None),
    ):
        cfg = await resolve_llm_config(
            _make_config(compaction="compaction-model"), "u", "main-model", True
        )

    assert cfg.credential_source == CredentialSource.BYOK
    assert cfg.llm_client.label == "main@None"
    assert cfg.subsidiary_llm_clients["compaction"].label == "main@medium"


@pytest.mark.asyncio
async def test_system_user_leaves_role_keys_absent():
    """A NONE-cred (system/platform) user stores no subsidiary clients (cheap name path)."""
    from src.server.services.llm.config import resolve_llm_config

    with _entered(_common_patches()):
        cfg = await resolve_llm_config(
            _make_config(compaction="compaction-model", fetch="fetch-model"),
            "u",
            "main-model",
            False,
        )

    assert cfg.credential_source == CredentialSource.NONE
    assert "compaction" not in cfg.subsidiary_llm_clients
    assert "fetch" not in cfg.subsidiary_llm_clients


# ---------------------------------------------------------------------------
# is_byok self-resolving net
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_is_byok_none_self_resolves():
    from src.server.services.llm.config import resolve_llm_config

    is_byok_active = AsyncMock(return_value=False)
    byok = AsyncMock(return_value=None)
    with (
        patch(
            f"{USER_MODELS}.classify_model",
            new_callable=AsyncMock,
            return_value=(__import__(USER_MODELS, fromlist=["ModelSource"]).ModelSource.SYSTEM, {}),
        ),
        patch(f"{USER_MODELS}.get_custom_provider_config", new_callable=AsyncMock, return_value=None),
        patch(f"{USER_MODELS}.get_model_preference", new_callable=AsyncMock, return_value={}),
        patch(f"{CLIENTS}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
        patch(f"{CLIENTS}.resolve_byok_llm_client", byok),
        patch("src.server.database.api_keys.is_byok_active", is_byok_active),
        patch("src.server.database.api_keys.get_byok_configs_for_providers", new_callable=AsyncMock, return_value={}),
        patch("src.llms.llm.create_llm", return_value=None),
    ):
        cfg = await resolve_llm_config(_make_config(), "u", "main-model", None)

    is_byok_active.assert_awaited_once_with("u")
    assert cfg.credential_source == CredentialSource.NONE


# ---------------------------------------------------------------------------
# role_registry
# ---------------------------------------------------------------------------


def _roster(**models):
    from ptc_agent.agent.subagents.definition import SubagentDefinition
    from ptc_agent.agent.subagents.merge import ResolvedSubagents

    return ResolvedSubagents(
        definitions={
            name: SubagentDefinition(name=name, description="", model=model)
            for name, model in models.items()
        },
        enabled=tuple(models),
    )


def test_role_registry_compaction_fetch_and_subagents():
    from src.server.services.llm.clients import LLMRole, role_registry
    from src.server.services.llm.subagents import subagent_roles

    cfg = _make_config(compaction="cm", fetch="fm")
    roster = _roster(research="sub-model", writer=None)

    roles = [*role_registry(cfg, {}), *subagent_roles(roster, {}, None)]
    keys = [r.key for r in roles]

    assert "compaction" in keys
    assert "fetch" in keys
    assert "subagent:research" in keys
    # subagent without a model: no role
    assert "subagent:writer" not in keys
    assert all(isinstance(r, LLMRole) for r in roles)


def test_role_registry_gives_each_role_its_own_models_effort():
    """A level set on a model is a fact about that model wherever it runs.

    The role list is where that gets attached, because it is the last point
    that still knows which model each slot is for. Compaction is the
    exception: a summary runs at the summary level whatever the user set.
    """
    from src.server.services.llm.clients import role_registry
    from src.server.services.llm.subagents import subagent_roles

    cfg = _make_config(compaction="cm", fetch="fm")
    roster = _roster(research="sub-model")  # sets no level of its own
    model_pref = {
        "reasoning_effort": "low",
        "profiles": {"sub-model": {"reasoning_effort": "high"}, "cm": {"reasoning_effort": "max"}},
    }
    model_entry = {"cm": {"reasoning": {"efforts": ["low", "medium", "high", "max"]}}}

    with patch(
        "src.server.services.llm.user_models.model_entry",
        side_effect=lambda _pref, name: model_entry.get(name),
    ):
        roles = {
            r.key: r
            for r in [*role_registry(cfg, model_pref), *subagent_roles(roster, model_pref, None)]
        }

    # The subagent's model carries a profile; fetch falls to the account.
    assert roles["subagent:research"].reasoning_effort == "high"
    assert roles["fetch"].reasoning_effort == "low"
    assert roles["compaction"].reasoning_effort == "medium"


@pytest.mark.parametrize(
    ("efforts", "expected"),
    [
        (["low", "medium", "high", "max"], "medium"),
        # glm-shaped: no medium, so a summary runs at high, never max.
        (["low", "high", "max"], "high"),
        (["none", "low", "high", "max"], "high"),
        # Neither on offer: the model keeps its own default.
        (["low", "max"], None),
        ([], None),
    ],
)
def test_compaction_runs_at_medium_else_high(efforts, expected):
    from src.server.services.llm.clients import role_registry

    with patch(
        "src.server.services.llm.user_models.model_entry",
        return_value={"reasoning": {"efforts": efforts}},
    ):
        (compaction,) = [
            r for r in role_registry(_make_config(compaction="cm"), {})
            if r.key == "compaction"
        ]

    assert compaction.reasoning_effort == expected


def test_role_registry_buys_no_priority_tier():
    """Priority is bought per call, so one fast turn must not become N.

    Decided rather than overlooked: the field exists on the role and is left
    unset here, which is the one place to change it.
    """
    from src.server.services.llm.clients import role_registry

    cfg = _make_config(compaction="cm", fetch="fm")
    model_pref = {"fast_mode": True, "profiles": {"cm": {"fast_mode": True}}}

    roles = role_registry(cfg, model_pref)

    assert [r.service_tier for r in roles] == [None, None]


def test_role_registry_compacts_on_the_turns_model_when_none_is_named():
    from src.server.services.llm.clients import role_registry

    cfg = _make_config()  # no compaction, no fetch, no background
    roles = role_registry(cfg, {})
    # A summary runs on the model the turn runs; fetch, with no background to
    # follow, has no model and so no role.
    assert [(r.key, r.model) for r in roles] == [("compaction", "main-model")]


def test_role_registry_summary_falls_back_to_the_turns_fallback():
    """A summary on the turn's own model falls back to the model the turn
    falls back to, at the summary level. Never to a copy of the main client,
    which is the model that just failed."""
    from src.server.services.llm.clients import role_registry

    cfg = _make_config(fallback=["fb-1", "fb-2"])
    with patch(
        f"{USER_MODELS}.model_entry",
        side_effect=lambda _pref, name: (
            {"reasoning": {"efforts": ["low", "high", "max"]}} if name == "fb-1" else None
        ),
    ):
        roles = {r.key: r for r in role_registry(cfg, {}, "main-model")}

    fallback = roles["compaction_fallback"]
    assert (fallback.model, fallback.reasoning_effort, fallback.fallback_to_main) == (
        "fb-1",
        "high",
        False,
    )


def test_role_registry_picked_summary_model_falls_back_to_the_turns_model():
    """A summary written by a model picked for it falls back to the model the
    turn runs, at the summary level rather than the turn's own."""
    from src.server.services.llm.clients import role_registry

    cfg = _make_config(compaction="cm", fallback=["fb-1"])
    with patch(
        f"{USER_MODELS}.model_entry",
        side_effect=lambda _pref, name: {"reasoning": {"efforts": ["low", "medium", "max"]}},
    ):
        roles = {r.key: r for r in role_registry(cfg, {}, "main-model")}

    fallback = roles["compaction_fallback"]
    assert (fallback.model, fallback.reasoning_effort, fallback.fallback_to_main) == (
        "main-model",
        "medium",
        False,
    )
