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
    from src.server.services.llm.config import ModelSource

    src = classify_source if classify_source is not None else ModelSource.SYSTEM

    patches = [
        patch(
            f"{HANDLER}.classify_model",
            new_callable=AsyncMock,
            return_value=(src, {}),
        ),
        patch(
            f"{HANDLER}.get_custom_provider_config",
            new_callable=AsyncMock,
            return_value=None,
        ),
        patch(
            f"{HANDLER}.get_model_preference",
            new_callable=AsyncMock,
            return_value={},
        ),
        patch(
            f"{HANDLER}.resolve_oauth_llm_client",
            new_callable=AsyncMock,
            return_value=oauth,
        ),
        patch(
            f"{HANDLER}.resolve_byok_llm_client",
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
    from src.server.services.llm.config import ModelSource, resolve_llm_config

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
async def test_byok_main_copied_into_keyless_role():
    """BYOK user with a compaction model they have no key for → role gets a main copy."""
    from src.server.services.llm.config import resolve_llm_config

    byok_client = _FakeClient("byok-main")

    # OAuth None always; BYOK returns the main client for the main model, but
    # None for the compaction role model.
    async def _byok(user_id, model_name, is_byok, *a, **k):
        return byok_client if model_name == "main-model" else None

    with (
        patch(
            f"{HANDLER}.classify_model",
            new_callable=AsyncMock,
            return_value=(__import__(HANDLER, fromlist=["ModelSource"]).ModelSource.SYSTEM, {}),
        ),
        patch(f"{HANDLER}.get_custom_provider_config", new_callable=AsyncMock, return_value=None),
        patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
        patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
        patch(f"{HANDLER}.resolve_byok_llm_client", side_effect=_byok),
        patch("src.server.database.api_keys.is_byok_active", new_callable=AsyncMock, return_value=True),
        patch("src.server.database.api_keys.get_byok_configs_for_providers", new_callable=AsyncMock, return_value={}),
        patch("src.llms.llm.create_llm", return_value=None),
    ):
        cfg = await resolve_llm_config(
            _make_config(compaction="compaction-model"), "u", "main-model", True
        )

    assert cfg.credential_source == CredentialSource.BYOK
    assert "compaction" in cfg.subsidiary_llm_clients
    # It is a COPY of the main client, not the same instance.
    copy = cfg.subsidiary_llm_clients["compaction"]
    assert copy is not byok_client
    assert getattr(copy, "_copied_from", None) is byok_client


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
            f"{HANDLER}.classify_model",
            new_callable=AsyncMock,
            return_value=(__import__(HANDLER, fromlist=["ModelSource"]).ModelSource.SYSTEM, {}),
        ),
        patch(f"{HANDLER}.get_custom_provider_config", new_callable=AsyncMock, return_value=None),
        patch(f"{HANDLER}.get_model_preference", new_callable=AsyncMock, return_value={}),
        patch(f"{HANDLER}.resolve_oauth_llm_client", new_callable=AsyncMock, return_value=None),
        patch(f"{HANDLER}.resolve_byok_llm_client", byok),
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


def test_role_registry_compaction_fetch_and_subagents():
    from src.server.services.llm.config import LLMRole, role_registry

    cfg = _make_config(compaction="cm", fetch="fm")
    sub_with_model = MagicMock()
    sub_with_model.model = "sub-model"
    sub_no_model = MagicMock()
    sub_no_model.model = None
    subagent_defs = {"research": sub_with_model, "writer": sub_no_model}

    roles = role_registry(cfg, ["research", "writer"], subagent_defs)
    keys = [r.key for r in roles]

    assert "compaction" in keys
    assert "fetch" in keys
    assert "subagent:research" in keys
    # subagent without a model: no role
    assert "subagent:writer" not in keys
    assert all(isinstance(r, LLMRole) for r in roles)


def test_role_registry_skips_missing_compaction_fetch():
    from src.server.services.llm.config import role_registry

    cfg = _make_config()  # no compaction, no fetch
    roles = role_registry(cfg, [], {})
    assert roles == []


def test_role_registry_unknown_enabled_name_skipped():
    """An enabled subagent absent from subagent_defs is skipped (no raise)."""
    from src.server.services.llm.config import role_registry

    cfg = _make_config(compaction="cm")
    # 'ghost' is enabled but not in defs (registry.get() returned None → absent)
    roles = role_registry(cfg, ["ghost"], {})
    keys = [r.key for r in roles]
    assert keys == ["compaction"]
    assert "subagent:ghost" not in keys
