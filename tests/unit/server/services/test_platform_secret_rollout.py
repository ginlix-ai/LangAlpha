"""Platform-secret rollout state machine, hot resync, and boot tests."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    PlatformSecretDefinition,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    PlatformSecretReconciliationError,
    ReconciledPlatformSecret,
    resolve_platform_secrets,
)
from ptc_agent.core.sandbox.runtime import ExecResult, RuntimeState
from src.server.services.platform_secret_rollout import (
    PlatformSecretReadinessError,
    PlatformSecretRollout,
    PlatformSecretRolloutSet,
    reconcile_platform_secrets_at_boot,
    resync_workspace_platform_secret,
)


PLACEHOLDER = "dtn_secret_opaque"

_MODULE = "src.server.services.platform_secret_rollout"


_FMP_DEFINITION = PlatformSecretDefinition(
    source_env_var="FMP_API_KEY",
    sandbox_env_var="FMP_API_KEY",
    name_suffix="platform-fmp-api-key",
    description="Platform FMP API key",
    hosts=("financialmodelingprep.com",),
)


def _config(
    *,
    provider: str = "daytona",
    platform_secrets: tuple[PlatformSecretDefinition, ...] = (_FMP_DEFINITION,),
) -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(
            provider=provider,
            daytona=DaytonaConfig(api_key="daytona-key", secret_namespace="prod"),
            platform_secrets=platform_secrets,
        ),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def _rollout_set(generation: int = 1) -> PlatformSecretRolloutSet:
    rollout = PlatformSecretRollout(
        secret_key="FMP_API_KEY",
        provider="daytona",
        secret_name="prod-platform-fmp-api-key",
        provider_secret_id="secret-id",
        placeholder_sha256=hashlib.sha256(PLACEHOLDER.encode()).hexdigest(),
        current_credential_sha256="current",
        generation=generation,
    )
    return PlatformSecretRolloutSet(rollouts=(rollout,), generation=generation)


class _Runtime:
    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        return ExecResult(f"{PLACEHOLDER}\n", "", 0)

    async def get_state(self) -> RuntimeState:
        return RuntimeState.RUNNING


async def _resync(
    config,
    runtime=None,
    *,
    sandbox_id: str | None = "sb",
    db_version: int = 0,
    applied_generation: int | None = None,
):
    return await resync_workspace_platform_secret(
        config,
        runtime if runtime is not None else _Runtime(),
        workspace_id="ws",
        sandbox_id=sandbox_id,
        db_version=db_version,
        applied_generation=applied_generation,
    )


@pytest.mark.asyncio
async def test_resync_is_inert_without_catalog(monkeypatch):
    # No catalog configured (opt-out) — the resync never touches rollout
    # state, regardless of host mode.
    monkeypatch.setattr("src.config.env.HOST_MODE", "oss")
    get_rollouts = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.get_platform_secret_rollouts", get_rollouts)

    assert await _resync(_config(platform_secrets=())) is None

    get_rollouts.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_short_circuits_on_session_generation(monkeypatch):
    # The session already applied the active generation: zero provider calls.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set()),
    )
    remount = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)

    result = await _resync(_config(), db_version=0, applied_generation=1)

    assert result == 1
    remount.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_trusts_a_certified_current_row(monkeypatch):
    # Row already at the active generation (fresh provision / sweep): bindings
    # are mounted; only the session stamp was missing.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set()),
    )
    remount = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)

    result = await _resync(_config(), db_version=1, applied_generation=None)

    assert result == 1
    remount.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_hot_swaps_verifies_and_stamps_a_certified_behind_row(
    monkeypatch,
):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    rollout_set = _rollout_set(generation=2)
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=rollout_set),
    )
    remount = AsyncMock()
    verify = AsyncMock()
    stamp = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)
    monkeypatch.setattr(f"{_MODULE}.verify_runtime_platform_secrets", verify)
    monkeypatch.setattr(f"{_MODULE}.stamp_workspace_platform_secret_version", stamp)
    runtime = _Runtime()

    result = await _resync(_config(), runtime, db_version=1)

    assert result == 2
    remount.assert_awaited_once_with(
        runtime,
        expected=rollout_set.placeholders,
        bindings={"FMP_API_KEY": "prod-platform-fmp-api-key"},
    )
    verify.assert_awaited_once_with(runtime, expected=rollout_set.placeholders)
    stamp.assert_awaited_once_with(
        "ws", expected_sandbox_id="sb", rollout_set=rollout_set
    )


@pytest.mark.asyncio
async def test_resync_remounts_a_legacy_row_without_certifying_it(monkeypatch):
    # version 0 = never certified: live processes may retain plaintext, so the
    # hot remount protects new processes but the row must stay behind for the
    # sweep's scrub-restart — no verify, no stamp.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set()),
    )
    remount = AsyncMock()
    verify = AsyncMock()
    stamp = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)
    monkeypatch.setattr(f"{_MODULE}.verify_runtime_platform_secrets", verify)
    monkeypatch.setattr(f"{_MODULE}.stamp_workspace_platform_secret_version", stamp)

    result = await _resync(_config(), db_version=0)

    assert result == 1
    remount.assert_awaited_once()
    verify.assert_not_awaited()
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_raises_on_remount_failure_without_stamping(monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set(generation=2)),
    )
    remount = AsyncMock(side_effect=RuntimeError("daemon unreachable"))
    stamp = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)
    monkeypatch.setattr(f"{_MODULE}.stamp_workspace_platform_secret_version", stamp)

    with pytest.raises(PlatformSecretReadinessError, match="ws"):
        await _resync(_config(), db_version=1)

    # The row stays behind; the next slow-path acquisition retries.
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_skips_sandboxless_workspace(monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set()),
    )
    remount = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)

    assert await _resync(_config(), sandbox_id=None) is None

    remount.assert_not_awaited()


@pytest.mark.asyncio
async def test_boot_missing_provider_credentials_fail_hosted_readiness(monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    config = _config()
    agent_config = SimpleNamespace(
        to_core_config=lambda: config,
        validate_api_keys=MagicMock(side_effect=ValueError("raw config error")),
    )

    with pytest.raises(PlatformSecretConfigurationError, match="credentials"):
        await reconcile_platform_secrets_at_boot(agent_config)


@pytest.mark.asyncio
async def test_boot_reconciles_and_registers_provider(monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    config = _config()
    agent_config = SimpleNamespace(
        to_core_config=lambda: config,
        validate_api_keys=MagicMock(),
    )
    definition = resolve_platform_secrets(
        config,
        environ={"FMP_API_KEY": "real-fmp-value"},
        host_mode="platform",
    )[0].definition
    reconciled = ReconciledPlatformSecret(
        definition=definition,
        name="prod-platform-fmp-api-key",
        provider_secret_id="secret-id",
        placeholder="dtn_secret_opaque",
    )
    provider = MagicMock()
    provider.reconcile_platform_secrets = AsyncMock(return_value=(reconciled,))
    provider.close = AsyncMock()
    register = AsyncMock(return_value=MagicMock())

    with (
        patch(
            "ptc_agent.core.sandbox.providers.create_provider",
            return_value=provider,
        ),
        patch(f"{_MODULE}.register_platform_secret_rollouts", register),
    ):
        await reconcile_platform_secrets_at_boot(agent_config)

    provider.reconcile_platform_secrets.assert_awaited_once()
    provider.close.assert_awaited_once()
    register.assert_awaited_once_with(
        (reconciled,),
        credential_values={"FMP_API_KEY": "real-fmp-value"},
        provider="daytona",
    )


@pytest.mark.asyncio
async def test_boot_without_catalog_skips_reconciliation(monkeypatch):
    # No catalog is the opt-out — boot reconciles nothing, in any host mode.
    monkeypatch.setattr("src.config.env.HOST_MODE", "oss")
    config = _config(platform_secrets=())
    agent_config = SimpleNamespace(
        to_core_config=lambda: config,
        validate_api_keys=MagicMock(),
    )

    with patch(
        "ptc_agent.core.sandbox.providers.create_provider"
    ) as create_provider:
        result = await reconcile_platform_secrets_at_boot(agent_config)

    assert result is None
    create_provider.assert_not_called()


@pytest.mark.asyncio
async def test_boot_oss_with_catalog_opts_in(monkeypatch):
    # OSS + Daytona + a configured catalog opts in: boot reconciles and
    # registers, no HOST_MODE=platform required.
    monkeypatch.setattr("src.config.env.HOST_MODE", "oss")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    config = _config()
    agent_config = SimpleNamespace(
        to_core_config=lambda: config,
        validate_api_keys=MagicMock(),
    )
    definition = resolve_platform_secrets(
        config, environ={"FMP_API_KEY": "real-fmp-value"}
    )[0].definition
    reconciled = ReconciledPlatformSecret(
        definition=definition,
        name="prod-platform-fmp-api-key",
        provider_secret_id="secret-id",
        placeholder="dtn_secret_opaque",
    )
    provider = MagicMock()
    provider.reconcile_platform_secrets = AsyncMock(return_value=(reconciled,))
    provider.close = AsyncMock()
    register = AsyncMock(return_value=MagicMock())

    with (
        patch(
            "ptc_agent.core.sandbox.providers.create_provider",
            return_value=provider,
        ),
        patch(f"{_MODULE}.register_platform_secret_rollouts", register),
    ):
        await reconcile_platform_secrets_at_boot(agent_config)

    provider.reconcile_platform_secrets.assert_awaited_once()
    register.assert_awaited_once_with(
        (reconciled,),
        credential_values={"FMP_API_KEY": "real-fmp-value"},
        provider="daytona",
    )


@pytest.mark.asyncio
async def test_boot_reconcile_softens_provider_outage_with_existing_rollout(
    monkeypatch,
):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    rollout_set = _rollout_set()
    provider = MagicMock()
    provider.reconcile_platform_secrets = AsyncMock(
        side_effect=ConnectionError("daytona down")
    )
    provider.close = AsyncMock()
    monkeypatch.setattr(
        "ptc_agent.core.sandbox.providers.create_provider", lambda _config: provider
    )
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=rollout_set),
    )
    agent_config = MagicMock()
    agent_config.to_core_config.return_value = _config()
    agent_config.validate_api_keys.return_value = None

    result = await reconcile_platform_secrets_at_boot(agent_config)

    assert result is rollout_set
    provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_boot_reconcile_count_mismatch_fails_closed_despite_prior_rollout(
    monkeypatch,
):
    # A reconciled/secrets count mismatch is a reconciler contract violation,
    # not a provider outage — it must fail closed even when a prior rollout
    # exists (the outage path would soften here; this must not).
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    provider = MagicMock()
    provider.reconcile_platform_secrets = AsyncMock(return_value=())  # 0 != 1
    provider.close = AsyncMock()
    monkeypatch.setattr(
        "ptc_agent.core.sandbox.providers.create_provider", lambda _config: provider
    )
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set()),
    )
    agent_config = MagicMock()
    agent_config.to_core_config.return_value = _config()
    agent_config.validate_api_keys.return_value = None

    with pytest.raises(
        PlatformSecretConfigurationError, match="reconciliation result"
    ):
        await reconcile_platform_secrets_at_boot(agent_config)
    provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_boot_reconcile_fails_closed_without_prior_rollout(monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    provider = MagicMock()
    provider.reconcile_platform_secrets = AsyncMock(
        side_effect=ConnectionError("daytona down")
    )
    provider.close = AsyncMock()
    monkeypatch.setattr(
        "ptc_agent.core.sandbox.providers.create_provider", lambda _config: provider
    )
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(side_effect=PlatformSecretReadinessError("not initialized")),
    )
    agent_config = MagicMock()
    agent_config.to_core_config.return_value = _config()
    agent_config.validate_api_keys.return_value = None

    with pytest.raises(PlatformSecretReconciliationError):
        await reconcile_platform_secrets_at_boot(agent_config)

    provider.close.assert_awaited_once()
