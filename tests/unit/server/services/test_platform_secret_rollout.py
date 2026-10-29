"""Platform-secret rollout state machine, hot resync, and boot tests."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    E2BConfig,
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
    PlatformSecretReplacementRequired,
    ReconciledPlatformSecret,
    resolve_platform_secrets,
)
from ptc_agent.core.sandbox.runtime import ExecResult, RuntimeState
from src.server.services.platform_secret_rollout import (
    PlatformSecretReadinessError,
    PlatformSecretRollout,
    PlatformSecretRolloutSet,
    certify_platform_secrets,
    reconcile_platform_secrets_at_boot,
    reset_platform_secret_rollout_cache,
    resync_computer_platform_secret,
    served_platform_secret_kinds,
)


PLACEHOLDER = "dtn_secret_opaque"

_MODULE = "src.server.services.platform_secret_rollout"
_BUILD_PROVIDER = "ptc_agent.core.sandbox.providers.build_provider"
_LIVE_KINDS = "src.server.database.computer.get_live_computer_kinds"


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
    e2b_key: str = "e2b-key",
    e2b_namespace: str = "prod",
    offered_providers: tuple[str, ...] = (),
) -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(
            provider=provider,
            offered_providers=offered_providers,
            daytona=DaytonaConfig(api_key="daytona-key", secret_namespace="prod"),
            e2b=E2BConfig(api_key=e2b_key, secret_namespace=e2b_namespace),
            platform_secrets=platform_secrets,
        ),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def _rollout_set(
    generation: int = 1, *, provider: str = "daytona"
) -> PlatformSecretRolloutSet:
    rollout = PlatformSecretRollout(
        secret_key="FMP_API_KEY",
        provider=provider,
        secret_name="prod-platform-fmp-api-key",
        provider_secret_id="secret-id",
        placeholder_sha256=hashlib.sha256(PLACEHOLDER.encode()).hexdigest(),
        current_credential_sha256="current",
        generation=generation,
    )
    return PlatformSecretRolloutSet(
        provider=provider, rollouts=(rollout,), generation=generation
    )


@pytest.fixture(autouse=True)
def _no_memo_between_tests():
    reset_platform_secret_rollout_cache()
    yield
    reset_platform_secret_rollout_cache()


@pytest.fixture(autouse=True)
def live_kinds():
    """The computers' kinds boot reads; none unless a test says otherwise."""
    with patch(_LIVE_KINDS, AsyncMock(return_value={})) as live:
        yield live


class _Runtime:
    updates_env = True

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
    kind: str = "daytona",
):
    return await resync_computer_platform_secret(
        config,
        runtime if runtime is not None else _Runtime(),
        kind=kind,
        computer_id="cmp",
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
    monkeypatch.setattr(f"{_MODULE}.stamp_platform_secret_version", stamp)
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
        computer_id="cmp", expected_sandbox_id="sb", rollout_set=rollout_set
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
    monkeypatch.setattr(f"{_MODULE}.stamp_platform_secret_version", stamp)

    result = await _resync(_config(), db_version=0)

    assert result == 1
    remount.assert_awaited_once()
    verify.assert_not_awaited()
    stamp.assert_not_awaited()


class _FixedEnvRuntime(_Runtime):
    """E2B's shape: env is what create set, with no way to update it."""

    updates_env = False


@pytest.mark.asyncio
async def test_resync_certifies_a_never_certified_fixed_env_sandbox(monkeypatch):
    # No scrub can change what a fixed env holds, so there is nothing to wait
    # for: a clean one is certified on the spot.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set()),
    )
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", AsyncMock())
    verify = AsyncMock()
    stamp = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.verify_runtime_platform_secrets", verify)
    monkeypatch.setattr(f"{_MODULE}.stamp_platform_secret_version", stamp)

    assert (
        await _resync(_config(), _FixedEnvRuntime(), db_version=0, kind="e2b") == 1
    )

    verify.assert_awaited_once()
    stamp.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("db_version", [0, 1])
async def test_resync_hands_a_sandbox_to_replace_back_unwrapped(
    monkeypatch, db_version
):
    """The caller serves it and the sweep rebuilds it; a turn never does."""
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=_rollout_set(generation=2)),
    )
    remount = AsyncMock()
    stamp = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)
    monkeypatch.setattr(
        f"{_MODULE}.verify_runtime_platform_secrets",
        AsyncMock(side_effect=PlatformSecretReplacementRequired("FMP_API_KEY")),
    )
    monkeypatch.setattr(f"{_MODULE}.stamp_platform_secret_version", stamp)

    with pytest.raises(PlatformSecretReplacementRequired):
        await _resync(
            _config(), _FixedEnvRuntime(), db_version=db_version, kind="e2b"
        )

    # The new rules are on before the refusal, protecting every new request.
    remount.assert_awaited_once()
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
    monkeypatch.setattr(f"{_MODULE}.stamp_platform_secret_version", stamp)

    with pytest.raises(PlatformSecretReadinessError, match="cmp"):
        await _resync(_config(), db_version=1)

    # The row stays behind; the next slow-path acquisition retries.
    stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_skips_a_machine_with_no_sandbox(monkeypatch):
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
        patch(_BUILD_PROVIDER, return_value=provider),
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

    with patch(_BUILD_PROVIDER) as build_provider:
        result = await reconcile_platform_secrets_at_boot(agent_config)

    assert result == {}
    build_provider.assert_not_called()


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
        patch(_BUILD_PROVIDER, return_value=provider),
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
    monkeypatch.setattr(_BUILD_PROVIDER, lambda *_a, **_k: provider)
    monkeypatch.setattr(
        f"{_MODULE}.get_platform_secret_rollouts",
        AsyncMock(return_value=rollout_set),
    )
    agent_config = MagicMock()
    agent_config.to_core_config.return_value = _config()
    agent_config.validate_api_keys.return_value = None

    result = await reconcile_platform_secrets_at_boot(agent_config)

    assert result == {"daytona": rollout_set}
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
    monkeypatch.setattr(_BUILD_PROVIDER, lambda *_a, **_k: provider)
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
    monkeypatch.setattr(_BUILD_PROVIDER, lambda *_a, **_k: provider)
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


# ---------------------------------------------------------------------------
# One rollout per provider kind
# ---------------------------------------------------------------------------


def _agent_config(config: CoreConfig) -> SimpleNamespace:
    return SimpleNamespace(to_core_config=lambda: config, validate_api_keys=MagicMock())


def _kind_providers(**failures: BaseException):
    """``build_provider`` stand-in recording each kind it builds.

    A kind named in ``failures`` raises that from its reconcile; any other
    reconciles every Secret it is given.
    """
    built: dict[str, MagicMock] = {}

    def build(kind, settings, **kwargs):
        provider = MagicMock(settings=settings)
        if kind in failures:
            provider.reconcile_platform_secrets = AsyncMock(side_effect=failures[kind])
        else:
            provider.reconcile_platform_secrets = AsyncMock(
                side_effect=lambda secrets: [
                    ReconciledPlatformSecret(
                        definition=secret.definition,
                        name=secret.name,
                        provider_secret_id=f"id-{secret.name}",
                        placeholder=f"placeholder-{kind}",
                    )
                    for secret in secrets
                ]
            )
        provider.close = AsyncMock()
        built[kind] = provider
        return provider

    return build, built


async def _register_by_kind(reconciled, *, credential_values, provider):
    return _rollout_set(provider=provider)


@pytest.mark.asyncio
async def test_served_kinds_are_the_deployments_plus_every_live_capable_kind(
    live_kinds,
):
    live_kinds.return_value = {"daytona": 2, "docker": 5}

    assert await served_platform_secret_kinds(_config(provider="e2b")) == {
        "daytona": 2,
        "e2b": 0,
    }
    live_kinds.return_value = {}
    assert await served_platform_secret_kinds(_config(provider="docker")) == {}


@pytest.mark.asyncio
async def test_an_offered_kind_is_served_before_its_first_computer(live_kinds):
    """Registered at boot and by the sweep, so the first computer created on
    it does not fail its start until a sweep comes round."""
    live_kinds.return_value = {"daytona": 3}

    served = await served_platform_secret_kinds(
        _config(provider="daytona", offered_providers=("e2b", "docker"))
    )

    assert served == {"daytona": 3, "e2b": 0}


@pytest.mark.asyncio
async def test_a_key_for_a_kind_neither_offered_nor_in_use_is_not_served(
    live_kinds,
):
    live_kinds.return_value = {}

    served = await served_platform_secret_kinds(
        _config(provider="daytona", e2b_key="e2b-key")
    )

    assert served == {"daytona": 0}


@pytest.mark.asyncio
async def test_boot_registers_an_offered_kind_with_no_computers(
    monkeypatch, live_kinds
):
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    live_kinds.return_value = {}
    config = _config(offered_providers=("e2b",))
    build, built = _kind_providers()
    register = AsyncMock(side_effect=_register_by_kind)

    with (
        patch(_BUILD_PROVIDER, side_effect=build),
        patch(f"{_MODULE}.register_platform_secret_rollouts", register),
    ):
        registered = await reconcile_platform_secrets_at_boot(_agent_config(config))

    assert set(registered) == {"daytona", "e2b"}
    assert set(built) == {"daytona", "e2b"}


@pytest.mark.asyncio
async def test_boot_reconciles_every_served_kind_with_its_own_settings(
    monkeypatch, live_kinds
):
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    live_kinds.return_value = {"e2b": 1}
    config = _config(e2b_namespace="prod-e2b")
    build, built = _kind_providers()
    register = AsyncMock(side_effect=_register_by_kind)

    with (
        patch(_BUILD_PROVIDER, side_effect=build),
        patch(f"{_MODULE}.register_platform_secret_rollouts", register),
    ):
        result = await reconcile_platform_secrets_at_boot(_agent_config(config))

    assert sorted(result) == ["daytona", "e2b"]
    # The deployment's own kind first, then each kind with its own account.
    assert [call.kwargs["provider"] for call in register.await_args_list] == [
        "daytona",
        "e2b",
    ]
    assert built["e2b"].settings is config.sandbox.e2b
    e2b_secrets = built["e2b"].reconcile_platform_secrets.await_args.args[0]
    assert [s.name for s in e2b_secrets] == ["prod-e2b-platform-fmp-api-key"]
    for provider in built.values():
        provider.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_boot_serves_computers_of_a_capable_kind_under_an_incapable_deployment(
    monkeypatch, live_kinds
):
    """A deployment that left Daytona for Docker still owes its Daytona
    computers their Secrets, or every one of their provisions fails."""
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    live_kinds.return_value = {"daytona": 3, "docker": 1}
    build, built = _kind_providers()

    with (
        patch(_BUILD_PROVIDER, side_effect=build),
        patch(
            f"{_MODULE}.register_platform_secret_rollouts",
            AsyncMock(side_effect=_register_by_kind),
        ),
    ):
        result = await reconcile_platform_secrets_at_boot(
            _agent_config(_config(provider="docker"))
        )

    assert sorted(result) == ["daytona"]
    assert sorted(built) == ["daytona"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "offered"), [("daytona", ("e2b",)), ("docker", ("daytona",))]
)
async def test_hosted_boot_with_an_empty_catalog_reconciles_nothing(
    monkeypatch, live_kinds, provider, offered
):
    # An empty catalog is a valid hosted state: nothing to register, so no
    # provider is built and no computer is read.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    config = _config(
        provider=provider, platform_secrets=(), offered_providers=offered
    )

    with patch(_BUILD_PROVIDER) as build_provider:
        assert await reconcile_platform_secrets_at_boot(_agent_config(config)) == {}

    build_provider.assert_not_called()
    live_kinds.assert_not_called()


@pytest.mark.asyncio
async def test_hosted_boot_refuses_an_offered_kind_that_cannot_hold_secrets(
    monkeypatch, live_kinds
):
    # Every computer created on it would carry the catalog's credentials as
    # plaintext env, so the deployment never comes up offering it.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")

    with (
        patch(_BUILD_PROVIDER) as build_provider,
        pytest.raises(
            PlatformSecretConfigurationError,
            match="docker cannot hold platform Secrets",
        ),
    ):
        await reconcile_platform_secrets_at_boot(
            _agent_config(_config(offered_providers=("docker",)))
        )

    build_provider.assert_not_called()


@pytest.mark.asyncio
async def test_boot_skips_another_kind_without_credentials(
    monkeypatch, live_kinds, caplog
):
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    live_kinds.return_value = {"daytona": 2, "e2b": 3}
    build, built = _kind_providers()

    with (
        patch(_BUILD_PROVIDER, side_effect=build),
        patch(
            f"{_MODULE}.register_platform_secret_rollouts",
            AsyncMock(side_effect=_register_by_kind),
        ),
        caplog.at_level("ERROR", logger=_MODULE),
    ):
        result = await reconcile_platform_secrets_at_boot(
            _agent_config(_config(e2b_key=""))
        )

    assert sorted(result) == ["daytona"]
    # No call is made to a provider the deployment holds no key for.
    assert sorted(built) == ["daytona"]
    [skipped] = [r for r in caplog.records if "not reconciled" in r.getMessage()]
    assert "e2b" in skipped.getMessage()
    assert "3 computer" in skipped.getMessage()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("e2b_namespace", "failure"),
    [
        ("", None),
        ("prod", PlatformSecretConfigurationError("E2B rejected the API key")),
    ],
    ids=["namespace-missing", "provider-refused"],
)
async def test_boot_fails_on_another_kinds_configuration_error(
    monkeypatch, live_kinds, e2b_namespace, failure
):
    """Holding its credentials means the deployment serves the kind, so its
    catalog has to be valid there too; a blue/green deploy then keeps the
    old color serving."""
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    live_kinds.return_value = {"e2b": 1}
    build, _ = _kind_providers(**({"e2b": failure} if failure else {}))

    with (
        patch(_BUILD_PROVIDER, side_effect=build),
        patch(
            f"{_MODULE}.register_platform_secret_rollouts",
            AsyncMock(side_effect=_register_by_kind),
        ),
        pytest.raises(PlatformSecretConfigurationError),
    ):
        await reconcile_platform_secrets_at_boot(
            _agent_config(_config(e2b_namespace=e2b_namespace))
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "earlier", [False, True], ids=["nothing-earlier", "earlier-rows"]
)
async def test_boot_survives_an_outage_on_another_kind(
    monkeypatch, live_kinds, earlier
):
    """With nothing registered its computers fail readiness until the sweep
    registers it; with earlier rows they carry on as before."""
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    live_kinds.return_value = {"e2b": 1}
    build, _ = _kind_providers(e2b=ConnectionError("e2b down"))
    earlier_set = _rollout_set(provider="e2b")
    existing = AsyncMock(return_value=earlier_set if earlier else None)

    with (
        patch(_BUILD_PROVIDER, side_effect=build),
        patch(
            f"{_MODULE}.register_platform_secret_rollouts",
            AsyncMock(side_effect=_register_by_kind),
        ),
        patch(f"{_MODULE}._existing_rollouts_or_none", existing),
    ):
        result = await reconcile_platform_secrets_at_boot(_agent_config(_config()))

    existing.assert_awaited_once_with("e2b")
    assert sorted(result) == (["daytona", "e2b"] if earlier else ["daytona"])
    if earlier:
        assert result["e2b"] is earlier_set


@pytest.mark.asyncio
async def test_certify_verifies_against_the_computers_own_kind_read_fresh(
    monkeypatch,
):
    """A computer of another kind than the deployment's is certified against
    its own provider's rows, read past the memo."""
    e2b_set = _rollout_set(generation=7, provider="e2b")
    get_rollouts = AsyncMock(return_value=e2b_set)
    verify = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.get_platform_secret_rollouts", get_rollouts)
    monkeypatch.setattr(f"{_MODULE}.verify_runtime_platform_secrets", verify)
    runtime = _Runtime()

    version = await certify_platform_secrets(
        _config(provider="daytona"), runtime=runtime, kind="e2b"
    )

    assert version == 7
    get_rollouts.assert_awaited_once_with("e2b", fresh=True)
    verify.assert_awaited_once_with(runtime, expected=e2b_set.placeholders)


@pytest.mark.asyncio
async def test_certify_is_inert_for_a_kind_without_platform_secrets(monkeypatch):
    get_rollouts = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.get_platform_secret_rollouts", get_rollouts)

    assert await certify_platform_secrets(_config(), runtime=None, kind="docker") == 0
    get_rollouts.assert_not_awaited()


@pytest.mark.asyncio
async def test_resync_reads_the_computers_own_kind(monkeypatch):
    get_rollouts = AsyncMock(return_value=_rollout_set(provider="e2b"))
    monkeypatch.setattr(f"{_MODULE}.get_platform_secret_rollouts", get_rollouts)

    assert await _resync(_config(), applied_generation=1, kind="e2b") == 1
    get_rollouts.assert_awaited_once_with("e2b")


@pytest.mark.asyncio
async def test_resync_rereads_when_the_computer_is_stamped_above_the_memo(
    monkeypatch,
):
    """Only another worker registering a newer identity stamps a computer above
    this worker's memo; remounting the memo's bindings would regress it."""
    get_rollouts = AsyncMock(side_effect=[_rollout_set(2), _rollout_set(5)])
    remount = AsyncMock()
    monkeypatch.setattr(f"{_MODULE}.get_platform_secret_rollouts", get_rollouts)
    monkeypatch.setattr(f"{_MODULE}.remount_platform_secret_bindings", remount)

    assert await _resync(_config(), db_version=5) == 5

    assert get_rollouts.await_args_list[1].kwargs == {"fresh": True}
    remount.assert_not_awaited()
