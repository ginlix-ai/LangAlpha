"""PostgreSQL invariants for trusted platform-secret rollout state."""

from __future__ import annotations

import hashlib

import pytest

from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretDefinition,
    ReconciledPlatformSecret,
)
from ptc_agent.core.sandbox.runtime import ExecResult


# The shared psycopg pool owns background connection workers on the session
# event loop. Run this module on that same loop so held ``db_conn`` fixtures can
# acquire a second service connection without starving a dormant pool worker.
pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


@pytest.fixture(autouse=True)
def _reset_rollout_cache():
    """The per-process rollout cache must not leak identity between tests."""

    from src.server.services import platform_secret_rollout

    platform_secret_rollout._active_rollout_set = None
    yield
    platform_secret_rollout._active_rollout_set = None


def _identity(*, secret_id: str, placeholder: str) -> ReconciledPlatformSecret:
    definition = PlatformSecretDefinition(
        source_env_var="FMP_API_KEY",
        sandbox_env_var="FMP_API_KEY",
        name_suffix="platform-fmp-api-key",
        description="Platform FMP API key",
        hosts=("financialmodelingprep.com",),
    )
    return ReconciledPlatformSecret(
        definition=definition,
        name=f"prod-{definition.name_suffix}",
        provider_secret_id=secret_id,
        placeholder=placeholder,
    )


def _capable_config():
    """CoreConfig stand-in for certify: a capable provider with a non-empty
    catalog, so ``platform_secrets_active`` is True (certify never iterates it)."""

    definition = PlatformSecretDefinition(
        source_env_var="FMP_API_KEY",
        sandbox_env_var="FMP_API_KEY",
        name_suffix="platform-fmp-api-key",
        description="Platform FMP API key",
        hosts=("financialmodelingprep.com",),
    )
    sandbox = type(
        "Sandbox", (), {"provider": "daytona", "platform_secrets": (definition,)}
    )()
    return type("Config", (), {"sandbox": sandbox})()


def _extra_identity() -> ReconciledPlatformSecret:
    definition = PlatformSecretDefinition(
        source_env_var="EXTRA_API_KEY",
        sandbox_env_var="EXTRA_API_KEY",
        name_suffix="platform-extra-api-key",
        description="Test-only extra platform key",
        hosts=("example.com",),
    )
    return ReconciledPlatformSecret(
        definition=definition,
        name=f"prod-{definition.name_suffix}",
        provider_secret_id="extra-id",
        placeholder="dtn_secret_extra",
    )


async def _register(*identities: ReconciledPlatformSecret, provider: str = "daytona"):
    from src.server.services.platform_secret_rollout import (
        register_platform_secret_rollouts,
    )

    return await register_platform_secret_rollouts(
        list(identities),
        credential_values={
            identity.definition.sandbox_env_var: "credential-value"
            for identity in identities
        },
        provider=provider,
    )


class _VerifiedRuntime:
    """Emulates the in-sandbox hashing probe for the registered placeholder."""

    def __init__(self, *placeholders: str):
        self._lines = "\n".join(
            hashlib.sha256(p.encode()).hexdigest() for p in placeholders
        )

    async def exec(self, command: str, timeout: int = 60) -> ExecResult:
        return ExecResult(f"{self._lines}\n", "", 0)


async def test_user_config_cannot_modify_trusted_rollout_columns(
    seed_workspace, patched_get_db_connection
):
    from src.server.database.workspace import get_workspace, update_workspace

    workspace_id = str(seed_workspace["workspace_id"])
    await update_workspace(
        workspace_id,
        config={"platform_secret_version": 999},
    )
    workspace = await get_workspace(workspace_id)

    assert workspace["config"]["platform_secret_version"] == 999
    assert workspace["platform_secret_version"] == 0


async def test_identity_change_bumps_generation_and_keeps_certified_version(
    seed_workspace, patched_get_db_connection, db_conn
):
    from src.server.services.platform_secret_rollout import (
        stamp_workspace_platform_secret_version,
    )

    first = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    workspace_id = str(seed_workspace["workspace_id"])
    await stamp_workspace_platform_secret_version(
        workspace_id,
        expected_sandbox_id=None,
        rollout_set=first,
    )

    second = await _register(
        _identity(secret_id="secret-2", placeholder="dtn_secret_two")
    )
    row = await db_conn.execute(
        "SELECT platform_secret_version FROM workspaces WHERE workspace_id = %s",
        (workspace_id,),
    )
    workspace = await row.fetchone()

    assert second.generation == first.generation + 1
    # The row keeps the generation it was certified at — behind the new
    # generation (so it re-pends), but never zeroed: version 0 stays reserved
    # for "never certified", preserving the plaintext/placeholder
    # discriminator that routes hot-swap vs scrub-restart.
    assert workspace["platform_secret_version"] == first.generation


async def test_set_membership_change_bumps_generation_monotonically(
    seed_workspace, patched_get_db_connection
):
    fmp = _identity(secret_id="secret-1", placeholder="dtn_secret_one")

    first = await _register(fmp)
    grown = await _register(fmp, _extra_identity())
    shrunk = await _register(fmp)

    # Addition and removal are both identity changes; the shared sequence
    # never regresses even though the removed row carried the highest value.
    assert grown.generation == first.generation + 1
    assert sorted(grown.bindings) == ["EXTRA_API_KEY", "FMP_API_KEY"]
    assert shrunk.generation == grown.generation + 1
    assert sorted(shrunk.bindings) == ["FMP_API_KEY"]


async def test_credential_rotation_without_identity_change_keeps_generation(
    seed_workspace, patched_get_db_connection
):
    from src.server.services.platform_secret_rollout import (
        register_platform_secret_rollouts,
    )

    identity = _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    first = await _register(identity)
    second = await register_platform_secret_rollouts(
        [identity],
        credential_values={"FMP_API_KEY": "rotated-credential-value"},
        provider="daytona",
    )

    assert second.generation == first.generation
    assert (
        second.rollouts[0].current_credential_sha256
        != first.rollouts[0].current_credential_sha256
    )


async def test_provider_change_is_an_identity_change(
    seed_workspace, patched_get_db_connection
):
    first = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    second = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one"),
        provider="microvm",
    )

    assert second.rollouts[0].provider == "microvm"
    assert second.generation == first.generation + 1


async def test_certification_attaches_a_verified_replacement(
    seed_workspace, patched_get_db_connection, db_conn
):
    from src.server.services.platform_secret_rollout import (
        certify_new_workspace_sandbox,
    )

    rollout_set = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    workspace_id = str(seed_workspace["workspace_id"])

    config = _capable_config()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
        workspace = await certify_new_workspace_sandbox(
            config,
            workspace_id=workspace_id,
            expected_previous_sandbox_id=None,
            sandbox_id="replacement-sandbox",
            runtime=_VerifiedRuntime("dtn_secret_one"),
        )

    assert workspace["platform_secret_version"] == rollout_set.generation
    assert workspace["sandbox_id"] == "replacement-sandbox"


async def test_stamp_cas_rejects_a_moved_workspace(
    seed_workspace, patched_get_db_connection
):
    from src.server.services.platform_secret_rollout import (
        stamp_workspace_platform_secret_version,
    )

    rollout_set = await _register(
        _identity(secret_id="secret-1", placeholder="dtn_secret_one")
    )
    workspace_id = str(seed_workspace["workspace_id"])

    with pytest.raises(RuntimeError, match="before platform Secret stamp"):
        await stamp_workspace_platform_secret_version(
            workspace_id,
            expected_sandbox_id="not-the-attached-sandbox",
            rollout_set=rollout_set,
        )


async def test_certify_cas_rejects_a_concurrent_attachment(
    seed_workspace, patched_get_db_connection
):
    from src.server.services.platform_secret_rollout import (
        certify_new_workspace_sandbox,
    )

    await _register(_identity(secret_id="secret-1", placeholder="dtn_secret_one"))
    workspace_id = str(seed_workspace["workspace_id"])

    config = _capable_config()
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
        with pytest.raises(RuntimeError, match="before platform Secret"):
            await certify_new_workspace_sandbox(
                config,
                workspace_id=workspace_id,
                expected_previous_sandbox_id="stale-previous-sandbox",
                sandbox_id="replacement-sandbox",
                runtime=_VerifiedRuntime("dtn_secret_one"),
            )


async def test_missing_rollout_row_fails_readiness(patched_get_db_connection):
    from src.server.services.platform_secret_rollout import (
        PlatformSecretReadinessError,
        get_platform_secret_rollouts,
    )

    with pytest.raises(PlatformSecretReadinessError, match="not initialized"):
        await get_platform_secret_rollouts()
