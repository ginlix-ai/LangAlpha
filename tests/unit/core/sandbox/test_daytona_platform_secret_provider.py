"""Daytona provider Secret reconciliation and mount forwarding tests."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import DaytonaConfig, PlatformSecretDefinition
from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    PlatformSecretReconciliationError,
    resolve_platform_secrets,
)


class _StatusError(Exception):
    def __init__(self, status: int, message: str = "provider error") -> None:
        super().__init__(message)
        self.status = status


def _resolved_secret():
    config = SimpleNamespace(
        sandbox=SimpleNamespace(
            provider="daytona",
            daytona=DaytonaConfig(api_key="daytona-key", secret_namespace="prod"),
            platform_secrets=(
                PlatformSecretDefinition(
                    source_env_var="FMP_API_KEY",
                    sandbox_env_var="FMP_API_KEY",
                    name_suffix="platform-fmp-api-key",
                    description="Platform FMP API key",
                    hosts=("financialmodelingprep.com",),
                ),
            ),
        )
    )
    return resolve_platform_secrets(
        config,
        environ={"FMP_API_KEY": "never-log-this-value"},
        host_mode="platform",
    )


def _provider(client):
    from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

    provider = DaytonaProvider.__new__(DaytonaProvider)
    provider._config = DaytonaConfig(api_key="daytona-key")
    provider._working_dir = "/home/workspace"
    provider._client = client
    return provider


def _secret_meta(
    *,
    secret_id: str = "secret-id",
    name: str = "prod-platform-fmp-api-key",
):
    return SimpleNamespace(
        id=secret_id,
        name=name,
        placeholder="dtn_secret_opaque",
    )


@pytest.mark.asyncio
async def test_reconcile_creates_absent_secret():
    client = MagicMock()
    client.secret.list = AsyncMock(
        return_value=SimpleNamespace(items=[], next_cursor=None)
    )
    client.secret.create = AsyncMock(return_value=_secret_meta())
    provider = _provider(client)

    reconciled = await provider.reconcile_platform_secrets(_resolved_secret())

    params = client.secret.create.await_args.args[0]
    assert params.name == "prod-platform-fmp-api-key"
    assert params.value == "never-log-this-value"
    assert params.hosts == ["financialmodelingprep.com"]
    assert reconciled[0].provider_secret_id == "secret-id"


@pytest.mark.asyncio
async def test_reconcile_paginates_exact_match_and_updates():
    client = MagicMock()
    partial = SimpleNamespace(id="partial", name="prod-platform-fmp-api-key-copy")
    exact = _secret_meta(secret_id="exact-id")
    client.secret.list = AsyncMock(
        side_effect=[
            SimpleNamespace(items=[partial], next_cursor="next"),
            SimpleNamespace(items=[exact], next_cursor=None),
        ]
    )
    client.secret.update = AsyncMock(return_value=exact)
    provider = _provider(client)

    await provider.reconcile_platform_secrets(_resolved_secret())

    assert client.secret.list.await_args_list[1].kwargs["cursor"] == "next"
    assert client.secret.update.await_args.args[0] == "exact-id"
    assert client.secret.update.await_args.args[1].value == "never-log-this-value"


@pytest.mark.asyncio
async def test_concurrent_create_conflict_refetches_and_updates():
    client = MagicMock()
    exact = _secret_meta(secret_id="winner-id")
    client.secret.list = AsyncMock(
        side_effect=[
            SimpleNamespace(items=[], next_cursor=None),
            SimpleNamespace(items=[exact], next_cursor=None),
        ]
    )
    client.secret.create = AsyncMock(side_effect=_StatusError(409))
    client.secret.update = AsyncMock(return_value=exact)
    provider = _provider(client)

    await provider.reconcile_platform_secrets(_resolved_secret())

    client.secret.update.assert_awaited_once()
    assert client.secret.update.await_args.args[0] == "winner-id"


@pytest.mark.asyncio
async def test_retryable_failure_retries_three_attempts():
    client = MagicMock()
    client.secret.list = AsyncMock(
        side_effect=[
            _StatusError(503),
            _StatusError(429),
            SimpleNamespace(items=[], next_cursor=None),
        ]
    )
    client.secret.create = AsyncMock(return_value=_secret_meta())
    provider = _provider(client)

    with patch(
        "ptc_agent.core.sandbox.providers.daytona_secrets.asyncio.sleep",
        AsyncMock(),
    ):
        await provider.reconcile_platform_secrets(_resolved_secret())

    assert client.secret.list.await_count == 3
    client.secret.create.assert_awaited_once()


@pytest.mark.asyncio
async def test_nonretryable_error_is_sanitized():
    client = MagicMock()
    client.secret.list = AsyncMock(side_effect=ValueError("never-log-this-value"))
    provider = _provider(client)

    with pytest.raises(PlatformSecretReconciliationError) as exc_info:
        await provider.reconcile_platform_secrets(_resolved_secret())

    assert "never-log-this-value" not in str(exc_info.value)
    assert client.secret.list.await_count == 1


@pytest.mark.asyncio
async def test_retry_exhaustion_is_sanitized_and_bounded():
    client = MagicMock()
    client.secret.list = AsyncMock(
        side_effect=[_StatusError(503, "never-log-this-value")] * 3
    )
    provider = _provider(client)

    with (
        patch(
            "ptc_agent.core.sandbox.providers.daytona_secrets.asyncio.sleep",
            AsyncMock(),
        ),
        pytest.raises(PlatformSecretReconciliationError) as exc_info,
    ):
        await provider.reconcile_platform_secrets(_resolved_secret())

    assert client.secret.list.await_count == 3
    assert "never-log-this-value" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_ordinary_4xx_is_not_retried():
    client = MagicMock()
    client.secret.list = AsyncMock(side_effect=_StatusError(400))
    provider = _provider(client)

    with pytest.raises(PlatformSecretReconciliationError):
        await provider.reconcile_platform_secrets(_resolved_secret())

    assert client.secret.list.await_count == 1


@pytest.mark.asyncio
async def test_status_less_transient_is_retried():
    # A transport failure (connection reset) carries no HTTP status; classify it
    # by message so a first-boot network blip retries instead of hard-failing
    # reconciliation (there is no prior rollout to fall back on at first boot).
    client = MagicMock()
    client.secret.list = AsyncMock(
        side_effect=[
            ConnectionError("Connection reset by peer"),
            SimpleNamespace(items=[], next_cursor=None),
        ]
    )
    client.secret.create = AsyncMock(return_value=_secret_meta())
    provider = _provider(client)

    with patch(
        "ptc_agent.core.sandbox.providers.daytona_secrets.asyncio.sleep",
        AsyncMock(),
    ):
        await provider.reconcile_platform_secrets(_resolved_secret())

    assert client.secret.list.await_count == 2
    client.secret.create.assert_awaited_once()


def test_is_transient_daytona_error_classification():
    from ptc_agent.core.sandbox.providers.daytona_secrets import (
        is_transient_daytona_error,
    )

    assert is_transient_daytona_error(ConnectionError("Connection reset by peer"))
    assert is_transient_daytona_error(TimeoutError("request timed out"))
    # Transport exception types are transient even with an empty message.
    assert is_transient_daytona_error(ConnectionResetError())
    assert is_transient_daytona_error(TimeoutError())
    assert is_transient_daytona_error(Exception("Session is closed"))
    assert is_transient_daytona_error(Exception("503 Service Unavailable"))
    # Bare digits never classify: a status-less "400 Bad Request" is terminal
    # (status-bearing errors are classified by status at the retry sites).
    assert not is_transient_daytona_error(Exception("400 Bad Request"))
    # Execution errors are terminal even when the server message says
    # "timeout", including when the SDK prefixes the message.
    assert not is_transient_daytona_error(
        Exception("Failed to execute command: timed out")
    )
    assert not is_transient_daytona_error(
        Exception("DaytonaError: Failed to execute command: timeout reached")
    )
    assert not is_transient_daytona_error(ValueError("bad secret name"))
    assert not is_transient_daytona_error(Exception(""))


@pytest.mark.asyncio
async def test_forbidden_error_names_required_secret_permission():
    client = MagicMock()
    client.secret.list = AsyncMock(side_effect=_StatusError(403))
    provider = _provider(client)

    with pytest.raises(
        PlatformSecretConfigurationError,
        match="manage:secrets",
    ):
        await provider.reconcile_platform_secrets(_resolved_secret())

    assert client.secret.list.await_count == 1


@pytest.mark.asyncio
async def test_ambiguous_create_error_refetches_committed_secret():
    client = MagicMock()
    exact = _secret_meta(secret_id="committed-id")
    client.secret.list = AsyncMock(
        side_effect=[
            SimpleNamespace(items=[], next_cursor=None),
            SimpleNamespace(items=[exact], next_cursor=None),
        ]
    )
    client.secret.create = AsyncMock(side_effect=ConnectionError("connection lost"))
    client.secret.update = AsyncMock(return_value=exact)
    provider = _provider(client)

    reconciled = await provider.reconcile_platform_secrets(_resolved_secret())

    assert reconciled[0].provider_secret_id == "committed-id"
    client.secret.update.assert_awaited_once()


def test_exception_status_supports_both_sdk_error_shapes():
    from ptc_agent.core.sandbox.providers.daytona_secrets import (
        DaytonaSecretReconciler,
    )

    assert DaytonaSecretReconciler._exception_status(_StatusError(409)) == 409
    assert (
        DaytonaSecretReconciler._exception_status(
            SimpleNamespace(status_code=503)  # type: ignore[arg-type]
        )
        == 503
    )


@pytest.mark.asyncio
async def test_create_forwards_secret_bindings():
    client = MagicMock()
    sdk_sandbox = MagicMock(id="new-id")
    client.create = AsyncMock(return_value=sdk_sandbox)
    provider = _provider(client)

    with patch.object(provider, "_ensure_snapshot", AsyncMock(return_value=None)):
        await provider.create(
            env_vars={"ORDINARY": "value"},
            platform_secret_bindings={"FMP_API_KEY": "prod-platform-fmp-api-key"},
        )

    params = client.create.await_args.args[0]
    assert params.env_vars == {"ORDINARY": "value"}
    assert params.secrets == {"FMP_API_KEY": "prod-platform-fmp-api-key"}


@pytest.mark.asyncio
async def test_runtime_wraps_update_env_and_update_secrets():
    from ptc_agent.core.sandbox.providers.daytona import DaytonaRuntime

    sdk_sandbox = MagicMock(id="sandbox-id")
    sdk_sandbox.update_env = AsyncMock()
    sdk_sandbox.update_secrets = AsyncMock()
    runtime = DaytonaRuntime(sdk_sandbox)

    await runtime.update_env({}, unset=["FMP_API_KEY"])
    await runtime.update_secrets({"FMP_API_KEY": "prod-platform-fmp-api-key"})

    sdk_sandbox.update_env.assert_awaited_once_with({}, unset=["FMP_API_KEY"])
    sdk_sandbox.update_secrets.assert_awaited_once_with(
        {"FMP_API_KEY": "prod-platform-fmp-api-key"}
    )
