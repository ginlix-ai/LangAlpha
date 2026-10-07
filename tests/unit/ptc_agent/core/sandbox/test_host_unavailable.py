"""A start refused while the sandbox's host recovers is waited on, never deleted.

The refusal is a 503 that only its wording tells apart from any other
overload, so detection is pinned on the exception the SDK really raises. Past
the policy's threshold the sandbox is called gone, which makes the start
rebuild from the backup; the sandbox itself may come back with files the
backup lacks, so nothing on that path may delete it.
"""

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from daytona._utils.errors import intercept_errors
from daytona.common.errors import DaytonaServiceUnavailableError
from daytona_api_client_async.exceptions import ServiceException

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import (
    HostUnavailablePolicy,
    RuntimeState,
    SandboxGoneError,
    SandboxHostLostError,
    SandboxHostUnavailableError,
    SandboxTransientError,
)

HOST_RECOVERING = (
    "Sandbox start is temporarily unavailable while the sandbox's host "
    "recovers. Please retry in a few moments."
)
SANDBOX = "sb-host-1"
OUTAGE_SINCE = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)


async def _sdk_start_error(message: str, status: int = 503) -> Exception:
    """The exception ``AsyncSandbox.start`` raises for this HTTP answer."""

    @intercept_errors(message_prefix="Failed to start sandbox: ")
    async def start():
        raise ServiceException(
            status=status,
            reason="Service Unavailable",
            body=json.dumps(
                {"statusCode": status, "message": message, "error": "Service Unavailable"}
            ),
        )

    try:
        await start()
    except Exception as exc:
        return exc
    raise AssertionError("the SDK call did not fail")


def _daytona() -> DaytonaProvider:
    return DaytonaProvider.__new__(DaytonaProvider)


@pytest.mark.asyncio
async def test_the_host_recovery_refusal_is_detected_on_the_real_sdk_error():
    exc = await _sdk_start_error(HOST_RECOVERING)

    assert isinstance(exc, DaytonaServiceUnavailableError)
    assert str(exc) == f"Failed to start sandbox: {HOST_RECOVERING}"
    assert _daytona().is_host_unavailable(exc) is True


@pytest.mark.asyncio
async def test_another_503_is_not_a_host_outage():
    exc = await _sdk_start_error("Service temporarily unavailable, try again later")

    assert isinstance(exc, DaytonaServiceUnavailableError)
    assert _daytona().is_host_unavailable(exc) is False


@pytest.mark.asyncio
async def test_the_wording_without_a_503_is_not_a_host_outage():
    exc = await _sdk_start_error(HOST_RECOVERING, status=500)

    assert _daytona().is_host_unavailable(exc) is False


class _Runtime:
    def __init__(self, failure: Exception | None):
        self._failure = failure
        self.delete = AsyncMock()
        self.stop = AsyncMock()
        self.fetch_working_dir = AsyncMock(return_value="/home/workspace")

    async def get_state(self):
        return RuntimeState.STOPPED

    async def start(self, timeout):
        if self._failure is not None:
            raise self._failure


class _Provider(DaytonaProvider):
    """Daytona's own classifiers over a runtime that never reaches the network."""

    def __init__(self, runtime: _Runtime):
        self._runtime = runtime

    async def get(self, sandbox_id, **kwargs):
        return self._runtime

    async def prepare_reconnect(self, runtime, *, tier=None):
        pass

    async def close(self):
        pass


def _policy(
    *,
    rebuild: bool = False,
    failure: Exception | None = None,
    current: bool | None = True,
):
    policy = AsyncMock(spec=HostUnavailablePolicy)
    policy.rebuild_now.return_value = OUTAGE_SINCE if rebuild else None
    policy.rebuild_now.side_effect = failure
    policy.reconnected.return_value = current
    return policy


def _sandbox(failure: Exception | None, policy=None) -> tuple[PTCSandbox, _Runtime]:
    config = CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )
    runtime = _Runtime(failure)
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=_Provider(runtime),
    ):
        sandbox = PTCSandbox(config, None, host_unavailable_policy=policy)
    sandbox._start_internal_mcp_servers = AsyncMock(return_value=True)
    return sandbox, runtime


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy",
    [None, _policy(rebuild=False), _policy(failure=RuntimeError("db down"))],
    ids=["no-policy", "within-threshold", "policy-failed"],
)
async def test_until_the_policy_says_rebuild_the_outage_is_transient(policy):
    failure = await _sdk_start_error(HOST_RECOVERING)
    sandbox, runtime = _sandbox(failure, policy)

    with pytest.raises(SandboxHostUnavailableError) as raised:
        await sandbox.reconnect(SANDBOX)

    assert isinstance(raised.value, SandboxTransientError)
    assert not isinstance(raised.value, SandboxGoneError)
    # The person reads the provider's own words, as before.
    assert str(raised.value) == str(failure)
    assert raised.value.__cause__ is failure
    if policy is not None:
        policy.rebuild_now.assert_awaited_once_with(SANDBOX)
        policy.reconnected.assert_not_awaited()
    await sandbox.cleanup()
    runtime.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_rebuild_calls_the_sandbox_gone_and_never_deletes_it():
    failure = await _sdk_start_error(HOST_RECOVERING)
    sandbox, runtime = _sandbox(failure, _policy(rebuild=True))

    with pytest.raises(SandboxHostLostError) as raised:
        await sandbox.reconnect(SANDBOX)

    assert isinstance(raised.value, SandboxGoneError)
    assert raised.value.sandbox_id == SANDBOX
    # The rebuild binds only while this outage is still the row's.
    assert raised.value.outage_since == OUTAGE_SINCE
    # Kept even if a later reconnect on this handle were to complete.
    sandbox._reconnect_incomplete = False
    await sandbox.cleanup()
    runtime.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_another_start_failure_never_asks_the_policy():
    failure = await _sdk_start_error("Service temporarily unavailable, try again later")
    policy = _policy(rebuild=True)
    sandbox, _ = _sandbox(failure, policy)

    with pytest.raises(DaytonaServiceUnavailableError):
        await sandbox.reconnect(SANDBOX)

    policy.rebuild_now.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_sandbox_that_comes_up_ends_the_outage():
    """However it was reached: a reconnect that finds the row already running
    writes no status, so this is what stops a stale stamp from cutting the
    next outage short."""
    policy = _policy()
    sandbox, _ = _sandbox(None, policy)

    await sandbox.reconnect(SANDBOX)

    policy.reconnected.assert_awaited_once_with(SANDBOX)
    policy.rebuild_now.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_outage_ends_before_any_later_setup_can_fail():
    """The sandbox is up once its start returns; a setup step failing after
    that must not leave the clock running for the next outage."""
    policy = _policy()
    sandbox, runtime = _sandbox(None, policy)
    runtime.fetch_working_dir.side_effect = SandboxTransientError("toolbox blip")

    with pytest.raises(SandboxTransientError):
        await sandbox.reconnect(SANDBOX)

    policy.reconnected.assert_awaited_once_with(SANDBOX)


@pytest.mark.asyncio
async def test_a_sandbox_a_rebuild_replaced_meanwhile_is_neither_used_nor_deleted():
    """Its host came back while a rebuild authorized by the outage ran, and
    that rebuild bound first: the machine is on the new sandbox now, and this
    one may hold files the backup lacks."""
    sandbox, runtime = _sandbox(None, _policy(current=False))

    with pytest.raises(SandboxGoneError) as raised:
        await sandbox.reconnect(SANDBOX)

    assert not isinstance(raised.value, SandboxHostLostError)
    runtime.fetch_working_dir.assert_not_awaited()
    sandbox._start_internal_mcp_servers.assert_not_awaited()
    # Started for nothing: stopped, so it neither runs unowned nor loses its disk.
    runtime.stop.assert_awaited_once()
    sandbox._reconnect_incomplete = False
    await sandbox.cleanup()
    runtime.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_sandbox_the_machine_has_not_named_yet_stays_in_use():
    """A sandbox being built to replace another reconnects on a transient
    during its own setup, before its bind."""
    sandbox, runtime = _sandbox(None, _policy(current=None))

    await sandbox.reconnect(SANDBOX)

    runtime.fetch_working_dir.assert_awaited_once()
    runtime.stop.assert_not_awaited()
    await sandbox.cleanup()
    runtime.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_without_an_answer_from_the_machine_the_reconnect_is_retried():
    """Used on without it, the sandbox could be one a rebuild is about to replace."""
    failure = RuntimeError("db down")
    policy = _policy()
    policy.reconnected.side_effect = failure
    sandbox, runtime = _sandbox(None, policy)

    with pytest.raises(SandboxTransientError) as raised:
        await sandbox.reconnect(SANDBOX)

    assert raised.value.__cause__ is failure
    runtime.fetch_working_dir.assert_not_awaited()
    await sandbox.cleanup()
    runtime.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_sandbox_lost_mid_turn_fails_its_handle_until_it_reconnects():
    """A ready handle that kept answering ready would be reused by every turn
    on this worker, and the rebuild would never run here."""
    failure = await _sdk_start_error(HOST_RECOVERING)
    sandbox, runtime = _sandbox(failure, _policy(rebuild=True))
    sandbox.sandbox_id = SANDBOX
    sandbox.runtime = runtime

    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=_Provider(runtime),
    ):
        with pytest.raises(SandboxHostLostError) as raised:
            await sandbox._ensure_sandbox_connected()
        assert sandbox.is_ready() is False
        assert sandbox.has_failed() is True
        assert sandbox.init_error is raised.value

        runtime._failure = None
        await sandbox._ensure_sandbox_connected()

    assert sandbox.is_ready() is True
    assert sandbox.has_failed() is False


@pytest.mark.asyncio
async def test_a_sandbox_still_the_machines_after_its_host_came_back_is_deleted_with_it():
    """A rebuild authorized on this handle that never bound leaves the sandbox
    the machine's; the handle reconnecting must not keep it from teardown."""
    failure = await _sdk_start_error(HOST_RECOVERING)
    policy = _policy(rebuild=True)
    sandbox, runtime = _sandbox(failure, policy)
    with pytest.raises(SandboxHostLostError):
        await sandbox.reconnect(SANDBOX)

    runtime._failure = None
    await sandbox.reconnect(SANDBOX)
    await sandbox.cleanup()

    runtime.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_replaced_sandbox_is_stopped_through_a_transport_blip():
    """No row names it any more, so this stop is the only one it gets."""
    sandbox, runtime = _sandbox(None, _policy(current=False))
    runtime.stop = AsyncMock(side_effect=[ConnectionResetError("reset"), None])

    with pytest.raises(SandboxGoneError):
        await sandbox.reconnect(SANDBOX)

    assert runtime.stop.await_count == 2


@pytest.mark.asyncio
async def test_a_transient_start_inside_a_reconnect_retries_the_start():
    """Reconnecting again from inside the reconnect waited on its own lock."""
    sandbox, runtime = _sandbox(None)
    sandbox.sandbox_id = SANDBOX
    sandbox.runtime = runtime
    runtime.start = AsyncMock(side_effect=[ConnectionResetError("reset"), None])

    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=_Provider(runtime),
    ):
        await asyncio.wait_for(sandbox._ensure_sandbox_connected(), timeout=10)

    assert runtime.start.await_count == 2
    assert sandbox.is_ready() is True
