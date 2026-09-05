"""An error-state sandbox is transient, because ``reconnect`` still revives it.

``reconnect`` answers state ``error`` with a recovery ``start`` rather than
giving up. If this module called that state absence instead, the caller would
act on ``SandboxGoneError`` by building a replacement and restoring from the
last backup, destroying a sandbox a ``start`` would have brought back and losing
every write since that backup. The two paths have to agree on what is fatal.

``reconnect`` used to disagree: it gave a sandbox about 20s to leave ``starting``
(10s for ``stopping``) and then raised ``SandboxGoneError``, so a slow but live
control-plane transition authorized a replacement. The last test here pins the
two paths together, because the classifier agreeing with itself proves nothing
about the function that actually destroys sandboxes.
"""

from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.files import _classify_by_liveness
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import (
    RuntimeState,
    SandboxGoneError,
    SandboxTransientError,
)


def _sandbox(state: RuntimeState):
    """A sandbox whose liveness probe reports *state*.

    Only the three attributes ``_classify_by_liveness`` reads, so nothing here
    depends on a provider or a live runtime.
    """

    class _Runtime:
        async def refresh_state(self):
            return state

    class _Provider:
        def classify_error(self, exc):  # never reached: the probe succeeds
            raise AssertionError("probe succeeded, so the error is not reclassified")

    class _Sandbox:
        runtime = _Runtime()
        provider = _Provider()
        sandbox_id = "sbx-test"

    return _Sandbox()


@pytest.mark.asyncio
async def test_error_state_is_transient_not_gone():
    result = await _classify_by_liveness(
        _sandbox(RuntimeState.ERROR), RuntimeError("boom"), op="download_file", path="/a.txt"
    )
    assert isinstance(result, SandboxTransientError)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    [
        RuntimeState.RUNNING,
        RuntimeState.STARTING,
        RuntimeState.STOPPED,
        RuntimeState.STOPPING,
        RuntimeState.ARCHIVED,
        RuntimeState.ERROR,
    ],
)
async def test_liveness_probe_calls_every_recoverable_state_transient(state):
    """What ``_classify_by_liveness`` alone decides, which is all this covers.

    Deliberately not named for ``reconnect``: that function is a separate path
    with its own verdict, pinned separately below. Parametrized rather than
    asserted against the frozenset so a regression fails on behavior, not on a
    container's contents.
    """
    result = await _classify_by_liveness(
        _sandbox(state), RuntimeError("boom"), op="list_files", path="/"
    )
    assert isinstance(result, SandboxTransientError)
    assert not isinstance(result, SandboxGoneError)


def _config() -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def _stuck_provider(state: RuntimeState):
    """A provider whose sandbox never leaves *state* but always answers."""

    class _Runtime:
        id = "sbx-stuck"

        async def get_state(self):
            return state

    class _Provider:
        async def get(self, sandbox_id, **kwargs):
            return _Runtime()

        def is_transient_error(self, exc):  # consulted by the retry wrapper
            return False

        def classify_error(self, exc):
            raise AssertionError("provider.get succeeded; nothing to classify")

    return _Provider()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state, expected_word",
    [(RuntimeState.STARTING, "starting"), (RuntimeState.STOPPING, "stopping")],
)
async def test_a_transition_that_times_out_is_transient_not_gone(state, expected_word):
    """The path that actually destroys sandboxes, not the classifier beside it.

    Reading the state back is positive evidence the sandbox exists, so a wait
    that runs out is a retry. ``SandboxGoneError`` is the authorization to
    replace, and its handlers in ``workspace_manager`` call ``_clear_session``
    before recovering, which deletes the runtime — so raising it here destroyed
    a live sandbox mid-boot and restored it from the last DB backup.
    """
    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = _stuck_provider(state)

    # The waits are ~20s and ~10s of real sleeping; only the verdict is at issue.
    with patch("asyncio.sleep", new=AsyncMock()):
        with pytest.raises(SandboxTransientError) as excinfo:
            await sandbox.reconnect("sbx-stuck")

    assert not isinstance(excinfo.value, SandboxGoneError)
    assert expected_word in str(excinfo.value).lower()
