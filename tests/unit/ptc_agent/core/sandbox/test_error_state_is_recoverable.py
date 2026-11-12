"""An error-state sandbox is transient, because ``reconnect`` decides its fate.

``reconnect`` answers state ``error`` by asking the runtime to recover it, and
only the provider can say a sandbox will not come back. If this module called
that state absence instead, the caller would act on ``SandboxGoneError`` by
building a replacement and restoring from the last backup, abandoning a sandbox
a recovery would have brought back and losing every write since that backup.
The two paths have to agree on what is fatal.

``reconnect`` used to disagree: it gave a sandbox about 20s to leave ``starting``
(10s for ``stopping``) and then raised ``SandboxGoneError``, so a slow but live
control-plane transition authorized a replacement. The last test here pins the
two paths together, because the classifier agreeing with itself proves nothing
about the function that actually destroys sandboxes.
"""

from datetime import datetime, timezone
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
from ptc_agent.core.sandbox.attach_only import SandboxNotRunning, attach_only
from ptc_agent.core.sandbox.files import _classify_by_liveness
from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox
from ptc_agent.core.sandbox.runtime import (
    HostUnavailablePolicy,
    RuntimeState,
    SandboxGoneError,
    SandboxHostLostError,
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
        _sandbox(RuntimeState.ERROR),
        RuntimeError("boom"),
        op="download_file",
        path="/a.txt",
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
        RuntimeState.ARCHIVING,
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

        async def prepare_reconnect(self, runtime, *, tier=None):
            pass

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


class _Clock(HostUnavailablePolicy):
    """An outage clock that answers *since* to every sighting it is shown."""

    def __init__(self, since):
        self.since = since
        self.asked: list[tuple[str, str | None]] = []
        self.seen: list[tuple[str, str]] = []

    async def rebuild_now(self, sandbox_id, stuck_in=None):
        self.asked.append((sandbox_id, stuck_in))
        return self.since

    async def observed(self, sandbox_id, state):
        self.seen.append((sandbox_id, state))

    async def reconnected(self, sandbox_id):
        return True


_STUCK = [RuntimeState.STARTING, RuntimeState.STOPPING, RuntimeState.ARCHIVING]


def _ticking_clock():
    """``time.monotonic`` that runs a minute a read, so a deadline wait ends at once."""
    now = iter(range(0, 10**6, 60))
    return lambda: next(now)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", _STUCK)
async def test_a_stuck_transition_waits_while_the_outage_clock_runs(state):
    """Each stuck read runs the computer's outage clock for that transition,
    and until it runs out the start is a retry, as it always was. The read
    that found the sandbox there is told to the clock first, which is what
    ends a clock a sandbox stuck elsewhere left running."""
    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = _stuck_provider(state)
    sandbox.host_unavailable_policy = clock = _Clock(None)

    with (
        patch("asyncio.sleep", new=AsyncMock()),
        patch("ptc_agent.core.sandbox.reconnect.time.monotonic", _ticking_clock()),
    ):
        with pytest.raises(SandboxTransientError) as excinfo:
            await sandbox.reconnect("sbx-stuck")

    assert not isinstance(excinfo.value, SandboxGoneError)
    assert clock.seen == [("sbx-stuck", state.value)]
    assert clock.asked == [("sbx-stuck", state.value)]
    assert sandbox._kept_for_salvage is False


def _reading(*reads: RuntimeState, then: RuntimeState):
    """A provider whose sandbox reads *reads* in turn, then *then* for good."""
    provider = _stuck_provider(then)
    stuck = provider.get
    queue = iter(reads)

    async def get(sandbox_id, **kwargs):
        runtime = await stuck(sandbox_id)
        runtime.get_state = AsyncMock(return_value=next(queue, then))
        return runtime

    provider.get = get
    return provider


async def _reconnect_with(provider, clock) -> SandboxTransientError:
    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = provider
    sandbox.host_unavailable_policy = clock
    with patch("asyncio.sleep", new=AsyncMock()):
        with pytest.raises(SandboxTransientError) as excinfo:
            await sandbox.reconnect("sbx-stuck")
    return excinfo.value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stuck, moved",
    [
        (RuntimeState.STARTING, RuntimeState.STOPPED),
        (RuntimeState.STOPPING, RuntimeState.RUNNING),
    ],
    ids=["start-fell-back", "stop-ran-again"],
)
async def test_a_sandbox_that_leaves_its_transition_mid_wait_is_told_to_the_clock(
    stuck, moved
):
    """A sandbox that left its transition and came back to it within one wait
    is a new attempt, so the clock hears the move before the stuck sighting,
    which ends whatever clock the earlier sighting started."""
    clock = _Clock(None)

    await _reconnect_with(_reading(stuck, moved, then=stuck), clock)

    assert clock.seen == [
        ("sbx-stuck", stuck.value),
        ("sbx-stuck", moved.value),
        ("sbx-stuck", stuck.value),
    ]
    assert clock.asked == [("sbx-stuck", stuck.value)]


@pytest.mark.asyncio
async def test_a_wait_that_ends_out_of_its_transition_is_no_stuck_sighting():
    """A start that fell back to stopped and stayed there is the next open's
    to start, not a sandbox stuck in stopped."""
    clock = _Clock(datetime(2026, 11, 12, tzinfo=timezone.utc))

    error = await _reconnect_with(
        _reading(RuntimeState.STARTING, then=RuntimeState.STOPPED), clock
    )

    assert not isinstance(error, SandboxHostLostError)
    assert clock.asked == []


@pytest.mark.asyncio
async def test_a_move_the_clock_did_not_hear_fails_the_open_before_any_rebuild():
    """A clock that missed a move would count the sandbox stuck since before
    it, so the open fails and is retried rather than asking it."""
    clock = _Clock(datetime(2026, 11, 12, tzinfo=timezone.utc))
    clock.observed = AsyncMock(side_effect=[None, RuntimeError("db down")])

    error = await _reconnect_with(
        _reading(
            RuntimeState.STARTING, RuntimeState.STOPPED, then=RuntimeState.STARTING
        ),
        clock,
    )

    assert "Could not record" in str(error)
    assert clock.asked == []


@pytest.mark.asyncio
@pytest.mark.parametrize("state", _STUCK)
async def test_a_transition_stuck_past_the_outage_rebuilds_and_keeps_the_sandbox(state):
    """A host that stopped making progress leaves the sandbox mid-transition
    and refuses every stop, so past the outage threshold it is lost like a
    host refusing starts: rebuilt from the backup, the old sandbox kept."""
    since = datetime(2026, 11, 12, 2, 9, tzinfo=timezone.utc)
    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = _stuck_provider(state)
    sandbox.host_unavailable_policy = _Clock(since)

    with (
        patch("asyncio.sleep", new=AsyncMock()),
        patch("ptc_agent.core.sandbox.reconnect.time.monotonic", _ticking_clock()),
    ):
        with pytest.raises(SandboxHostLostError) as excinfo:
            await sandbox.reconnect("sbx-stuck")

    assert excinfo.value.outage_since == since
    assert sandbox._kept_for_salvage is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state", [RuntimeState.STOPPED, RuntimeState.STARTING, RuntimeState.ARCHIVED]
)
async def test_an_attach_only_reconnect_never_brings_a_sandbox_up(state):
    """A reader nobody waits on uses a sandbox only while it runs."""
    sandbox = PTCSandbox(_config(), None)
    sandbox.provider = _stuck_provider(state)

    with attach_only(), patch("asyncio.sleep", new=AsyncMock()):
        with pytest.raises(SandboxNotRunning):
            await sandbox.reconnect("sbx-stuck")
