"""A pooled SSH connection can read as open after its machine went away.

Only a new dial says whether the machine is there, so a state read or a start
that finds its connection dead dials once more. A slow command is not a dead
connection and is not retried.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime, _ConnectionLost
from ptc_agent.core.sandbox.runtime import RuntimeState, SandboxTransientError


def _runtime(*, reads) -> SSHRuntime:
    # The constructor needs a target and a key loader; these paths read neither.
    runtime = object.__new__(SSHRuntime)
    runtime._lifecycle_link = AsyncMock(side_effect=["link-1", "link-2", "link-3"])
    runtime._read_state = AsyncMock(side_effect=reads)
    return runtime


@pytest.mark.asyncio
async def test_a_connection_lost_under_a_state_read_dials_again():
    runtime = _runtime(reads=[_ConnectionLost("gone"), RuntimeState.RUNNING])

    assert await runtime.get_state() is RuntimeState.RUNNING

    assert runtime._lifecycle_link.await_count == 2
    assert [c.args[0] for c in runtime._read_state.await_args_list] == [
        "link-1",
        "link-2",
    ]


@pytest.mark.asyncio
async def test_a_second_lost_connection_is_not_retried_again():
    runtime = _runtime(reads=[_ConnectionLost("gone"), _ConnectionLost("still gone")])

    with pytest.raises(_ConnectionLost):
        await runtime.get_state()

    assert runtime._lifecycle_link.await_count == 2


@pytest.mark.asyncio
async def test_a_slow_command_is_not_taken_for_a_dead_connection():
    runtime = _runtime(reads=[SandboxTransientError("command timed out")])

    with pytest.raises(SandboxTransientError):
        await runtime.get_state()

    assert runtime._lifecycle_link.await_count == 1
