"""How a dial through the tunnel gateway that failed reads to the machine's owner.

The gateway gives the reason; the worker turns it into a connect code and a
sentence. A tunnel a newer pairing superseded is a machine that moved, which
waiting will not bring back, so it reads as answered, unlike one that is away.
"""

import asyncssh
import pytest

from ptc_agent.core.sandbox.providers._ssh_connect import (
    GATEWAY_NOT_CONNECTED,
    GATEWAY_PAIRED_ELSEWHERE,
    GATEWAY_STARTING,
    SSHTarget,
    _tunnel_error,
)

_TARGET = SSHTarget(host="mac", port=22, username="alex", host_key=None, key_owner="u1")


@pytest.mark.parametrize(
    ("reason", "code", "answered", "says"),
    [
        (
            GATEWAY_PAIRED_ELSEWHERE,
            "paired_elsewhere",
            True,
            "mac is now connected to another LangAlpha account or server. "
            "Reconnect it to use it here.",
        ),
        (
            GATEWAY_NOT_CONNECTED,
            "not_connected",
            False,
            "mac is not connected. It connects by itself whenever it is on and "
            "online, so check that it is awake and has internet.",
        ),
        (
            GATEWAY_STARTING,
            "gateway_unreachable",
            False,
            "The tunnel that machines connect through is not answering right "
            "now. Try again in a moment.",
        ),
        (
            "Connection refused",
            "refused",
            True,
            "mac is connected, but its SSH server did not take the connection "
            "(Connection refused). On a Mac, check that Remote Login is on in "
            "System Settings, General, Sharing.",
        ),
    ],
)
def test_the_gateways_reason_picks_the_code_and_the_words(reason, code, answered, says):
    exc = _tunnel_error(
        _TARGET, asyncssh.ChannelOpenError(asyncssh.OPEN_CONNECT_FAILED, reason)
    )

    assert exc.code == code
    assert exc.answered is answered
    assert exc.for_owner == says


def test_a_machine_whose_own_end_refused_is_not_away():
    """Its tunnel is up and it answered, so its owner fixes it: an automation
    run on it fails and strikes rather than waiting for it to wake."""
    from ptc_agent.core.sandbox.unreachable import unreachable_code

    exc = _tunnel_error(
        _TARGET,
        asyncssh.ChannelOpenError(asyncssh.OPEN_CONNECT_FAILED, "Connection refused"),
    )

    assert not exc.machine_silent
    assert unreachable_code(exc) is None


def test_a_machine_that_answers_clears_the_silence_an_earlier_call_left():
    """A tool call whose first sandbox call found the machine silent and whose
    next found it answering and refusing ended on the earlier silence, so the
    turn stopped as though the machine were gone. Its answer clears it."""
    from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
    from ptc_agent.core.sandbox.unreachable import note_call, watch_unreachable

    sandbox = object()
    with watch_unreachable(sandbox) as watch:
        note_call(sandbox, SSHConnectError("not_connected", "asleep"))
        assert watch.failure is not None
        note_call(sandbox, SSHConnectError("auth_failed", "refused"))
        assert watch.failure is None
        note_call(sandbox, RuntimeError("a command failed"))
        assert watch.failure is None
