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
    ],
)
def test_the_gateways_reason_picks_the_code_and_the_words(reason, code, answered, says):
    exc = _tunnel_error(
        _TARGET, asyncssh.ChannelOpenError(asyncssh.OPEN_CONNECT_FAILED, reason)
    )

    assert exc.code == code
    assert exc.answered is answered
    assert exc.for_owner == says
