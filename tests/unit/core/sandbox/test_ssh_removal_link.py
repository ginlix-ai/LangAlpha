"""A removed computer's delete leaves its connection for the removal's last steps.

Taking the key's line away runs there, not on a new login: a machine the
delete could not reach is not dialed again, and the uninstall after the key
needs no key.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers._ssh_transport import Link, pool
from ptc_agent.core.sandbox.providers.ssh import removal_link, ssh_target
from ptc_agent.core.sandbox.providers.ssh_runtime import SSHRuntime

_HOST_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"
)
_CONFIG = SSHConfig(
    host="203.0.113.10", username="dev", host_key=_HOST_KEY, key_owner="user-1"
)


def _pooled() -> Link:
    conn = MagicMock()
    conn.is_closed.return_value = False
    conn._channels = {}
    target = ssh_target(_CONFIG)
    link = Link(target, conn)
    pool()._links[target.pool_key] = link
    return link


@pytest.mark.asyncio
async def test_the_delete_leaves_its_connection_open():
    link = _pooled()
    runtime = object.__new__(SSHRuntime)
    runtime._target = link.target
    runtime._working_dir = "/home/dev/langalpha"
    runtime._dir = lambda _link: "/home/dev/.langalpha/computers/abc"
    runtime._link = AsyncMock(return_value=link)
    runtime._stop_processes = AsyncMock()
    runtime._run = AsyncMock(return_value="")

    await runtime.delete()

    assert pool().peek(link.target) is link
    link.conn.abort.assert_not_called()


@pytest.mark.asyncio
async def test_the_removal_gets_that_connection_and_closes_it_after():
    link = _pooled()

    async with removal_link(_CONFIG) as found:
        assert found is link
        link.conn.abort.assert_not_called()

    link.conn.abort.assert_called_once()
    assert pool().peek(link.target) is None


@pytest.mark.asyncio
async def test_one_another_computer_on_the_login_is_using_stays_open():
    link = _pooled()
    link.active = 1

    async with removal_link(_CONFIG):
        pass

    link.conn.abort.assert_not_called()
    assert pool().peek(link.target) is link
    pool().evict(link)


@pytest.mark.asyncio
async def test_no_connection_is_none_and_nothing_is_dialed():
    async with removal_link(_CONFIG) as found:
        assert found is None
