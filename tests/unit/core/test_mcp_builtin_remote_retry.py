"""Boot discovery retries a built-in remote server only on what a restart looks like.

A built-in HTTP/SSE server that is restarting is waited for; a 4xx, a user
server and a stdio server fail on their first attempt as before. A built-in
remote server that never connects is recorded, which is what the health check
reads.
"""

import asyncio
import time
from types import SimpleNamespace

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_registry import MCPRegistry, MCPServerConnector

pytestmark = pytest.mark.asyncio

_TRANSIENT = ("server unavailable (HTTP 503)", True)
_AUTH = ("server refused the credentials (HTTP 401)", False)


@pytest.fixture
def scripted(monkeypatch):
    """Replace each connect attempt with the next scripted outcome per server."""
    monkeypatch.setattr(MCPRegistry, "BUILTIN_REMOTE_BACKOFF_S", 0.0)
    scripts: dict[str, list] = {}
    attempts: dict[str, int] = {}

    async def fake_connect(self, timeout=None):
        attempts[self.config.name] = attempts.get(self.config.name, 0) + 1
        outcome = scripts[self.config.name].pop(0)
        if outcome is None:
            return
        self.failure_reason, self.failure_transient = outcome
        self._connection_error = RuntimeError(outcome[0])
        raise self._connection_error

    monkeypatch.setattr(MCPServerConnector, "connect", fake_connect)
    return scripts, attempts


def _registry(*servers: MCPServerConfig) -> MCPRegistry:
    return MCPRegistry(SimpleNamespace(mcp=SimpleNamespace(servers=list(servers))))


def _http(name: str, *, source: str = "builtin", transport: str = "http") -> MCPServerConfig:
    return MCPServerConfig(
        name=name, transport=transport, url=f"http://127.0.0.1:1/{name}", source=source
    )


async def test_restarting_builtin_is_retried_until_it_connects(scripted):
    scripts, attempts = scripted
    scripts["market"] = [_TRANSIENT, _TRANSIENT, None]
    registry = _registry(_http("market"))

    await registry.connect_all(retry_builtin_remote=True)

    assert attempts["market"] == 3
    assert "market" in registry.connectors
    assert registry.failed_builtin_remote == {}


async def test_builtin_that_never_connects_is_recorded_after_every_attempt(scripted):
    scripts, attempts = scripted
    scripts["market"] = [_TRANSIENT] * MCPRegistry.BUILTIN_REMOTE_ATTEMPTS
    registry = _registry(_http("market", transport="sse"))

    await registry.connect_all(retry_builtin_remote=True)

    assert attempts["market"] == MCPRegistry.BUILTIN_REMOTE_ATTEMPTS
    assert "market" not in registry.connectors
    assert registry.failed_builtin_remote == {"market": _TRANSIENT[0]}


async def test_client_error_is_not_retried(scripted):
    scripts, attempts = scripted
    scripts["market"] = [_AUTH]
    registry = _registry(_http("market"))

    await registry.connect_all(retry_builtin_remote=True)

    assert attempts["market"] == 1
    assert registry.failed_builtin_remote == {"market": _AUTH[0]}


async def test_user_and_stdio_servers_keep_failing_fast(scripted):
    scripts, attempts = scripted
    scripts["user_http"] = [_TRANSIENT]
    scripts["local"] = [("process exited", False)]
    stdio = MCPServerConfig(name="local", transport="stdio", command="nonexistent")
    registry = _registry(_http("user_http", source="user"), stdio)

    await registry.connect_all(retry_builtin_remote=True)

    assert attempts == {"user_http": 1, "local": 1}
    assert registry.failed_builtin_remote == {}


async def test_without_the_boot_flag_a_builtin_fails_fast(scripted):
    scripts, attempts = scripted
    scripts["market"] = [_TRANSIENT]
    registry = _registry(_http("market"))

    await registry.connect_all()

    assert attempts["market"] == 1
    assert registry.failed_builtin_remote == {"market": _TRANSIENT[0]}


async def test_a_stalled_teardown_does_not_hold_the_attempt_past_its_deadline(monkeypatch):
    """Cancelling an http connection sends a session DELETE on the transport's
    300 s read timeout, so a server that stalls it would otherwise hold boot or
    shutdown long past the attempt's own timeout."""
    grace = 0.05
    monkeypatch.setattr(MCPServerConnector, "ABANDON_GRACE_S", grace)

    async def never_answers_then_stalls_teardown(self):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(5)

    monkeypatch.setattr(MCPServerConnector, "_run_connection", never_answers_then_stalls_teardown)
    connector = MCPServerConnector(_http("market"), quiet=True)

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        await connector.connect(timeout=0.1)

    assert time.monotonic() - started < 0.1 + 2 * grace + 0.25
    assert connector.failure_transient
    assert connector._connection_task.done()
