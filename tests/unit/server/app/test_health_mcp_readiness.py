"""/health answers 503 when a built-in remote MCP server failed boot discovery.

Readiness probes read the status code, not the body, so a process serving
without a built-in server's tools has to say so in the code.
"""

from types import SimpleNamespace

import pytest

import ptc_agent.core.mcp_registry as mcp_registry
from src.server.app.utilities import health_check

pytestmark = pytest.mark.asyncio


def _install(monkeypatch, failed: dict[str, str] | None) -> None:
    registry = None if failed is None else SimpleNamespace(failed_builtin_remote=failed)
    monkeypatch.setattr(mcp_registry, "_GLOBAL_REGISTRY", registry)


async def test_failed_builtin_remote_server_fails_the_check(monkeypatch):
    _install(monkeypatch, {"market": "server unavailable (HTTP 503)"})

    response = await health_check()

    assert response.status_code == 503
    body = response.body.decode()
    assert '"status":"unhealthy"' in body
    assert '"failed_servers":["market"]' in body
    # The diagnosis can name internal addresses; it stays in the logs.
    assert "HTTP 503" not in body


@pytest.mark.parametrize("failed", [{}, None], ids=["all-connected", "no-global-registry"])
async def test_no_failed_builtin_keeps_the_plain_200_body(monkeypatch, failed):
    _install(monkeypatch, failed)

    result = await health_check()

    assert isinstance(result, dict)
    assert result["status"] == "healthy"
    assert "mcp" not in result
