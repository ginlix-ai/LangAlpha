"""A key saved after boot turns MCP servers on or off for new sessions.

Sessions borrow the global registry frozen at boot, so the config alone does
not reach them: a successor is published with the newly enabled servers
connected and the disabled ones gone, the others kept as they were.
"""

from types import SimpleNamespace

import pytest
from pydantic import BaseModel

import ptc_agent.core.mcp_registry as mcp_registry
from ptc_agent.config.core import MCPConfig, MCPServerConfig
from ptc_agent.core.mcp_registry import (
    MCPRegistry,
    MCPServerConnector,
    follow_config,
    get_global_registry,
    set_global_registry,
)
from ptc_agent.core.mcp_schema import MCPToolInfo

pytestmark = pytest.mark.asyncio


class _Config(BaseModel):
    mcp: MCPConfig


@pytest.fixture
def connects(monkeypatch):
    """The servers each connect attempt reached, failing those named in ``down``."""
    monkeypatch.setattr(mcp_registry, "_GLOBAL_REGISTRY", None)
    seen = SimpleNamespace(names=[], down=set())

    async def fake_connect(self, timeout=None):
        seen.names.append(self.config.name)
        if self.config.name in seen.down:
            self.failure_reason = "refused"
            raise RuntimeError("refused")
        self.tools = [
            MCPToolInfo(name="get", description="", input_schema={}, server_name=self.config.name)
        ]

    async def fake_exit(self, *exc):
        self.session = None

    monkeypatch.setattr(MCPServerConnector, "connect", fake_connect)
    monkeypatch.setattr(MCPServerConnector, "__aexit__", fake_exit)
    return seen


def _server(name: str, enabled: bool = True) -> MCPServerConfig:
    return MCPServerConfig(
        name=name, transport="http", url=f"http://127.0.0.1:1/{name}", source="builtin", enabled=enabled
    )


def _config(*servers: MCPServerConfig) -> _Config:
    return _Config(mcp=MCPConfig(servers=list(servers)))


def _booted(config: _Config) -> MCPRegistry:
    registry = MCPRegistry(config)
    for server in config.mcp.servers:
        if server.enabled:
            registry.connectors[server.name] = MCPServerConnector(server)
    registry._frozen = True
    set_global_registry(registry)
    return registry


async def test_a_server_turned_on_is_connected_in_config_order(connects):
    booted = _booted(_config(_server("price"), _server("crawl", enabled=False), _server("news")))

    await follow_config(_config(_server("price"), _server("crawl"), _server("news")))

    successor = get_global_registry()
    assert successor is not booted and successor._frozen
    assert list(successor.connectors) == ["price", "crawl", "news"]
    assert connects.names == ["crawl"]
    assert successor.connectors["price"] is booted.connectors["price"]
    assert [t.name for t in successor.connectors["crawl"].tools] == ["get"]


async def test_a_server_turned_off_leaves_the_registry(connects):
    _booted(_config(_server("price"), _server("crawl")))

    await follow_config(_config(_server("price"), _server("crawl", enabled=False)))

    assert list(get_global_registry().connectors) == ["price"]
    assert connects.names == []


async def test_a_turned_on_server_that_is_down_is_tried_again_at_the_next_save(connects):
    connects.down.add("crawl")
    _booted(_config(_server("price"), _server("crawl", enabled=False)))
    on = _config(_server("price"), _server("crawl"))

    await follow_config(on)

    successor = get_global_registry()
    assert list(successor.connectors) == ["price"]
    assert successor.failed_builtin_remote == {}

    connects.down.clear()
    await follow_config(on)

    assert list(get_global_registry().connectors) == ["price", "crawl"]
    assert connects.names == ["crawl", "crawl"]


async def test_the_same_servers_keep_the_registry(connects):
    booted = _booted(_config(_server("price"), _server("crawl", enabled=False)))

    await follow_config(_config(_server("price"), _server("crawl", enabled=False)))

    assert get_global_registry() is booted
    assert connects.names == []


async def test_no_registry_before_boot_is_left_alone(connects):
    await follow_config(_config(_server("price")))

    assert get_global_registry() is None
