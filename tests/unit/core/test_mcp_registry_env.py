"""Tests for MCP registry environment scrubbing.

Verifies that _prepare_env() starts from a safe subset of os.environ
instead of the full environment, preventing host secret leakage to
MCP discovery subprocesses.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_registry import (
    MCPRegistry,
    MCPServerConnector,
    MCPToolInfo,
    clear_global_registry,
    get_global_registry,
    set_global_registry,
)


class TestPrepareEnvSafety:
    """Verify _prepare_env() only forwards safe env vars."""

    def _make_connector(self, env: dict[str, str] | None = None) -> MCPServerConnector:
        config = MCPServerConfig(
            name="test-server",
            command="echo",
            args=["hello"],
            env=env or {},
        )
        return MCPServerConnector(config)

    def test_no_env_config_excludes_host_secrets(self):
        """MCP server with no env block should NOT inherit host secrets."""
        fake_environ = {
            "PATH": "/usr/bin",
            "HOME": "/home/user",
            "ANTHROPIC_API_KEY": "sk-ant-secret",
            "DB_PASSWORD": "supersecret",
            "BYOK_ENCRYPTION_KEY": "enckey123",
        }
        connector = self._make_connector(env={})
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert "PATH" in result
        assert "HOME" in result
        assert "ANTHROPIC_API_KEY" not in result
        assert "DB_PASSWORD" not in result
        assert "BYOK_ENCRYPTION_KEY" not in result

    def test_empty_env_config_returns_safe_subset(self):
        """Empty env config (falsy) returns only safe vars."""
        connector = self._make_connector(env={})
        # MCPServerConfig with empty dict - config.env is truthy but empty
        # Force it to be falsy for the early return path
        connector.config.env = {}

        fake_environ = {
            "PATH": "/usr/bin",
            "HOME": "/home/user",
            "SHELL": "/bin/zsh",
            "SECRET_TOKEN": "tok_abc123",
        }
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert result == {"PATH": "/usr/bin", "HOME": "/home/user", "SHELL": "/bin/zsh"}
        assert "SECRET_TOKEN" not in result

    def test_declared_env_vars_are_expanded(self):
        """Declared env vars with ${VAR} placeholders are resolved."""
        connector = self._make_connector(
            env={"FMP_API_KEY": "${FMP_API_KEY}"}
        )
        fake_environ = {
            "PATH": "/usr/bin",
            "FMP_API_KEY": "fmp_real_key_value",
        }
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert result["PATH"] == "/usr/bin"
        assert result["FMP_API_KEY"] == "fmp_real_key_value"

    def test_literal_env_values_pass_through(self):
        """Literal (non-placeholder) env values are included as-is."""
        connector = self._make_connector(
            env={"MY_SETTING": "literal_value"}
        )
        fake_environ = {"PATH": "/usr/bin"}
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert result["MY_SETTING"] == "literal_value"
        assert result["PATH"] == "/usr/bin"

    def test_safe_vars_forwarded(self):
        """All categories of safe vars are forwarded when present."""
        fake_environ = {
            # OS basics
            "PATH": "/usr/bin",
            "HOME": "/home/user",
            "USER": "testuser",
            "LANG": "en_US.UTF-8",
            # Temp
            "TMPDIR": "/tmp",
            # Node.js
            "NODE_PATH": "/usr/lib/node_modules",
            "NODE_ENV": "production",
            # Python
            "VIRTUAL_ENV": "/home/user/.venv",
            "PYTHONPATH": "/opt/lib",
            # XDG
            "XDG_CACHE_HOME": "/home/user/.cache",
            # Should NOT appear
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI",
            "OPENAI_API_KEY": "sk-openai-secret",
        }
        connector = self._make_connector(env={})
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert result["PATH"] == "/usr/bin"
        assert result["NODE_PATH"] == "/usr/lib/node_modules"
        assert result["VIRTUAL_ENV"] == "/home/user/.venv"
        assert result["XDG_CACHE_HOME"] == "/home/user/.cache"
        assert "AWS_SECRET_ACCESS_KEY" not in result
        assert "OPENAI_API_KEY" not in result

    def test_declared_env_overrides_safe_var(self):
        """Declared env vars can override safe vars (e.g., custom PATH)."""
        connector = self._make_connector(
            env={"PATH": "/custom/bin:/usr/bin"}
        )
        fake_environ = {"PATH": "/usr/bin"}
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert result["PATH"] == "/custom/bin:/usr/bin"

    def test_missing_safe_vars_are_skipped(self):
        """Safe vars not in os.environ are simply absent, no KeyError."""
        connector = self._make_connector(env={})
        fake_environ = {"PATH": "/usr/bin"}  # Only PATH, no HOME etc.
        with patch.dict("os.environ", fake_environ, clear=True):
            result = connector._prepare_env()

        assert result == {"PATH": "/usr/bin"}


class TestFreezeAndGlobalRegistry:
    """Verify the frozen-snapshot path used at lifespan startup."""

    def _make_connector_with_tools(
        self, server_name: str, tool_names: list[str]
    ) -> MCPServerConnector:
        """Build a connector populated as if connect_all already ran."""
        config = MCPServerConfig(
            name=server_name, command="echo", args=["hi"], env={}
        )
        connector = MCPServerConnector(config)
        connector.tools = [
            MCPToolInfo(
                name=name,
                description=f"{name} desc",
                input_schema={"type": "object"},
                server_name=server_name,
            )
            for name in tool_names
        ]
        return connector

    def _make_registry_with_servers(
        self, servers: dict[str, list[str]]
    ) -> MCPRegistry:
        """Build a registry whose connectors look freshly connected."""
        config = MagicMock()
        config.mcp.servers = []  # not exercised by freeze() / connect_all no-op
        registry = MCPRegistry(config)
        for server_name, tool_names in servers.items():
            registry.connectors[server_name] = self._make_connector_with_tools(
                server_name, tool_names
            )
        return registry

    @pytest.mark.asyncio
    async def test_freeze_terminates_subprocesses_but_keeps_tools(self):
        """freeze() exits each connector context and preserves the schema dict.

        ``__aexit__`` is mocked to confirm it is awaited per connector;
        ``connectors`` itself stays populated so consumers reading
        ``get_all_tools`` still see the snapshot.
        """
        registry = self._make_registry_with_servers(
            {"alpha": ["a1", "a2"], "beta": ["b1"]}
        )
        for connector in registry.connectors.values():
            connector.__aexit__ = AsyncMock(return_value=None)

        await registry.freeze()

        for connector in registry.connectors.values():
            connector.__aexit__.assert_awaited_once_with(None, None, None)
        assert registry.frozen is True
        assert set(registry.connectors.keys()) == {"alpha", "beta"}
        assert {t.name for t in registry.connectors["alpha"].tools} == {"a1", "a2"}
        assert {t.name for t in registry.connectors["beta"].tools} == {"b1"}

    @pytest.mark.asyncio
    async def test_freeze_is_idempotent(self):
        """Calling freeze() twice doesn't double-call __aexit__."""
        registry = self._make_registry_with_servers({"alpha": ["a1"]})
        registry.connectors["alpha"].__aexit__ = AsyncMock(return_value=None)

        await registry.freeze()
        await registry.freeze()

        registry.connectors["alpha"].__aexit__.assert_awaited_once()
        assert registry.frozen is True

    @pytest.mark.asyncio
    async def test_connect_all_is_noop_when_frozen(self):
        """A frozen registry never re-spawns subprocesses on connect_all().

        This is what lets a Session that calls ``connect_all`` against the
        shared global registry skip subprocess startup entirely.
        """
        registry = self._make_registry_with_servers({"alpha": ["a1"]})
        registry.connectors["alpha"].__aexit__ = AsyncMock(return_value=None)
        await registry.freeze()

        # Stash a sentinel so we can prove connectors weren't replaced.
        sentinel_connector = registry.connectors["alpha"]
        await registry.connect_all()
        assert registry.connectors["alpha"] is sentinel_connector
        assert registry.frozen is True

    @pytest.mark.asyncio
    async def test_disconnect_all_is_noop_when_frozen(self):
        """A frozen registry's disconnect_all preserves the snapshot.

        Sessions that don't own the registry will still call
        ``disconnect_all`` during stop/cleanup error paths; the no-op keeps
        the shared snapshot intact.
        """
        registry = self._make_registry_with_servers({"alpha": ["a1"]})
        registry.connectors["alpha"].__aexit__ = AsyncMock(return_value=None)
        await registry.freeze()
        # Reset the mock so we can detect any further __aexit__ calls.
        registry.connectors["alpha"].__aexit__.reset_mock()

        await registry.disconnect_all()

        registry.connectors["alpha"].__aexit__.assert_not_called()
        assert "alpha" in registry.connectors
        assert registry.connectors["alpha"].tools[0].name == "a1"

    def test_global_registry_lifecycle(self):
        """get/set/clear preserve identity and clear actually clears."""
        assert get_global_registry() is None

        registry = MCPRegistry(MagicMock(mcp=MagicMock(servers=[])))
        registry._frozen = True  # set_global_registry requires frozen
        set_global_registry(registry)
        assert get_global_registry() is registry

        clear_global_registry()
        assert get_global_registry() is None

    def test_set_global_registry_rejects_unfrozen(self):
        """Installing an unfrozen registry would defeat the snapshot
        invariant — set_global_registry must raise."""
        registry = MCPRegistry(MagicMock(mcp=MagicMock(servers=[])))
        with pytest.raises(ValueError, match="frozen"):
            set_global_registry(registry)
        assert get_global_registry() is None

    @pytest.mark.asyncio
    async def test_connect_all_drops_failed_connectors(self):
        """A connector whose __aenter__ raises must be removed from the
        registry, not retained with empty tools. Otherwise the frozen
        snapshot leaks a phantom server for the process lifetime."""
        config = MagicMock()
        config.mcp.servers = [
            MCPServerConfig(name="alpha", command="echo", args=["hi"], env={}, enabled=True),
            MCPServerConfig(name="beta", command="echo", args=["hi"], env={}, enabled=True),
            MCPServerConfig(name="gamma", command="echo", args=["hi"], env={}, enabled=True),
        ]
        registry = MCPRegistry(config)

        # Patch __aenter__ via MCPServerConnector creation hook
        original_init = MCPServerConnector.__init__

        def patched_init(self, cfg):
            original_init(self, cfg)
            should_fail = cfg.name == "beta"
            if should_fail:
                async def failing(*a, **kw):
                    raise RuntimeError(f"{cfg.name} boot failed")
                self.__aenter__ = failing  # type: ignore[method-assign]
            else:
                self.tools = [
                    MCPToolInfo(
                        name=f"{cfg.name}_tool",
                        description="",
                        input_schema={},
                        server_name=cfg.name,
                    )
                ]
                async def succeeding(*a, **kw):
                    return self
                self.__aenter__ = succeeding  # type: ignore[method-assign]

        with patch.object(MCPServerConnector, "__init__", patched_init):
            await registry.connect_all()

        assert "alpha" in registry.connectors
        assert "gamma" in registry.connectors
        assert "beta" not in registry.connectors
        assert len(registry.connectors) == 2

    @pytest.mark.asyncio
    async def test_force_disconnect_all_runs_when_frozen(self):
        """_force_disconnect_all bypasses the frozen-state early-return so
        lifespan-startup error rollback can drop subprocesses regardless of
        how far freeze() got before raising."""
        registry = self._make_registry_with_servers({"alpha": ["a1"]})
        alpha_aexit = AsyncMock(return_value=None)
        registry.connectors["alpha"].__aexit__ = alpha_aexit
        await registry.freeze()
        alpha_aexit.reset_mock()

        await registry._force_disconnect_all()

        alpha_aexit.assert_awaited_once_with(None, None, None)
        assert registry.connectors == {}

    @pytest.mark.asyncio
    async def test_call_tool_raises_when_frozen(self):
        """A frozen registry has no live sessions; call_tool must fail loud
        instead of dispatching to a connector with session=None."""
        registry = self._make_registry_with_servers({"alpha": ["a1"]})
        registry.connectors["alpha"].__aexit__ = AsyncMock(return_value=None)
        await registry.freeze()

        with pytest.raises(RuntimeError, match="frozen"):
            await registry.call_tool("alpha", "a1", {})
