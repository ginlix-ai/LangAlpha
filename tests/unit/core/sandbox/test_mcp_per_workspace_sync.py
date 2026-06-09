"""Per-workspace MCP sandbox sync: manifest regression, config hash, discovery.

Covers the sandbox-side deliverables: a zero-user-server workspace's manifest
inputs stay byte-identical (regression #1), the user-server config hash is gated
on the presence of user servers, the effective/builtin server split routes each
audited read site correctly, and discover_user_mcp_schemas isolates per-server
errors + parses file-IPC output.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    MCPServerConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.runtime import ExecResult, SandboxProvider, SandboxRuntime


def _make_config(servers=None) -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
        security=SecurityConfig(),
        mcp=MCPConfig(servers=servers or []),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def _builtin(name, **kw):
    return MCPServerConfig(name=name, source="builtin", **kw)


def _user(name, **kw):
    return MCPServerConfig(name=name, source="workspace", **kw)


def _make_sandbox(config):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    with patch("ptc_agent.core.sandbox.ptc_sandbox.create_provider"):
        sandbox = PTCSandbox(config=config)
    return sandbox


# ---------------------------------------------------------------------------
# Regression #1 — zero-user-server manifest inputs unchanged
# ---------------------------------------------------------------------------


class TestManifestRegression:
    """A builtin-only workspace's manifest inputs are byte-identical to the
    pre-change algorithm (gated user_mcp_config hash never appears)."""

    def test_user_mcp_config_hash_empty_for_builtin_only(self):
        config = _make_config(
            servers=[_builtin("yfinance"), _builtin("sec", transport="http", url="https://x")]
        )
        sandbox = _make_sandbox(config)
        # No user servers ⇒ empty hash, so the source_versions dict is untouched.
        assert sandbox._compute_user_mcp_config_hash() == ""

    def test_user_mcp_config_hash_present_with_user_server(self):
        config = _make_config(
            servers=[
                _builtin("yfinance"),
                _user("notes", transport="http", url="https://example.test/mcp"),
            ]
        )
        sandbox = _make_sandbox(config)
        h = sandbox._compute_user_mcp_config_hash()
        assert h != ""
        # Stable across calls (deterministic).
        assert h == sandbox._compute_user_mcp_config_hash()

    def test_user_mcp_config_hash_ignores_secret_values(self):
        """Hash captures header/env key NAMES only — never their values, so a
        rotated secret does not churn the manifest."""
        c1 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"Authorization": "${vault:TOKEN_A}"},
                )
            ]
        )
        c2 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"Authorization": "${vault:TOKEN_B}"},
                )
            ]
        )
        assert (
            _make_sandbox(c1)._compute_user_mcp_config_hash()
            == _make_sandbox(c2)._compute_user_mcp_config_hash()
        )

    def test_user_mcp_config_hash_changes_on_header_name(self):
        """Adding a header NAME (config-only edit) changes the hash → re-upload."""
        c1 = _make_config(
            servers=[_user("notes", transport="http", url="https://example.test/mcp")]
        )
        c2 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"X-Api-Key": "${vault:K}"},
                )
            ]
        )
        assert (
            _make_sandbox(c1)._compute_user_mcp_config_hash()
            != _make_sandbox(c2)._compute_user_mcp_config_hash()
        )

    @pytest.mark.asyncio
    async def test_manifest_tool_modules_omits_user_key_builtin_only(self):
        """A builtin-only config's tool_modules.source_versions has NO
        user_mcp_config key — identical to pre-change (regression #1)."""
        config = _make_config(servers=[_builtin("yfinance")])
        sandbox = _make_sandbox(config)
        sandbox.mcp_registry = MagicMock()
        sandbox.mcp_registry.get_all_tools = MagicMock(return_value={})
        manifest = await sandbox._compute_sandbox_manifest()
        source_versions = manifest["modules"]["tool_modules"]["source_versions"]
        assert "user_mcp_config" not in source_versions
        assert set(source_versions.keys()) == {"mcp_servers", "tool_schemas"}

    @pytest.mark.asyncio
    async def test_manifest_tool_modules_includes_user_key_with_user_server(self):
        """A user server adds the gated user_mcp_config component → tool_modules
        version changes, re-uploading the regenerated client."""
        config = _make_config(
            servers=[
                _builtin("yfinance"),
                _user("notes", transport="http", url="https://example.test/mcp"),
            ]
        )
        sandbox = _make_sandbox(config)
        sandbox.mcp_registry = MagicMock()
        sandbox.mcp_registry.get_all_tools = MagicMock(return_value={})
        manifest = await sandbox._compute_sandbox_manifest()
        source_versions = manifest["modules"]["tool_modules"]["source_versions"]
        assert "user_mcp_config" in source_versions


# ---------------------------------------------------------------------------
# Effective vs built-in server split — per-site audit
# ---------------------------------------------------------------------------


class TestServerSplit:
    """_builtin_servers / _user_servers partition the effective set so each
    audited read site sees the right subset."""

    def test_split(self):
        config = _make_config(
            servers=[
                _builtin("yfinance"),
                _user("notes", transport="http", url="https://example.test"),
                _builtin("sec"),
            ]
        )
        sandbox = _make_sandbox(config)
        assert [s.name for s in sandbox._builtin_servers()] == ["yfinance", "sec"]
        assert [s.name for s in sandbox._user_servers()] == ["notes"]

    def test_mcp_packages_excludes_user_npx(self):
        """A user npx server must NOT be pre-installed globally (call-time fetch)."""
        config = _make_config(
            servers=[
                _builtin("bi", transport="stdio", command="npx", args=["-y", "builtin-pkg"]),
                _user("up", transport="stdio", command="npx", args=["-y", "user-pkg"]),
            ]
        )
        sandbox = _make_sandbox(config)
        assert sandbox._get_mcp_packages() == ["builtin-pkg"]

    def test_build_env_vars_excludes_user_env(self):
        """User-server env is never injected into the sandbox os.environ."""
        config = _make_config(
            servers=[
                _builtin("bi", env={"BUILTIN_KEY": "literal-val"}),
                _user("up", env={"USER_KEY": "${vault:SECRET}"}),
            ]
        )
        sandbox = _make_sandbox(config)
        env = sandbox._build_sandbox_env_vars()
        assert env.get("BUILTIN_KEY") == "literal-val"
        assert "USER_KEY" not in env


# ---------------------------------------------------------------------------
# discover_user_mcp_schemas — file IPC, per-server isolation, timeout
# ---------------------------------------------------------------------------


@pytest.fixture
def discovery_sandbox():
    config = _make_config(
        servers=[
            _user("alpha", transport="http", url="https://a.test"),
            _user("beta", transport="http", url="https://b.test"),
        ]
    )
    runtime = AsyncMock(spec=SandboxRuntime)
    runtime.id = "rt-1"
    runtime.working_dir = "/home/workspace"
    runtime.exec = AsyncMock(return_value=ExecResult("", "", 0))
    runtime.upload_file = AsyncMock()
    provider = AsyncMock(spec=SandboxProvider)
    provider.is_transient_error = MagicMock(return_value=False)
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider", return_value=provider
    ):
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        sandbox = PTCSandbox(config=config)
    sandbox.runtime = runtime
    sandbox.tool_generator = MagicMock()
    sandbox.tool_generator.generate_mcp_client_code = MagicMock(return_value="# client")
    return sandbox


class TestDiscoverUserMcpSchemas:
    @pytest.mark.asyncio
    async def test_parses_file_ipc_and_isolates_errors(self, discovery_sandbox):
        sandbox = discovery_sandbox

        async def fake_download(path):
            if "alpha" not in path and "beta" not in path:
                return None
            # The temp file name uses a hash of the server name; route by which
            # download call this is via a counter on the mock.
            return None

        # Map output files by server: alpha → ok with one tool, beta → error.
        results_by_server = {
            "alpha": {
                "server": "alpha",
                "status": "ok",
                "error": "",
                "tools": [{"name": "do_a", "description": "d", "input_schema": {}}],
            },
            "beta": {
                "server": "beta",
                "status": "error",
                "error": "boom",
                "tools": [],
            },
        }
        # Track which out_path maps to which server by intercepting exec.
        path_to_server = {}

        async def fake_exec(cmd, **kwargs):
            # The discover command embeds the server name and out path.
            for name in ("alpha", "beta"):
                if f"discover '{name}'" in cmd or f"discover {name} " in cmd:
                    # Last token is the out path.
                    out = cmd.strip().split()[-1].strip("'")
                    path_to_server[out] = name
            return ExecResult("", "", 0)

        async def fake_download_bytes(path):
            server = path_to_server.get(path)
            if server is None:
                return None
            import json

            return json.dumps(results_by_server[server]).encode()

        sandbox.runtime.exec = AsyncMock(side_effect=fake_exec)
        sandbox.adownload_file_bytes = AsyncMock(side_effect=fake_download_bytes)

        out = await sandbox.discover_user_mcp_schemas(sandbox._user_servers())

        assert set(out.keys()) == {"alpha", "beta"}
        assert out["alpha"]["status"] == "ok"
        assert out["alpha"]["tools"][0]["name"] == "do_a"
        assert out["beta"]["status"] == "error"
        assert out["beta"]["error"] == "boom"

    @pytest.mark.asyncio
    async def test_missing_output_is_error(self, discovery_sandbox):
        sandbox = discovery_sandbox
        sandbox.adownload_file_bytes = AsyncMock(return_value=None)

        out = await sandbox.discover_user_mcp_schemas(
            [_user("alpha", transport="http", url="https://a.test")]
        )
        assert out["alpha"]["status"] == "error"
        assert "no output" in out["alpha"]["error"]

    @pytest.mark.asyncio
    async def test_exec_timeout_isolated_to_one_server(self, discovery_sandbox):
        sandbox = discovery_sandbox

        async def fake_exec(cmd, **kwargs):
            if "alpha" in cmd:
                raise TimeoutError("discovery timed out")
            return ExecResult("", "", 0)

        async def fake_download_bytes(path):
            import json

            return json.dumps(
                {"server": "beta", "status": "ok", "error": "", "tools": []}
            ).encode()

        sandbox.runtime.exec = AsyncMock(side_effect=fake_exec)
        sandbox.adownload_file_bytes = AsyncMock(side_effect=fake_download_bytes)

        out = await sandbox.discover_user_mcp_schemas(sandbox._user_servers())
        # One server timing out must not starve the other.
        assert out["alpha"]["status"] == "error"
        assert out["beta"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_uploads_client_before_discovery(self, discovery_sandbox):
        """mcp_client.py is uploaded FIRST (bootstrapping order) so discovery
        runs against the current config."""
        sandbox = discovery_sandbox
        call_order = []

        async def track_upload(*a, **k):
            call_order.append("upload_client")

        async def track_exec(cmd, **k):
            if "discover" in cmd:
                call_order.append("discover")
            return ExecResult("", "", 0)

        sandbox.runtime.upload_file = AsyncMock(side_effect=track_upload)
        sandbox.runtime.exec = AsyncMock(side_effect=track_exec)
        sandbox.adownload_file_bytes = AsyncMock(return_value=None)

        await sandbox.discover_user_mcp_schemas(
            [_user("alpha", transport="http", url="https://a.test")]
        )
        assert call_order.index("upload_client") < call_order.index("discover")
