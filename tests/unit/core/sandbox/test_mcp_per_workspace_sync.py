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

    def test_user_mcp_config_hash_changes_on_literal_value(self):
        """Rotating a literal (non-vault) value under the same key MUST churn the
        manifest — the regenerated client embeds that literal, so a stale value
        would otherwise never re-upload. Stored values are vault-ref strings or
        non-secret literals (e.g. ``MODE=prod`` -> ``staging``), never a resolved
        secret, so hashing them leaks nothing (regression: literal edits were
        silently ignored)."""
        c1 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"X-Mode": "prod"},
                )
            ]
        )
        c2 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"X-Mode": "staging"},
                )
            ]
        )
        assert (
            _make_sandbox(c1)._compute_user_mcp_config_hash()
            != _make_sandbox(c2)._compute_user_mcp_config_hash()
        )

    def test_user_mcp_config_hash_changes_on_vault_ref_retarget(self):
        """Retargeting a vault ref under the SAME key (${vault:A} → ${vault:B})
        changes which secret the regenerated client embeds, so the hash MUST
        churn → re-upload (regression: stale secret ref otherwise)."""
        c1 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"Authorization": "${vault:SECRET_A}"},
                )
            ]
        )
        c2 = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="http",
                    url="https://example.test/mcp",
                    headers={"Authorization": "${vault:SECRET_B}"},
                )
            ]
        )
        assert (
            _make_sandbox(c1)._compute_user_mcp_config_hash()
            != _make_sandbox(c2)._compute_user_mcp_config_hash()
        )

    def test_user_mcp_config_hash_changes_on_url_vault_ref_retarget(self):
        """A vault ref retarget inside the URL also churns the hash."""
        c1 = _make_config(
            servers=[
                _user("notes", transport="http", url="https://example.test/${vault:SECRET_A}")
            ]
        )
        c2 = _make_config(
            servers=[
                _user("notes", transport="http", url="https://example.test/${vault:SECRET_B}")
            ]
        )
        assert (
            _make_sandbox(c1)._compute_user_mcp_config_hash()
            != _make_sandbox(c2)._compute_user_mcp_config_hash()
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

    def test_user_mcp_config_hash_changes_on_discovery_uses_secrets_toggle(self):
        """Flipping discovery_uses_secrets changes the generated client's vault
        gating, so the manifest hash MUST churn → re-upload.

        Uses a STDIO server: the flag is meaningful there (it guards an
        untrusted subprocess). For a remote server with a vault-ref header the
        effective value is always on, so the toggle is a no-op — covered by
        ``test_remote_auth_header_forces_discovery_secrets_in_hash`` below.
        """
        c_off = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="stdio",
                    command="npx",
                    args=["x"],
                    env={"TOK": "${vault:K}"},
                    discovery_uses_secrets=False,
                )
            ]
        )
        c_on = _make_config(
            servers=[
                _user(
                    "notes",
                    transport="stdio",
                    command="npx",
                    args=["x"],
                    env={"TOK": "${vault:K}"},
                    discovery_uses_secrets=True,
                )
            ]
        )
        assert (
            _make_sandbox(c_off)._compute_user_mcp_config_hash()
            != _make_sandbox(c_on)._compute_user_mcp_config_hash()
        )

    def test_remote_auth_header_forces_discovery_secrets_in_hash(self):
        """A remote server with a vault-ref header is authenticated: its
        effective discovery-uses-secrets is on regardless of the stored flag, so
        toggling the stored flag does NOT churn the hash (the runtime behavior
        is identical)."""
        def _cfg(flag):
            return _make_config(
                servers=[
                    _user(
                        "notes",
                        transport="http",
                        url="https://example.test/mcp",
                        headers={"Authorization": "${vault:K}"},
                        discovery_uses_secrets=flag,
                    )
                ]
            )

        assert (
            _make_sandbox(_cfg(False))._compute_user_mcp_config_hash()
            == _make_sandbox(_cfg(True))._compute_user_mcp_config_hash()
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
        # client_codegen is folded in unconditionally (forces re-upload on a
        # codegen bump); user_mcp_config stays gated on user-server presence.
        assert set(source_versions.keys()) == {
            "mcp_servers",
            "tool_schemas",
            "client_codegen",
        }

    @pytest.mark.asyncio
    async def test_manifest_tool_modules_carries_codegen_version(self):
        """tool_modules.source_versions pins MCP_CLIENT_CODEGEN_VERSION, so a
        generator-only change (invisible to the input hashes) re-uploads the
        regenerated mcp_client.py to existing sandboxes on the next sync."""
        from ptc_agent.core.tool_generator import MCP_CLIENT_CODEGEN_VERSION

        config = _make_config(servers=[_builtin("yfinance")])
        sandbox = _make_sandbox(config)
        sandbox.mcp_registry = MagicMock()
        sandbox.mcp_registry.get_all_tools = MagicMock(return_value={})
        manifest = await sandbox._compute_sandbox_manifest()
        source_versions = manifest["modules"]["tool_modules"]["source_versions"]
        assert source_versions["client_codegen"] == MCP_CLIENT_CODEGEN_VERSION

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

    @pytest.mark.asyncio
    async def test_manifest_ships_shared_runtime_siblings_with_server_files(self):
        """When a builtin ``uv run python`` server file ships, the shared
        runtime siblings (_bootstrap.py, _envelope.py) are hashed with it —
        the servers import them, so an unsynced sibling crashes the server."""
        config = _make_config(
            servers=[
                _builtin(
                    "price",
                    transport="stdio",
                    command="uv",
                    args=["run", "python", "mcp_servers/price_data_mcp_server.py"],
                )
            ]
        )
        sandbox = _make_sandbox(config)
        sandbox.mcp_registry = MagicMock()
        sandbox.mcp_registry.get_all_tools = MagicMock(return_value={})
        manifest = await sandbox._compute_sandbox_manifest()
        mcp_files = manifest["modules"]["mcp_servers"]["files"]
        assert "price_data_mcp_server.py" in mcp_files
        assert "_bootstrap.py" in mcp_files
        assert "_envelope.py" in mcp_files

    @pytest.mark.asyncio
    async def test_manifest_omits_shared_siblings_without_server_files(self):
        """No builtin uv-run server file → no shared siblings either; the
        mcp_servers hash stays byte-identical for such workspaces."""
        config = _make_config(servers=[_builtin("yfinance")])
        sandbox = _make_sandbox(config)
        sandbox.mcp_registry = MagicMock()
        sandbox.mcp_registry.get_all_tools = MagicMock(return_value={})
        manifest = await sandbox._compute_sandbox_manifest()
        assert manifest["modules"]["mcp_servers"]["files"] == {}

    @pytest.mark.asyncio
    async def test_manifest_ships_internal_packages_with_data_seed(self):
        """The builtin MCP servers import ``src.data_client``/``src.market_protocol``
        at the sandbox boundary, so both are mirrored into ``_internal/src`` and
        hashed as one all-or-nothing manifest module — every file, data seeds
        included. An unsynced package crashes every server on import."""
        config = _make_config(servers=[_builtin("yfinance")])
        sandbox = _make_sandbox(config)
        sandbox.mcp_registry = MagicMock()
        sandbox.mcp_registry.get_all_tools = MagicMock(return_value={})
        manifest = await sandbox._compute_sandbox_manifest()
        files = manifest["modules"]["internal_packages"]["files"]
        assert "__init__.py" in files
        # Both packages present (any module file — names may be refactored)…
        assert any(f.startswith("data_client/") and f.endswith(".py") for f in files)
        assert any(f.startswith("market_protocol/") and f.endswith(".py") for f in files)
        # …and the load-bearing non-.py seed is hashed, so it can't drop silently.
        assert "market_protocol/instruments.yaml" in files
        assert manifest["modules"]["internal_packages"]["version"]


# ---------------------------------------------------------------------------
# Regression #3 — doc filename can't traverse out of the docs dir
# ---------------------------------------------------------------------------


class TestDocPathTraversal:
    """A hostile workspace tool name maps to a contained doc filename."""

    def _doc_path(self, work_dir, server_name, tool_name, source):
        # Mirrors the filename logic in PTCSandbox._install_tool_modules.
        from ptc_agent.core.mcp_sanitize import sanitize_tool_name

        if source == "workspace":
            doc_name = sanitize_tool_name(tool_name) or "_invalid_tool"
        else:
            doc_name = tool_name
        return f"{work_dir}/tools/docs/{server_name}/{doc_name}.md"

    def test_traversal_name_is_contained(self):
        work_dir = "/home/workspace"
        server = "user_srv"
        base = f"{work_dir}/tools/docs/{server}/"
        for hostile in ("../mcp_client", "../../_internal/.vault_secrets", "a/b", ".."):
            path = self._doc_path(work_dir, server, hostile, "workspace")
            assert path.startswith(base)
            # No traversal component or separator escapes the server's docs dir.
            assert ".." not in path[len(base):]
            assert "/" not in path[len(base):].removesuffix(".md")

    def test_builtin_doc_path_unchanged(self):
        # Builtin names are already valid identifiers ⇒ byte-identical path.
        work_dir = "/home/workspace"
        path = self._doc_path(work_dir, "market", "get_price", "builtin")
        assert path == "/home/workspace/tools/docs/market/get_price.md"


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
    async def test_pending_server_merged_into_discovery_client(self, discovery_sandbox):
        """On-demand discovery of a server the live session has not re-resolved
        yet (added/edited post-warm) regenerates the client INCLUDING it,
        without dropping the session's other servers (the /discover staleness
        fix)."""
        sandbox = discovery_sandbox
        captured: dict[str, list[str]] = {}

        def capture(servers, working_dir="/home/workspace"):
            captured["names"] = [s.name for s in servers]
            return "# client"

        sandbox.tool_generator.generate_mcp_client_code = MagicMock(side_effect=capture)
        sandbox.adownload_file_bytes = AsyncMock(return_value=None)

        # 'gamma' is NOT in the session config (alpha, beta) — a pending add.
        gamma = _user("gamma", transport="http", url="https://g.test")
        await sandbox.discover_user_mcp_schemas([gamma])

        assert "gamma" in captured["names"]  # pending server reaches the client
        assert {"alpha", "beta"}.issubset(
            set(captured["names"])
        )  # session servers not dropped

    @pytest.mark.asyncio
    async def test_discovery_client_path_unique_per_call(self, discovery_sandbox):
        """Each discovery call uploads its client to its own temp path — never
        the runtime ``tools/mcp_client.py`` — and removes it afterwards."""
        sandbox = discovery_sandbox
        uploaded: list[str] = []
        exec_cmds: list[str] = []

        async def fake_upload(data, path, **k):
            uploaded.append(path)

        async def fake_exec(cmd, **k):
            exec_cmds.append(cmd)
            return ExecResult("", "", 0)

        sandbox.runtime.upload_file = AsyncMock(side_effect=fake_upload)
        sandbox.runtime.exec = AsyncMock(side_effect=fake_exec)
        sandbox.adownload_file_bytes = AsyncMock(return_value=None)

        server = _user("alpha", transport="http", url="https://a.test")
        await sandbox.discover_user_mcp_schemas([server])
        await sandbox.discover_user_mcp_schemas([server])

        client_paths = [p for p in uploaded if p.endswith(".py")]
        assert len(client_paths) == 2
        assert len(set(client_paths)) == 2  # unique per call
        for path in client_paths:
            assert "/_internal/" in path
            assert not path.endswith("tools/mcp_client.py")
        # The discover exec targets the client uploaded by ITS OWN call, and
        # the client temp file is cleaned up afterwards.
        first_discover = next(c for c in exec_cmds if " discover " in c)
        assert client_paths[0] in first_discover
        # python3 explicitly: the no-snapshot image has no `python` alias.
        assert " python3 " in first_discover
        assert any(
            c.startswith("rm -f") and client_paths[0] in c for c in exec_cmds
        )

    @pytest.mark.asyncio
    async def test_concurrent_discoveries_do_not_clobber_each_other(
        self, discovery_sandbox
    ):
        """Regression: two concurrent discovery calls (bulk-import probe storm)
        must each exec against a client containing THEIR server. With a shared
        client path, the second upload clobbers the first and discovery reports
        a spurious ``unknown server``."""
        import asyncio
        import shlex

        sandbox = discovery_sandbox
        uploads: dict[str, str] = {}
        both_uploaded = asyncio.Event()

        def gen(servers, working_dir="/home/workspace"):
            return "# client " + ",".join(s.name for s in servers)

        sandbox.tool_generator.generate_mcp_client_code = MagicMock(side_effect=gen)

        async def fake_upload(data, path, **k):
            uploads[path] = data.decode()
            if len([p for p in uploads if p.endswith(".py")]) >= 2:
                both_uploaded.set()

        seen: dict[str, bool] = {}

        async def fake_exec(cmd, **k):
            if " discover " in cmd:
                # Hold every exec until BOTH calls uploaded their client, so a
                # shared-path implementation is guaranteed to be clobbered.
                await asyncio.wait_for(both_uploaded.wait(), timeout=5)
                tokens = shlex.split(cmd)
                client = next(t for t in tokens if t.endswith(".py"))
                name = tokens[tokens.index("discover") + 1]
                seen[name] = name in uploads.get(client, "")
            return ExecResult("", "", 0)

        sandbox.runtime.upload_file = AsyncMock(side_effect=fake_upload)
        sandbox.runtime.exec = AsyncMock(side_effect=fake_exec)
        sandbox.adownload_file_bytes = AsyncMock(return_value=None)

        await asyncio.gather(
            sandbox.discover_user_mcp_schemas(
                [_user("gamma", transport="http", url="https://g.test")]
            ),
            sandbox.discover_user_mcp_schemas(
                [_user("delta", transport="http", url="https://d.test")]
            ),
        )
        assert seen == {"gamma": True, "delta": True}

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
