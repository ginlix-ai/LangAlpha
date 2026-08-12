"""Tests for user (workspace) MCP server codegen in tool_generator.

Covers vault-only secret resolution (no os.environ fallback), per-server env
scoping, http/sse header injection, the discover() output shape, no-vault
discovery, and builtin byte-stability invariants.
"""

import ast
import json
import os

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from ptc_agent.core.tool_generator import ToolFunctionGenerator


def _exec_client(code: str) -> dict:
    """Compile + exec generated client source, returning its namespace."""
    ast.parse(code)  # must be valid Python
    ns: dict = {}
    exec(compile(code, "gen_mcp_client", "exec"), ns)  # noqa: S102 - testing generated code
    return ns


def _write_vault(tmp_path, secrets: dict) -> str:
    """Write a vault file under tmp_path/_internal and return the working dir."""
    internal = tmp_path / "_internal"
    internal.mkdir(parents=True, exist_ok=True)
    (internal / ".vault_secrets.json").write_text(json.dumps(secrets))
    return str(tmp_path)


class TestBuiltinByteStability:
    """Builtin-only codegen must not gain vault machinery."""

    def test_no_vault_refs_in_builtin_client(self):
        gen = ToolFunctionGenerator()
        servers = [
            MCPServerConfig(
                name="data_srv",
                transport="stdio",
                command="node",
                args=["srv.js"],
                env={"PLACEHOLDER_KEY": "x"},
            ),
            MCPServerConfig(
                name="remote_srv", transport="sse", url="https://example.test/mcp"
            ),
        ]
        code = gen.generate_mcp_client_code(servers)
        # No vault resolution helpers leak into builtin-only output.
        assert "_load_vault" not in code
        assert "_VAULT_SECRETS_FILE" not in code
        assert "_build_proc_env" not in code
        assert "_resolve_cmd_args" not in code
        assert "def discover(" not in code
        # The stdio cmd stays the byte-identical raw-args form.
        assert 'cmd = [config["command"]] + config["args"]' in code

    def test_builtin_stdio_uses_os_environ(self):
        gen = ToolFunctionGenerator()
        servers = [
            MCPServerConfig(
                name="data_srv", transport="stdio", command="node", args=["srv.js"]
            )
        ]
        code = gen.generate_mcp_client_code(servers)
        # Builtin env resolution still reads os.environ.
        assert "os.environ.copy()" in code
        assert 'for key in config.get("env_keys", []):' in code


class TestVaultOnlyResolution:
    """Workspace servers resolve secrets vault-only, no host-env fallback."""

    def test_no_os_environ_fallback_for_workspace_secret(self, tmp_path):
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "resolved-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["-y", "@scope/pkg"],
            env={
                "TOKEN": "${vault:USER_TOKEN}",
                # A bare ${VAR} naming a platform var must NOT resolve from host env.
                "LEAK": "${PLATFORM_TOKEN}",
                "LITERAL": "plain",
            },
            source="workspace",
        )
        code = gen.generate_mcp_client_code([server], working_dir=workdir)
        ns = _exec_client(code)

        os.environ["PLATFORM_TOKEN"] = "must-not-leak"
        try:
            env = ns["_build_proc_env"](ns["_SERVER_CONFIGS"]["user_srv"], "user_srv")
        finally:
            del os.environ["PLATFORM_TOKEN"]

        assert env["TOKEN"] == "resolved-secret"
        assert env["LITERAL"] == "plain"
        # Bare ${VAR} is left as an inert placeholder, never host-resolved.
        assert env["LEAK"] == "${PLATFORM_TOKEN}"
        assert "must-not-leak" not in json.dumps(env)

    def test_missing_secret_raises_naming_secret_not_value(self, tmp_path):
        workdir = _write_vault(tmp_path, {})  # empty vault
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            env={"TOKEN": "${vault:NEEDED_NAME}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        with pytest.raises(RuntimeError) as exc:
            ns["_build_proc_env"](ns["_SERVER_CONFIGS"]["user_srv"], "user_srv")
        assert "NEEDED_NAME" in str(exc.value)

    def test_args_vault_ref_resolved_at_spawn(self, tmp_path):
        # A ${vault:NAME} ref in args (e.g. from an imported `--api-key=...`)
        # resolves vault-only at spawn — never left as a literal placeholder.
        workdir = _write_vault(tmp_path, {"API_KEY": "resolved-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["-y", "@scope/pkg", "--api-key=${vault:API_KEY}"],
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        resolved = ns["_resolve_cmd_args"](ns["_SERVER_CONFIGS"]["user_srv"], "user_srv")
        assert resolved == ["-y", "@scope/pkg", "--api-key=resolved-secret"]

    def test_args_missing_secret_raises_naming_secret_not_value(self, tmp_path):
        workdir = _write_vault(tmp_path, {})  # empty vault
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["--token=${vault:NEEDED_ARG_SECRET}"],
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        with pytest.raises(RuntimeError) as exc:
            ns["_resolve_cmd_args"](ns["_SERVER_CONFIGS"]["user_srv"], "user_srv")
        assert "NEEDED_ARG_SECRET" in str(exc.value)

    def test_args_discovery_secretless_does_not_raise(self, tmp_path):
        # Secret-less discovery (default) must tolerate an unresolved arg ref —
        # it becomes an inert placeholder, never an exception on the probe path.
        workdir = _write_vault(tmp_path, {})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["--api-key=${vault:API_KEY}"],
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        out = ns["_resolve_cmd_args"](
            ns["_SERVER_CONFIGS"]["user_srv"], "user_srv", discovery=True
        )
        assert isinstance(out, list) and len(out) == 1


class TestPerServerScoping:
    """Workspace stdio env is minimal — never the full os.environ."""

    def test_env_scoped_to_declared_values(self, tmp_path):
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "s"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            env={"TOKEN": "${vault:USER_TOKEN}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))

        os.environ["SOME_UNRELATED_HOST_VAR"] = "secret-host-value"
        try:
            env = ns["_build_proc_env"](ns["_SERVER_CONFIGS"]["user_srv"], "user_srv")
        finally:
            del os.environ["SOME_UNRELATED_HOST_VAR"]

        # Full host env is not handed to the untrusted subprocess.
        assert "SOME_UNRELATED_HOST_VAR" not in env
        # Only declared + safe-base + PYTHONPATH keys present.
        declared = {
            k
            for k in env
            if k not in ("PATH", "HOME", "LANG", "LC_ALL", "PYTHONPATH")
        }
        assert declared == {"TOKEN"}


class TestHeaderInjection:
    """Workspace sse/http servers send vault-resolved headers."""

    def test_url_and_headers_resolved(self, tmp_path):
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "abc123"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_http",
            transport="http",
            url="https://example.test/${vault:USER_TOKEN}",
            headers={"Authorization": "Bearer ${vault:USER_TOKEN}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        url, headers = ns["_resolve_sse"](
            ns["_SERVER_CONFIGS"]["user_http"], "user_http"
        )
        assert url == "https://example.test/abc123"
        assert headers["Authorization"] == "Bearer abc123"


class TestNoVaultDiscovery:
    """Discovery tolerates a missing vault file (inert placeholders)."""

    def test_stdio_env_placeholder_when_no_vault(self, tmp_path):
        # No vault file written at all.
        workdir = str(tmp_path)
        (tmp_path / "_internal").mkdir()
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            env={"TOKEN": "${vault:USER_TOKEN}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        env = ns["_build_proc_env"](
            ns["_SERVER_CONFIGS"]["user_srv"], "user_srv", discovery=True
        )
        # Discovery substitutes inert empty string, never raises.
        assert env["TOKEN"] == ""

    def test_http_header_placeholder_when_no_vault(self, tmp_path):
        workdir = str(tmp_path)
        (tmp_path / "_internal").mkdir()
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_http",
            transport="http",
            url="https://example.test/mcp",
            headers={"Authorization": "Bearer ${vault:USER_TOKEN}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        _url, headers = ns["_resolve_sse"](
            ns["_SERVER_CONFIGS"]["user_http"], "user_http", discovery=True
        )
        assert headers["Authorization"] == "Bearer "


class TestDiscoveryUsesSecrets:
    """Per-server discovery_uses_secrets gates whether discovery resolves real
    secrets. Default (False) = secret-less probe even when the secret exists.
    """

    def test_default_off_discovery_ignores_present_stdio_secret(self, tmp_path):
        # Secret IS present in the vault, but discovery_uses_secrets defaults off.
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "real-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            env={"TOKEN": "${vault:USER_TOKEN}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        env = ns["_build_proc_env"](
            ns["_SERVER_CONFIGS"]["user_srv"], "user_srv", discovery=True
        )
        # Secret-less posture: the present secret is NOT resolved during discovery.
        assert env["TOKEN"] == ""
        assert "real-secret" not in json.dumps(env)
        # Normal (non-discovery) calls still resolve the real secret.
        env_call = ns["_build_proc_env"](
            ns["_SERVER_CONFIGS"]["user_srv"], "user_srv"
        )
        assert env_call["TOKEN"] == "real-secret"

    def test_opt_in_discovery_resolves_present_stdio_secret(self, tmp_path):
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "real-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            env={"TOKEN": "${vault:USER_TOKEN}"},
            source="workspace",
            discovery_uses_secrets=True,
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        env = ns["_build_proc_env"](
            ns["_SERVER_CONFIGS"]["user_srv"], "user_srv", discovery=True
        )
        # Explicit opt-in: discovery resolves the real secret (today's behavior).
        assert env["TOKEN"] == "real-secret"

    def test_default_flag_http_auth_header_resolves_during_discovery(self, tmp_path):
        # An authenticated remote server self-enables secret discovery even with
        # the default flag — otherwise tools/list returns 401 (see
        # discovery_should_use_secrets). Contrast the stdio default-off case.
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "real-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_http",
            transport="http",
            url="https://example.test/mcp",
            headers={"Authorization": "Bearer ${vault:USER_TOKEN}"},
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        _url, headers = ns["_resolve_sse"](
            ns["_SERVER_CONFIGS"]["user_http"], "user_http", discovery=True
        )
        assert headers["Authorization"] == "Bearer real-secret"
        # Normal call also resolves the real secret.
        _u2, headers2 = ns["_resolve_sse"](
            ns["_SERVER_CONFIGS"]["user_http"], "user_http"
        )
        assert headers2["Authorization"] == "Bearer real-secret"

    def test_opt_in_discovery_resolves_present_http_secret(self, tmp_path):
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "real-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_http",
            transport="http",
            url="https://example.test/mcp",
            headers={"Authorization": "Bearer ${vault:USER_TOKEN}"},
            source="workspace",
            discovery_uses_secrets=True,
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        _url, headers = ns["_resolve_sse"](
            ns["_SERVER_CONFIGS"]["user_http"], "user_http", discovery=True
        )
        assert headers["Authorization"] == "Bearer real-secret"

    def test_flag_embedded_in_workspace_config(self, tmp_path):
        workdir = _write_vault(tmp_path, {})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            source="workspace",
            discovery_uses_secrets=True,
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        assert ns["_SERVER_CONFIGS"]["user_srv"]["discovery_uses_secrets"] is True

    def test_remote_vault_header_auto_enables_discovery_secrets(self, tmp_path):
        """A workspace remote server whose header references a vault secret is
        authenticated, so the generated client resolves secrets during discovery
        even though the stored flag is the default (False)."""
        workdir = _write_vault(tmp_path, {"USER_TOKEN": "real-secret"})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_http",
            transport="http",
            url="https://example.test/mcp",
            headers={"Authorization": "Bearer ${vault:USER_TOKEN}"},
            source="workspace",
            # NOT set — defaults to False; the vault-ref header forces it on.
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        assert ns["_SERVER_CONFIGS"]["user_http"]["discovery_uses_secrets"] is True
        _url, headers = ns["_resolve_sse"](
            ns["_SERVER_CONFIGS"]["user_http"], "user_http", discovery=True
        )
        assert headers["Authorization"] == "Bearer real-secret"

    def test_builtin_only_client_omits_flag_byte_stable(self):
        """The new key only appears for workspace servers; a builtin-only config
        must not gain it (or any vault machinery)."""
        gen = ToolFunctionGenerator()
        servers = [
            MCPServerConfig(
                name="data_srv",
                transport="stdio",
                command="node",
                args=["srv.js"],
                env={"PLACEHOLDER_KEY": "x"},
            ),
            MCPServerConfig(
                name="remote_srv", transport="sse", url="https://example.test/mcp"
            ),
        ]
        code = gen.generate_mcp_client_code(servers)
        assert "discovery_uses_secrets" not in code


class TestDiscoverEntrypoint:
    """discover() shape + presence."""

    def test_discover_present_and_compiles_for_workspace(self, tmp_path):
        workdir = _write_vault(tmp_path, {})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            source="workspace",
        )
        code = gen.generate_mcp_client_code([server], working_dir=workdir)
        assert "def discover(" in code
        assert '__name__ == "__main__"' in code
        ns = _exec_client(code)
        # Unknown server returns the structured error shape, never raises.
        res = ns["discover"]("does_not_exist")
        assert res == {
            "server": "does_not_exist",
            "status": "error",
            "error": "unknown server",
            "tools": [],
        }


class TestGeneratedRegexMirrorsConstant:
    """The in-sandbox vault regex must match mcp_sanitize.VAULT_REF_RE."""

    def test_pattern_in_sync(self, tmp_path):
        workdir = _write_vault(tmp_path, {})
        gen = ToolFunctionGenerator()
        server = MCPServerConfig(
            name="user_srv",
            transport="stdio",
            command="npx",
            args=["x"],
            source="workspace",
        )
        ns = _exec_client(gen.generate_mcp_client_code([server], working_dir=workdir))
        assert ns["_VAULT_REF_RE"].pattern == VAULT_REF_RE.pattern


class TestWorkspaceToolTextSanitized:
    """Workspace tool text is sanitized in wrappers; builtins unchanged."""

    def test_workspace_docstring_neutralizes_breakout(self):
        from ptc_agent.core.mcp_registry import MCPToolInfo

        evil_desc = 'desc """ injected """ tail'
        tool = MCPToolInfo(
            name="probe",
            description=evil_desc,
            input_schema={"type": "object", "properties": {}},
            server_name="user_srv",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("user_srv", [tool], source="workspace")
        # The generated module must compile — the breakout is inert.
        ast.parse(module)

    def test_builtin_text_unchanged(self):
        from ptc_agent.core.mcp_registry import MCPToolInfo

        tool = MCPToolInfo(
            name="probe",
            description="A plain builtin description.",
            input_schema={"type": "object", "properties": {}},
            server_name="srv",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("srv", [tool])
        assert "A plain builtin description." in module


def _has_call(node) -> bool:
    """True if the AST subtree contains any function/attribute Call."""
    return any(isinstance(n, ast.Call) for n in ast.walk(node))


class TestToolNameInjection:
    """§1 — a hostile tool name can't escape the _call_mcp_tool string literal."""

    def test_hostile_tool_name_does_not_inject_code(self):
        from ptc_agent.core.mcp_registry import MCPToolInfo

        # A name crafted to break out of the f-string literal and run code.
        hostile = 'x", __import__("os").system("touch /tmp/pwned") and (arguments) #'
        tool = MCPToolInfo(
            name=hostile,
            description="probe",
            input_schema={"type": "object", "properties": {}},
            server_name="user_srv",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("user_srv", [tool], source="workspace")
        tree = ast.parse(module)  # must parse — the breakout is inert

        # The hostile name survives ONLY as inert data inside a string literal
        # passed to _call_mcp_tool; the only Call in any wrapper body is
        # _call_mcp_tool and no __import__/system Call was injected.
        funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
        assert funcs, "expected at least one generated wrapper"
        for fn in funcs:
            call_names = {
                getattr(c.func, "id", getattr(c.func, "attr", None))
                for c in ast.walk(fn)
                if isinstance(c, ast.Call)
            }
            # Only the wrapper's own calls survive (`_call_mcp_tool` + the
            # template's `arguments.items()` None-strip); no injected call.
            assert call_names <= {"_call_mcp_tool", "items"}
            # No __import__ name reference smuggled in.
            assert not any(
                isinstance(n, ast.Name) and n.id == "__import__"
                for n in ast.walk(fn)
            )

    def test_builtin_call_line_byte_identical(self):
        """Builtin codegen keeps the historical double-quoted literal."""
        from ptc_agent.core.mcp_registry import MCPToolInfo

        tool = MCPToolInfo(
            name="get_price",
            description="probe",
            input_schema={"type": "object", "properties": {}},
            server_name="market",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("market", [tool])
        assert '_call_mcp_tool("market", "get_price", arguments)' in module


class TestParamNameInjection:
    """§2 — a hostile param name can't inject code into the signature/arg dict."""

    def test_hostile_param_name_skipped_module_parses(self):
        from ptc_agent.core.mcp_registry import MCPToolInfo

        tool = MCPToolInfo(
            name="probe",
            description="probe",
            input_schema={
                "type": "object",
                "properties": {
                    # Hostile key that would break the signature / arg-dict.
                    'q): import os; os.system("x") #': {"type": "string"},
                    # A salvageable name survives, sanitized to an identifier.
                    "ok-name": {"type": "string"},
                },
                "required": [],
            },
            server_name="user_srv",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("user_srv", [tool], source="workspace")
        tree = ast.parse(module)  # must parse cleanly
        # The hostile key was sanitized to an identifier — no os.system Call and
        # no `import os` statement was injected. (The module legitimately
        # contains the template's `import json`.)
        assert not any(
            isinstance(n, ast.Attribute) and n.attr == "system"
            for n in ast.walk(tree)
        )
        imported = {
            alias.name
            for n in ast.walk(tree)
            if isinstance(n, ast.Import)
            for alias in n.names
        }
        assert "os" not in imported
        # The salvageable param survives under its sanitized identifier.
        assert "ok_name" in module

    def test_workspace_arg_dict_key_is_repr(self):
        from ptc_agent.core.mcp_registry import MCPToolInfo

        tool = MCPToolInfo(
            name="probe",
            description="probe",
            input_schema={
                "type": "object",
                "properties": {"sym": {"type": "string"}},
                "required": ["sym"],
            },
            server_name="user_srv",
        )
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("user_srv", [tool], source="workspace")
        ast.parse(module)
        # Key emitted via repr (single-quoted), value references the identifier.
        assert "'sym': sym," in module


class TestParamTypeInjection:
    """A hostile schema `type` can't terminate the generated docstring."""

    def _tool(self, hostile_type):
        from ptc_agent.core.mcp_registry import MCPToolInfo

        return MCPToolInfo(
            name="probe",
            description="probe",
            input_schema={
                "type": "object",
                "properties": {
                    "sym": {"type": hostile_type, "description": "a param"},
                },
                "required": ["sym"],
            },
            server_name="user_srv",
        )

    def test_hostile_param_type_does_not_break_docstring(self):
        hostile = '"""x", __import__("os").system("touch /tmp/pwned") #'
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._tool(hostile)], source="workspace"
        )
        tree = ast.parse(module)  # must parse — the breakout is inert
        assert not any(
            isinstance(n, ast.Name) and n.id == "__import__"
            for n in ast.walk(tree)
        )

    def test_non_str_param_type_coerced(self):
        # An unhashable `type` must neither crash codegen nor inject quotes.
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module(
            "user_srv", [self._tool({"evil": '"""'})], source="workspace"
        )
        ast.parse(module)
        # The closed type_map falls back to Any in the signature.
        assert "def probe(sym: Any)" in module

    def test_hostile_param_type_sanitized_in_docs(self):
        hostile = '"""x"""'
        gen = ToolFunctionGenerator()
        doc = gen.generate_tool_documentation(self._tool(hostile), source="workspace")
        assert '"""' not in doc

    def test_builtin_param_type_byte_stable(self):
        gen = ToolFunctionGenerator()
        module = gen.generate_tool_module("srv", [self._tool("string")])
        assert "sym (string) (required): a param" in module
