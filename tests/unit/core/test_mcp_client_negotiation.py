"""Failure-injection tests for the generated sandbox MCP client.

Each test generates the real client code, execs it, and points it at a scripted
stdio server that misbehaves in one specific way. This is the negotiation state
machine's contract: every fallback edge (crash, silence, era latch, -32022
retry) must land on a working era or a clear error — never a hang.
"""

import json
import subprocess
import sys
import threading
import time

import pytest

from ptc_agent.config.core import MCPServerConfig
from ptc_agent.core.tool_generator import ToolFunctionGenerator

# Line-oriented JSON-RPC stdio server, parameterized by mode. State that must
# survive a client-initiated restart (spawn counts, first-spawn behavior) lives
# in files under the state dir.
_FAKE_SERVER = '''
import json, os, sys, time

MODE = sys.argv[1]
STATE = sys.argv[2]

def send(obj):
    sys.stdout.write(json.dumps(obj) + "\\n")
    sys.stdout.flush()

def record(name, text):
    with open(os.path.join(STATE, name), "a") as f:
        f.write(text + "\\n")

first_spawn = not os.path.exists(os.path.join(STATE, "spawns"))
record("spawns", "spawn")

if MODE == "import_crash":
    sys.stderr.write(
        "Traceback (most recent call last):\\n"
        "  File \\"server.py\\", line 1, in <module>\\n"
        "ModuleNotFoundError: No module named 'mcp.server.fastmcp'\\n"
    )
    sys.stderr.flush()
    sys.exit(1)

MODERN = {"supportedVersions": ["2026-07-28"], "resultType": "complete"}
seen_discover = False

def has_meta(msg):
    meta = (msg.get("params") or {}).get("_meta") or {}
    return ("io.modelcontextprotocol/protocolVersion" in meta
            and "io.modelcontextprotocol/clientCapabilities" in meta)

while True:
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    msg = json.loads(line)
    method = msg.get("method")
    mid = msg.get("id")
    if method is None:
        record("client_replies.jsonl", json.dumps(msg))
        continue
    if method == "notifications/initialized":
        continue
    if method == "server/discover":
        if not has_meta(msg):
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32602, "message": "missing _meta"}})
            continue
        if MODE == "legacy_polite":
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32601, "message": "Method not found"}})
        elif MODE == "crash_on_unknown":
            if first_spawn:
                sys.exit(1)
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32601, "message": "Method not found"}})
        elif MODE == "silent_probe":
            if first_spawn:
                time.sleep(60)
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32601, "message": "Method not found"}})
        elif MODE == "era_latch":
            seen_discover = True
            send({"jsonrpc": "2.0", "id": mid,
                  "result": {"supportedVersions": ["2099-01-01"],
                             "resultType": "complete"}})
        elif MODE == "retry_32022":
            if not seen_discover:
                seen_discover = True
                send({"jsonrpc": "2.0", "id": mid,
                      "error": {"code": -32022, "message": "unsupported version",
                                "data": {"supportedVersions": ["2026-07-28"]}}})
            else:
                send({"jsonrpc": "2.0", "id": mid, "result": MODERN})
        else:
            seen_discover = True
            send({"jsonrpc": "2.0", "id": mid, "result": MODERN})
        continue
    if method == "initialize":
        if MODE == "era_latch" and seen_discover:
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32022,
                            "message": "initialize handshake is not accepted"}})
            continue
        offered = (msg.get("params") or {}).get("protocolVersion")
        record("offered.txt", str(offered))
        send({"jsonrpc": "2.0", "id": mid,
              "result": {"protocolVersion": "2025-03-26", "capabilities": {},
                         "serverInfo": {"name": "fake", "version": "0"}}})
        continue
    if method == "tools/list":
        send({"jsonrpc": "2.0", "id": mid,
              "result": {"tools": [{"name": "t", "description": "d",
                                    "inputSchema": {"type": "object"}}]}})
        continue
    if method == "tools/call":
        if MODE in ("modern", "modern_noise", "retry_32022", "input_required",
                    "wrapped_result") and not has_meta(msg):
            send({"jsonrpc": "2.0", "id": mid,
                  "error": {"code": -32602, "message": "missing _meta on call"}})
            continue
        base = {"content": [{"type": "text", "text": json.dumps({"data": "text"})}],
                "isError": False, "resultType": "complete"}
        if MODE in ("modern", "retry_32022"):
            base["structuredContent"] = {"data": "structured"}
            send({"jsonrpc": "2.0", "id": mid, "result": base})
        elif MODE == "modern_noise":
            send({"jsonrpc": "2.0", "method": "notifications/progress",
                  "params": {}})
            noise = dict(base)
            noise["structuredContent"] = {"data": "stale"}
            send({"jsonrpc": "2.0", "id": 999999, "result": noise})
            send({"jsonrpc": "2.0", "id": 424242, "method": "roots/list",
                  "params": {}})
            real = dict(base)
            real["structuredContent"] = {"data": "real"}
            send({"jsonrpc": "2.0", "id": mid, "result": real})
        elif MODE == "input_required":
            base["structuredContent"] = {"question": "?"}
            base["resultType"] = "input_required"
            send({"jsonrpc": "2.0", "id": mid, "result": base})
        elif MODE == "wrapped_result":
            base["structuredContent"] = {"result": [1, 2, 3]}
            send({"jsonrpc": "2.0", "id": mid, "result": base})
        else:
            send({"jsonrpc": "2.0", "id": mid,
                  "result": {"content": [{"type": "text",
                                          "text": json.dumps({"ok": True})}],
                             "isError": False}})
        continue
    send({"jsonrpc": "2.0", "id": mid,
          "error": {"code": -32601, "message": "Method not found"}})
'''


def _client_ns(tmp_path, mode, transport="stdio"):
    """Generate the real client for a scripted server and exec it."""
    server_py = tmp_path / "fake_server.py"
    server_py.write_text(_FAKE_SERVER)
    state = tmp_path / f"state_{mode}"
    state.mkdir()
    config = MCPServerConfig(
        name="fake",
        transport=transport,
        url="http://127.0.0.1:1/mcp" if transport != "stdio" else None,
        command=sys.executable,
        args=[str(server_py), mode, str(state)],
        env={},
    )
    code = ToolFunctionGenerator().generate_mcp_client_code(
        [config], working_dir=str(tmp_path)
    )
    ns = {"__name__": "mcp_client_under_test"}
    exec(compile(code, "mcp_client.py", "exec"), ns)  # noqa: S102
    ns["_PROBE_TIMEOUT"] = 1.0  # keep silence/timeout scenarios fast
    return ns, state


def _spawn_count(state):
    return len((state / "spawns").read_text().splitlines())


class TestNegotiation:
    def test_modern_server_negotiates_modern(self, tmp_path):
        ns, state = _client_ns(tmp_path, "modern")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"data": "structured"}  # structuredContent preferred
        assert ns["_PROTO"]["fake"] == {
            "mode": "modern", "version": "2026-07-28", "session_id": None,
            # The 2026-era identity stamp is optional and this server omits it.
            "server_info": None,
        }
        assert _spawn_count(state) == 1
        ns["cleanup_mcp_servers"]()

    def test_polite_method_error_falls_back_same_stream(self, tmp_path):
        ns, state = _client_ns(tmp_path, "legacy_polite")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"ok": True}
        proto = ns["_PROTO"]["fake"]
        assert proto["mode"] == "legacy"
        assert proto["version"] == "2025-03-26"  # adopted from the server
        assert _spawn_count(state) == 1  # same stream, no restart
        # the fallback offered the newest legacy revision, not 2024-11-05
        assert (state / "offered.txt").read_text().strip() == "2025-11-25"
        ns["cleanup_mcp_servers"]()

    def test_crash_on_unknown_method_restarts_once(self, tmp_path):
        ns, state = _client_ns(tmp_path, "crash_on_unknown")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"ok": True}
        assert ns["_PROTO"]["fake"]["mode"] == "legacy"
        assert _spawn_count(state) == 2
        ns["cleanup_mcp_servers"]()

    def test_import_crash_reports_cause_and_isolation_hint(self, tmp_path):
        """A server that dies on import (era-locked runtime) must surface the
        child's stderr cause and the uvx/npx isolation hint, not a bare
        "closed connection"."""
        ns, state = _client_ns(tmp_path, "import_crash")
        with pytest.raises(RuntimeError) as exc_info:
            ns["_call_mcp_tool"]("fake", "t", {})
        message = str(exc_info.value)
        assert "No module named 'mcp.server.fastmcp'" in message
        assert "uvx/npx" in message
        assert _spawn_count(state) == 2  # probe EOF still restarts once
        ns["cleanup_mcp_servers"]()

    def test_silent_probe_restarts_once(self, tmp_path):
        ns, state = _client_ns(tmp_path, "silent_probe")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"ok": True}
        assert ns["_PROTO"]["fake"]["mode"] == "legacy"
        assert _spawn_count(state) == 2
        ns["cleanup_mcp_servers"]()

    def test_era_latch_forces_fresh_stream_for_legacy(self, tmp_path):
        """Discover succeeded but advertised no mutual modern version: the
        connection is era-latched, so initialize MUST go to a fresh process
        (same-stream would get -32022 and the whole negotiation would die)."""
        ns, state = _client_ns(tmp_path, "era_latch")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"ok": True}
        assert ns["_PROTO"]["fake"]["mode"] == "legacy"
        assert _spawn_count(state) == 2
        ns["cleanup_mcp_servers"]()

    def test_32022_with_mutual_version_retries_discover_once(self, tmp_path):
        ns, state = _client_ns(tmp_path, "retry_32022")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"data": "structured"}
        assert ns["_PROTO"]["fake"]["mode"] == "modern"
        assert _spawn_count(state) == 1
        ns["cleanup_mcp_servers"]()

    def test_two_thread_cold_start_spawns_once(self, tmp_path):
        ns, state = _client_ns(tmp_path, "modern")
        results, errors = [], []

        def call():
            try:
                results.append(ns["_call_mcp_tool"]("fake", "t", {}))
            except Exception as e:  # noqa: BLE001 - collected for assertion
                errors.append(e)

        threads = [threading.Thread(target=call) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not errors
        assert results == [{"data": "structured"}] * 2
        assert _spawn_count(state) == 1  # one spawn + one negotiation total
        ns["cleanup_mcp_servers"]()


class TestReplyHandling:
    def test_noise_skipped_and_server_request_refused(self, tmp_path):
        """Notifications and stale-id replies are skipped; a server-initiated
        request gets a -32601 refusal; the id-matched reply is returned."""
        ns, state = _client_ns(tmp_path, "modern_noise")
        result = ns["_call_mcp_tool"]("fake", "t", {})
        assert result == {"data": "real"}
        deadline = time.monotonic() + 5
        replies_file = state / "client_replies.jsonl"
        refusals = []
        while time.monotonic() < deadline and not refusals:
            if replies_file.exists():
                lines = replies_file.read_text().splitlines()
                try:
                    refusals = [json.loads(x) for x in lines]
                except json.JSONDecodeError:
                    refusals = []  # caught the file mid-write
            if not refusals:
                time.sleep(0.05)
        assert refusals[0]["id"] == 424242
        assert refusals[0]["error"]["code"] == -32601
        ns["cleanup_mcp_servers"]()

    def test_input_required_raises_clearly(self, tmp_path):
        ns, _ = _client_ns(tmp_path, "input_required")
        with pytest.raises(RuntimeError, match="input_required"):
            ns["_call_mcp_tool"]("fake", "t", {})
        ns["cleanup_mcp_servers"]()

    def test_single_result_key_wrapper_unwrapped(self, tmp_path):
        ns, _ = _client_ns(tmp_path, "wrapped_result")
        assert ns["_call_mcp_tool"]("fake", "t", {}) == [1, 2, 3]
        ns["cleanup_mcp_servers"]()


class TestTransportRouting:
    def test_legacy_sse_transport_rejected(self, tmp_path):
        ns, _ = _client_ns(tmp_path, "modern", transport="sse")
        with pytest.raises(RuntimeError, match="'http'"):
            ns["_call_mcp_tool"]("fake", "t", {})

    def test_legacy_sse_discovery_refuses_too(self, tmp_path):
        # Discovery used to POST an sse server like streamable HTTP, so a
        # connector saved as "connected" and then failed every call mid-turn.
        ns, _ = _client_ns(tmp_path, "modern", transport="sse")
        result = ns["discover"]("fake")
        assert result["status"] == "error"
        assert "'http'" in result["error"]
        assert result["tools"] == []


class TestCliDiscover:
    def test_cli_discover_runs_against_the_applied_config(self, tmp_path):
        # Regression: the CLI dispatch must run from the generated epilogue,
        # AFTER _apply_config_dict. A guard inside the runtime source would
        # dispatch against the placeholder config and report "unknown server"
        # for every configured server. Only a real subprocess (__main__)
        # exercises that ordering — the exec-based tests above cannot.
        server_py = tmp_path / "fake_server.py"
        server_py.write_text(_FAKE_SERVER)
        state = tmp_path / "state_cli"
        state.mkdir()
        config = MCPServerConfig(
            name="fake",
            transport="stdio",
            command=sys.executable,
            args=[str(server_py), "modern", str(state)],
            env={},
        )
        code = ToolFunctionGenerator().generate_mcp_client_code(
            [config], working_dir=str(tmp_path)
        )
        client = tmp_path / "mcp_client.py"
        client.write_text(code)
        out = tmp_path / "out.json"
        proc = subprocess.run(
            [sys.executable, str(client), "discover", "fake", str(out)],
            capture_output=True,
            timeout=60,
        )
        assert proc.returncode == 0, proc.stderr.decode()
        result = json.loads(out.read_text())
        assert result["status"] == "ok", result
        assert [t["name"] for t in result["tools"]] == ["t"]

    def test_cli_discover_unknown_name_still_reports_it(self, tmp_path):
        code = ToolFunctionGenerator().generate_mcp_client_code(
            [], working_dir=str(tmp_path)
        )
        client = tmp_path / "mcp_client.py"
        client.write_text(code)
        out = tmp_path / "out.json"
        proc = subprocess.run(
            [sys.executable, str(client), "discover", "ghost", str(out)],
            capture_output=True,
            timeout=60,
        )
        assert proc.returncode == 0, proc.stderr.decode()
        result = json.loads(out.read_text())
        assert result == {"server": "ghost", "status": "error",
                          "error": "unknown server", "tools": []}
