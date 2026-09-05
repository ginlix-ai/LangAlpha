"""Configured connector headers must not override protocol-owned MCP headers.

The config model's key regex allows hyphens, so nothing upstream stops a
workspace server entry from carrying ``MCP-Protocol-Version`` or a forged
``Mcp-Session-Id`` — and a plain-dict merge would even emit both casings of
the same name on the wire. The strip in ``_mcp_headers`` is the one guard.
"""

import json as _json
from types import SimpleNamespace

import httpx

from ptc_agent.core.sandbox import mcp_client_runtime as m


class TestReservedHeaderStrip:
    def test_modern_protocol_headers_win_over_configured_ones(self):
        proto = {"mode": "modern", "version": "2026-07-28", "session_id": None}

        headers = m._mcp_headers(
            "tools/call",
            "get_quote",
            proto,
            {
                "MCP-Protocol-Version": "1999-01-01",
                "mcp-method": "tools/other",
                "Mcp-Name": "spoofed",
                "Mcp-Session-Id": "forged",
                "X-Api-Key": "k-123",
            },
        )

        assert headers["MCP-Protocol-Version"] == "2026-07-28"
        assert headers["Mcp-Method"] == "tools/call"
        assert headers["Mcp-Name"] == "get_quote"
        # No alternate-casing duplicate survives to be sent alongside.
        assert "mcp-method" not in headers
        # Modern mode has no session; a configured one must not invent it.
        assert not any(k.lower() == "mcp-session-id" for k in headers)
        assert headers["X-Api-Key"] == "k-123"

    def test_legacy_session_id_cannot_be_forged(self):
        proto = {"mode": "legacy", "version": "2025-11-25", "session_id": "s-live"}

        headers = m._mcp_headers(
            "tools/call", "", proto, {"mcp-session-id": "forged"}
        )

        assert headers["Mcp-Session-Id"] == "s-live"
        assert "mcp-session-id" not in headers

    def test_non_reserved_headers_still_apply_last(self):
        # The override lane stays open for everything the protocol does not
        # own — Accept included, for servers with quirky content negotiation.
        proto = {"mode": "modern", "version": "2026-07-28", "session_id": None}

        headers = m._mcp_headers(
            "tools/list", "", proto, {"Accept": "application/json"}
        )

        assert headers["Accept"] == "application/json"


class TestProbeHeaderStrip:
    """The discover probe was the one request built outside ``_mcp_headers``:
    a configured reserved header reached the wire only there, silently
    desyncing negotiation. Pin that the probe now goes through the filter."""

    def test_probe_strips_reserved_configured_headers(self, monkeypatch):
        captured: list[dict] = []

        class _Resp:
            status_code = 200
            headers = {"content-type": "application/json"}

            def __init__(self, body: bytes):
                self._body = body

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def iter_bytes(self):
                yield self._body

            def raise_for_status(self):
                pass

        class _Client:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def stream(self, method, url, *, json=None, headers=None):
                captured.append(dict(headers or {}))
                body = _json.dumps({
                    "jsonrpc": "2.0",
                    "id": json["id"],
                    "result": {"supportedVersions": [m._MODERN_VERSIONS[0]]},
                }).encode()
                return _Resp(body)

        monkeypatch.setattr(
            m,
            "_SERVER_CONFIGS",
            {"srv": m._normalize("srv", {"transport": "http", "untrusted": False, "url": "https://mcp.invalid/rpc"})},
        )
        monkeypatch.setattr(m, "_PROTO", {})
        monkeypatch.setattr(
            m,
            "_resolve_http",
            lambda cfg, discovery=False: (
                "https://mcp.invalid/rpc",
                {
                    "Mcp-Session-Id": "forged",
                    "mcp-protocol-version": "1999-01-01",
                    "X-Api-Key": "k-123",
                },
            ),
        )
        monkeypatch.setattr(
            m, "httpx", SimpleNamespace(Client=lambda **kw: _Client(), HTTPError=httpx.HTTPError)
        )

        assert m._ensure_http_server("srv")["mode"] == "modern"

        probe = captured[0]
        assert probe["Mcp-Method"] == "server/discover"
        assert probe["MCP-Protocol-Version"] == m._MODERN_VERSIONS[0]
        assert "mcp-protocol-version" not in probe  # no alternate-casing dup
        assert not any(k.lower() == "mcp-session-id" for k in probe)
        assert probe["X-Api-Key"] == "k-123"
        assert "Mcp-Name" not in probe  # probe has no tool name to send
