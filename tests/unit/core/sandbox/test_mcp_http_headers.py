"""Configured connector headers must not override protocol-owned MCP headers.

The config model's key regex allows hyphens, so nothing upstream stops a
workspace server entry from carrying ``MCP-Protocol-Version`` or a forged
``Mcp-Session-Id`` — and a plain-dict merge would even emit both casings of
the same name on the wire. The strip in ``_mcp_headers`` is the one guard.

The closing sections cover the other half of a configured header: its value.
``_resolve_http`` is where a stored vault secret becomes one, and storage keeps
a paste intact, so this is the sink that trims what came with the paste and
refuses what would split the request, by the same rule the host applies.
"""

import dataclasses
import json as _json
from types import SimpleNamespace

import httpx
import pytest

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


class TestReservedSetParity:
    """One row, three senders: the host probe, the egress relay and this
    runtime. They agree only if the set does, and this module is uploaded into
    the sandbox, so it cannot import the host's copy to stay in step."""

    def test_the_runtime_set_matches_the_hosts(self):
        from src.server.utils.egress_guard import RESERVED_HEADERS

        assert m._RESERVED_HEADERS == RESERVED_HEADERS

    def test_a_configured_framing_header_is_dropped_too(self):
        # ``Host`` is the one the sandbox runtime used to forward: httpx frames
        # the request, so a row supplying it addresses somewhere else.
        proto = {"mode": "modern", "version": "2026-07-28", "session_id": None}

        headers = m._mcp_headers(
            "tools/call",
            "get_quote",
            proto,
            {"Host": "elsewhere.invalid", "Content-Length": "0", "X-Api-Key": "k-123"},
        )

        assert not any(k.lower() in ("host", "content-length") for k in headers)
        assert headers["X-Api-Key"] == "k-123"


class TestResolvedHeaderValues:
    """A vault value becomes a header here, and the vault stores what was
    pasted: the trailing newline off a copied key rides along, and httpx would
    refuse the request with the resolved credential quoted in the error."""

    @staticmethod
    def _cfg_with_vault(monkeypatch, tmp_path, value, header="Authorization"):
        secrets = tmp_path / ".vault_secrets.json"
        # json.dumps is how the host hands the sandbox its secrets, and it is
        # lossless, so whatever storage kept arrives intact.
        secrets.write_text(_json.dumps({"K": value}))
        monkeypatch.setattr(m, "_VAULT_SECRETS_FILE", str(secrets))
        return m._normalize(
            "srv",
            {
                "transport": "http",
                "untrusted": True,
                "url": "https://mcp.invalid/rpc",
                "headers": {header: "Bearer ${vault:K}"},
            },
        )

    def test_a_pasted_trailing_newline_is_trimmed_before_it_is_sent(
        self, monkeypatch, tmp_path
    ):
        cfg = self._cfg_with_vault(monkeypatch, tmp_path, "sk-live\n")

        url, headers = m._resolve_http(cfg)

        assert url == "https://mcp.invalid/rpc"
        assert headers == {"Authorization": "Bearer sk-live"}

    def test_a_line_break_inside_the_value_names_the_header_and_not_the_secret(
        self, monkeypatch, tmp_path
    ):
        cfg = self._cfg_with_vault(
            monkeypatch, tmp_path, "sk-live\r\nX-Evil: 1", header="X-Api-Key"
        )

        with pytest.raises(RuntimeError) as caught:
            m._resolve_http(cfg)

        message = str(caught.value)
        assert "X-Api-Key" in message
        assert "sk-live" not in message
        assert "X-Evil" not in message

    def test_discovery_refuses_the_same_value_a_call_would(
        self, monkeypatch, tmp_path
    ):
        # Discovery resolves secrets too when a server needs auth to list, so
        # the refusal has to bite there rather than surface as a probe timeout.
        cfg = dataclasses.replace(
            self._cfg_with_vault(monkeypatch, tmp_path, "one\ntwo"),
            discovery_uses_secrets=True,
        )

        with pytest.raises(RuntimeError):
            m._resolve_http(cfg, discovery=True)


class TestHeaderValueRuleParity:
    """The host applies this rule before it probes or relays a row; the sandbox
    applies it before it dials one directly. A value that reaches the wire on
    one path and is refused on the other makes the same server read two ways,
    and this module cannot import the host's copy to stay in step."""

    @pytest.mark.parametrize(
        "value",
        [
            "sk-live",
            "sk-live\n",
            "sk-live\r\n",
            "sk-live  ",
            "one\ntwo",
            "one\r\ntwo",
            "one\rtwo",
            "\nsk-live",
            "-----BEGIN KEY-----\nabc\n-----END KEY-----",
        ],
        ids=[
            "clean", "lf", "crlf", "trailing-spaces", "embedded-lf",
            "embedded-crlf", "embedded-cr", "leading-lf", "pem",
        ],
    )
    def test_both_sides_accept_strip_or_refuse_the_same_values(
        self, monkeypatch, tmp_path, value
    ):
        from src.server.services.mcp_oauth import discovery

        try:
            host, _ = discovery.resolve_header_refs(
                {"X-Api-Key": "Bearer ${vault:K}"}, {"K": value}
            )
            host_outcome = host["X-Api-Key"]
        except discovery.RejectedHeaderValue as e:
            host_outcome = ("refused", e.header)

        cfg = TestResolvedHeaderValues._cfg_with_vault(
            monkeypatch, tmp_path, value, header="X-Api-Key"
        )
        try:
            _, sandbox = m._resolve_http(cfg)
            sandbox_outcome = sandbox["X-Api-Key"]
        except RuntimeError:
            sandbox_outcome = ("refused", "X-Api-Key")

        assert sandbox_outcome == host_outcome
