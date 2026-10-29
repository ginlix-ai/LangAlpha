"""The header half of the egress pin: what a caller's map may still carry.

A configured MCP row supplies its own headers, and two of them belong to
nobody but the transport. A dict is case-sensitive where HTTP is not, so the
rule can only hold if it is applied case-folded -- a row spelling ``host`` in
lowercase is the shape that put two authorities on one request.
"""

from __future__ import annotations

from src.server.utils.egress_guard import (
    MCP_PROTOCOL_HEADERS,
    RESERVED_HEADERS,
    RESERVED_REQUEST_HEADERS,
    PinnedTarget,
    strip_configured_headers,
    strip_reserved_headers,
)


def _target() -> PinnedTarget:
    return PinnedTarget(
        url="https://93.184.216.34/mcp",
        host="api.example.com",
        ip="93.184.216.34",
        authority="api.example.com",
    )


def test_the_framing_headers_go_whatever_their_spelling():
    sent = strip_reserved_headers(
        {
            "Host": "elsewhere.example.com",
            "transfer-encoding": "chunked",
            "CONNECTION": "keep-alive",
        }
    )

    assert sent == {}


def test_the_caller_keeps_its_own_headers():
    sent = strip_reserved_headers(
        {"Content-Type": "application/json", "mcp-name": "fuyao_meta"}
    )

    assert sent == {"Content-Type": "application/json", "mcp-name": "fuyao_meta"}


def test_a_lowercase_host_does_not_survive_beside_the_pin():
    """Two Host keys in one dict is two authorities on the wire, and the one
    the row wrote is the one the pin exists to overrule."""
    _url, headers, _extensions = _target().pinned_kwargs({"host": "attacker.example"})

    assert [k for k in headers if k.lower() == "host"] == ["Host"]
    assert headers["Host"] == "api.example.com"


def test_the_pin_travels_as_all_three_parts():
    url, headers, extensions = _target().pinned_kwargs({"Accept": "text/event-stream"})

    assert url == "https://93.184.216.34/mcp"
    assert headers["Accept"] == "text/event-stream"
    assert extensions == {"sni_hostname": "api.example.com"}


def test_the_configured_strip_also_drops_the_protocol_names():
    """A row's map is merged into a request somebody else framed, so the names
    that request negotiated are not the row's to supply."""
    sent = strip_configured_headers(
        {
            "MCP-Protocol-Version": "1999-01-01",
            "Mcp-Session-Id": "forged",
            "mcp-name": "place_order",
            "Host": "elsewhere.example.com",
            "X-Api-Key": "k-123",
        }
    )

    assert sent == {"X-Api-Key": "k-123"}


def test_the_transport_strip_keeps_what_the_transport_may_send():
    """The two strips are not interchangeable: the preflight's own protocol
    version and the DELETE's session id travel through the narrow one."""
    sent = strip_reserved_headers(
        {"MCP-Protocol-Version": "2025-06-18", "mcp-session-id": "s-live"}
    )

    assert sent == {"MCP-Protocol-Version": "2025-06-18", "mcp-session-id": "s-live"}


def test_the_configured_set_is_the_union():
    assert RESERVED_HEADERS == RESERVED_REQUEST_HEADERS | MCP_PROTOCOL_HEADERS
    assert all(name == name.lower() for name in RESERVED_HEADERS)
