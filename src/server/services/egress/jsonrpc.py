"""Canonical JSON-RPC reserialization for the egress relay.

The relay's tool policy (Part 2) is enforced on the request body, so the body
we forward must be exactly the body we inspected: parse strictly, reject every
shape a smuggling attempt could hide in (batches, duplicate keys, non-object
frames), and re-serialize compactly. The vendor never sees the client's raw
bytes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

__all__ = ["JsonRpcRejected", "CanonicalRequest", "canonicalize_request"]

MAX_BODY_BYTES = 256 * 1024


class JsonRpcRejected(ValueError):
    """The request body failed strict JSON-RPC canonicalization."""


@dataclass(frozen=True)
class CanonicalRequest:
    body: bytes
    method: str
    tool_name: str | None  # params.name when method == "tools/call"
    is_notification: bool


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    obj: dict[str, object] = {}
    for key, value in pairs:
        if key in obj:
            raise JsonRpcRejected(f"duplicate key {key!r} in JSON-RPC body")
        obj[key] = value
    return obj


def canonicalize_request(
    raw: bytes,
    *,
    max_bytes: int = MAX_BODY_BYTES,
) -> CanonicalRequest:
    """Parse ``raw`` strictly and return the canonical bytes to forward."""
    if len(raw) > max_bytes:
        raise JsonRpcRejected(f"JSON-RPC body exceeds {max_bytes} bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise JsonRpcRejected("JSON-RPC body is not valid UTF-8") from exc
    try:
        parsed = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except JsonRpcRejected:
        raise
    except json.JSONDecodeError as exc:
        raise JsonRpcRejected(f"JSON-RPC body is not valid JSON: {exc.msg}") from exc

    if isinstance(parsed, list):
        raise JsonRpcRejected("JSON-RPC batch requests are not allowed")
    if not isinstance(parsed, dict):
        raise JsonRpcRejected("JSON-RPC body must be a single object")
    if parsed.get("jsonrpc") != "2.0":
        raise JsonRpcRejected('JSON-RPC body must declare "jsonrpc": "2.0"')

    method = parsed.get("method")
    if not isinstance(method, str) or not method:
        raise JsonRpcRejected("JSON-RPC body must carry a string method")

    tool_name: str | None = None
    if method == "tools/call":
        params = parsed.get("params")
        if not isinstance(params, dict):
            raise JsonRpcRejected("tools/call requires an object params")
        name = params.get("name")
        if not isinstance(name, str) or not name:
            raise JsonRpcRejected("tools/call requires a string params.name")
        tool_name = name

    body = json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return CanonicalRequest(
        body=body,
        method=method,
        tool_name=tool_name,
        is_notification="id" not in parsed,
    )
