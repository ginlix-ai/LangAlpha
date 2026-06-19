"""Shared provenance types for tracking data the agent accessed.

Defines the wire/row shape for a single accessed source plus pure helpers to
fingerprint a tool result and build the custom ``provenance`` stream event. The
fingerprint logic is mirrored in-sandbox by the generated MCP client, so any
change here must stay deterministic and byte-for-byte reproducible.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

# Canonical snippet cap. Single source of truth: imported by the host-side
# ProvenanceMiddleware and interpolated into the in-sandbox MCP client template
# (tool_generator) so host and sandbox truncate identically — equal snippets are
# required for cross-surface dedup.
SNIPPET_MAX_CHARS = 500


@dataclass
class ProvenanceSource:
    """One piece of external data the agent read (URL, file, symbol, MCP call).

    ``agent`` defaults to None on purpose: the streaming handler resolves agent
    attribution (``main`` vs ``task:{id}``) from the LangGraph namespace, so
    producers must not hardcode it.
    """

    record_id: str
    source_type: str  # web_search|web_fetch|file_read|memo_read|memory_read|sec_filing|market_data|mcp_tool
    identifier: str  # url | file_path | symbol | "server:tool"
    timestamp: str  # ISO 8601
    title: str | None = None
    # Kind of data within this source type, as a stable slug the UI i18n-maps
    # (e.g. "company_overview", "daily_prices"). Lets the Sources panel group by
    # identifier yet still distinguish, in a hover, the several data products a
    # single ticker was accessed through. None when there's nothing to add.
    detail: str | None = None
    provider: str | None = None  # tavily|edgar|market_data_proxy|"mcp:{server}"
    tool_call_id: str | None = None
    args_fingerprint: dict | None = None
    # Tool-call args with secrets filtered out (deny-list); reaches shared chats.
    args: dict | None = None
    result_sha256: str | None = None
    result_size: int | None = None
    result_snippet: str | None = None
    agent: str | None = None


def _canonicalize(value: object) -> str:
    """Stable string form of a value for hashing/snippeting.

    dict/list go through ``json.dumps(sort_keys=True)`` so key order never
    changes the hash; everything else is ``str()``. Never raises — falls back to
    ``str(value)`` on any serialization error.
    """
    try:
        if isinstance(value, (dict, list)):
            return json.dumps(
                value, sort_keys=True, default=str, ensure_ascii=False
            )
        return str(value)
    except Exception:
        try:
            return str(value)
        except Exception:
            return ""


def fingerprint_result(value: object) -> tuple[str, int, str]:
    """Fingerprint a tool result as ``(sha256_hex, utf8_byte_size, snippet)``.

    Deterministic for equal inputs (dict key order is normalized) and safe on
    odd inputs (None, bytes, nested objects). The snippet is the first 500
    characters of the canonical string, truncated on a char boundary; it may
    contain NUL — downstream Postgres sanitization strips that, not this.
    """
    canonical = _canonicalize(value)
    encoded = canonical.encode("utf-8")
    sha256_hex = hashlib.sha256(encoded).hexdigest()
    size = len(encoded)
    snippet = canonical[:SNIPPET_MAX_CHARS]
    return sha256_hex, size, snippet


def hash_args(args: object) -> dict | None:
    """Reduce raw tool-call args to a non-reversible fingerprint.

    Args are LLM-authored and may carry secrets/PII (e.g. a vault value passed
    as a parameter), so we never persist them verbatim. Returns ``{"sha256":
    hex}`` over the canonical form, or None when there are no args.
    """
    if not args:
        return None
    digest = hashlib.sha256(_canonicalize(args).encode("utf-8")).hexdigest()
    return {"sha256": digest}


_REDACTED = "[redacted]"
_MAX_ARG_STR = 256      # clamp any string value
_MAX_ARG_KEYS = 32      # cap dict breadth
_MAX_ARG_ITEMS = 16     # cap list length
_MAX_ARG_DEPTH = 4      # cap recursion depth

# Normalized (separators stripped) key names that mark a value secret. Exact set
# is for short/ambiguous tokens; the parts set matches as a substring anywhere.
_SECRET_KEY_EXACT = frozenset({
    "auth", "authorization", "token", "secret", "password", "passwd", "pwd",
    "passphrase", "credential", "credentials", "cred", "creds", "apikey",
    "accesskey", "secretkey", "privatekey", "clientsecret", "session",
    "cookie", "signature", "sig", "otp", "pin", "key",
})
_SECRET_KEY_PARTS = (
    "token", "secret", "password", "passwd", "passphrase", "apikey",
    "accesskey", "privatekey", "clientsecret", "authorization", "bearer",
    "credential", "cookie",
)
# Values that look like a credential even under an innocent key.
_SECRET_VALUE_RES = (
    re.compile(r"^Bearer\s+\S+", re.I),            # Bearer xxxxx
    re.compile(r"^eyJ[\w-]+\.[\w-]+\.[\w-]+$"),     # JWT
    re.compile(r"^(sk|pk|rk)-[A-Za-z0-9]{16,}"),    # OpenAI-style keys
    re.compile(r"^gh[pousr]_[A-Za-z0-9]{20,}"),     # GitHub PAT
    re.compile(r"^xox[baprs]-[A-Za-z0-9-]{10,}"),   # Slack
    re.compile(r"^AKIA[0-9A-Z]{16}$"),              # AWS access key id
    re.compile(r"^[A-Fa-f0-9]{40,}$"),              # long hex (keys / HMAC)
)


def _is_secret_key(key: str) -> bool:
    k = re.sub(r"[^a-z0-9]", "", key.lower())
    return k in _SECRET_KEY_EXACT or any(part in k for part in _SECRET_KEY_PARTS)


def _is_secret_value(value: str) -> bool:
    return any(rx.match(value) for rx in _SECRET_VALUE_RES)


def _redact(value: object, depth: int) -> object:
    if depth > _MAX_ARG_DEPTH:
        return _REDACTED
    if isinstance(value, dict):
        out: dict = {}
        for i, (k, v) in enumerate(value.items()):
            if i >= _MAX_ARG_KEYS:
                break
            ks = str(k)[:_MAX_ARG_STR]
            out[ks] = _REDACTED if _is_secret_key(ks) else _redact(v, depth + 1)
        return out
    if isinstance(value, list):
        return [_redact(v, depth + 1) for v in value[:_MAX_ARG_ITEMS]]
    if isinstance(value, str):
        return _REDACTED if _is_secret_value(value) else value[:_MAX_ARG_STR]
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)[:_MAX_ARG_STR]


def redact_args(args: object) -> dict | None:
    """Capture tool-call args with secrets filtered out (deny-list).

    Keeps meaningful args verbatim (symbol, dates, filters, query) but replaces
    values under secret-ish keys or values that look like credentials with
    "[redacted]". Strings are clamped and the structure is bounded. Never raises.
    """
    if not isinstance(args, dict) or not args:
        return None
    try:
        return _redact(args, 0)
    except Exception:
        return None


def build_provenance_event(
    source: ProvenanceSource | None = None,
    *,
    record_id: str | None = None,
    source_type: str | None = None,
    identifier: str | None = None,
    timestamp: str | None = None,
    title: str | None = None,
    detail: str | None = None,
    provider: str | None = None,
    tool_call_id: str | None = None,
    args_fingerprint: dict | None = None,
    args: dict | None = None,
    result_sha256: str | None = None,
    result_size: int | None = None,
    result_snippet: str | None = None,
    agent: str | None = None,
) -> dict:
    """Build the internal ``{"type": "provenance", ...}`` custom stream event.

    Accepts a ``ProvenanceSource`` or the individual fields (kwargs override the
    source). ``record_id`` and ``timestamp`` are generated when omitted.
    """
    if source is not None:
        record_id = record_id if record_id is not None else source.record_id
        source_type = source_type if source_type is not None else source.source_type
        identifier = identifier if identifier is not None else source.identifier
        timestamp = timestamp if timestamp is not None else source.timestamp
        title = title if title is not None else source.title
        detail = detail if detail is not None else source.detail
        provider = provider if provider is not None else source.provider
        tool_call_id = (
            tool_call_id if tool_call_id is not None else source.tool_call_id
        )
        args_fingerprint = (
            args_fingerprint
            if args_fingerprint is not None
            else source.args_fingerprint
        )
        args = args if args is not None else source.args
        result_sha256 = (
            result_sha256 if result_sha256 is not None else source.result_sha256
        )
        result_size = (
            result_size if result_size is not None else source.result_size
        )
        result_snippet = (
            result_snippet
            if result_snippet is not None
            else source.result_snippet
        )
        agent = agent if agent is not None else source.agent

    return {
        "type": "provenance",
        "record_id": record_id if record_id is not None else str(uuid.uuid4()),
        "source_type": source_type,
        "identifier": identifier,
        "title": title,
        "detail": detail,
        "provider": provider,
        "tool_call_id": tool_call_id,
        "args_fingerprint": args_fingerprint,
        "args": args,
        "result_sha256": result_sha256,
        "result_size": result_size,
        "result_snippet": result_snippet,
        "timestamp": (
            timestamp
            if timestamp is not None
            else datetime.now(timezone.utc).isoformat()
        ),
        "agent": agent,
    }
