"""Sanitization helpers for untrusted (user-configured) MCP server schemas.

Untrusted MCP servers (``source`` ``"workspace"`` or ``"user"``) report
arbitrary tool names, parameter names, and descriptions. That text reaches
generated Python code (docstrings, wrapper modules) and the system prompt, so
it is hostile input. These helpers bound identifiers and text before either
boundary.

The ``${vault:NAME}`` reference regex is defined here ONCE
(``VAULT_REF_RE``) and imported by every lane that resolves or validates
vault references — the generated client, the API validator, and the resolver
must agree on the exact syntax.
"""

from __future__ import annotations

import keyword
import re
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Vault reference syntax — the single source of truth.
# ---------------------------------------------------------------------------
# A vault reference looks like ``${vault:SECRET_NAME}``. Only this exact form
# resolves for workspace servers; a bare ``${VAR}`` is intentionally NOT a
# vault reference, so a user cannot name ``${INTERNAL_SERVICE_TOKEN}`` and have
# it resolve from anything. Secret names follow the same identifier shape the
# vault API enforces.
VAULT_REF_RE = re.compile(r"\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}")

# Default bounds for untrusted tool text entering docstrings / docs / prompt.
DEFAULT_TOOL_TEXT_MAX_LEN = 2048

# An env/header literal reads as a credential when its key name does, or the
# value is a long opaque token. Every lane that must separate credentials from
# ordinary config (import-time vault extraction, output redaction) shares this
# ONE heuristic so a value one lane vaults is never a value another serves.
_SECRET_KEY_RE = re.compile(
    r"(?i)(secret|token|password|passwd|pwd|apikey|api[_-]?key|access[_-]?key|"
    r"authorization|auth|bearer|credential|cred|private[_-]?key|\bpat\b|\bkey\b)"
)
_OPAQUE_TOKEN_MIN_LEN = 20


def looks_like_secret(key: str, value: str) -> bool:
    """Heuristic: is this env/header literal a credential rather than config?"""
    if _SECRET_KEY_RE.search(key or ""):
        return True
    v = value or ""
    return len(v) >= _OPAQUE_TOKEN_MIN_LEN and " " not in v and not v.isdigit()


def iter_arg_flag_pairs(args) -> "list[tuple[int, str, str]]":
    """``(value_index, flag, value)`` for space-separated ``--token VALUE`` pairs.

    The one arg shape every lane agrees on, and the reason it is here rather
    than inlined three times: vault extraction and the export scrub have to
    rewrite the value, so they need its index, while output redaction only
    needs the pair — but all three must reach the same verdict or one lane
    serves a value another vaulted.

    Key-signal only, deliberately: the element after a flag is usually a path,
    a port, or a URL, and the opaque-value heuristic reads plenty of those as
    credentials. The flag naming itself is the only signal narrow enough to act
    on, so a bare positional secret is accepted as missed.
    """
    found: list[tuple[int, str, str]] = []
    prev_flag = ""
    for i, arg in enumerate(args or []):
        if not isinstance(arg, str):
            prev_flag = ""
            continue
        if arg.startswith("-"):
            flag, sep, _ = arg.partition("=")
            prev_flag = flag if not sep else ""
            continue
        if (
            prev_flag
            and _SECRET_KEY_RE.search(prev_flag)
            and not VAULT_REF_RE.search(arg)
        ):
            found.append((i, prev_flag.lstrip("-"), arg))
        prev_flag = ""
    return found


def iter_arg_credentials(args) -> "list[tuple[str, str]]":
    """Credential literals in a stdio arg list: ``--token=X`` and ``--token X``.

    Key-signal only — no opaque-value fallback: arg lists are full of paths and
    URLs that the value heuristic reads as secrets, and the redaction lanes
    consuming this would scrub ordinary arguments out of served files. A bare
    positional secret with no flag naming it is accepted as missed.
    """
    found: list[tuple[str, str]] = []
    for arg in args or []:
        if not isinstance(arg, str) or not arg.startswith("-"):
            continue
        flag, sep, val = arg.partition("=")
        if (
            sep
            and val
            and not VAULT_REF_RE.search(val)
            and _SECRET_KEY_RE.search(flag)
        ):
            found.append((flag.lstrip("-"), val))
    found.extend((flag, val) for _, flag, val in iter_arg_flag_pairs(args))
    return found


@dataclass(frozen=True)
class SanitizedToolSet:
    """Result of sanitizing one server's tool list.

    ``kept`` preserves input order; ``skipped`` records ``(original_name,
    reason)`` so callers can surface "N tools skipped: reasons".
    """

    kept: list = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)


def vault_refs(value: str) -> list[str]:
    """Return the vault secret names referenced in ``value`` (may be empty)."""
    return VAULT_REF_RE.findall(value or "")


def is_untrusted_server(server) -> bool:
    """True for user-configured servers (``source`` 'workspace' or 'user').

    The single definition of the trust-boundary predicate — built-ins (no
    ``source`` attr, or ``'builtin'``) are trusted; workspace-local and
    user-level (workspace-inherited) servers are not. Named for the property
    that matters: ``source='user'`` is one of the untrusted tiers, not the
    complement of this predicate.
    """
    return getattr(server, "source", "builtin") in ("workspace", "user")


def discovery_should_use_secrets(server) -> bool:
    """Whether tool discovery should resolve vault secrets for a server.

    The stored ``discovery_uses_secrets`` flag wins when set. Beyond that, a
    workspace ``sse``/``http`` server whose headers reference a vault secret is
    authenticated: it needs that header even to ``tools/list``, and the
    credential goes to the user's own URL (no untrusted-subprocess concern), so
    discovery resolves secrets for it automatically. Stdio servers keep the
    secret-less default — there the flag guards an untrusted subprocess.
    """
    if bool(getattr(server, "discovery_uses_secrets", False)):
        return True
    if is_untrusted_server(server) and getattr(server, "transport", None) in ("sse", "http"):
        headers = getattr(server, "headers", {}) or {}
        return any(VAULT_REF_RE.search(str(v)) for v in headers.values())
    return False


def discovery_affecting_payload(server, *, include_identity: bool = False) -> dict:
    """Canonical view of the config that changes a server's ``tools/list`` result
    or its generated client — the single source of truth for both the per-server
    discovery-cache key and the workspace asset-upload hash, so the two can never
    silently disagree.

    Includes transport/command/args/url, the EFFECTIVE secret-less-discovery
    decision, and the FULL env/header maps. The stored env/header/url values are
    ``${vault:NAME}`` reference strings or non-secret literals (e.g.
    ``MODE=prod``) — never a resolved secret, which exists only in the sandbox
    vault file at runtime — so a literal edit (``prod`` -> ``staging``) and a
    vault-ref retarget (``${vault:OLD}`` -> ``${vault:NEW}``) both correctly
    churn the hash. ``include_identity`` adds ``name`` + ``enabled`` for the
    whole-workspace upload hash; the per-server discovery key omits them (it is
    keyed by name already and reuses its cache across an enable/disable toggle).
    """
    env = dict(getattr(server, "env", {}) or {})
    headers = dict(getattr(server, "headers", {}) or {})
    payload: dict = {
        "transport": getattr(server, "transport", None),
        "command": getattr(server, "command", None),
        "args": list(getattr(server, "args", []) or []),
        "url": getattr(server, "url", None),
        "discovery_uses_secrets": discovery_should_use_secrets(server),
        "env": {k: env[k] for k in sorted(env)},
        "headers": {k: headers[k] for k in sorted(headers)},
    }
    if include_identity:
        payload["name"] = getattr(server, "name", None)
        payload["enabled"] = bool(getattr(server, "enabled", True))
    return payload


def sanitize_tool_name(name: str) -> str | None:
    """Coerce a tool name into a legal, non-keyword Python identifier.

    Returns the sanitized identifier, or ``None`` when the name cannot be
    salvaged (empty, or all characters illegal). The transformation matches the
    historical ``foo-bar``/``foo.bar`` -> ``foo_bar`` rewrite but is applied to
    every illegal character, so two distinct inputs CAN collapse to the same
    identifier — callers must run collision detection (see
    :func:`sanitize_tool_set`).
    """
    if not name:
        return None
    # Replace every character that is not a valid identifier char with "_".
    candidate = re.sub(r"[^0-9A-Za-z_]", "_", name)
    # A leading digit is illegal; prefix to make it valid.
    if candidate and candidate[0].isdigit():
        candidate = f"_{candidate}"
    if not candidate or candidate == "_" * len(candidate):
        return None
    if not candidate.isidentifier():
        return None
    if keyword.iskeyword(candidate) or keyword.issoftkeyword(candidate):
        candidate = f"{candidate}_"
    return candidate


def unsalvageable_required_params(input_schema) -> list[str]:
    """Required wire params whose name can never become a Python identifier.

    Codegen drops such a param from the wrapper signature, so a tool that
    REQUIRES one can never be called correctly — discovery drops the whole tool
    instead of advertising it healthy. Name COLLISIONS are deliberately not
    counted: two params that sanitize to the same identifier are salvageable
    (codegen de-duplicates them) and the tool stays callable.
    """
    if not isinstance(input_schema, dict):
        return []
    properties = input_schema.get("properties")
    required = input_schema.get("required")
    if not isinstance(properties, dict) or not isinstance(required, list):
        return []
    return [
        name
        for name in required
        if isinstance(name, str)
        and name in properties
        and sanitize_tool_name(name) is None
    ]


def sanitize_tool_text(text: str | None, max_len: int = DEFAULT_TOOL_TEXT_MAX_LEN) -> str:
    """Make untrusted text safe to embed inside a triple-quoted docstring.

    Neutralizes triple-quote breakouts, escapes trailing backslashes that would
    eat the closing quotes, strips control characters, and length-caps. The
    result is plain data — it cannot terminate the docstring early or smuggle
    code past the wrapper. Total over untrusted input: a non-string (a hostile
    schema can put a number or a dict where a description belongs) collapses to
    "" instead of raising mid-codegen.
    """
    if not isinstance(text, str) or not text:
        return ""
    # Strip control chars except tab/newline (which docstrings tolerate).
    cleaned = "".join(
        ch for ch in text if ch in ("\t", "\n") or (ord(ch) >= 32 and ord(ch) != 127)
    )
    # Double real backslashes FIRST (so a trailing `\` can't eat the closing
    # quotes), THEN neutralize triple-quote runs. This order needs no undo step:
    # the lone backslashes the neutralization inserts are meant to stay single.
    # (Doing it the other way required un-doubling, which also wrongly
    # un-doubled pre-existing `\"` sequences in the input.)
    cleaned = cleaned.replace("\\", "\\\\")
    cleaned = cleaned.replace('"""', '\\"\\"\\"').replace("'''", "\\'\\'\\'")
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len] + "…(truncated)"
    return cleaned


def sanitize_tool_set(tools: list) -> SanitizedToolSet:
    """Validate + de-collide a server's tool names.

    Each tool is expected to expose a ``.name`` attribute (``MCPToolInfo``).
    Tools whose name cannot be made a legal identifier are skipped with a
    reason; tools whose sanitized name collides with an already-kept tool are
    skipped (the first occurrence wins, deterministically).
    """
    result = SanitizedToolSet()
    seen: set[str] = set()
    for tool in tools:
        original = getattr(tool, "name", "") or ""
        sanitized = sanitize_tool_name(original)
        if sanitized is None:
            result.skipped.append((original, "name is not a valid Python identifier"))
            continue
        if sanitized in seen:
            result.skipped.append(
                (original, f"sanitized name {sanitized!r} collides with another tool")
            )
            continue
        seen.add(sanitized)
        result.kept.append(tool)
    return result
