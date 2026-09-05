"""mcp.json validation and translation into catalog-server inputs.

Failure isolation per the spec: an invalid document (or one whose declared
schema version differs from plugin.json's) drops the MCP component only;
an invalid entry drops that entry only. Valid entries translate into
``McpServerInput`` kwargs — literal values untouched — and the shared import
loop applies our request-model policy per entry.

One deliberate v1 deviation, a legal per-entry skip reported with
``spec_ref``: entries that interpolate ``${PLUGIN_ROOT}``/``${PLUGIN_DATA}``
are held back until the sandbox materializes plugin roots, because the path
they name would not exist. ``cwd`` is accepted and ignored, which costs the
rare entry that truly depends on it a warning rather than an install.
"""

from dataclasses import dataclass, field
from json import JSONDecodeError, loads
from typing import Any, ClassVar

from jsonschema.validators import validator_for
from referencing import Registry

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.models.mcp_server import coerce_mcp_name
from src.server.models.plugin import Diagnostic
from src.server.services.plugins.manifest import SPEC_URL, check_schema_version
from src.server.services.plugins.schemas import (
    MCP_SCHEMA,
    SCHEMA_URL_RE,
    describe_schema_error,
)

_NO_REMOTE_REFS = Registry()
_validator_cls = validator_for(MCP_SCHEMA)
_VALIDATOR = _validator_cls(MCP_SCHEMA, registry=_NO_REMOTE_REFS)

_PLUGIN_VARS = ("${PLUGIN_ROOT}", "${PLUGIN_DATA}")

# The archive caps bound the package, not this one file inside it, and both of
# the costs here scale with the document rather than the archive: schema
# validation walks it whole, and every surviving key becomes an entry plan the
# install then works through. A real package declares a handful of servers, so
# these sit far above anything legitimate and only catch a document whose size
# is the point.
MAX_DOCUMENT_BYTES = 1024 * 1024
MAX_ENTRIES = 100


@dataclass
class McpEntryPlan:
    """One mcp.json entry, classified and (when installable) translated."""

    # How this plan reports itself (ComponentResult.of).
    kind: ClassVar[str] = "mcp"

    key: str
    name: str | None
    renamed: bool
    transport: str  # stdio / http / sse ('' when skipped before translation)
    # McpServerInput kwargs, literal secrets still inline. Extension binds
    # materialize into this dict before the import loop runs.
    config: dict[str, Any] = field(default_factory=dict)
    skip_code: str | None = None
    skip_reason: str | None = None
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @property
    def installable(self) -> bool:
        return self.skip_code is None and self.transport in ("stdio", "http")


def _entry_diag(
    key: str, code: str, message: str, *, level: str = "warning"
) -> Diagnostic:
    # jsonschema interpolates the failing instance into oneOf messages, so an
    # entry-sized blob can ride in; clip it — the wire report is a summary.
    if len(message) > 400:
        message = message[:400] + " …"
    return Diagnostic(
        level=level,
        scope="entry",
        target=key,
        code=code,
        message=message,
        spec_ref=SPEC_URL,
    )


def _mentions_plugin_vars(entry: dict[str, Any]) -> bool:
    def _hit(value: Any) -> bool:
        return isinstance(value, str) and any(v in value for v in _PLUGIN_VARS)

    if _hit(entry.get("command")) or _hit(entry.get("url")):
        return True
    if any(_hit(a) for a in entry.get("args") or []):
        return True
    for mapping in (entry.get("env"), entry.get("headers")):
        if isinstance(mapping, dict) and any(_hit(v) for v in mapping.values()):
            return True
    return False


def _skip(plan: McpEntryPlan, code: str, reason: str) -> McpEntryPlan:
    plan.skip_code = code
    plan.skip_reason = reason
    plan.diagnostics.append(_entry_diag(plan.key, code, reason))
    return plan


def _plan_entry(key: str, entry: dict[str, Any]) -> McpEntryPlan:
    name, renamed = coerce_mcp_name(key)
    plan = McpEntryPlan(
        key=key, name=name, renamed=renamed, transport=entry.get("type") or ""
    )
    if name is None:
        return _skip(plan, "bad_name", "server key cannot be coerced to a legal name")

    headers = entry.get("headers")
    if isinstance(headers, dict):
        lowered = [str(k).lower() for k in headers]
        if len(set(lowered)) != len(lowered):
            return _skip(
                plan, "duplicate_header",
                "header names must be unique case-insensitively",
            )

    if _mentions_plugin_vars(entry):
        return _skip(
            plan, "plugin_tree_unsupported",
            "entries that interpolate ${PLUGIN_ROOT} or ${PLUGIN_DATA} are "
            "not supported yet",
        )
    if entry.get("cwd") is not None:
        plan.diagnostics.append(
            _entry_diag(
                plan.key, "cwd_ignored",
                f"cwd {entry['cwd']!r} is ignored; the server runs from the "
                f"sandbox working directory",
            )
        )

    if plan.transport == "stdio":
        command = entry.get("command") or ""
        plan.config = {
            "name": name,
            "transport": "stdio",
            "command": command,
            "args": list(entry.get("args") or []),
            "env": dict(entry.get("env") or {}),
        }
    else:
        # streamable-http installs as our 'http'; legacy sse is held back for
        # the consented upgrade probe rather than installed as-is.
        plan.transport = "http" if plan.transport == "streamable-http" else "sse"
        plan.config = {
            "name": name,
            "transport": "http",
            "url": entry.get("url"),
            "headers": dict(headers or {}),
        }

    # Our private ref syntax sighted in portable fields: kept literal per the
    # spec's variables rule, which in this deployment means it will resolve as
    # a vault reference at runtime — worth a warning either way.
    # args included: a stdio entry can carry the ref in argv just as easily,
    # and scanning only the two maps left the loudest case (an entry handing
    # this account's secret to a package fetched from npm) with no diagnostic.
    args = plan.config.get("args")
    sighted = [
        v
        for values in (
            (plan.config.get("env") or {}).values(),
            (plan.config.get("headers") or {}).values(),
            args if isinstance(args, list) else (),
        )
        for v in values
        if isinstance(v, str) and "${vault:" in v
    ]
    if any(VAULT_REF_RE.search(v) for v in sighted):
        plan.diagnostics.append(
            _entry_diag(
                key, "vault_ref_in_portable",
                "portable mcp.json carries a ${vault:...} reference; kept "
                "as written and resolved from this account's vault",
            )
        )
    # A name the syntax cannot hold (a hyphen is the usual one) is not a
    # reference at all, so nothing downstream resolves it: the import loop
    # reads it as an ordinary literal, vaults it for looking like a secret,
    # and the entry then sends the text verbatim. Saying "kept as written and
    # resolved" about that is the one thing that would stop the user from
    # spotting the typo before their server 401s.
    if any(v.count("${vault:") > len(VAULT_REF_RE.findall(v)) for v in sighted):
        plan.diagnostics.append(
            _entry_diag(
                key, "vault_ref_malformed",
                "portable mcp.json carries something shaped like a "
                "${vault:...} reference whose name this account cannot hold "
                "(letters, digits and underscore only, starting with a "
                "letter or underscore); it is not a reference, and the text "
                "is sent as written",
            )
        )
    return plan


def validate_mcp_document(
    raw: bytes, *, plugin_schema: Any
) -> tuple[dict[str, Any] | None, list[McpEntryPlan], list[Diagnostic]]:
    """Validate mcp.json; return (verbatim doc or None, entry plans, diags).

    A None document means the MCP component was dropped whole (unreadable,
    schema-invalid at document level, or version-mismatched) — the plans list
    is then empty and the diagnostics say why.
    """

    def _component_error(code: str, message: str):
        return None, [], [
            Diagnostic(
                level="error", scope="mcp", code=code, message=message,
                spec_ref=SPEC_URL,
            )
        ]

    if len(raw) > MAX_DOCUMENT_BYTES:
        return _component_error(
            "document_too_large",
            f"mcp.json is limited to {MAX_DOCUMENT_BYTES} bytes",
        )
    try:
        doc = loads(raw.decode("utf-8"))
    except (JSONDecodeError, UnicodeDecodeError) as e:
        return _component_error("invalid_json", f"mcp.json is not valid JSON: {e}")
    if not isinstance(doc, dict):
        return _component_error("invalid_json", "mcp.json must be a JSON object")
    entries = doc.get("mcpServers")
    if isinstance(entries, dict) and len(entries) > MAX_ENTRIES:
        return _component_error(
            "too_many_entries",
            f"mcp.json declares {len(entries)} servers; the limit is "
            f"{MAX_ENTRIES}",
        )

    version = check_schema_version(doc.get("$schema"), kind="mcp")
    if version is not None:
        return _component_error(
            "unsupported_version",
            f"mcp.json targets Agent Plugins {version}; this deployment "
            "supports 1.0.0",
        )
    plugin_match = (
        SCHEMA_URL_RE.match(plugin_schema)
        if isinstance(plugin_schema, str) else None
    )
    mcp_match = (
        SCHEMA_URL_RE.match(doc["$schema"])
        if isinstance(doc.get("$schema"), str) else None
    )
    if plugin_match and mcp_match and plugin_match.group(1) != mcp_match.group(1):
        return _component_error(
            "schema_version_mismatch",
            "mcp.json must target the same Agent Plugins version as "
            "plugin.json",
        )

    entry_errors: dict[str, list[str]] = {}
    for error in _VALIDATOR.iter_errors(doc):
        path = list(error.absolute_path)
        message = describe_schema_error(error)
        if len(path) >= 2 and path[0] == "mcpServers":
            entry_errors.setdefault(str(path[1]), []).append(message)
        else:
            loc = ".".join(str(p) for p in path) or "mcp.json"
            return _component_error("schema_invalid", f"{loc}: {message}")

    plans: list[McpEntryPlan] = []
    for key, entry in (doc.get("mcpServers") or {}).items():
        if key in entry_errors:
            name, renamed = coerce_mcp_name(key)
            plan = McpEntryPlan(
                key=key, name=name, renamed=renamed, transport=""
            )
            # One entry rarely produces more than one top-level error, and
            # each has already been reduced to the key at fault.
            _skip(plan, "schema", entry_errors[key][0])
            plans.append(plan)
            continue
        plans.append(_plan_entry(key, dict(entry)))
    return doc, plans, []
