"""Every server a plugin writes has passed its check.

The plugin doors follow the rule every server write follows
(``services/mcp_validation``), for all of a plugin's servers together: each
server an install, an update or an sse upgrade would write is built exactly as
it would be saved and checked, and the write happens only once all of them
have passed. A plugin with one broken server installs nothing, rather than
landing the rest around a row that can only fail.

The checks run before the per-user fan-out lock is taken, because a command
server's check can start the user's computer, far longer than another plugin
write should wait on that lock. So the write proves what it saves against what
passed: ``CheckedServers`` holds the dial fields of every server that passed,
and the fan-out refuses a server it does not hold.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_config import reserved_catalog_names
from src.server.services.mcp_dial import dial_of
from src.server.services.mcp_import import import_refusal, saved_server
from src.server.services.mcp_validation import SignInNeeded
from src.server.services.plugins.errors import PluginRefused
from src.server.services.plugins.extension import LangalphaExtension, PluginSecret
from src.server.services.plugins.grants import secret_names_in
from src.server.services.plugins.mcp import McpEntryPlan

#: What a write reports for a server whose dial fields moved after its check.
NOT_CHECKED_REASON = "this server changed after its check passed; try again"


def _masked(value: Any) -> Any:
    if isinstance(value, str):
        return VAULT_REF_RE.sub("${vault}", value)
    if isinstance(value, list):
        return [_masked(v) for v in value]
    if isinstance(value, dict):
        return {k: _masked(v) for k, v in value.items()}
    return value


def dial_signature(server: McpServerInput, *, masked: bool = True) -> str:
    """The dial fields of ``server`` as one comparable string.

    Reference names are masked by default. The name an inline credential is
    vaulted under depends on what the vault holds at the moment of the write,
    so the write can mint a different name than the check did for the same
    value; what the check proved is the destination and the package the
    values came from, and those are compared whole.
    """
    fields = dial_of(server.to_catalog_fields())
    return json.dumps(
        {"name": server.name, **(_masked(fields) if masked else fields)},
        sort_keys=True,
    )


def row_server(row: Mapping[str, Any]) -> McpServerInput:
    """A stored row as the check reads it.

    Constructed rather than validated: the row passed validation when it was
    written, and a rule tightened since must not turn a key change into a
    refusal about a field the change does not touch.
    """
    return McpServerInput.model_construct(
        name=row["name"],
        transport=row.get("transport") or "stdio",
        command=row.get("command"),
        args=list(row.get("args") or []),
        url=row.get("url"),
        env=dict(row.get("env") or {}),
        headers=dict(row.get("headers") or {}),
    )


def reads_any(config: Mapping[str, Any], names: Iterable[str]) -> bool:
    """Whether a server config resolves any of ``names`` from the vault."""
    return bool(secret_names_in(dict(config)) & set(names))


def server_secret_names(server: McpServerInput) -> set[str]:
    """Every vault name ``server`` resolves, read off its dial fields."""
    return secret_names_in(
        {
            "url": server.url,
            "env": server.env,
            "headers": server.headers,
            "args": server.args,
        }
    )


@dataclass(frozen=True)
class CheckedServers:
    """The servers that passed, as the write that follows must save them."""

    signatures: frozenset[str] = frozenset()
    exact: frozenset[str] = frozenset()

    @classmethod
    def of(cls, servers: Iterable[McpServerInput]) -> CheckedServers:
        servers = list(servers)
        return cls(
            frozenset(dial_signature(s) for s in servers),
            frozenset(dial_signature(s, masked=False) for s in servers),
        )

    def approves(self, server: McpServerInput, *, stored: bool = False) -> bool:
        """``stored``: a row as the database holds it, whose references name
        their keys themselves and so must match name for name. Masked, a row
        edited from ``${vault:B}`` to ``${vault:C}`` while a check proved it
        with B would pass as checked."""
        if stored:
            return dial_signature(server, masked=False) in self.exact
        return dial_signature(server) in self.signatures


@dataclass(frozen=True)
class Prospect:
    """A server a write would create, built the way the import loop saves it."""

    plan: McpEntryPlan
    server: McpServerInput
    # The inline credentials the write vaults, under the names this server's
    # references give them.
    literals: dict[str, str] = field(default_factory=dict)


def creatable_servers(
    plans: Iterable[McpEntryPlan],
    *,
    existing_names: set[str],
    secret_names: set[str],
    room: int | None = None,
) -> list[Prospect]:
    """The servers ``run_mcp_import`` would create from ``plans``, as saved.

    An entry the import would refuse writes nothing and has nothing to prove,
    so it is left out. ``room`` is how many rows the account's cap still
    allows; None counts none, for a caller that refuses an overflow outright
    instead.
    """
    reserved = reserved_catalog_names()
    seen: set[str] = set()
    allocated: dict[str, str] = {}
    used = set(secret_names)
    out: list[Prospect] = []
    for plan in plans:
        if not plan.installable or not plan.name:
            continue
        if import_refusal(
            plan.name,
            reserved=reserved,
            taken=existing_names,
            seen=seen,
            room=None if room is None else room - len(out),
        ):
            continue
        try:
            server, extraction = saved_server(
                plan.name, plan.config, allocated=allocated, used_secret_names=used
            )
        except ValidationError:
            continue
        seen.add(plan.name)
        allocated.update(extraction.refs)
        used.update(extraction.literals)
        out.append(Prospect(plan, server, extraction.literals))
    return out


def missing_declarations(
    extension: LangalphaExtension,
    held: Iterable[str],
    supplied: Mapping[str, str],
    *,
    among: set[str] | None = None,
) -> list[PluginSecret]:
    """Declared secrets with no value in the vault or the request.

    ``among`` narrows the question to the names a set of servers references.
    """
    have = set(held) | set(supplied)
    return [
        s
        for s in extension.secrets
        if s.name not in have and (among is None or s.name in among)
    ]


def asked_secrets(
    extension: LangalphaExtension, names: Iterable[str]
) -> list[PluginSecret]:
    """``names`` as ``secrets_required`` asks for them: by the plugin's own
    declaration, or under the bare name for a reference its entries write
    without declaring, which has nothing else to show."""
    declared = {s.name: s for s in extension.secrets}
    return [
        declared.get(n)
        # Constructed: a reference's name may run past what a declaration's
        # name and label allow.
        or PluginSecret.model_construct(
            name=n, label=n, description="", docs_url=None, regex=None, bind=[]
        )
        for n in sorted(names)
    ]


def secrets_required(missing: list[PluginSecret]) -> PluginRefused:
    """The refusal the wizard turns into a form: one field per secret."""
    labels = ", ".join(s.label or s.name for s in missing)
    return PluginRefused(
        "plugin_secrets_required",
        f"This plugin needs a value for {labels} before its servers can be "
        "checked.",
        secrets=[
            {
                "name": s.name,
                "label": s.label,
                **({"description": s.description} if s.description else {}),
                **({"regex": s.regex} if s.regex else {}),
                **({"docs_url": s.docs_url} if s.docs_url else {}),
            }
            for s in missing
        ],
    )


def sign_in_unsupported(servers: list[SignInNeeded], message: str) -> PluginRefused:
    """A sign-in this door cannot carry the user through."""
    return PluginRefused(
        "plugin_sign_in_unsupported",
        message,
        servers=[s.as_detail() for s in servers],
    )
