"""Which vault secrets a plugin references, and which of those the user already had.

Declaring a secret is a request, and the answer is yes. A plugin the user chose
to install may reference any name in their own vault: the credential and the
risk are both theirs, and the same package on a local coding agent would
resolve the reference without asking anyone. Refusing here bought a narrower
blast radius than the sandbox it protects already has, and paid for it in the
failure users actually hit — a server that installs looking healthy and then
401s on its first call, over a name as ordinary as ``GITHUB_TOKEN``.

What survives the grant is disclosure. A name this package introduced is
unremarkable, because the wizard's blueprint step was the consent. A name the
vault already held for some other reason is worth saying out loud, since
nothing else on the path would say it: the wizard lists only names the vault is
missing, the portable document is stored verbatim so ``/export`` shows the
plugin's own text rather than the row's, and the entry's diagnostics are
computed before any bind lands. So this module still reads the vault. It
reports rather than refuses.

A declaration is not the only way an entry can name a credential. The portable
mcp.json can write ``${vault:NAME}`` straight into a header or a url with no
extension block at all, so a rule reading only ``extension.secrets`` would
speak about the package that declares honestly and stay silent about the one
that says nothing. Both doors are judged from the entry configs after
materialization, the point where they state everything that will be referenced.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.plugins import (
    list_plugin_owned_secrets,
    list_plugin_referenced_secrets,
)
from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.models.plugin import Diagnostic, InstallReport
from src.server.services.plugins.extension import LangalphaExtension
from src.server.services.plugins.mcp import McpEntryPlan


@dataclass(frozen=True, slots=True)
class BindGrants:
    """Declared secret names, plus which of them the user already held."""

    granted: frozenset[str] = frozenset()
    # Referenced names the vault held for some other reason. Disclosure only —
    # they are granted like any other; the report says so because no other
    # surface would.
    preexisting: tuple[str, ...] = ()
    # The vault facts the split was computed from, kept so a raw reference can
    # be judged by the same rule without a second round trip.
    held: frozenset[str] = frozenset()
    already: frozenset[str] = frozenset()

    def is_preexisting(self, name: str) -> bool:
        """Whether the user held ``name`` for something other than this plugin."""
        return name in self.held and name not in self.already

    def disclosure_reason(self) -> str:
        names = ", ".join(self.preexisting)
        return (
            f"this plugin uses credentials already in your vault ({names}). "
            f"Customize the server and remove the reference if that is not "
            f"what you want."
        )


def secret_names_in(config: dict) -> set[str]:
    """Every ``${vault:NAME}`` referenced by one server config's fields.

    ``url`` is scanned even though ``McpServerInput.validate_remote_url``
    refuses any ``${`` before a remote entry can persist. The sandbox client
    resolves refs in the URL as readily as in headers, so this scan and that
    validator are the only two things standing between a ref and a resolved
    credential — and nothing else ties them together.
    """
    found: set[str] = set()
    url = config.get("url")
    if isinstance(url, str):
        found.update(VAULT_REF_RE.findall(url))
    for section in ("env", "headers"):
        mapping = config.get(section)
        if isinstance(mapping, dict):
            for value in mapping.values():
                if isinstance(value, str):
                    found.update(VAULT_REF_RE.findall(value))
    args = config.get("args")
    if isinstance(args, list):
        for arg in args:
            if isinstance(arg, str):
                found.update(VAULT_REF_RE.findall(arg))
    return found


async def resolve_bind_grants(
    user_id: str,
    extension: LangalphaExtension,
    *,
    plugin_id: str | None,
) -> BindGrants:
    """Grant a plugin's declared secret names, flagging the pre-existing ones.

    ``plugin_id`` is None during a first install, where there are no owned rows
    to attribute a name to and every held name reads as pre-existing.

    The vault is read even when nothing is declared: the result also has to
    answer for references the portable document carries on its own, and those
    need no declaration to exist.
    """
    held = frozenset(await get_user_secret_names(user_id))
    already: frozenset[str] = frozenset()
    if plugin_id is not None:
        already = frozenset(
            await list_plugin_referenced_secrets(user_id, plugin_id)
            | await list_plugin_owned_secrets(user_id, plugin_id)
        )
    declared = [s.name for s in extension.secrets]
    return BindGrants(
        granted=frozenset(declared),
        preexisting=tuple(n for n in declared if n in held and n not in already),
        held=held,
        already=already,
    )


def _destination(config: dict[str, Any]) -> str:
    """Where this entry would have sent the credential, for the diagnostic."""
    url = config.get("url")
    if isinstance(url, str) and url:
        return url
    command = config.get("command")
    return str(command) if command else "this entry"


def strip_refs(config: dict[str, Any], names: frozenset[str]) -> None:
    """Drop every reference to ``names`` from one entry's config, in place.

    Used when an entry's execution identity moves under a carried-forward
    reference, where the credential would otherwise follow the config to a
    destination it was never given for.

    env and headers lose the whole key rather than keeping an emptied one: an
    empty credential header is still a header the endpoint sees, and absence is
    the shape an unwritten bind already has. args and url keep their token and
    lose only the reference text, because dropping an arg outright would shift
    every positional after it, and a url with the ref cut out is at worst an
    endpoint that refuses the connection.
    """
    url = config.get("url")
    if isinstance(url, str):
        config["url"] = VAULT_REF_RE.sub(
            lambda m: "" if m.group(1) in names else m.group(0), url
        )
    for section in ("env", "headers"):
        mapping = config.get(section)
        if not isinstance(mapping, dict):
            continue
        config[section] = {
            k: v
            for k, v in mapping.items()
            if not (isinstance(v, str) and names & set(VAULT_REF_RE.findall(v)))
        }
    args = config.get("args")
    if isinstance(args, list):
        config["args"] = [
            VAULT_REF_RE.sub(lambda m: "" if m.group(1) in names else m.group(0), arg)
            if isinstance(arg, str)
            else arg
            for arg in args
        ]


def disclose_vaulted_literals(report: InstallReport) -> None:
    """Say why a vault secret the user never asked for now exists.

    The spec forbids shipping credentials inline, and the import path lifts
    any it finds into the vault rather than refusing the package. That is the
    right trade, but silently it reads as a bug: the user gets a new secret
    with a name they did not choose and no account of where it came from. The
    same line covers the typo case, where a reference too malformed to parse
    is indistinguishable from a literal and gets vaulted whole — so the value
    is safe, but it will never resolve, and this is the only place that says
    so before the server 401s.
    """
    if not report.secrets_created:
        return
    names = ", ".join(sorted(report.secrets_created))
    report.diagnostics.append(
        Diagnostic(
            level="warning",
            scope="plugin",
            code="embedded_credential",
            message=(
                f"this package carried credential values inline; they were "
                f"moved into your vault as {names} rather than left in the "
                f"server config. Check them there — a value that was meant to "
                f"be a ${{vault:NAME}} reference but was mistyped lands here "
                f"too, and would be sent as written."
            ),
            spec_ref="https://agent-plugins.org/specification",
        )
    )


def disclose_undeclared_refs(
    plans: Iterable[McpEntryPlan], grants: BindGrants
) -> None:
    """Name every pre-existing credential an entry reaches for without declaring it.

    Runs after ``materialize_binds``, the point where the plan configs are the
    complete statement of what the entry will reference. Declared names are
    left to the plugin-level disclosure, which covers them even when no entry
    survived to carry one; what this catches is the other door, a reference the
    package wrote into the portable document itself, which reaches the same
    endpoint with the same credential and answers to no declaration. Naming
    the destination is the point — it is the one fact that separates a plugin
    using your token for its own API from one forwarding it somewhere else.
    """
    for plan in plans:
        undeclared = sorted(
            n
            for n in secret_names_in(plan.config)
            if grants.is_preexisting(n) and n not in grants.granted
        )
        if not undeclared:
            continue
        plan.diagnostics.append(
            Diagnostic(
                level="warning",
                scope="entry",
                target=plan.key,
                code="secret_undeclared",
                message=(
                    f"this entry sends credentials already in your vault "
                    f"({', '.join(undeclared)}) to {_destination(plan.config)} "
                    f"without declaring them. Customize the server and remove "
                    f"the reference if that is not what you want."
                ),
            )
        )
