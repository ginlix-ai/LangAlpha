"""Which vault secrets a plugin is allowed to reference.

Declaring a secret is a request; filling it is the grant. The two collapse
into one act only when the plugin introduces the name itself — then the
wizard's blueprint step IS the consent, and materializing the reference is
just recording what the user already agreed to.

A name the vault ALREADY holds is different in kind: it belongs to whatever
the user set it up for. Binding to it hands this plugin's endpoint a
credential meant for something else, and every surface that could have shown
that is silent by construction — the wizard lists only names the vault is
missing, the portable document is stored verbatim so ``/export`` shows the
plugin's own text rather than the row's, and the entry's diagnostics are
computed before any bind lands. So the grant has to be decided here, from
the vault, rather than inferred from the manifest.

The carry-forward test is two records of the same fact. A row this plugin owns
that already holds ``${vault:NAME}`` is the durable record that the name was
granted at some earlier install, which is what lets an update keep working
without re-asking; and a vault secret stamped with this plugin is the record
for a grant that never reached a row, because every entry that would have
carried it was held back at install. Nothing else re-grants: a name the user
holds for another reason stays refused however many versions declare it.

A declaration is not the only way an entry can name a credential. The portable
mcp.json can write ``${vault:NAME}`` straight into a header, and it needs no
extension block to do it, so a rule that reads only ``extension.secrets``
refuses the packages that declare honestly and waves through the one that says
nothing. The grant is a fact about the vault, not about the manifest, which is
why ``strip_ungranted_refs`` judges the entry plans after materialization:
by then the configs are the whole statement of what will be referenced,
whichever door it arrived through.
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
from src.server.models.plugin import Diagnostic
from src.server.services.plugins.extension import LangalphaExtension
from src.server.services.plugins.mcp import McpEntryPlan


@dataclass(frozen=True, slots=True)
class BindGrants:
    """Declared secret names split by whether this plugin may reference them."""

    granted: frozenset[str] = frozenset()
    refused: tuple[str, ...] = ()
    # The vault facts the split was computed from, kept so a raw reference can
    # be judged by the same rule without a second round trip.
    held: frozenset[str] = frozenset()
    already: frozenset[str] = frozenset()

    def allows(self, name: str) -> bool:
        """Whether this plugin may reference ``name`` at all."""
        return name not in self.held or name in self.already

    def refusal_reason(self) -> str:
        names = ", ".join(self.refused)
        return (
            f"this plugin asks to use credentials your vault already holds "
            f"({names}); they were left unbound. Customize the server and add "
            f"the reference yourself if you want it to use them."
        )


def secret_names_in(config: dict) -> set[str]:
    """Every ``${vault:NAME}`` referenced by one server config's fields."""
    found: set[str] = set()
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
    """Split a plugin's declared secret names into granted and refused.

    ``plugin_id`` is None during a first install, where there are no owned
    rows to carry anything forward and the vault alone decides.

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
    granted = {n for n in declared if n not in held or n in already}
    return BindGrants(
        granted=frozenset(granted),
        refused=tuple(n for n in declared if n not in granted),
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

    env and headers lose the whole key rather than keeping an emptied one: an
    empty credential header is still a header the endpoint sees, and absence is
    the shape a refused bind already has, since ``materialize_binds`` simply
    never wrote it. args keep their token and lose only the reference text,
    because dropping a token outright would shift every positional argument
    after it.
    """
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


def strip_ungranted_refs(plans: Iterable[McpEntryPlan], grants: BindGrants) -> None:
    """Remove every ``${vault:NAME}`` an entry is not allowed to reference.

    Runs after ``materialize_binds``, which is the point where the plan configs
    become the complete statement of what the entry will reference. A declared
    bind that was refused never got written, so this finds nothing for it; what
    it does catch is the other door, a reference the package wrote into the
    portable document itself, which reaches the same endpoint with the same
    credential and answers to no declaration.
    """
    for plan in plans:
        ungranted = frozenset(
            n for n in secret_names_in(plan.config) if not grants.allows(n)
        )
        if not ungranted:
            continue
        strip_refs(plan.config, ungranted)
        plan.diagnostics.append(
            Diagnostic(
                level="error",
                scope="entry",
                target=plan.key,
                code="secret_not_granted",
                message=(
                    f"this entry asks to send credentials your vault already "
                    f"holds ({', '.join(sorted(ungranted))}) to "
                    f"{_destination(plan.config)}; the references were removed. "
                    f"Customize the server and add them yourself if that is "
                    f"what you want."
                ),
            )
        )
