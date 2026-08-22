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

The carry-forward test is the components themselves. A row this plugin owns
that already holds ``${vault:NAME}`` is the durable record that the name was
granted to this plugin at some earlier install, which is what lets an update
keep working without re-asking. Nothing else re-grants: a name the user holds
for another reason stays refused however many versions declare it.
"""

from __future__ import annotations

from dataclasses import dataclass

from ptc_agent.core.mcp_sanitize import VAULT_REF_RE
from src.server.database.plugins import list_plugin_referenced_secrets
from src.server.database.user_vault_secrets import get_user_secret_names
from src.server.services.plugins.extension import LangalphaExtension


@dataclass(frozen=True, slots=True)
class BindGrants:
    """Declared secret names split by whether this plugin may reference them."""

    granted: frozenset[str] = frozenset()
    refused: tuple[str, ...] = ()

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
    """
    declared = [s.name for s in extension.secrets]
    if not declared:
        return BindGrants()

    held = set(await get_user_secret_names(user_id))
    already = (
        await list_plugin_referenced_secrets(user_id, plugin_id)
        if plugin_id is not None
        else set()
    )
    granted = {n for n in declared if n not in held or n in already}
    return BindGrants(
        granted=frozenset(granted),
        refused=tuple(n for n in declared if n not in granted),
    )
