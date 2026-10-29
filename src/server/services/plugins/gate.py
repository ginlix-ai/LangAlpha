"""The questions every plugin door asks before its check and again under the lock.

Install, update, key rotation and the sse upgrade each check the servers they
would write before taking the per-user fan-out lock, because a command
server's check can start the user's computer, and write under it. What the
check relied on can change in between: a secret created or deleted, a vault
filled, a row edited to read a key, the plugin uninstalled. So each door asks
the same questions in both places, and they live here so the two passes, and
the four doors, ask them alike. What stays in a door is its diff (which
servers are due, and why) and its write.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from src.server.database.plugins import get_plugin
from src.server.models.mcp_server import McpServerInput
from src.server.services.mcp_dial import consented_bearer
from src.server.services.mcp_validation import (
    CheckFailure,
    CheckOutcome,
    SignInNeeded,
    check_secrets,
    check_servers,
)
from src.server.services.plugins.checks import (
    CheckedServers,
    missing_declarations,
    reads_any,
    row_server,
    secrets_required,
)
from src.server.services.plugins.extension import LangalphaExtension
from src.server.services.plugins.grants import BindGrants, resolve_bind_grants
from src.server.services.plugins.supplied_secrets import (
    refuse_overwrites,
    refuse_vault_overflow,
)


async def secret_gate(
    user_id: str,
    extension: LangalphaExtension,
    supplied: Mapping[str, str],
    *,
    plugin_id: str | None,
    require_declared: bool = False,
) -> BindGrants:
    """Refuse values the vault cannot take, and return the grants they were
    judged by.

    A value for a name the user holds for something else, and values past the
    vault's room, are refused before anything is dialled and again under the
    lock, where the refusal is binding. ``require_declared`` also refuses a
    declared secret with no value in the vault or the request: install's rule.
    An update asks only where a server it checks reads one, and a rotation
    asks for nothing.
    """
    grants = await resolve_bind_grants(user_id, extension, plugin_id=plugin_id)
    refuse_overwrites(supplied, grants)
    if require_declared and (
        missing := missing_declarations(extension, grants.held, supplied)
    ):
        raise secrets_required(missing)
    refuse_vault_overflow(grants.held, supplied)
    return grants


@dataclass(frozen=True)
class Due:
    """A server a door owes a check, built as the door will leave it."""

    server: McpServerInput
    # New, or rewritten with dial fields that move. A sign-in it asks for is
    # the door's to allow or refuse; one asked by a server that stays where
    # it was is a failure, since only the value it was handed changed.
    moves: bool
    # It resolves a value the request supplies, so the write that stores the
    # value has to find it approved.
    reads: bool = False
    # An existing row keeping its name, whose live sign-in the check may use.
    signed_in: bool = False
    # The inline credentials the write vaults for it, by their vault names.
    literals: Mapping[str, str] = field(default_factory=dict)
    # The row as the database holds it, not rebuilt from a package.
    stored: bool = False


def readers(rows: Iterable[Mapping[str, Any]], names: Iterable[str]) -> list[Due]:
    """Every row that resolves one of ``names``, as the check reads it.

    Whoever owns it: a value written to the vault reaches every row that
    references the name, so each door that writes one owes these rows the
    check, left where they are.
    """
    names = set(names)
    return [
        Due(row_server(r), moves=False, reads=True, signed_in=True, stored=True)
        for r in rows
        if reads_any(r, names)
    ]


async def connected_bearers(
    user_id: str, servers: Iterable[McpServerInput]
) -> dict[str, str]:
    """The live sign-in token of each server, by name, where one holds for
    the address it names."""
    bearers: dict[str, str] = {}
    for server in servers:
        if (bearer := await consented_bearer(user_id, server)) is not None:
            bearers[server.name] = bearer
    return bearers


def _sign_in_instead_of_key(needed: SignInNeeded) -> CheckFailure:
    return CheckFailure(
        server=needed.server,
        verdict="needs_credential",
        reason="the server asks for a sign-in rather than this key",
    )


async def check_due(
    user_id: str,
    due: Sequence[Due],
    *,
    supplied: Mapping[str, str] | None = None,
) -> CheckOutcome:
    """Check every due server together. Writes nothing and raises nothing.

    Every server sees the request's values and every due server's inline
    credentials ahead of the vault: two entries carrying one literal share the
    one name it is vaulted under. The outcome's ``failed`` counts the sign-in
    of a server that does not move, and ``sign_in`` keeps only those of
    servers that do, for the door's own policy.
    """
    literals: dict[str, str] = {}
    for d in due:
        literals.update(d.literals)
    extras = check_secrets(literals, supplied)
    outcome = CheckOutcome.of(
        await check_servers(
            user_id,
            [(d.server, extras) for d in due],
            bearers=await connected_bearers(
                user_id, [d.server for d in due if d.signed_in]
            ),
        )
    )
    moving = {d.server.name for d in due if d.moves}
    return CheckOutcome(
        passed=outcome.passed,
        failed=sorted(
            [
                *outcome.failed,
                *(
                    _sign_in_instead_of_key(n)
                    for n in outcome.sign_in
                    if n.server not in moving
                ),
            ],
            key=lambda f: f.server,
        ),
        sign_in=[n for n in outcome.sign_in if n.server in moving],
    )


def refuse_unapproved(due: Iterable[Due], checked: CheckedServers) -> None:
    """Refuse the write when a server reading a supplied value is not one the
    check passed. Under the lock, right before the vault write.

    The fan-out proves each server it writes against the check, but a vault
    write has no fan-out: without this, a row edited or added to read the key
    while the check ran would get the new value unchecked. A ValueError, so
    the routes answer 409 and the same request passes once sent again.
    """
    stale = sorted(
        d.server.name
        for d in due
        if d.reads and not checked.approves(d.server, stored=d.stored)
    )
    if stale:
        raise ValueError(
            f"{', '.join(stale)} changed while the new values were being "
            "checked; try again"
        )


async def still_installed(
    user_id: str, plugin: Mapping[str, Any], waited: str
) -> dict[str, Any]:
    """The plugin's row under the lock. Raises ValueError when the plugin the
    caller read before its check was uninstalled since, or uninstalled and
    installed again under the same name."""
    stored = await get_plugin(user_id, plugin["name"])
    if stored is None or stored["user_plugin_id"] != plugin["user_plugin_id"]:
        raise ValueError(f"Plugin {plugin['name']!r} was uninstalled while {waited}")
    return stored
