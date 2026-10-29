"""Credential values a request hands a plugin, from the request to the vault.

Install, update and key rotation take values for the secrets a plugin asks
for, and all three use them twice: first as the values the servers' checks
resolve, and only once every check has passed, as vault writes. Nothing here
checks; it keeps the two uses agreeing on which names are acceptable and how
they land.
"""

import copy
import logging
import re
from collections.abc import Iterable, Mapping

from ptc_agent.core.mcp_sanitize import looks_like_placeholder
from src.server.database.user_vault_secrets import (
    MAX_SECRETS_PER_USER,
    create_user_secret,
    delete_user_secret,
    update_user_secret,
)
from src.server.models.vault import validate_secret_value
from src.server.services.mcp_import import plan_vault_extraction
from src.server.services.plugins.errors import PluginRejected
from src.server.services.plugins.extension import LangalphaExtension
from src.server.services.plugins.grants import BindGrants, secret_names_in
from src.server.services.plugins.mcp import McpEntryPlan

logger = logging.getLogger(__name__)

MAX_SECRET_VALUE_CHARS = 4096

_AUTH_SCHEME_RE = re.compile(r"(Bearer|Token|Bot|Basic)\s+(\S.*)", re.IGNORECASE)


def _in_place_of(blank: str, ref: str) -> str:
    """``ref`` where ``blank`` was, behind the auth scheme the docs wrote in
    front of it, so the value asked for is the key alone: the field shows a
    name and nothing says the scheme word is part of the answer."""
    scheme = _AUTH_SCHEME_RE.fullmatch(blank.strip())
    if scheme and looks_like_placeholder(scheme.group(2)):
        return f"{scheme.group(1)} {ref}"
    return ref


def bind_placeholders(entries: Iterable[McpEntryPlan], *, taken: Iterable[str]) -> None:
    """Write each stand-in an entry ships where a key goes as a reference to
    the name the import would vault it under. In place.

    ``<your-api-key>`` is a blank for the user to fill, not a credential. As a
    literal its value is asked for under the name the import gives it, then
    refused as a name the plugin never asked for, and the blank is vaulted
    beside it under a ``_2``; as a reference it is asked for, accepted and
    landed under one name, like a ``${vault:NAME}`` the package wrote. Never
    a name in ``taken`` (the vault's): the name comes from the server's,
    which another vendor's package can share.
    """
    used = set(taken)
    for entry in entries:
        if entry.skip_code is not None or not entry.name:
            continue
        drafted = copy.deepcopy(entry.config)
        extraction = plan_vault_extraction(
            entry.name, drafted, allocated={}, used_secret_names=used
        )
        blanks = {
            ref
            for value, ref in extraction.refs.items()
            if looks_like_placeholder(value)
        }
        if not blanks:
            continue
        # The extraction's own rewrite, kept only where it replaced a blank: a
        # real literal is the import's to vault, under the names it allocates.
        for section in ("env", "headers"):
            mapping = entry.config.get(section)
            if isinstance(mapping, dict):
                for key, value in drafted[section].items():
                    if value in blanks:
                        mapping[key] = _in_place_of(mapping[key], value)
        args = entry.config.get("args")
        if isinstance(args, list):
            for i, arg in enumerate(drafted["args"]):
                if isinstance(arg, str) and any(ref in arg for ref in blanks):
                    args[i] = arg
        used.update(
            s.name for s in extraction.secrets if looks_like_placeholder(s.value)
        )


def validate_supplied(
    extension: LangalphaExtension,
    secrets: Mapping[str, str] | None,
    *,
    entries: Iterable[McpEntryPlan],
) -> dict[str, str]:
    """The request's values, refused unless the plugin asks for each name.

    A plugin asks by declaring a name or by writing ``${vault:NAME}`` into one
    of its own ``entries`` (a stand-in counts once ``bind_placeholders`` has
    written it as one), which ``grants`` counts as the same request; the
    check reports the second kind missing like the first, so the user's answer
    has to travel the same checked path. Entries the package dropped are not
    read, since nothing of theirs will ever send the value. Any other name is
    not this door's to write: it would be a general vault write wearing a
    plugin's consent screen.
    """
    if not secrets:
        return {}
    asked = {s.name for s in extension.secrets}
    for entry in entries:
        if entry.skip_code is None:
            asked |= secret_names_in(entry.config)
    unknown = sorted(set(secrets) - asked)
    if unknown:
        raise PluginRejected(f"not asked for by this plugin: {', '.join(unknown)}")
    for name, value in secrets.items():
        if not isinstance(value, str) or not value or len(value) > MAX_SECRET_VALUE_CHARS:
            raise PluginRejected(
                f"secret values must be 1-{MAX_SECRET_VALUE_CHARS} characters"
            )
        try:
            validate_secret_value(value)
        except ValueError as e:
            raise PluginRejected(f"{name}: {e}") from e
    return dict(secrets)


def refuse_overwrites(supplied: Mapping[str, str], grants: BindGrants) -> None:
    """Refuse a value for a name the user holds for something else.

    Declaring a name is not owning it: the value already there may be what
    another server sends, and a plugin's form is not where it gets replaced.
    A name this plugin introduced or its rows already use is its own to set.
    """
    taken = sorted(n for n in supplied if grants.is_preexisting(n))
    if taken:
        raise PluginRejected(
            f"Secrets already holds {', '.join(taken)} for something "
            "else; edit it there if you want to change the value"
        )


def refuse_vault_overflow(held: Iterable[str], names: Iterable[str]) -> None:
    """Refuse values the vault has no room for, before anything is dialled.

    ``names`` is every secret the write leaves in the vault: the request's
    values, and the inline credentials its servers' import vaults, which take
    a slot as surely and otherwise fail one server after every check passed.
    """
    held = set(held)
    new = set(names) - held
    if new and len(held) + len(new) > MAX_SECRETS_PER_USER:
        raise ValueError(f"Maximum of {MAX_SECRETS_PER_USER} secrets per user reached")


async def write_supplied(
    user_id: str,
    plugin_name: str,
    extension: LangalphaExtension,
    secrets: Mapping[str, str],
    *,
    held: Iterable[str],
) -> tuple[list[str], list[str]]:
    """Create or overwrite each value. Returns (written, introduced).

    ``introduced`` is the names this call created, the only ones a caller may
    take back: an overwritten value's predecessor is gone either way. A write
    that fails part way takes back what it created before raising. A name the
    plugin only references has no description of its own, so it gets the one
    a declaration without one gets.
    """
    declared = {s.name: s.description for s in extension.secrets}
    held = set(held)
    written: list[str] = []
    introduced: list[str] = []
    try:
        for name, value in secrets.items():
            if name in held:
                await update_user_secret(user_id, name, value=value, description=None)
            else:
                await create_user_secret(
                    user_id,
                    name,
                    value,
                    declared.get(name) or f"Required by plugin {plugin_name}",
                )
                introduced.append(name)
            written.append(name)
    except BaseException:
        await drop_secrets(user_id, introduced)
        raise
    return written, introduced


async def drop_secrets(user_id: str, names: Iterable[str]) -> None:
    """Take back secrets a failed write created. Best effort, logged."""
    for name in names:
        try:
            await delete_user_secret(user_id, name)
        except Exception:
            logger.exception(
                "[plugins] could not take back vault secret %s for user %s", name, user_id
            )
