"""Bulk import of standard ``mcpServers`` JSON into the user's Plugins catalog.

Every door that lands servers from a blob (a workspace's MCP tab, the Plugins
page, a plugin install) accepts the same inline credentials and runs the same
per-entry gauntlet: skip reserved or duplicate names, enforce the account cap,
rewrite credential-looking literals to ``${vault:NAME}`` refs and validate, all
of it pure and writing nothing, then commit the entry's vault secrets and its
server row in ONE transaction. An entry either lands whole or not at all, and
its failure never touches the others. ``catalog_import_scope`` builds what the
doors share; a door supplies its server writer, the prose for a name that is
taken, and the check its entries get.

A door that saves a user's own servers checks every entry before its commit
(``mcp_validation``): an entry whose server does not answer is reported, not
created, and one that needs a sign-in is reported as needing one. The checks
all run ahead of the commits, a few at a time and outside any transaction, so
a slow vendor holds no connection and no lock. The plugin install passes no
check, having checked all of its servers together before it gets here.

``import_refusal`` and ``saved_server`` are the gauntlet and the build, shared
with every door that predicts what an import would create.
"""

from __future__ import annotations

import copy
import re
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

# looks_like_secret decides which import-time literals get auto-extracted into
# vault secrets; benign config (``MODE=prod``, ``LOG_LEVEL=ERROR``) stays an
# inline literal so we don't clutter the vault. Defined in mcp_sanitize so the
# redaction lanes apply the identical credential test, and so the import dialog
# and ``looks_like_placeholder`` read a stand-in the same way.
from ptc_agent.core.mcp_sanitize import (
    VAULT_REF_RE,
    iter_arg_flag_pairs,
    looks_like_placeholder,
    looks_like_secret,
)
from src.server.database.mcp_servers import MAX_CATALOG_SERVERS_PER_USER
from src.server.database.pool import get_db_connection
from src.server.database.user_vault_secrets import (
    create_user_secret,
    get_user_secret_names,
)
from src.server.services.mcp_config import reserved_catalog_names
from src.server.services.mcp_validation import (
    CheckFailure,
    CheckResult,
    Passed,
    check_secrets,
)
from src.server.utils.error_sanitization import validation_error_text

if TYPE_CHECKING:
    from src.server.models.mcp_server import McpServerInput, ParsedMcpServer

CAP_MESSAGE = f"Plugins server cap ({MAX_CATALOG_SERVERS_PER_USER}) reached"


def vault_secret_name(server_name: str, key: str, used: set[str]) -> str:
    """Allocate a unique, NAME_RE-legal vault secret name for ``server.key``."""
    base = re.sub(r"[^A-Za-z0-9_]", "_", f"{server_name}_{key}".upper())
    if base and base[0].isdigit():
        base = f"_{base}"
    base = base[:64] or "IMPORTED_SECRET"
    name = base
    i = 2
    while name in used:
        suffix = f"_{i}"
        name = f"{base[: 64 - len(suffix)]}{suffix}"
        i += 1
    return name


@dataclass(frozen=True)
class PlannedSecret:
    """A vault secret an entry needs, not yet written."""

    name: str
    value: str
    description: str


# Both writers run inside ONE per-entry transaction, on the connection they are
# handed. ``persist`` returns True when the server was created, False when the
# name turned out to be taken; ValueError is a scope-level refusal (cap, raced
# duplicate) — either way the transaction is rolled back whole.
SecretWriter = Callable[[Any, PlannedSecret], Awaitable[None]]
# ``persist`` is handed the originating entry as well as the validated server:
# the row's provenance (which mcp.json key installed it) is the entry's, and
# two entries can normalize to one name, so the name cannot recover it.
ServerWriter = Callable[[Any, Any, "ParsedMcpServer"], Awaitable[bool]]
# Checks servers by position, each with the values its extracted secrets would
# hold: the check sees them ahead of the vault and never stores them.
ImportCheck = Callable[
    [Sequence[tuple["McpServerInput", Mapping[str, str]]]],
    Awaitable[list[CheckResult]],
]


@dataclass(frozen=True)
class ImportScope:
    """What one import surface contributes to the shared per-entry loop."""

    reserved_names: set[str]
    # Every row already counted against ``cap``.
    existing_names: set[str]
    cap: int
    cap_message: str
    exists_message: str
    # Names already in the scope's vault — allocation must not collide with them.
    existing_secret_names: set[str]
    create_secret: SecretWriter
    persist: ServerWriter
    # None commits unchecked: the plugin install checks its servers all
    # together before it gets here.
    check: ImportCheck | None


async def catalog_import_scope(
    user_id: str,
    *,
    existing_names: set[str],
    persist: ServerWriter,
    exists_message: str,
    check: ImportCheck | None,
) -> ImportScope:
    """The scope of an import into ``user_id``'s catalog, whichever door it
    came through: one account cap, one vault, one set of reserved names."""

    async def create_secret(conn: Any, secret: PlannedSecret) -> None:
        await create_user_secret(
            user_id, secret.name, secret.value, secret.description, conn=conn
        )

    return ImportScope(
        reserved_names=reserved_catalog_names(),
        existing_names=existing_names,
        cap=MAX_CATALOG_SERVERS_PER_USER,
        cap_message=CAP_MESSAGE,
        exists_message=exists_message,
        existing_secret_names=set(await get_user_secret_names(user_id)),
        create_secret=create_secret,
        persist=persist,
        check=check,
    )


@dataclass
class ImportReport:
    results: list[dict[str, Any]] = field(default_factory=list)
    created: int = 0
    secrets_created: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EntryPlan:
    """One entry's vault extraction: the secrets its refs need, unwritten."""

    secrets: tuple[PlannedSecret, ...]
    # literal value → ``${vault:NAME}``, merged into the run-wide dedupe map
    # only once this entry commits.
    refs: dict[str, str]

    @property
    def literals(self) -> dict[str, str]:
        """The extracted values, by the names the refs give them."""
        return {s.name: s.value for s in self.secrets}


def import_refusal(
    name: str,
    *,
    reserved: Collection[str],
    taken: Collection[str],
    seen: Collection[str],
    room: int | None,
    exists_message: str = "a server with this name already exists",
    cap_message: str = CAP_MESSAGE,
) -> dict[str, str] | None:
    """The result fields of an entry no import may land, or None.

    The one gauntlet every door that creates servers runs, in this order.
    ``seen`` is the names landed earlier in the same run, ``room`` how many
    more rows the account takes (None for a caller that refuses an overflow
    whole instead).
    """
    from src.server.models.mcp_server import sandbox_name_error

    # Every import and install door mints its names here, and the model leaves
    # this check to the doors. Parsing renames a reserved key already; this
    # holds for a caller handing over a name it did not coerce.
    if reason := sandbox_name_error(name):
        return {"status": "invalid", "error": reason}
    if name in reserved:
        return {"status": "skipped", "reason": "collides with a name this build reserves"}
    if name in seen:
        return {"status": "skipped", "reason": "duplicate name after normalization"}
    if name in taken:
        return {"status": "exists", "reason": exists_message}
    if room is not None and room <= 0:
        return {"status": "error", "error": cap_message}
    return None


def saved_server(
    name: str,
    config: Mapping[str, Any],
    *,
    allocated: Mapping[str, str],
    used_secret_names: set[str],
) -> tuple[McpServerInput, EntryPlan]:
    """``config`` as the catalog saves it under ``name``, and the secrets its
    vaulted literals need. Raises ValidationError.

    What a check dials is built here and so is what the write saves, so the
    two cannot drift. Writes nothing: ``config`` is copied, and the refs the
    extraction allocates are the caller's to publish once the server lands.
    """
    from src.server.models.mcp_server import McpServerInput

    config = copy.deepcopy(dict(config))
    config["name"] = name
    plan = plan_vault_extraction(
        name, config, allocated=allocated, used_secret_names=used_secret_names
    )
    # An authenticated remote server needs its header even to list tools, so
    # discovery must resolve secrets; set explicitly so the stored value (and
    # the UI toggle) is honest (matches discovery_should_use_secrets). Headers
    # of any other shape are the model's to refuse, as one invalid entry.
    headers = config.get("headers")
    if (
        config.get("transport") in ("http", "sse")
        and isinstance(headers, dict)
        and any(VAULT_REF_RE.search(str(v)) for v in headers.values())
    ):
        config["discovery_uses_secrets"] = True
    return McpServerInput(**config), plan


def check_refusal(result: CheckResult) -> dict[str, Any] | None:
    """An entry's result fields when its check did not pass, else None."""
    match result:
        case Passed():
            return None
        case CheckFailure():
            return {
                "status": "check_failed",
                "reason": result.reason,
                "verdict": result.verdict,
                "missing_secrets": list(result.missing_secrets),
            }
        case _:
            return {
                "status": "sign_in_required",
                "reason": "Sign in to the server to add it.",
                "url": result.url,
            }


class _NameTaken(Exception):
    """Raised to roll back an entry whose server name lost the insert race."""


async def run_mcp_import(
    parsed: Sequence[ParsedMcpServer], *, scope: ImportScope
) -> ImportReport:
    """Import each parsed entry into ``scope``, reporting per-entry outcomes.

    A partial import is the normal case: every entry that fails is reported in
    place (``invalid`` / ``skipped`` / ``exists`` / ``error``, and with a
    checking scope ``check_failed`` / ``sign_in_required``) and the rest
    continue, so one bad server never aborts the blob.

    Each entry that could land is built as it would be saved, its literal
    credentials standing in for the vault secrets they become, and checked
    before any commit. The commits then run in order.
    """
    report = ImportReport()
    # Committed state only: an identical token reused across servers is stored
    # once (a placeholder spelling excepted, see ``_ref_for``), but a ref is
    # published here only after its entry lands, so a failed entry can never
    # leave a later one pointing at a secret that was never made.
    allocated: dict[str, str] = {}
    used_secret_names = set(scope.existing_secret_names)

    # The gauntlet runs once, before anything is checked, and an entry it
    # takes holds its name and a row of the account's room whether or not it
    # then lands. Only what it takes is checked, so a blob of thousands costs
    # at most the account's room in checks, and nothing it left unchecked can
    # reach a commit. An entry that fails validation was never going to land,
    # so it holds nothing and a later one under the same name gets its turn.
    refused: dict[int, dict[str, Any]] = {}
    candidates: dict[int, tuple[McpServerInput, dict[str, str]]] = {}
    held_names: set[str] = set()
    for index, entry in enumerate(parsed):
        outcome = (
            {"status": "invalid", "error": entry.error}
            if entry.error
            else import_refusal(
                entry.name,
                reserved=scope.reserved_names,
                taken=scope.existing_names,
                seen=held_names,
                room=scope.cap - len(scope.existing_names) - len(candidates),
                exists_message=scope.exists_message,
                cap_message=scope.cap_message,
            )
        )
        if outcome is None:
            try:
                server, plan = saved_server(
                    entry.name,
                    entry.config,
                    allocated={},
                    used_secret_names=used_secret_names,
                )
            except ValidationError as e:
                outcome = {"status": "invalid", "error": validation_error_text(e)}
        if outcome is not None:
            refused[index] = outcome
            continue
        held_names.add(entry.name)
        candidates[index] = (server, check_secrets(plan.literals))
    checks: dict[int, CheckResult] = {}
    if scope.check is not None and candidates:
        results = await scope.check(list(candidates.values()))
        checks = dict(zip(candidates, results, strict=True))

    for index, entry in enumerate(parsed):
        base = {
            "original_name": entry.original_name,
            "name": entry.name,
            "renamed": entry.renamed,
        }
        outcome = refused.get(index)
        if outcome is None and index in checks:
            outcome = check_refusal(checks[index])
        if outcome is not None:
            report.results.append({**base, **outcome})
            continue
        try:
            server, plan = saved_server(
                entry.name,
                entry.config,
                allocated=allocated,
                used_secret_names=used_secret_names,
            )
            created = await _commit_entry(scope, server, entry, plan.secrets)
        except ValidationError as e:
            report.results.append(
                {**base, "status": "invalid", "error": validation_error_text(e)}
            )
            continue
        except ValueError as e:
            report.results.append({**base, "status": "error", "error": str(e)})
            continue
        if not created:
            report.results.append({**base, "status": "exists"})
            continue

        allocated.update(plan.refs)
        used_secret_names.update(plan.literals)
        report.secrets_created.extend(plan.literals)
        report.created += 1
        report.results.append({**base, "status": "created"})

    return report


async def _commit_entry(
    scope: ImportScope,
    server: Any,
    entry: Any,
    secrets: tuple[PlannedSecret, ...],
) -> bool:
    """Write one entry's vault secrets and its server row in a single transaction.

    Postgres owns the rollback: a vault cap, a duplicate, or a lost insert race
    aborts the whole entry, so there is no compensation to write (or to get
    wrong) on the way out.

    The values are checked here rather than by the writers: an extracted
    literal never passes through ``CreateSecretRequest``, and the model that
    would have caught a pasted newline sees the ``${vault:NAME}`` ref that
    replaced it.
    """
    from src.server.models.vault import validate_secret_value

    for secret in secrets:
        try:
            validate_secret_value(secret.value)
        except ValueError as e:
            raise ValueError(f"{secret.name}: {e}") from e
    try:
        async with get_db_connection() as conn:
            async with conn.transaction():
                for secret in secrets:
                    await scope.create_secret(conn, secret)
                if not await scope.persist(conn, server, entry):
                    raise _NameTaken
    except _NameTaken:
        return False
    return True


def plan_vault_extraction(
    server_name: str,
    config: dict[str, Any],
    *,
    allocated: Mapping[str, str],
    used_secret_names: set[str],
) -> EntryPlan:
    """Rewrite credential-looking env/header/arg literals in ``config`` to
    ``${vault:NAME}`` refs and return the secrets that must exist for them.

    Pure apart from ``config``: nothing is written, and neither ``allocated``
    (literal → ref, committed entries only) nor ``used_secret_names`` is
    mutated. Existing refs and benign literals are left alone.
    """
    secrets: list[PlannedSecret] = []
    refs: dict[str, str] = {}
    used = set(used_secret_names)

    def _ref_for(value: str, key_hint: str) -> str:
        # A stand-in is not a credential. Every vendor's docs print
        # ``<your-api-key>`` where the key goes, so two servers carrying that
        # spelling are two blanks, and sharing one vault secret between them
        # would send the key filled in for the first vendor to the second.
        # Within ONE server the same spelling is still one blank, which is what
        # the entry-local ``refs`` map keeps.
        ref = refs.get(value)
        if ref is None and not looks_like_placeholder(value):
            ref = allocated.get(value)
        if ref is not None:
            return ref
        name = vault_secret_name(server_name, key_hint, used)
        used.add(name)
        ref = f"${{vault:{name}}}"
        secrets.append(
            PlannedSecret(name, value, f"Imported with connector {server_name}")
        )
        refs[value] = ref
        return ref

    for section in ("env", "headers"):
        mapping = config.get(section)
        if not isinstance(mapping, dict):
            continue
        config[section] = {
            k: (
                _ref_for(v, str(k))
                if isinstance(v, str)
                and v.strip()
                # ``search``, not ``fullmatch``: a reference almost never fills
                # the field it sits in. ``Bearer ${vault:TOKEN}`` is the shape
                # an auth header actually takes, and reading it as a literal
                # vaults the template text — scheme word and all — under a new
                # name, so the entry authenticates with the string
                # ``Bearer ${vault:TOKEN}`` and the user's real secret is never
                # consulted. Every other lane already matches this way.
                and not VAULT_REF_RE.search(v)
                and looks_like_secret(str(k), v)
                else v
            )
            for k, v in mapping.items()
        }

    # stdio ``args`` is a list; the common credential shape is a single
    # ``--flag=VALUE`` token (or ``KEY=VALUE``). Split on the first ``=`` and
    # vault the value half when the flag or value looks secret, rewriting the
    # arg to ``--flag=${vault:NAME}`` (the generated client resolves refs in args).
    args = config.get("args")
    if isinstance(args, list):
        new_args: list[Any] = []
        for arg in args:
            if not isinstance(arg, str) or "=" not in arg:
                new_args.append(arg)
                continue
            flag, _, val = arg.partition("=")
            if (
                not val.strip()
                or VAULT_REF_RE.search(val)
                or not looks_like_secret(flag, val)
            ):
                new_args.append(arg)
                continue
            new_args.append(f"{flag}={_ref_for(val, flag.lstrip('-') or 'arg')}")
        # ``--token VALUE`` is the other half of the same credential, and the
        # generated client resolves a whole-element ref the same way it
        # resolves one inside ``--flag=``. Scanned on the original list, which
        # new_args mirrors index for index.
        for i, flag, val in iter_arg_flag_pairs(args):
            new_args[i] = _ref_for(val, flag or "arg")
        config["args"] = new_args

    return EntryPlan(secrets=tuple(secrets), refs=refs)
