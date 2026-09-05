"""Scope-neutral bulk import of standard ``mcpServers`` JSON.

Both import surfaces (per-workspace servers and the user-level Plugins
catalog) accept the same blob with inline credentials and run the same
per-entry gauntlet: skip reserved/duplicate names, enforce the scope's cap,
rewrite credential-looking literals to ``${vault:NAME}`` refs, validate — all
of it pure, writing nothing — then commit the entry's vault secrets and its
server row in ONE transaction. An entry either lands whole or not at all, and
its failure never touches the others. A scope supplies only what genuinely
differs (its cap, its prose, its two writers).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from pydantic import ValidationError

# looks_like_secret decides which import-time literals get auto-extracted into
# vault secrets; benign config (``MODE=prod``, ``LOG_LEVEL=ERROR``) stays an
# inline literal so we don't clutter the vault. Defined in mcp_sanitize so the
# redaction lanes apply the identical credential test.
from ptc_agent.core.mcp_sanitize import (
    VAULT_REF_RE,
    iter_arg_flag_pairs,
    looks_like_secret,
)
from src.server.database.pool import get_db_connection


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
ServerWriter = Callable[[Any, Any], Awaitable[bool]]


@dataclass(frozen=True)
class ImportScope:
    """What one import surface contributes to the shared per-entry loop."""

    reserved_names: set[str]
    existing_names: set[str]
    # Rows already counted against ``cap`` (the workspace surface counts only
    # its OWN servers, not inherited/marker rows, so it can't be derived from
    # ``existing_names``).
    current_count: int
    cap: int
    cap_message: str
    exists_message: str
    # Names already in the scope's vault — allocation must not collide with them.
    existing_secret_names: set[str]
    create_secret: SecretWriter
    persist: ServerWriter


@dataclass
class ImportReport:
    results: list[dict[str, Any]] = field(default_factory=list)
    created: int = 0
    secrets_created: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _EntryPlan:
    """One entry's fully-validated intent: what to write, and under what refs."""

    secrets: tuple[PlannedSecret, ...]
    # literal value → ``${vault:NAME}``, merged into the run-wide dedupe map
    # only once this entry commits.
    refs: dict[str, str]


class _NameTaken(Exception):
    """Raised to roll back an entry whose server name lost the insert race."""


async def run_mcp_import(parsed: list[Any], *, scope: ImportScope) -> ImportReport:
    """Import each parsed entry into ``scope``, reporting per-entry outcomes.

    A partial import is the normal case: every entry that fails is reported in
    place (``invalid`` / ``skipped`` / ``exists`` / ``error``) and the rest
    continue, so one bad server never aborts the blob.
    """
    from src.server.models.mcp_server import (
        McpServerInput,
        _format_validation_error,
    )

    report = ImportReport()
    seen_names: set[str] = set()
    # Committed state only: an identical token reused across servers is stored
    # once, but a ref is published here only after its entry lands — so a failed
    # entry can never leave a later one pointing at a secret that was never made.
    allocated: dict[str, str] = {}
    used_secret_names = set(scope.existing_secret_names)

    for entry in parsed:
        base = {
            "original_name": entry.original_name,
            "name": entry.name,
            "renamed": entry.renamed,
        }
        if entry.error:
            report.results.append({**base, "status": "invalid", "error": entry.error})
            continue
        if entry.name in scope.reserved_names:
            report.results.append(
                {**base, "status": "skipped", "reason": "collides with a name this build reserves"}
            )
            continue
        if entry.name in seen_names or entry.name in scope.existing_names:
            duplicate = entry.name in seen_names
            report.results.append({
                **base,
                "status": "skipped" if duplicate else "exists",
                "reason": (
                    "duplicate name after normalization"
                    if duplicate
                    else scope.exists_message
                ),
            })
            continue
        if scope.current_count + report.created >= scope.cap:
            report.results.append(
                {**base, "status": "error", "error": scope.cap_message}
            )
            continue

        seen_names.add(entry.name)
        config = dict(entry.config)
        plan = plan_vault_extraction(
            entry.name,
            config,
            allocated=allocated,
            used_secret_names=used_secret_names,
        )

        # An authenticated remote server needs its header even to list tools, so
        # discovery must resolve secrets — set it explicitly so the stored value
        # (and the UI toggle) is honest (matches discovery_should_use_secrets).
        if config.get("transport") in ("http", "sse"):
            headers = config.get("headers") or {}
            if any(VAULT_REF_RE.search(str(v)) for v in headers.values()):
                config["discovery_uses_secrets"] = True

        try:
            server = McpServerInput(**config)
        except ValidationError as e:
            report.results.append(
                {**base, "status": "invalid", "error": _format_validation_error(e)}
            )
            continue

        try:
            created = await _commit_entry(scope, server, plan.secrets)
        except ValueError as e:
            report.results.append({**base, "status": "error", "error": str(e)})
            continue
        if not created:
            report.results.append({**base, "status": "exists"})
            continue

        allocated.update(plan.refs)
        used_secret_names.update(s.name for s in plan.secrets)
        report.secrets_created.extend(s.name for s in plan.secrets)
        report.created += 1
        report.results.append({**base, "status": "created"})

    return report


async def _commit_entry(
    scope: ImportScope, server: Any, secrets: tuple[PlannedSecret, ...]
) -> bool:
    """Write one entry's vault secrets and its server row in a single transaction.

    Postgres owns the rollback: a vault cap, a duplicate, or a lost insert race
    aborts the whole entry, so there is no compensation to write (or to get
    wrong) on the way out.
    """
    try:
        async with get_db_connection() as conn:
            async with conn.transaction():
                for secret in secrets:
                    await scope.create_secret(conn, secret)
                if not await scope.persist(conn, server):
                    raise _NameTaken
    except _NameTaken:
        return False
    return True


def plan_vault_extraction(
    server_name: str,
    config: dict[str, Any],
    *,
    allocated: dict[str, str],
    used_secret_names: set[str],
) -> _EntryPlan:
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
        ref = allocated.get(value) or refs.get(value)
        if ref is not None:
            return ref
        name = vault_secret_name(server_name, key_hint, used)
        used.add(name)
        ref = f"${{vault:{name}}}"
        secrets.append(
            PlannedSecret(name, value, f"Imported with MCP server {server_name}")
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

    return _EntryPlan(secrets=tuple(secrets), refs=refs)
