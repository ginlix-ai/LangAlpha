"""Migration 055: MCP servers and vault secrets belong to the user, chosen per workspace.

A name now means one server and one secret across all of a user's workspaces.
Workspace-local server definitions (``workspace_mcp_servers`` rows with
``source='workspace'``) and the workspace vault are retired. What a workspace
still decides is which of the user's servers run there, through the rows that
already say so: a ``source='user'`` tombstone switches a user server off in one
workspace, a ``source='builtin'`` marker a built-in. Both stay.

Each local server in a live workspace becomes a user server under its own name,
or a suffixed one when the name is taken. An enabled user server runs in every
workspace of its user, so the promoted row is tombstoned everywhere except the
workspace that had it, and there too when it never ran there: switched off, or
under a running built-in's or a brokerage's name, which the resolver skipped;
the operator's ``agent_config.yaml`` says which built-ins ran. Such a copy is
also switched off on the account, where a switched-on row is probed from the
Plugins page and would dial an endpoint nothing used. A local row also
shadowed a same-named user server in its workspace; with the row gone that
server would start running there beside its promoted copy, so the original name
is tombstoned in that workspace as well. A promoted server also starts off in
workspaces created later, as one added from a workspace does now:
``user_mcp_servers.enabled_in_new_workspaces`` is new here, on for every row
already there and off for the promoted ones.

Each workspace secret is copied into the user vault with its ciphertext as it
is: both tiers encrypt with the same key and the same options. A name the user
already holds keeps its value. An equal value collapses into it; a different
one is copied under a suffixed name, and the references in the servers promoted
from that workspace follow the rename. Catalog servers and plugins resolved
against the user vault alone, so none of them could see a workspace's value
before, and a name one of them already asks for is treated as taken too.

Code cannot follow a rename. The sandbox's ``vault.get`` and ``load_env`` read
the workspace's merged set, where its own value won, so code there that asks
for a renamed secret by its old name now gets the account's value or none, and
code importing a renamed server's module no longer finds it. Each workspace's
renames are logged and kept in its ``config`` under ``mcp_migration_renames``,
where the product can show them to its user.

The tables stay, emptied, and refuse the writes that belonged to them: the
previous build keeps serving through the cutover and would otherwise write rows
nothing reads any more. It also creates workspaces without the tombstones a new
one starts with, so an insert trigger on ``workspaces``
(``trg_workspaces_start_mcp_selection``) writes them whichever build inserts.
Rolling back is ``alembic downgrade 054``, run from this build since the
previous one does not know 055, which drops that guard and leaves the data as
055 wrote it, then a redeploy of the previous build. It reads the result
correctly, since it already merges the user tier and honours tombstones. The
new column and the insert trigger stay too: the previous build never reads the
one and still needs the other. The migration that drops the retired tables,
once no build before 055 can serve, drops the guards and the insert trigger.
"""

# No ``from __future__ import annotations``: alembic executes this file without
# registering it in sys.modules, and @dataclass resolves string annotations
# through that registry.
import json
import logging
import os
import random
import re
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import yaml

from alembic import op
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

revision = "055"
down_revision = "054"
branch_labels = None
depends_on = None

logger = logging.getLogger("alembic.runtime.migration")

# Restated rather than imported: a migration runs without the application
# stack. Server and secret names share this limit.
_NAME_MAX = 64
_VAULT_REF_RE = re.compile(r"\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}")
# The fields the sandbox client resolves vault references in.
_RESOLVED_FIELDS = ("args", "url", "env", "headers")
_MAX_SECRETS_PER_USER = 50
_MAX_SERVERS_PER_USER = 100
# A bound, so a name rule that can never be met fails the migration instead
# of spinning under its table locks.
_SUFFIX_TRIES = 10_000

# Pinned as 030 pins its names, and unioned with a scan of plugins/ so a bundle
# that ships after this revision is still reserved when an older database
# catches up.
_BUNDLED_SERVERS = frozenset({
    "yf_price", "yf_market", "yf_fundamentals", "yf_analysis",
    "x_api", "scrape",
    "price_data", "fundamentals", "macro", "options",
})
_BROKERAGES = frozenset({"robinhood", "ibkr", "webull", "moomoo"})
# A server name becomes a Python module beside the sandbox runtime's own
# mcp_client.py, so these would shadow it or fail to import; a dunder is
# refused separately. Soft keywords import fine. Python 3.13's keyword.kwlist
# is written out so the plan does not move with the interpreter running it.
_MODULE_NAMES = frozenset({
    "mcp_client",
    "False", "None", "True", "and", "as", "assert", "async", "await", "break",
    "class", "continue", "def", "del", "elif", "else", "except", "finally",
    "for", "from", "global", "if", "import", "in", "is", "lambda", "nonlocal",
    "not", "or", "pass", "raise", "return", "try", "while", "with", "yield",
})
_TRANSPORTS = frozenset({"stdio", "sse", "http"})
_EXPOSURE_MODES = frozenset({"summary", "detailed"})


# ---------------------------------------------------------------------------
# The plan: pure functions over plain rows, deterministic in their input order
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Workspace:
    id: Any
    user_id: str
    status: str

    @property
    def live(self) -> bool:
        # 'flash' is the sandbox-less workspace, as live as any other.
        return self.status != "deleted"


@dataclass(frozen=True)
class Fork:
    id: Any
    workspace_id: Any
    name: str
    enabled: bool
    config: Any
    created_at: Any
    updated_at: Any


@dataclass(frozen=True)
class Promotion:
    fork_id: Any
    user_id: str
    workspace_id: Any
    source_name: str
    name: str
    columns: dict[str, Any]
    created_at: Any
    updated_at: Any
    # Whether it ran in its workspace. One that never did stays off on the
    # account, so nothing dials it until the user switches it on.
    enabled: bool
    refs_rewritten: bool = False


@dataclass(frozen=True)
class ForkPlan:
    promotions: list[Promotion]
    # (workspace_id, name), each a source='user' enabled=false row.
    tombstones: list[tuple[Any, str]]
    # Local rows of deleted workspaces: removed, never promoted.
    dropped: list[Fork]


@dataclass(frozen=True)
class WorkspaceSecret:
    id: Any
    workspace_id: Any
    name: str
    created_at: Any
    # Keyed digest of the plaintext, set only where a clash needs comparing.
    digest: str | None = None


@dataclass(frozen=True)
class VaultPlan:
    # (workspace secret id, user id, name in the user vault).
    copies: list[tuple[Any, str, str]]
    # workspace_id -> {name in that workspace: name in the user vault}.
    renames: dict[Any, dict[str, str]]
    promotions: list[Promotion]
    # user_id -> (secrets it held, secrets copied in), for users now over the
    # cap a create enforces.
    over_cap: dict[str, tuple[int, int]]


def _created_order(row: Fork | WorkspaceSecret) -> tuple[Any, str]:
    return row.created_at, str(row.id)


def _suffixed(base: str, n: int) -> str:
    # "_" plus "_2" would start with "__", which no server name may.
    suffix = f"_{n}" if base.strip("_") else str(n)
    return base[: _NAME_MAX - len(suffix)] + suffix


def _candidates(base: str) -> Iterator[str]:
    yield base
    for n in range(2, _SUFFIX_TRIES):
        yield _suffixed(base, n)


def _module_safe(name: str) -> bool:
    return name not in _MODULE_NAMES and not name.startswith("__")


def _legal_base(name: str) -> str:
    """The name a fork is promoted under before any clash is considered."""
    base = re.sub(r"[^0-9A-Za-z_]", "_", name or "")
    if base.startswith("__"):
        base = base.lstrip("_")
    if base[:1].isdigit():
        base = f"_{base}"
    if base in _MODULE_NAMES:
        base = f"{base}_server"
    return base[:_NAME_MAX] or "server"


def _catalog_columns(config: Any) -> dict[str, Any]:
    """A fork's config blob as ``user_mcp_servers`` columns, with its defaults."""
    cfg = config if isinstance(config, Mapping) else {}

    def string(key: str) -> str | None:
        value = cfg.get(key)
        return value if isinstance(value, str) and value else None

    def mapping(key: str) -> dict[str, Any]:
        value = cfg.get(key)
        return dict(value) if isinstance(value, Mapping) else {}

    args = cfg.get("args")
    transport = string("transport")
    mode = string("tool_exposure_mode")
    return {
        "transport": transport if transport in _TRANSPORTS else "stdio",
        "command": string("command"),
        "args": list(args) if isinstance(args, list) else [],
        "url": string("url"),
        "env": mapping("env"),
        "headers": mapping("headers"),
        "description": string("description") or "",
        "instruction": string("instruction") or "",
        "tool_exposure_mode": mode if mode in _EXPOSURE_MODES else "summary",
        "discovery_uses_secrets": bool(cfg.get("discovery_uses_secrets")),
    }


def _fork_name(
    fork: Fork, *, taken: set[str], reserved: frozenset[str], others: set[str]
) -> str:
    # ``others`` holds every fork name of the user: a suffix never takes the
    # name another fork arrives with, so that one keeps it.
    for name in _candidates(_legal_base(fork.name)):
        if name in reserved or name in taken or not _module_safe(name):
            continue
        if name != fork.name and name in others:
            continue
        return name
    raise RuntimeError(f"055: no free name for MCP server {fork.name!r}")


def plan_forks(
    forks: Iterable[Fork],
    workspaces: Iterable[Workspace],
    *,
    user_servers: Mapping[str, Iterable[str]],
    oauth_names: Mapping[str, Iterable[str]],
    markers: Mapping[Any, Iterable[str]],
    reserved: Iterable[str],
    skipped: Iterable[str],
) -> ForkPlan:
    """Promote every local server of a live workspace into its user's tier.

    ``markers`` is each workspace's non-fork rows: a stale tombstone there
    would switch the promoted server off in the one workspace that had it.
    ``reserved`` is every name a promoted server may not take, ``skipped``
    the narrower set whose local rows the resolver never ran.
    """
    by_id = {w.id: w for w in workspaces}
    live_by_user: dict[str, list[Any]] = defaultdict(list)
    for workspace in sorted(by_id.values(), key=lambda w: str(w.id)):
        if workspace.live:
            live_by_user[workspace.user_id].append(workspace.id)
    reserved = frozenset(reserved)
    skipped = frozenset(skipped)

    live: list[Fork] = []
    dropped: list[Fork] = []
    for fork in sorted(forks, key=_created_order):
        (live if by_id[fork.workspace_id].live else dropped).append(fork)
    originals: dict[str, set[str]] = defaultdict(set)
    for fork in live:
        originals[by_id[fork.workspace_id].user_id].add(fork.name)

    claimed: dict[str, set[str]] = defaultdict(set)
    promotions: list[Promotion] = []
    tombstones: set[tuple[Any, str]] = set()
    for fork in live:
        user = by_id[fork.workspace_id].user_id
        # An OAuth connection is keyed by server name, so a server promoted
        # onto one would be bound to somebody else's token.
        held = {*user_servers.get(user, ()), *oauth_names.get(user, ())}
        name = _fork_name(
            fork,
            taken=held | claimed[user] | set(markers.get(fork.workspace_id, ())),
            reserved=reserved,
            others=originals[user],
        )
        claimed[user].add(name)
        # The resolver skipped a local row under a running built-in's name or
        # a brokerage's rather than let it shadow anything, so such a row never
        # ran, even enabled, and shadowed nothing. Its copy stays off where it
        # was, like a disabled one: the rename must not start what was inert.
        # Under a built-in the operator switched off, a row ran like any other.
        ran = fork.enabled and fork.name not in skipped
        promotions.append(Promotion(
            fork_id=fork.id,
            user_id=user,
            workspace_id=fork.workspace_id,
            source_name=fork.name,
            name=name,
            columns=_catalog_columns(fork.config),
            created_at=fork.created_at,
            updated_at=fork.updated_at,
            enabled=ran,
        ))
        for workspace_id in live_by_user[user]:
            if workspace_id != fork.workspace_id or not ran:
                tombstones.add((workspace_id, name))
        # A row the resolver skipped shadowed nothing; any other, on or off,
        # kept its user server out of this workspace.
        if fork.name in held and fork.name not in skipped:
            tombstones.add((fork.workspace_id, fork.name))

    return ForkPlan(
        promotions=promotions,
        tombstones=sorted(tombstones, key=lambda t: (str(t[0]), t[1])),
        dropped=dropped,
    )


def secrets_needing_compare(
    secrets: Iterable[WorkspaceSecret],
    workspaces: Iterable[Workspace],
    user_secret_names: Mapping[str, Iterable[str]],
) -> set[str]:
    """Users with a workspace secret whose name is already held, the only ones
    whose values are worth decrypting."""
    by_id = {w.id: w for w in workspaces}
    seen: dict[str, set[str]] = defaultdict(set)
    for user, names in user_secret_names.items():
        seen[user].update(names)
    clashing: set[str] = set()
    for secret in sorted(secrets, key=_created_order):
        workspace = by_id[secret.workspace_id]
        if not workspace.live:
            continue
        if secret.name in seen[workspace.user_id]:
            clashing.add(workspace.user_id)
        seen[workspace.user_id].add(secret.name)
    return clashing


def _vault_names(value: Any) -> Iterator[str]:
    """Every ``${vault:NAME}`` in any string anywhere inside ``value``."""
    if isinstance(value, str):
        yield from _VAULT_REF_RE.findall(value)
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _vault_names(item)
    elif isinstance(value, list):
        for item in value:
            yield from _vault_names(item)


def _declared_secrets(manifest: Any) -> Iterator[str]:
    """The names a plugin's manifest declares, granted at install whether or
    not a server row references them yet."""
    extensions = manifest.get("extensions") if isinstance(manifest, Mapping) else None
    payload = extensions.get("ai.langalpha") if isinstance(extensions, Mapping) else None
    secrets = payload.get("secrets") if isinstance(payload, Mapping) else None
    for secret in secrets if isinstance(secrets, list) else ():
        name = secret.get("name") if isinstance(secret, Mapping) else None
        if isinstance(name, str):
            yield name


def _secret_name(
    secret: WorkspaceSecret,
    held: Mapping[str, str | None],
    others: set[str],
    asked: set[str],
) -> str:
    # Walks the suffix chain, so a value already copied under a suffix is found
    # again rather than copied twice. An unknown digest never matches.
    for name in _candidates(secret.name):
        if name in held:
            if secret.digest is not None and held[name] == secret.digest:
                return name
            continue
        if name in asked:
            continue
        if name != secret.name and name in others:
            continue
        return name
    raise RuntimeError(f"055: no free name for vault secret {secret.name!r}")


def _rewrite_refs(columns: dict[str, Any], renames: Mapping[str, str]) -> dict[str, Any]:
    # One substitution per field, so a chain like A->B, B->C cannot compose,
    # and whole tokens only: ${vault:KEY} never touches ${vault:KEY_B}.
    def sub(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return _VAULT_REF_RE.sub(
            lambda m: "${vault:" + renames.get(m.group(1), m.group(1)) + "}", value
        )

    return {
        **columns,
        "args": [sub(arg) for arg in columns["args"]],
        "url": sub(columns["url"]),
        "env": {key: sub(value) for key, value in columns["env"].items()},
        "headers": {key: sub(value) for key, value in columns["headers"].items()},
    }


def plan_vault(
    secrets: Iterable[WorkspaceSecret],
    workspaces: Iterable[Workspace],
    *,
    user_secrets: Mapping[str, Mapping[str, str | None]],
    referenced: Mapping[str, Iterable[str]],
    promotions: Iterable[Promotion],
    cap: int = _MAX_SECRETS_PER_USER,
) -> VaultPlan:
    """Merge every live workspace vault into its user's.

    ``user_secrets`` maps each user's existing names to their digests (None
    when not compared). ``referenced`` is each user's names that a catalog
    server or a plugin asks for; a promoted server asking for a name its own
    workspace lacks joins them, since that too read the user vault. The cap is
    only reported: a create enforces it, and failing the deploy over it would
    strand every other user's move.
    """
    by_id = {w.id: w for w in workspaces}
    live = [
        s for s in sorted(secrets, key=_created_order)
        if by_id[s.workspace_id].live
    ]
    originals: dict[str, set[str]] = defaultdict(set)
    local: dict[Any, set[str]] = defaultdict(set)
    for secret in live:
        originals[by_id[secret.workspace_id].user_id].add(secret.name)
        local[secret.workspace_id].add(secret.name)
    promotions = list(promotions)
    asked: dict[str, set[str]] = defaultdict(set)
    for user, names in referenced.items():
        asked[user].update(names)
    for promotion in promotions:
        resolved = [promotion.columns[key] for key in _RESOLVED_FIELDS]
        asked[promotion.user_id].update(
            name for name in _vault_names(resolved)
            if name not in local[promotion.workspace_id]
        )

    held: dict[str, dict[str, str | None]] = defaultdict(dict)
    for user, names in user_secrets.items():
        held[user].update(names)
    added: dict[str, int] = defaultdict(int)
    copies: list[tuple[Any, str, str]] = []
    renames: dict[Any, dict[str, str]] = defaultdict(dict)
    for secret in live:
        user = by_id[secret.workspace_id].user_id
        name = _secret_name(secret, held[user], originals[user], asked[user])
        if name not in held[user]:
            held[user][name] = secret.digest
            copies.append((secret.id, user, name))
            added[user] += 1
        if name != secret.name:
            renames[secret.workspace_id][secret.name] = name

    over_cap = {
        user: (len(held[user]) - added[user], added[user])
        for user in sorted(added)
        if len(held[user]) > cap
    }

    rewritten: list[Promotion] = []
    for promotion in promotions:
        mapping = renames.get(promotion.workspace_id)
        columns = _rewrite_refs(promotion.columns, mapping) if mapping else None
        if columns is not None and columns != promotion.columns:
            promotion = replace(promotion, columns=columns, refs_rewritten=True)
        rewritten.append(promotion)
    return VaultPlan(
        copies=copies,
        renames={ws: dict(names) for ws, names in renames.items()},
        promotions=rewritten,
        over_cap=over_cap,
    )


def plan_tool_schemas(
    promotions: Iterable[Promotion], dropped: Iterable[Fork]
) -> tuple[list[tuple[Any, str]], list[tuple[Any, str, str]]]:
    """Which cached discovery snapshots to delete, and which to rename.

    The fingerprint covers neither the name nor the source, so a promoted
    server's snapshots in its workspace stay valid under its new name. A
    rewritten vault ref is in the fingerprint, so those are deleted instead.
    """
    deletes = {(fork.workspace_id, fork.name) for fork in dropped}
    renames: list[tuple[Any, str, str]] = []
    for promotion in promotions:
        if promotion.refs_rewritten:
            deletes.add((promotion.workspace_id, promotion.source_name))
            deletes.add((promotion.workspace_id, promotion.name))
        elif promotion.name != promotion.source_name:
            renames.append(
                (promotion.workspace_id, promotion.source_name, promotion.name)
            )
    return sorted(deletes, key=lambda d: (str(d[0]), d[1])), renames


def plan_renames(
    promotions: Iterable[Promotion], secret_renames: Mapping[Any, Mapping[str, str]]
) -> dict[Any, dict[str, dict[str, str]]]:
    """Per workspace, each server and secret name its code may still use,
    mapped to the name it has now."""
    renames: dict[Any, dict[str, dict[str, str]]] = defaultdict(
        lambda: {"secrets": {}, "servers": {}}
    )
    for promotion in promotions:
        if promotion.name != promotion.source_name:
            renames[promotion.workspace_id]["servers"][promotion.source_name] = (
                promotion.name
            )
    for workspace_id, names in secret_renames.items():
        renames[workspace_id]["secrets"].update(names)
    return dict(renames)


# ---------------------------------------------------------------------------
# Reads and writes
# ---------------------------------------------------------------------------

# Every table 055 writes, and user_plugins, whose manifests reserve secret
# names. EXCLUSIVE lets plain reads through and nothing else; on workspaces it
# also keeps out the row locks the version bump would wait on.
_LOCKS_SQL = (
    "LOCK TABLE workspaces IN EXCLUSIVE MODE NOWAIT",
    "LOCK TABLE workspace_mcp_servers, workspace_vault_secrets, "
    "user_mcp_servers, user_vault_secrets, workspace_mcp_tool_schemas, "
    "user_plugins IN EXCLUSIVE MODE NOWAIT",
)
_LOCK_BUDGET_S = 30

_FORKS_SQL = """
    SELECT workspace_mcp_server_id AS id, workspace_id, name, enabled, config,
           created_at, updated_at
      FROM workspace_mcp_servers
     WHERE source = 'workspace'
"""
_SECRETS_SQL = """
    SELECT workspace_vault_secret_id AS id, workspace_id, name, created_at
      FROM workspace_vault_secrets
"""
# Every workspace of every user who owns one of the rows above.
_WORKSPACES_SQL = """
    SELECT workspace_id AS id, user_id, status
      FROM workspaces
     WHERE user_id IN (
         SELECT user_id FROM workspaces
          WHERE workspace_id = ANY(CAST(:ids AS uuid[]))
     )
"""
_USER_SERVERS_SQL = """
    SELECT user_id AS owner, name FROM user_mcp_servers
     WHERE user_id = ANY(CAST(:users AS text[]))
"""
_OAUTH_NAMES_SQL = """
    SELECT user_id AS owner, server_name AS name FROM user_mcp_oauth_connections
     WHERE user_id = ANY(CAST(:users AS text[]))
"""
_USER_SECRETS_SQL = """
    SELECT user_id AS owner, name FROM user_vault_secrets
     WHERE user_id = ANY(CAST(:users AS text[]))
"""
# What already resolves against the user vault, switched on or not: every
# catalog row, a plugin's included, and each plugin's own documents.
_USER_TIER_REFS_SQL = """
    SELECT user_id AS owner, jsonb_build_array(env, headers, args, url) AS doc,
           CAST(NULL AS jsonb) AS manifest
      FROM user_mcp_servers
     WHERE user_id = ANY(CAST(:users AS text[]))
    UNION ALL
    SELECT user_id, mcp_document, manifest
      FROM user_plugins
     WHERE user_id = ANY(CAST(:users AS text[]))
"""
_MARKERS_SQL = """
    SELECT workspace_id AS owner, name FROM workspace_mcp_servers
     WHERE source <> 'workspace'
       AND workspace_id = ANY(CAST(:ids AS uuid[]))
"""
# A built-in the operator configured once but no longer lists is visible only
# through what users did with it: a workspace marker or an account-wide
# disable. Such a name is reserved, but proves nothing ran under it.
_BUILTIN_NAMES_SQL = """
    SELECT name FROM workspace_mcp_servers WHERE source = 'builtin'
    UNION
    SELECT name FROM user_mcp_builtin_disables WHERE kind = 'server'
"""
# A per-run salt: equal values still meet, and what leaves the database is no
# stable fingerprint of a secret.
_DIGESTS_SQL = """
    SELECT u.user_id AS owner, u.name, CAST(NULL AS uuid) AS id,
           encode(hmac(pgp_sym_decrypt(u.value, :key), :salt, 'sha256'), 'hex')
               AS digest
      FROM user_vault_secrets u
     WHERE u.user_id = ANY(CAST(:users AS text[]))
    UNION ALL
    SELECT ws.user_id, s.name, s.workspace_vault_secret_id,
           encode(hmac(pgp_sym_decrypt(s.value, :key), :salt, 'sha256'), 'hex')
      FROM workspace_vault_secrets s
      JOIN workspaces ws ON ws.workspace_id = s.workspace_id
     WHERE ws.user_id = ANY(CAST(:users AS text[])) AND ws.status <> 'deleted'
"""

_NEW_WORKSPACES_COLUMN_SQL = """
    ALTER TABLE user_mcp_servers
        ADD COLUMN IF NOT EXISTS enabled_in_new_workspaces BOOLEAN NOT NULL DEFAULT TRUE
"""
_INSERT_SERVER_SQL = """
    INSERT INTO user_mcp_servers
        (user_id, name, transport, command, args, url, env, headers,
         description, instruction, tool_exposure_mode, discovery_uses_secrets,
         enabled, enabled_in_new_workspaces, created_at, updated_at)
    VALUES (:user_id, :name, :transport, :command, CAST(:args AS jsonb), :url,
            CAST(:env AS jsonb), CAST(:headers AS jsonb), :description,
            :instruction, :tool_exposure_mode, :discovery_uses_secrets,
            :enabled, :enabled_in_new_workspaces, :created_at, :updated_at)
"""
# Once the local rows are gone, what can still sit under a planned name is a
# stale user-sourced row: one already off is left alone, one that claims to be
# on is switched off. A built-in marker never does, its name being reserved.
_TOMBSTONE_SQL = """
    INSERT INTO workspace_mcp_servers (workspace_id, name, source, enabled, config)
    VALUES (:workspace_id, :name, 'user', FALSE, NULL)
    ON CONFLICT (workspace_id, name) DO UPDATE SET enabled = FALSE
     WHERE workspace_mcp_servers.source = 'user' AND workspace_mcp_servers.enabled
"""
_SCHEMA_DELETE_SQL = """
    DELETE FROM workspace_mcp_tool_schemas
     WHERE workspace_id = :workspace_id AND server_name = :name
"""
_SCHEMA_RENAME_SQL = """
    UPDATE workspace_mcp_tool_schemas SET server_name = :new
     WHERE workspace_id = :workspace_id AND server_name = :old
"""
# The ciphertext moves inside the database and never through this process.
_COPY_SECRET_SQL = """
    INSERT INTO user_vault_secrets
        (user_id, name, value, description, created_at, updated_at)
    SELECT CAST(:user_id AS text), CAST(:name AS text), s.value, s.description,
           s.created_at, s.updated_at
      FROM workspace_vault_secrets s
     WHERE s.workspace_vault_secret_id = :id
"""
# What the plan wrote, read back before the source rows are deleted. Every
# planned name was free, so each match is a row this run wrote; a secret must
# also carry its source's ciphertext.
_SERVERS_LANDED_SQL = """
    SELECT count(*) AS n
      FROM unnest(CAST(:users AS text[]), CAST(:names AS text[])) AS p(user_id, name)
      JOIN user_mcp_servers s ON s.user_id = p.user_id AND s.name = p.name
"""
_SECRETS_LANDED_SQL = """
    SELECT count(*) AS n
      FROM unnest(CAST(:ids AS uuid[]), CAST(:users AS text[]),
                  CAST(:names AS text[])) AS p(id, user_id, name)
      JOIN workspace_vault_secrets w ON w.workspace_vault_secret_id = p.id
      JOIN user_vault_secrets u
        ON u.user_id = p.user_id AND u.name = p.name AND u.value = w.value
"""
# Merged into what is there, so an upgrade after a downgrade keeps what the
# first one recorded.
_RENAMES_SQL = """
    UPDATE workspaces
       SET config = jsonb_set(
               COALESCE(config, CAST('{}' AS jsonb)),
               '{mcp_migration_renames}',
               jsonb_build_object(
                   'secrets',
                   COALESCE(config #> '{mcp_migration_renames,secrets}',
                            CAST('{}' AS jsonb)) || CAST(:secrets AS jsonb),
                   'servers',
                   COALESCE(config #> '{mcp_migration_renames,servers}',
                            CAST('{}' AS jsonb)) || CAST(:servers AS jsonb)
               )
           )
     WHERE workspace_id = :workspace_id
"""
# The previous build serves until its drain ends, and its workspace add, adopt,
# import and vault writes would land in rows nothing reads now; adopt would
# then delete the catalog row it moved. Refusing them fails the request
# instead of losing the server or the secret. Tombstones and built-in markers
# are still the new build's own rows.
_GUARD_FUNCTION_SQL = """
    CREATE OR REPLACE FUNCTION workspace_tier_retired_guard()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        RAISE EXCEPTION '% is retired by migration 055; write to % instead',
            TG_TABLE_NAME, TG_ARGV[0]
            USING ERRCODE = 'feature_not_supported';
    END;
    $$
"""
_GUARD_TRIGGERS_SQL = (
    """
    CREATE TRIGGER trg_workspace_mcp_servers_retired
    BEFORE INSERT OR UPDATE ON workspace_mcp_servers
    FOR EACH ROW WHEN (NEW.source = 'workspace')
    EXECUTE FUNCTION workspace_tier_retired_guard('user_mcp_servers')
    """,
    """
    CREATE TRIGGER trg_workspace_vault_secrets_retired
    BEFORE INSERT OR UPDATE ON workspace_vault_secrets
    FOR EACH ROW
    EXECUTE FUNCTION workspace_tier_retired_guard('user_vault_secrets')
    """,
)
# ``start_new_workspace_selection`` for a build that never calls it, so a
# promoted server does not start in the previous build's new workspaces.
# The lock is the one a server create holds to its commit, already held by
# the new build. The previous build takes it after its insert, which can
# deadlock and fail a request, rather than miss a server created meanwhile.
_SELECTION_FUNCTION_SQL = """
    CREATE OR REPLACE FUNCTION workspaces_start_mcp_selection()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        PERFORM pg_advisory_xact_lock(hashtext(NEW.user_id::text));
        INSERT INTO workspace_mcp_servers
            (workspace_id, name, source, enabled, config, created_at, updated_at)
        SELECT NEW.workspace_id, s.name, 'user', FALSE, NULL, NOW(), NOW()
          FROM user_mcp_servers s
         WHERE s.user_id = NEW.user_id AND NOT s.enabled_in_new_workspaces
        ON CONFLICT (workspace_id, name) DO NOTHING;
        RETURN NULL;
    END;
    $$
"""
# Dropped first: a downgrade keeps the trigger, so an upgrade after one finds it.
_SELECTION_TRIGGER_SQL = (
    "DROP TRIGGER IF EXISTS trg_workspaces_start_mcp_selection ON workspaces",
    """
    CREATE TRIGGER trg_workspaces_start_mcp_selection
    AFTER INSERT ON workspaces
    FOR EACH ROW EXECUTE FUNCTION workspaces_start_mcp_selection()
    """,
)


def _rows(bind, sql: str, **params: Any) -> list[Mapping[str, Any]]:
    return list(bind.execute(text(sql), params).mappings().all())


def _grouped(rows: Iterable[Mapping[str, Any]]) -> dict[Any, set[str]]:
    grouped: dict[Any, set[str]] = defaultdict(set)
    for row in rows:
        grouped[row["owner"]].add(row["name"])
    return dict(grouped)


def _bundled_server_names() -> frozenset[str]:
    names = set(_BUNDLED_SERVERS)
    root = Path(__file__).resolve().parents[2] / "plugins"
    for manifest in sorted(root.glob("*/mcp.json")):
        try:
            servers = json.loads(manifest.read_text()).get("mcpServers") or {}
            names.update(name for name in servers if isinstance(name, str))
        except (OSError, ValueError, AttributeError, TypeError):
            continue
    return frozenset(names)


def _substituted(value: Any) -> Any:
    # The runtime's substitute_env_vars, frozen, over every string value the
    # way its loader walks them: ``${VAR}`` anywhere, left as written when
    # unset, then a whole-string ``$VAR``, which falls back to the bare name.
    if isinstance(value, dict):
        return {key: _substituted(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_substituted(item) for item in value]
    if not isinstance(value, str):
        return value
    value = re.sub(r"\$\{([^}]+)\}", lambda m: os.getenv(m.group(1), m.group(0)), value)
    bare = value[1:]
    if value.startswith("$") and not value.startswith("${") and bare.isidentifier():
        return os.getenv(bare, bare)
    return value


def _agent_config() -> Any:
    # Searched and substituted as the runtime does, so this is the document the
    # resolver read; restated, since a migration runs without the app stack.
    explicit = os.getenv("PTC_CONFIG_FILE")
    cwd = Path.cwd()
    root = next((p for p in (cwd, *cwd.parents) if (p / ".git").exists()), cwd)
    candidates = [Path(explicit)] if explicit else []
    candidates += [
        p / "agent_config.yaml" for p in (cwd, root, Path.home() / ".ptc-agent")
    ]
    path = next((p for p in candidates if p.is_file()), None)
    if path is None:
        return None
    return _substituted(_with_overlay(path, yaml.safe_load(path.read_text())))


def _with_overlay(path: Path, config: Any) -> Any:
    # The runtime layers <stem>.<APP_ENV><suffix> over the file it found before
    # substituting, so an overlay's ``enabled`` is the one the backend acted on.
    app_env = os.getenv("APP_ENV", "").strip()
    overlay = path.with_name(f"{path.stem}.{app_env}{path.suffix}")
    if not app_env or not overlay.is_file():
        return config
    return _merged(config, yaml.safe_load(overlay.read_text()) or {})


def _merged(base: Any, overlay: Any) -> Any:
    # Maps merge, anything else (``mcp.servers`` included) replaces, and a null
    # sets nothing at any depth.
    if not isinstance(overlay, Mapping):
        return overlay
    merged = dict(base) if isinstance(base, Mapping) else {}
    for key, value in overlay.items():
        if value is not None:
            merged[key] = _merged(merged.get(key), value)
    return merged


_TRUE_WORDS = frozenset({"1", "on", "t", "true", "y", "yes"})
_FALSE_WORDS = frozenset({"0", "off", "f", "false", "n", "no"})


def _as_bool(value: Any, name: str) -> bool:
    """``enabled`` as Pydantic 2's lax mode read it into ``MCPServerConfig``.

    A value it refuses stopped the previous build at startup, so the config
    read here is not the one that ran, and either guess could start a server
    that never ran or stop one that did.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.lower() in _TRUE_WORDS | _FALSE_WORDS:
        return value.lower() in _TRUE_WORDS
    raise RuntimeError(
        f"055: agent_config.yaml sets enabled={value!r} on MCP server {name!r}, "
        "which the backend refuses at startup; run the upgrade with the "
        "environment the backend runs with"
    )


def _builtin_servers() -> dict[str, bool]:
    """Each configured built-in name and whether it ran, merged by name the way
    the runtime lays ``mcp.servers`` over the bundled servers."""
    config = _agent_config()
    mcp = config.get("mcp") if isinstance(config, Mapping) else None
    servers = mcp.get("servers") if isinstance(mcp, Mapping) else None
    runs = dict.fromkeys(_bundled_server_names(), True)
    for server in servers if isinstance(servers, list) else ():
        name = server.get("name") if isinstance(server, Mapping) else None
        if isinstance(name, str):
            runs[name] = (
                _as_bool(server["enabled"], name)
                if "enabled" in server
                else runs.get(name, True)
            )
    return runs


def _user_tier_refs(bind, users: list[str]) -> dict[str, set[str]]:
    refs: dict[str, set[str]] = defaultdict(set)
    for row in _rows(bind, _USER_TIER_REFS_SQL, users=users):
        refs[row["owner"]].update(_vault_names(row["doc"]))
        refs[row["owner"]].update(_vault_names(row["manifest"]))
        refs[row["owner"]].update(_declared_secrets(row["manifest"]))
    return dict(refs)


def _secret_digests(
    bind, users: list[str], key: str | None
) -> tuple[dict[tuple[str, str], str], dict[Any, str]]:
    """Keyed digests of both tiers' values for ``users``, or none at all.

    No key, or one that cannot decrypt what is stored, leaves every clash
    uncompared, and an uncompared clash is suffixed: keeping both values is
    always safe, merging two it could not read is not.
    """
    if not users:
        return {}, {}
    if not key:
        logger.warning(
            "055: BYOK_ENCRYPTION_KEY is not set; the clashing names of %d "
            "user(s) are suffixed without comparing values",
            len(users),
        )
        return {}, {}
    salt = os.urandom(16).hex()
    try:
        with bind.begin_nested():
            rows = _rows(bind, _DIGESTS_SQL, key=key, salt=salt, users=users)
    except DBAPIError as e:
        # The SQLSTATE only: the error's text carries the bound key.
        logger.warning(
            "055: BYOK_ENCRYPTION_KEY is set but does not decrypt the stored "
            "vault secrets (SQLSTATE %s); the clashing names of %d user(s) are "
            "suffixed without comparing values",
            getattr(e.orig, "sqlstate", None),
            len(users),
        )
        return {}, {}
    user_digests = {
        (row["owner"], row["name"]): row["digest"]
        for row in rows if row["id"] is None
    }
    workspace_digests = {
        row["id"]: row["digest"] for row in rows if row["id"] is not None
    }
    return user_digests, workspace_digests


def _check_landed(bind, sql: str, what: str, planned: int, **params: Any) -> None:
    """Abort the upgrade when fewer copies landed than were planned.

    A copy that selects nothing, a secret whose row is missing say, inserts
    nothing without an error, and the delete that follows would lose it.
    """
    landed = _rows(bind, sql, **params)[0]["n"]
    if landed != planned:
        raise RuntimeError(
            f"055 planned {planned} {what} but {landed} landed; the upgrade "
            "is aborted and rolls back"
        )


def _arrows(names: Mapping[str, str]) -> str:
    return ", ".join(f"{old} -> {new}" for old, new in sorted(names.items())) or "none"


def _server_params(promotion: Promotion) -> dict[str, Any]:
    columns = promotion.columns
    return {
        "user_id": promotion.user_id,
        "name": promotion.name,
        "transport": columns["transport"],
        "command": columns["command"],
        "args": json.dumps(columns["args"]),
        "url": columns["url"],
        "env": json.dumps(columns["env"]),
        "headers": json.dumps(columns["headers"]),
        "description": columns["description"],
        "instruction": columns["instruction"],
        "tool_exposure_mode": columns["tool_exposure_mode"],
        "discovery_uses_secrets": columns["discovery_uses_secrets"],
        "enabled": promotion.enabled,
        # It ran in one workspace, so a workspace created later starts it off.
        "enabled_in_new_workspaces": False,
        "created_at": promotion.created_at,
        "updated_at": promotion.updated_at,
    }


def _lock_tables(bind) -> None:
    """Take every table lock 055 needs at once, without waiting, or none.

    The plan is made from one read and applied in the same transaction, so
    nothing may write these tables in between: a local row or workspace secret
    the previous build adds after the read would be deleted without being
    carried over, and a user-tier write could take a name the plan just handed
    out. A plugin installed after the read could declare a name the plan
    copies a workspace secret to, and inherit a secret it never had. Plain
    reads carry on. OAuth connections need no lock, since connecting one needs
    a user_mcp_servers row first.

    The previous build reaches these tables in no single order: a catalog
    delete takes user_mcp_servers before workspace_mcp_servers, and nearly
    every write ends on the workspaces version bump. Any fixed order would
    wait on one of them while holding what it needs next, so an attempt that
    finds one busy lets go of all of them and tries again. ``workspaces`` is
    held against row locks too, so the version bump never waits on a row.
    """
    deadline = time.monotonic() + _LOCK_BUDGET_S
    while True:
        try:
            with bind.begin_nested():
                for statement in _LOCKS_SQL:
                    bind.execute(text(statement))
            return
        except DBAPIError as e:
            if getattr(e.orig, "sqlstate", None) != "55P03":
                raise
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"055 could not lock the MCP and vault tables within "
                f"{_LOCK_BUDGET_S}s; they stayed busy. Run the upgrade again."
            )
        time.sleep(random.uniform(0.02, 0.2))


def _move_to_user_tier(bind) -> None:
    forks = [Fork(**row) for row in _rows(bind, _FORKS_SQL)]
    secrets = [WorkspaceSecret(**row) for row in _rows(bind, _SECRETS_SQL)]
    if not forks and not secrets:
        return

    owned = sorted({f.workspace_id for f in forks} | {s.workspace_id for s in secrets}, key=str)
    workspaces = [Workspace(**row) for row in _rows(bind, _WORKSPACES_SQL, ids=owned)]
    by_id = {w.id: w for w in workspaces}
    users = sorted({w.user_id for w in workspaces})
    workspace_ids = [w.id for w in workspaces]

    user_secret_names = _grouped(_rows(bind, _USER_SECRETS_SQL, users=users))
    user_servers = _grouped(_rows(bind, _USER_SERVERS_SQL, users=users))
    # Only a fork's plan reads it, so a config the backend would refuse stops
    # no upgrade that has none.
    builtins = _builtin_servers() if forks else {}
    fork_plan = plan_forks(
        forks,
        workspaces,
        user_servers=user_servers,
        oauth_names=_grouped(_rows(bind, _OAUTH_NAMES_SQL, users=users)),
        markers=_grouped(_rows(bind, _MARKERS_SQL, ids=workspace_ids)),
        reserved=(
            builtins.keys()
            | _BROKERAGES
            | {row["name"] for row in _rows(bind, _BUILTIN_NAMES_SQL)}
        ),
        skipped={name for name, runs in builtins.items() if runs} | _BROKERAGES,
    )

    user_digests, workspace_digests = _secret_digests(
        bind,
        sorted(secrets_needing_compare(secrets, workspaces, user_secret_names)),
        os.environ.get("BYOK_ENCRYPTION_KEY"),
    )
    vault_plan = plan_vault(
        [replace(s, digest=workspace_digests.get(s.id)) for s in secrets],
        workspaces,
        user_secrets={
            user: {name: user_digests.get((user, name)) for name in names}
            for user, names in user_secret_names.items()
        },
        referenced=_user_tier_refs(bind, users),
        promotions=fork_plan.promotions,
    )
    schema_deletes, schema_renames = plan_tool_schemas(
        vault_plan.promotions, fork_plan.dropped
    )

    # Each copy is counted before the rows it came from are deleted.
    if vault_plan.promotions:
        bind.execute(
            text(_INSERT_SERVER_SQL),
            [_server_params(p) for p in vault_plan.promotions],
        )
        _check_landed(
            bind, _SERVERS_LANDED_SQL, "MCP server(s)", len(vault_plan.promotions),
            users=[p.user_id for p in vault_plan.promotions],
            names=[p.name for p in vault_plan.promotions],
        )
    # Every row is gone before a tombstone is written, so no local row of
    # another workspace can still hold a name a tombstone is about to take.
    bind.execute(text("DELETE FROM workspace_mcp_servers WHERE source = 'workspace'"))
    if fork_plan.tombstones:
        bind.execute(
            text(_TOMBSTONE_SQL),
            [{"workspace_id": ws, "name": name} for ws, name in fork_plan.tombstones],
        )
    # A rename target can only hold snapshots nothing reads any more; clearing
    # it first keeps the rename off the (workspace, name, hash) key.
    stale = schema_deletes + [(ws, new) for ws, _old, new in schema_renames]
    if stale:
        bind.execute(
            text(_SCHEMA_DELETE_SQL),
            [{"workspace_id": ws, "name": name} for ws, name in stale],
        )
    if schema_renames:
        bind.execute(
            text(_SCHEMA_RENAME_SQL),
            [
                {"workspace_id": ws, "old": old, "new": new}
                for ws, old, new in schema_renames
            ],
        )
    if vault_plan.copies:
        bind.execute(
            text(_COPY_SECRET_SQL),
            [
                {"id": secret_id, "user_id": user, "name": name}
                for secret_id, user, name in vault_plan.copies
            ],
        )
        _check_landed(
            bind, _SECRETS_LANDED_SQL, "vault secret(s)", len(vault_plan.copies),
            ids=[secret_id for secret_id, _user, _name in vault_plan.copies],
            users=[user for _id, user, _name in vault_plan.copies],
            names=[name for _id, _user, name in vault_plan.copies],
        )
    bind.execute(text("DELETE FROM workspace_vault_secrets"))

    renames = sorted(
        plan_renames(vault_plan.promotions, vault_plan.renames).items(),
        key=lambda r: str(r[0]),
    )
    affected = sorted(
        {by_id[f.workspace_id].user_id for f in forks if by_id[f.workspace_id].live}
        | {by_id[s.workspace_id].user_id for s in secrets if by_id[s.workspace_id].live}
    )
    if affected:
        # The version is what makes a session re-resolve; the computer's own
        # column only records what a sync last built, so it is left alone.
        # The updated_at trigger would stamp every bumped workspace as just
        # edited and float it to the top of the gallery, as 051 notes.
        op.execute("ALTER TABLE workspaces DISABLE TRIGGER trg_workspaces_updated_at")
        bind.execute(
            text(
                "UPDATE workspaces SET mcp_config_version = mcp_config_version + 1 "
                "WHERE user_id = ANY(CAST(:users AS text[]))"
            ),
            {"users": affected},
        )
        if renames:
            bind.execute(
                text(_RENAMES_SQL),
                [
                    {
                        "workspace_id": workspace_id,
                        "secrets": json.dumps(names["secrets"]),
                        "servers": json.dumps(names["servers"]),
                    }
                    for workspace_id, names in renames
                ],
            )
        op.execute("ALTER TABLE workspaces ENABLE TRIGGER trg_workspaces_updated_at")

    # Names only, never values. A warning, since code in the workspace that
    # uses an old name now reads another value or fails to import.
    for workspace_id, names in renames:
        logger.warning(
            "055: user %s workspace %s: MCP server(s) renamed %s; vault "
            "secret(s) renamed %s",
            by_id[workspace_id].user_id, workspace_id,
            _arrows(names["servers"]), _arrows(names["secrets"]),
        )
    for user, (held, copied) in vault_plan.over_cap.items():
        logger.warning(
            "055: user %s holds %d vault secret(s), %d already and %d copied "
            "from workspaces, over the %d a create allows; nothing was dropped",
            user, held + copied, held, copied, _MAX_SECRETS_PER_USER,
        )
    for user, promoted in sorted(Counter(p.user_id for p in vault_plan.promotions).items()):
        held = len(user_servers.get(user, ()))
        if held + promoted > _MAX_SERVERS_PER_USER:
            logger.warning(
                "055: user %s holds %d MCP server(s), %d already and %d promoted "
                "from workspaces, over the %d a create allows; nothing was dropped",
                user, held + promoted, held, promoted, _MAX_SERVERS_PER_USER,
            )
    logger.info(
        "055: %d local MCP server(s) promoted (%d renamed), %d tombstone(s), "
        "%d dropped with their deleted workspace; %d workspace secret(s) "
        "copied, %d workspace(s) with renames",
        len(vault_plan.promotions),
        sum(p.name != p.source_name for p in vault_plan.promotions),
        len(fork_plan.tombstones),
        len(fork_plan.dropped),
        len(vault_plan.copies),
        len(renames),
    )


def upgrade() -> None:
    # Own transaction: ADD COLUMN's ACCESS EXCLUSIVE would outlive the lock retries.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(_NEW_WORKSPACES_COLUMN_SQL)
        op.execute("RESET lock_timeout")
    op.execute("SET LOCAL lock_timeout = '5s'")
    bind = op.get_bind()
    _lock_tables(bind)
    _move_to_user_tier(bind)
    # Last, so none of the writes above meets it.
    op.execute(_GUARD_FUNCTION_SQL)
    for statement in _GUARD_TRIGGERS_SQL:
        op.execute(statement)
    op.execute(_SELECTION_FUNCTION_SQL)
    for statement in _SELECTION_TRIGGER_SQL:
        op.execute(statement)


def downgrade() -> None:
    # Only the guard comes off. A promoted server or a merged secret cannot be
    # attributed back to one workspace once its user has edited it, and the
    # previous build reads the data as 055 left it. The insert trigger stays:
    # the previous build creates workspaces without their selection.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "DROP TRIGGER IF EXISTS trg_workspace_mcp_servers_retired ON workspace_mcp_servers"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_workspace_vault_secrets_retired ON workspace_vault_secrets"
    )
    op.execute("DROP FUNCTION IF EXISTS workspace_tier_retired_guard()")
