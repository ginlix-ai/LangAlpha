"""Platform-secret rollout state machine, one rollout per provider kind.

A computer keeps the provider it was created on, so a deployment that has
switched providers serves computers of both kinds, and each kind needs its own
provider's Secrets. Every kind the deployment serves registers its own rows
and generation, and a computer is certified, resynced and swept against the
rollout of its own ``computers.kind``.

A computer's ``platform_secret_version`` is a generation of its own kind's
rollout or 0: ``0`` always means "never certified, the sandbox may hold
plaintext env", and any other value below the kind's generation means
"certified placeholders, behind an identity bump". Generations come from one
Postgres sequence shared by every kind and are never reused, so a version can
only ever equal the generation of the identity it was certified against, and a
bump re-pends exactly that kind's computers while certified rows keep their
version. The identity only changes at registration (boot, or the sweep's retry
for a kind with no rows), so each kind's set is memoized per process and the
per-acquisition resync costs zero extra DB queries. Sandbox-facing mechanics
live in ``ptc_agent.core.sandbox.platform_secrets``; the scrub-restart for
never-certified sandboxes is owned by the background ``PlatformSecretSweeper``
and the request path only ever hot-remounts.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    PlatformSecretError,
    PlatformSecretReconciliationError,
    PlatformSecretReplacementRequired,
    ReconciledPlatformSecret,
    platform_secrets_active,
    provider_credentials_configured,
    provider_kind_supports_platform_secrets,
    remount_platform_secret_bindings,
    require_platform_secret_capable,
    resolve_platform_secrets,
    verify_runtime_platform_secrets,
)
from ptc_agent.core.sandbox.providers import provider_class
from src.server.database.pool import get_db_connection


logger = logging.getLogger(__name__)


class PlatformSecretReadinessError(PlatformSecretError):
    """The rollout identity is missing or the workspace cannot be served."""


@dataclass(frozen=True)
class PlatformSecretRollout:
    secret_key: str
    provider: str
    secret_name: str
    provider_secret_id: str
    placeholder_sha256: str
    current_credential_sha256: str
    generation: int
    egress_rule_sha256: str = ""


@dataclass(frozen=True)
class PlatformSecretRolloutSet:
    """One provider kind's active Secret set and that kind's generation."""

    provider: str
    rollouts: tuple[PlatformSecretRollout, ...]
    generation: int

    @property
    def bindings(self) -> dict[str, str]:
        return {r.secret_key: r.secret_name for r in self.rollouts}

    @property
    def placeholders(self) -> dict[str, str]:
        return {r.secret_key: r.placeholder_sha256 for r in self.rollouts}


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _egress_rule_sha256(secret: ReconciledPlatformSecret) -> str:
    # Empty stays empty, so a provider that keeps the routing itself matches
    # the column's default and an upgrade re-pends nothing.
    return _sha256(secret.egress_rule) if secret.egress_rule else ""


def _rollout_from_row(row: dict[str, Any]) -> PlatformSecretRollout:
    return PlatformSecretRollout(
        secret_key=str(row["secret_key"]),
        provider=str(row["provider"]),
        secret_name=str(row["secret_name"]),
        provider_secret_id=str(row["provider_secret_id"]),
        placeholder_sha256=str(row["placeholder_sha256"]),
        current_credential_sha256=str(row["current_credential_sha256"]),
        generation=int(row["generation"]),
        egress_rule_sha256=str(row.get("egress_rule_sha256") or ""),
    )


def _rollout_set_from_rows(
    provider: str, rows: Sequence[Any]
) -> PlatformSecretRolloutSet:
    rollouts = tuple(
        sorted(
            (_rollout_from_row(dict(row)) for row in rows),
            key=lambda r: r.secret_key,
        )
    )
    return PlatformSecretRolloutSet(
        provider=provider,
        rollouts=rollouts,
        generation=max(r.generation for r in rollouts),
    )


#: Per-process memo of each kind's set, for the request path's zero-query
#: resync. Another worker can register a newer identity behind it, so it only
#: ever holds rows read from Postgres, never their absence, and a computer
#: stamped above it sends the reader back to Postgres. Certification and the
#: sweep read fresh instead.
_active_rollout_sets: dict[str, PlatformSecretRolloutSet] = {}


def reset_platform_secret_rollout_cache() -> None:
    _active_rollout_sets.clear()


async def register_platform_secret_rollouts(
    secrets: Sequence[ReconciledPlatformSecret],
    *,
    credential_values: Mapping[str, str],
    provider: str,
) -> PlatformSecretRolloutSet:
    """Record one provider kind's identity set; bump its generation when it changes.

    Any row's identity change, or a removal from the catalog, takes a new
    generation from the shared sequence and stamps it on every row of this
    kind, which alone re-pends that kind's live computers (their stored
    versions now compare behind). Other kinds' rows are never touched, and
    computer rows are never zeroed: version 0 stays reserved for "never
    certified" so the plaintext/placeholder discriminator survives bumps.
    Credential rotation without an identity change updates the stored hash in
    place.
    """

    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent registration (multi-worker boot, the
                # sweep's retry); plain reads are not blocked by this mode.
                await cur.execute(
                    "LOCK TABLE platform_secret_rollouts IN EXCLUSIVE MODE"
                )
                await cur.execute(
                    "SELECT * FROM platform_secret_rollouts WHERE provider = %s",
                    (provider,),
                )
                existing = {
                    str(row["secret_key"]): row for row in await cur.fetchall()
                }

                def identity_changed(secret: ReconciledPlatformSecret) -> bool:
                    row = existing.get(secret.definition.sandbox_env_var)
                    return row is None or any(
                        str(row[field]) != value
                        for field, value in (
                            ("secret_name", secret.name),
                            ("provider_secret_id", secret.provider_secret_id),
                            ("placeholder_sha256", _sha256(secret.placeholder)),
                            (
                                "egress_rule_sha256",
                                _egress_rule_sha256(secret),
                            ),
                        )
                    )

                removed = sorted(
                    set(existing)
                    - {s.definition.sandbox_env_var for s in secrets}
                )
                set_changed = bool(removed) or any(
                    identity_changed(secret) for secret in secrets
                )
                if set_changed:
                    await cur.execute(
                        "SELECT nextval('platform_secret_generation_seq')"
                        " AS generation"
                    )
                    generation = int((await cur.fetchone())["generation"])
                else:
                    generation = max(
                        int(row["generation"]) for row in existing.values()
                    )

                if removed:
                    await cur.execute(
                        "DELETE FROM platform_secret_rollouts"
                        " WHERE provider = %s AND secret_key = ANY(%s)",
                        (provider, removed),
                    )
                for secret in secrets:
                    secret_key = secret.definition.sandbox_env_var
                    await cur.execute(
                        """
                        INSERT INTO platform_secret_rollouts (
                            secret_key, provider, secret_name,
                            provider_secret_id, placeholder_sha256,
                            current_credential_sha256, generation,
                            egress_rule_sha256
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (provider, secret_key) DO UPDATE SET
                            secret_name = EXCLUDED.secret_name,
                            provider_secret_id = EXCLUDED.provider_secret_id,
                            placeholder_sha256 = EXCLUDED.placeholder_sha256,
                            current_credential_sha256 =
                                EXCLUDED.current_credential_sha256,
                            generation = EXCLUDED.generation,
                            egress_rule_sha256 = EXCLUDED.egress_rule_sha256,
                            updated_at = NOW()
                        """,
                        (
                            secret_key,
                            provider,
                            secret.name,
                            secret.provider_secret_id,
                            _sha256(secret.placeholder),
                            _sha256(credential_values[secret_key]),
                            generation,
                            _egress_rule_sha256(secret),
                        ),
                    )

                await cur.execute(
                    "SELECT * FROM platform_secret_rollouts WHERE provider = %s",
                    (provider,),
                )
                rows = await cur.fetchall()
                rollout_set = _rollout_set_from_rows(provider, rows)

    _active_rollout_sets[provider] = rollout_set
    return rollout_set


async def get_platform_secret_rollouts(
    kind: str, *, fresh: bool = False
) -> PlatformSecretRolloutSet:
    """One kind's active set; a kind with no rows is unready.

    ``fresh`` skips the memo, for a caller that must not act on an identity
    another worker has since replaced.
    """
    if not fresh:
        cached = _active_rollout_sets.get(kind)
        if cached is not None:
            return cached
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT * FROM platform_secret_rollouts WHERE provider = %s",
                (kind,),
            )
            rows = await cur.fetchall()
    if not rows:
        _active_rollout_sets.pop(kind, None)
        raise PlatformSecretReadinessError(
            f"Platform-secret rollout identity is not initialized for {kind}"
        )
    rollout_set = _rollout_set_from_rows(kind, rows)
    _active_rollout_sets[kind] = rollout_set
    return rollout_set


async def get_all_platform_secret_rollouts() -> dict[str, PlatformSecretRolloutSet]:
    """Every registered kind's set, read fresh, keyed by kind."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM platform_secret_rollouts")
            rows = await cur.fetchall()
    by_kind: dict[str, list[Any]] = {}
    for row in rows:
        by_kind.setdefault(str(row["provider"]), []).append(row)
    return {
        kind: _rollout_set_from_rows(kind, kind_rows)
        for kind, kind_rows in by_kind.items()
    }


async def list_workspaces_behind_platform_secret(
    rollout_sets: Mapping[str, PlatformSecretRolloutSet],
    *,
    limit: int = 25,
    after: Any = None,
) -> list[dict[str, Any]]:
    """Running machines behind their own kind's generation, one row per machine.

    A machine is compared only with the rollout of its own kind, and a kind
    absent from ``rollout_sets`` is never listed: there is nothing to
    converge it onto.

    Only 'running' rows: a stopped sandbox has no live processes to scrub and
    converges through normal bringup plus a later sweep pass once running.

    One row per computer, because what converges is the one sandbox the machine
    owns: a per-workspace row shape makes the sweep scrub a five-project machine
    five times and lets that one machine consume the whole batch. The
    representative ``workspace_id`` is the oldest live project on it, carried for
    the busy gate and the logs. A LEFT JOIN so a machine with no project left is
    still converged, since its sandbox is running either way.

    ``after`` resumes past the last machine a previous page returned. Without
    it, machines that fail on every pass would fill each batch and keep every
    machine sorted after them waiting.
    """

    kinds = sorted(
        kind for kind in rollout_sets
        if provider_kind_supports_platform_secrets(kind)
    )
    if not kinds:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT DISTINCT ON (c.computer_id)
                       c.computer_id, w.workspace_id,
                       c.provider_ref AS sandbox_id, c.provider_ref,
                       c.kind, c.provider_config, c.root_dir,
                       c.platform_secret_version, c.is_always_on
                FROM computers c
                JOIN unnest(%(kinds)s::text[], %(generations)s::int[])
                     AS g(kind, generation)
                  ON g.kind = c.kind
                LEFT JOIN workspaces w
                  ON w.computer_id = c.computer_id AND w.status <> 'deleted'
                WHERE c.status = 'running'
                  AND c.provider_ref IS NOT NULL
                  AND COALESCE(c.platform_secret_version, 0) <> g.generation
                  AND (%(after)s::uuid IS NULL OR c.computer_id > %(after)s::uuid)
                ORDER BY c.computer_id, w.created_at ASC
                LIMIT %(limit)s
                """,
                {
                    "kinds": kinds,
                    "generations": [rollout_sets[k].generation for k in kinds],
                    "after": after,
                    "limit": limit,
                },
            )
            return [dict(row) for row in await cur.fetchall()]


async def certify_platform_secrets(config: Any, *, runtime: Any, kind: str) -> int:
    """Verify the placeholders landed in this sandbox; return the generation to stamp.

    ``kind`` is the computer's provider, whose own rollout the sandbox was
    built with. Returns 0, the "never certified, may hold plaintext env"
    sentinel from migration 021, when no catalog applies. The rows are read
    fresh: a provision takes seconds, and certifying against an identity
    another worker has since replaced would stamp a version that reads
    current. A pure contributor: it writes nothing and decides no control
    flow, so the workspace binding stays owned by the caller that owns the
    sandbox.
    """
    if not platform_secrets_active(config, kind=kind):
        return 0
    rollout_set = await get_platform_secret_rollouts(kind, fresh=True)
    await verify_runtime_platform_secrets(runtime, expected=rollout_set.placeholders)
    return rollout_set.generation


async def stamp_platform_secret_version(
    *,
    computer_id: str,
    expected_sandbox_id: str | None,
    rollout_set: PlatformSecretRolloutSet,
) -> None:
    """CAS the exact sandbox (or lack of one) to its kind's generation.

    One statement stamps the computer and its workspace shadows together, so no
    reader can see a machine certified on one row and behind on the other. The
    raise stays: the caller has just scrubbed a live sandbox, and a stamp that
    did not land means the row moved under it, which makes the convergence it
    just performed unattributable.
    """
    from src.server.database.computer import (
        stamp_computer_platform_secret_version,
    )

    stamped = await stamp_computer_platform_secret_version(
        rollout_set.generation,
        computer_id=computer_id,
        expected_provider_ref=expected_sandbox_id,
    )
    if stamped != 1:
        raise RuntimeError(
            "Computer changed before its platform secret generation was stamped"
        )


async def resync_computer_platform_secret(
    config: Any,
    runtime: Any,
    *,
    kind: str,
    computer_id: str,
    sandbox_id: str | None,
    db_version: int,
    applied_generation: int | None,
) -> int | None:
    """Hot-resync one live sandbox onto its kind's rollout; never restarts.

    The request-path half of convergence, run on every session (re)init and
    warm slow-path acquisition. ``applied_generation`` is the session's stamp
    of the generation already applied (short-circuits the common case with
    zero provider calls); ``db_version`` is the machine's certified generation,
    piggybacked off an existing read. Returns the generation now applied, for
    the caller to stamp on the session, or None when managed secrets are
    inactive or the machine has no sandbox.

    Only a certified-but-behind sandbox (``0 < db_version < generation``,
    placeholders throughout) is verified and stamped here. A never-certified
    sandbox (``db_version == 0``) still gets the hot remount, protecting every
    new process immediately, but keeps its row behind: live processes may
    retain plaintext, and the scrub-restart that purges them belongs to the
    background sweep, never to a turn.

    A kind that never scrubs, its env fixed at create, has no scrub to wait
    for, since what its env holds now it holds for good, so it is verified and
    stamped at any version. A value there that is not its marker raises
    ``PlatformSecretReplacementRequired`` unwrapped: the caller keeps serving
    the sandbox, and the sweep rebuilds it while idle.
    """

    if not platform_secrets_active(config, kind=kind):
        return None
    rollout_set = await get_platform_secret_rollouts(kind)
    if db_version > rollout_set.generation:
        # Only another worker registering a newer identity puts a computer
        # above this memo. Acting on the memo would remount the old bindings
        # over a sandbox certified at the new generation.
        rollout_set = await get_platform_secret_rollouts(kind, fresh=True)
    generation = rollout_set.generation
    if applied_generation == generation:
        return generation
    if not sandbox_id:
        # Nothing to converge; the fresh-provision path certifies atomically.
        return None
    if db_version == generation:
        # Certified out-of-band (fresh provision, sweep): bindings are
        # already mounted; only the session stamp was missing.
        return generation

    certifiable = db_version > 0 or not provider_class(kind).scrubs_platform_secrets
    try:
        await remount_platform_secret_bindings(
            runtime,
            expected=rollout_set.placeholders,
            bindings=rollout_set.bindings,
        )
        if certifiable:
            await verify_runtime_platform_secrets(
                runtime, expected=rollout_set.placeholders
            )
    except PlatformSecretReplacementRequired:
        raise
    except Exception as exc:
        logger.warning(
            "Platform-secret hot resync failed",
            extra={
                "computer_id": computer_id,
                "sandbox_id": sandbox_id,
                "error_type": type(exc).__name__,
            },
        )
        raise PlatformSecretReadinessError(
            f"Platform-secret resync failed for computer {computer_id}"
        ) from exc

    if certifiable:
        # A stamp CAS conflict means the machine moved under us; the failure
        # propagates and the next acquisition retries.
        await stamp_platform_secret_version(
            computer_id=computer_id,
            expected_sandbox_id=sandbox_id,
            rollout_set=rollout_set,
        )
    logger.info(
        "Platform secret hot-resynced",
        extra={
            "computer_id": computer_id,
            "sandbox_id": sandbox_id,
            "provider": kind,
            "generation": generation,
            "certified": db_version > 0,
        },
    )
    return generation


async def served_platform_secret_kinds(core_config: Any) -> dict[str, int]:
    """The capable kinds this deployment serves, each with its live computer count.

    Every kind a new computer may be created on (the deployment's provider and
    the ones it offers), so a kind is registered before its first computer
    starts, plus every kind that still has a computer. The rest is derived from
    the rows: a provider switch keeps the old kind served for exactly as long
    as computers of it exist, and a key for a kind neither offered nor in use
    is never used.
    """
    from src.server.database.computer import get_live_computer_kinds

    kinds = dict(await get_live_computer_kinds())
    for kind in core_config.sandbox.creatable_providers():
        kinds.setdefault(kind, 0)
    return {
        kind: count
        for kind, count in kinds.items()
        if provider_kind_supports_platform_secrets(kind)
    }


async def reconcile_and_register_kind(
    core_config: Any, kind: str
) -> PlatformSecretRolloutSet:
    """Write one kind's Secrets to its provider, then register their identity.

    Each kind is reconciled with that provider's own account and namespace,
    the same settings its computers are built with.
    """
    from ptc_agent.core.sandbox.providers import build_provider

    secrets = resolve_platform_secrets(core_config, kind=kind)
    provider = build_provider(
        kind,
        core_config.sandbox.settings_for(kind),
        working_dir=core_config.filesystem.working_directory,
        platform_secrets=core_config.sandbox.platform_secrets,
    )
    try:
        reconciled = await provider.reconcile_platform_secrets(secrets)
        if len(reconciled) != len(secrets):
            # A count mismatch is a reconciler contract violation, not a
            # provider outage: fail closed rather than let a caller soften it
            # to a prior rollout and mask the bug.
            raise PlatformSecretConfigurationError(
                "Unexpected platform Secret reconciliation result "
                f"(expected {len(secrets)}, got {len(reconciled)})"
            )
        return await register_platform_secret_rollouts(
            reconciled,
            credential_values={
                secret.definition.sandbox_env_var: secret.value
                for secret in secrets
            },
            provider=kind,
        )
    finally:
        await provider.close()


async def reconcile_platform_secrets_at_boot(
    agent_config: Any,
) -> dict[str, PlatformSecretRolloutSet]:
    """Reconcile every served kind's Secret catalog before the application is ready.

    A no-op when no catalog is configured, hosted or not. The deployment's own
    provider fails closed on missing credentials, on configuration, and on an
    outage with nothing registered before. An offered kind needs credentials too
    (``validate_api_keys``). Any other kind is served only because computers of
    it exist, and none blocks boot for want of credentials or of a provider:
    without credentials it is skipped, and on an outage with nothing registered
    its computers fail readiness until the sweep registers it. A configuration
    error is fatal for every kind with credentials, since holding them means
    the deployment serves that kind. An outage after an earlier registration
    continues on those rows for any kind.
    """

    core_config = agent_config.to_core_config()
    try:
        agent_config.validate_api_keys()
    except Exception:
        if platform_secrets_active(core_config):
            raise PlatformSecretConfigurationError(
                "Sandbox provider credentials are required when a platform "
                "Secret catalog is configured"
            ) from None
        raise
    if not core_config.sandbox.platform_secrets:
        return {}
    # Each create on such a kind would refuse anyway; boot says so first.
    require_platform_secret_capable(
        core_config, core_config.sandbox.creatable_providers()
    )

    deployment_kind = core_config.sandbox.provider
    served = await served_platform_secret_kinds(core_config)
    registered: dict[str, PlatformSecretRolloutSet] = {}
    for kind in sorted(served, key=lambda k: (k != deployment_kind, k)):
        primary = kind == deployment_kind
        if not primary and not provider_credentials_configured(core_config, kind):
            logger.error(
                "Platform Secrets for %s are not reconciled: %d computer(s) "
                "run on it and its credentials are not configured, so they "
                "cannot start until they are",
                kind,
                served[kind],
            )
            continue
        rollout_set = await _register_kind_at_boot(
            core_config, kind, primary=primary
        )
        if rollout_set is not None:
            registered[kind] = rollout_set
    return registered


async def _register_kind_at_boot(
    core_config: Any, kind: str, *, primary: bool
) -> PlatformSecretRolloutSet | None:
    try:
        return await reconcile_and_register_kind(core_config, kind)
    except PlatformSecretConfigurationError:
        raise
    except Exception as exc:
        existing = await _existing_rollouts_or_none(kind)
        if existing is not None:
            logger.error(
                "Platform Secret reconciliation failed at boot for %s; "
                "continuing on the previously registered rollout "
                "(error_type=%s)",
                kind,
                type(exc).__name__,
            )
            return existing
        if not primary:
            logger.error(
                "Platform Secret reconciliation failed at boot for %s and it "
                "has no earlier rollout; its computers fail readiness until "
                "the sweep registers it (error_type=%s)",
                kind,
                type(exc).__name__,
            )
            return None
        if isinstance(exc, PlatformSecretError):
            raise
        raise PlatformSecretReconciliationError(
            "Failed to initialize platform Secret reconciliation "
            f"(error_type={type(exc).__name__})"
        ) from None


async def _existing_rollouts_or_none(kind: str) -> PlatformSecretRolloutSet | None:
    try:
        return await get_platform_secret_rollouts(kind, fresh=True)
    except Exception:
        return None
