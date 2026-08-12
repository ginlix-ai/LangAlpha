"""Platform-secret rollout state machine for hosted workspaces.

The rollout rows record the active provider Secret identity SET; a
workspace's ``platform_secret_version`` records the fleet generation its
sandbox was last certified against, so ``version == 0`` always means "never
certified — the sandbox may hold plaintext env" and ``0 < version <
generation`` means "certified placeholders, behind an identity bump".
Generations are one shared sequence: the fleet generation is the MAX over
rollout rows, and any row's identity change (or removal) bumps it — which
alone re-pends every workspace, so certified rows keep their generation and
the plaintext/placeholder discriminator survives bumps. The identity only
changes at boot registration, so the active set is cached per process and
the per-acquisition resync costs zero extra DB queries. Sandbox-facing
mechanics live in ``ptc_agent.core.sandbox.platform_secrets``; the
scrub-restart for never-certified sandboxes is owned by the background
``PlatformSecretSweeper`` — the request path only ever hot-remounts.
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
    ReconciledPlatformSecret,
    platform_secrets_active,
    remount_platform_secret_bindings,
    resolve_platform_secrets,
    verify_runtime_platform_secrets,
)
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


@dataclass(frozen=True)
class PlatformSecretRolloutSet:
    """The active secret set; ``generation`` is the fleet generation."""

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


def _rollout_from_row(row: dict[str, Any]) -> PlatformSecretRollout:
    return PlatformSecretRollout(
        secret_key=str(row["secret_key"]),
        provider=str(row["provider"]),
        secret_name=str(row["secret_name"]),
        provider_secret_id=str(row["provider_secret_id"]),
        placeholder_sha256=str(row["placeholder_sha256"]),
        current_credential_sha256=str(row["current_credential_sha256"]),
        generation=int(row["generation"]),
    )


def _rollout_set_from_rows(rows: Sequence[Any]) -> PlatformSecretRolloutSet:
    rollouts = tuple(
        sorted(
            (_rollout_from_row(dict(row)) for row in rows),
            key=lambda r: r.secret_key,
        )
    )
    return PlatformSecretRolloutSet(
        rollouts=rollouts,
        generation=max(r.generation for r in rollouts),
    )


#: Per-process cache of the active set. The identity only changes at boot
#: registration, so a populated cache never goes stale within a process.
_active_rollout_set: PlatformSecretRolloutSet | None = None


async def register_platform_secret_rollouts(
    secrets: Sequence[ReconciledPlatformSecret],
    *,
    credential_values: Mapping[str, str],
    provider: str,
) -> PlatformSecretRolloutSet:
    """Record the provider identity set; bump the generation when it changes.

    Any row's identity change — or a removal from the catalog — bumps the
    shared fleet generation, which alone re-pends every live workspace
    (their stored versions now compare behind). Workspace rows are never
    zeroed: version 0 stays reserved for "never certified" so the
    plaintext/placeholder discriminator survives identity bumps. Every
    rollout row is stamped with the fleet generation, so MAX over rows never
    regresses and generation values are never reused. Credential rotation
    without an identity change updates the stored hash in place.
    """

    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # Serialize concurrent boot registration (multi-worker uvicorn).
                await cur.execute(
                    "LOCK TABLE platform_secret_rollouts IN EXCLUSIVE MODE"
                )
                await cur.execute("SELECT * FROM platform_secret_rollouts")
                existing = {
                    str(row["secret_key"]): row for row in await cur.fetchall()
                }
                fleet_generation = max(
                    (int(row["generation"]) for row in existing.values()),
                    default=0,
                )

                def identity_changed(secret: ReconciledPlatformSecret) -> bool:
                    row = existing.get(secret.definition.sandbox_env_var)
                    return row is None or any(
                        str(row[field]) != value
                        for field, value in (
                            ("provider", provider),
                            ("secret_name", secret.name),
                            ("provider_secret_id", secret.provider_secret_id),
                            ("placeholder_sha256", _sha256(secret.placeholder)),
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
                    fleet_generation += 1

                if removed:
                    await cur.execute(
                        "DELETE FROM platform_secret_rollouts"
                        " WHERE secret_key = ANY(%s)",
                        (removed,),
                    )
                for secret in secrets:
                    secret_key = secret.definition.sandbox_env_var
                    await cur.execute(
                        """
                        INSERT INTO platform_secret_rollouts (
                            secret_key, provider, secret_name,
                            provider_secret_id, placeholder_sha256,
                            current_credential_sha256, generation
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (secret_key) DO UPDATE SET
                            provider = EXCLUDED.provider,
                            secret_name = EXCLUDED.secret_name,
                            provider_secret_id = EXCLUDED.provider_secret_id,
                            placeholder_sha256 = EXCLUDED.placeholder_sha256,
                            current_credential_sha256 =
                                EXCLUDED.current_credential_sha256,
                            generation = EXCLUDED.generation,
                            updated_at = NOW()
                        """,
                        (
                            secret_key,
                            provider,
                            secret.name,
                            secret.provider_secret_id,
                            _sha256(secret.placeholder),
                            _sha256(credential_values[secret_key]),
                            fleet_generation,
                        ),
                    )

                await cur.execute("SELECT * FROM platform_secret_rollouts")
                rows = await cur.fetchall()
                rollout_set = _rollout_set_from_rows(rows)

    global _active_rollout_set
    _active_rollout_set = rollout_set
    return rollout_set


async def get_platform_secret_rollouts() -> PlatformSecretRolloutSet:
    global _active_rollout_set
    if _active_rollout_set is not None:
        return _active_rollout_set
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM platform_secret_rollouts")
            rows = await cur.fetchall()
    if not rows:
        raise PlatformSecretReadinessError(
            "Platform-secret rollout identity is not initialized"
        )
    _active_rollout_set = _rollout_set_from_rows(rows)
    return _active_rollout_set


async def list_workspaces_behind_platform_secret(
    rollout_set: PlatformSecretRolloutSet,
    *,
    limit: int = 25,
) -> list[dict[str, Any]]:
    """Running workspaces whose sandbox is behind the fleet generation.

    Only 'running' rows: a stopped sandbox has no live processes to scrub and
    converges through normal bringup plus a later sweep pass once running.
    """

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT workspace_id, sandbox_id, platform_secret_version,
                       is_always_on
                FROM workspaces
                WHERE status = 'running'
                  AND sandbox_id IS NOT NULL
                  AND COALESCE(platform_secret_version, 0) != %s
                ORDER BY updated_at ASC
                LIMIT %s
                """,
                (rollout_set.generation, limit),
            )
            return [dict(row) for row in await cur.fetchall()]


async def certify_new_workspace_sandbox(
    config: Any,
    *,
    workspace_id: str,
    expected_previous_sandbox_id: str | None,
    sandbox_id: str,
    runtime: Any,
) -> dict[str, Any] | None:
    """Verify and atomically attach one newly created hosted sandbox."""

    if not platform_secrets_active(config):
        return None
    rollout_set = await get_platform_secret_rollouts()
    await verify_runtime_platform_secrets(
        runtime, expected=rollout_set.placeholders
    )

    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                UPDATE workspaces
                SET status = 'running',
                    sandbox_id = %s,
                    platform_secret_version = %s,
                    updated_at = NOW()
                WHERE workspace_id = %s
                  AND status != 'deleted'
                  AND sandbox_id IS NOT DISTINCT FROM %s
                RETURNING workspace_id
                """,
                (
                    sandbox_id,
                    rollout_set.generation,
                    workspace_id,
                    expected_previous_sandbox_id,
                ),
            )
            row = await cur.fetchone()
    if row is None:
        raise RuntimeError("Workspace changed before platform Secret certification")
    from src.server.database.workspace import get_workspace

    return await get_workspace(workspace_id)


async def stamp_workspace_platform_secret_version(
    workspace_id: str,
    *,
    expected_sandbox_id: str | None,
    rollout_set: PlatformSecretRolloutSet,
) -> None:
    """CAS the exact sandbox (or lack of one) to the current fleet generation."""

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE workspaces
                SET platform_secret_version = %s,
                    updated_at = NOW()
                WHERE workspace_id = %s
                  AND status != 'deleted'
                  AND sandbox_id IS NOT DISTINCT FROM %s
                """,
                (
                    rollout_set.generation,
                    workspace_id,
                    expected_sandbox_id,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("Workspace changed before platform Secret stamp")


async def resync_workspace_platform_secret(
    config: Any,
    runtime: Any,
    *,
    workspace_id: str,
    sandbox_id: str | None,
    db_version: int,
    applied_generation: int | None,
) -> int | None:
    """Hot-resync one live sandbox onto the active rollout; never restarts.

    The request-path half of convergence, run on every session (re)init and
    warm slow-path acquisition. ``applied_generation`` is the session's stamp
    of the generation already applied (short-circuits the common case with
    zero provider calls); ``db_version`` is the workspace row's certified
    generation, piggybacked off an existing read. Returns the generation now
    applied — for the caller to stamp on the session — or None when managed
    secrets are inactive or the workspace has no sandbox.

    Only a certified-but-behind sandbox (``0 < db_version < generation``,
    placeholders throughout) is verified and stamped here. A never-certified
    sandbox (``db_version == 0``) still gets the hot remount — protecting
    every new process immediately — but keeps its row behind: live processes
    may retain plaintext, and the scrub-restart that purges them belongs to
    the background sweep, never to a turn.
    """

    if not platform_secrets_active(config):
        return None
    rollout_set = await get_platform_secret_rollouts()
    generation = rollout_set.generation
    if applied_generation == generation:
        return generation
    if not sandbox_id:
        # Nothing to converge; the fresh-provision path certifies atomically.
        return None
    if db_version == generation:
        # Certified out-of-band (fresh provision, sweep) — bindings are
        # already mounted; only the session stamp was missing.
        return generation

    try:
        await remount_platform_secret_bindings(
            runtime,
            expected=rollout_set.placeholders,
            bindings=rollout_set.bindings,
        )
        if db_version > 0:
            await verify_runtime_platform_secrets(
                runtime, expected=rollout_set.placeholders
            )
    except Exception as exc:
        logger.warning(
            "Platform-secret hot resync failed",
            extra={
                "workspace_id": workspace_id,
                "sandbox_id": sandbox_id,
                "error_type": type(exc).__name__,
            },
        )
        raise PlatformSecretReadinessError(
            f"Platform-secret resync failed for workspace {workspace_id}"
        ) from exc

    if db_version > 0:
        # A stamp CAS conflict means the workspace row moved under us — the
        # failure propagates and the next acquisition retries.
        await stamp_workspace_platform_secret_version(
            workspace_id,
            expected_sandbox_id=sandbox_id,
            rollout_set=rollout_set,
        )
    logger.info(
        "Platform secret hot-resynced",
        extra={
            "workspace_id": workspace_id,
            "sandbox_id": sandbox_id,
            "generation": generation,
            "certified": db_version > 0,
        },
    )
    return generation


async def reconcile_platform_secrets_at_boot(
    agent_config: Any,
) -> PlatformSecretRolloutSet | None:
    """Reconcile the managed Secret catalog before the application is ready.

    A no-op when no catalog is configured. Fails closed on configuration.
    Provider unavailability is softened when a previously registered rollout
    exists: boot continues on that identity (sandboxes still converge lazily)
    instead of crash-looping the server.
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
    secrets = resolve_platform_secrets(core_config)
    if not secrets:
        return None

    provider = None
    try:
        from ptc_agent.core.sandbox.providers import create_provider

        provider = create_provider(core_config)
        reconciled = await provider.reconcile_platform_secrets(secrets)
        if len(reconciled) != len(secrets):
            # A count mismatch is a reconciler contract violation, not a
            # provider outage — fail closed (hard) rather than letting the broad
            # handler below soften it to a prior rollout and mask the bug.
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
            provider=core_config.sandbox.provider,
        )
    except PlatformSecretConfigurationError:
        raise
    except Exception as exc:
        existing = await _existing_rollouts_or_none()
        if existing is not None:
            logger.error(
                "Platform Secret reconciliation failed at boot; continuing on "
                "the previously registered rollout (error_type=%s)",
                type(exc).__name__,
            )
            return existing
        if isinstance(exc, PlatformSecretError):
            raise
        raise PlatformSecretReconciliationError(
            "Failed to initialize platform Secret reconciliation "
            f"(error_type={type(exc).__name__})"
        ) from None
    finally:
        if provider is not None:
            await provider.close()


async def _existing_rollouts_or_none() -> PlatformSecretRolloutSet | None:
    try:
        return await get_platform_secret_rollouts()
    except Exception:
        return None
