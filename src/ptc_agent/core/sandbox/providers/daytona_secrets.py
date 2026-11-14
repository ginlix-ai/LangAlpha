"""Daytona organization-Secret reconciliation, split from the provider proper."""

from typing import Any

import structlog
from daytona import CreateSecretParams, UpdateSecretParams

from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretReconciler,
    PlatformSecretReconciliationError,
    ReconciledPlatformSecret,
    ResolvedPlatformSecret,
)
from ptc_agent.core.sandbox.providers._daytona_errors import (
    daytona_error_status,
    is_transient_daytona_error,
)

logger = structlog.get_logger(__name__)


class DaytonaSecretReconciler(PlatformSecretReconciler):
    """Creates or updates organization-scoped Daytona Secrets idempotently."""

    label = "Daytona"
    key_error_statuses = frozenset({403})
    key_error_message = (
        "Daytona API key is missing the manage:secrets permission required "
        "for platform Secret reconciliation"
    )

    def __init__(self, client: Any) -> None:
        self._client = client

    async def _reconcile_one(
        self, secret: ResolvedPlatformSecret
    ) -> ReconciledPlatformSecret:
        existing = await self._find_secret_exact(secret.name)
        if existing is None:
            try:
                created_secret = await self._client.secret.create(
                    CreateSecretParams(
                        name=secret.name,
                        value=secret.value,
                        description=secret.definition.description,
                        hosts=list(secret.definition.hosts),
                    )
                )
                logger.info(
                    "Created Daytona platform Secret",
                    secret_name=secret.name,
                    hosts=list(secret.definition.hosts),
                )
                return self._reconciled_secret(secret, created_secret)
            except Exception as exc:
                status = self._status(exc)
                if status not in (None, 409):
                    raise
                # A 409 means another worker won the create. A status-less
                # transport error is ambiguous: the create may have committed
                # before the connection dropped. Refetch before retrying so we
                # never create a duplicate or fail readiness unnecessarily.
                existing = await self._find_secret_exact(secret.name)
                if existing is None:
                    raise

        updated = await self._client.secret.update(
            existing.id,
            UpdateSecretParams(
                value=secret.value,
                description=secret.definition.description,
                hosts=list(secret.definition.hosts),
            ),
        )
        logger.info(
            "Updated Daytona platform Secret",
            secret_name=secret.name,
            hosts=list(secret.definition.hosts),
        )
        return self._reconciled_secret(secret, updated)

    @staticmethod
    def _reconciled_secret(
        secret: ResolvedPlatformSecret,
        provider_secret: Any,
    ) -> ReconciledPlatformSecret:
        provider_secret_id = str(getattr(provider_secret, "id", "") or "")
        placeholder = str(getattr(provider_secret, "placeholder", "") or "")
        if not provider_secret_id or not placeholder.startswith("dtn_secret_"):
            raise PlatformSecretReconciliationError(
                "Daytona returned incomplete platform Secret metadata"
            )
        return ReconciledPlatformSecret(
            definition=secret.definition,
            name=secret.name,
            provider_secret_id=provider_secret_id,
            placeholder=placeholder,
        )

    async def _find_secret_exact(self, name: str) -> Any | None:
        cursor: str | None = None
        while True:
            page = await self._client.secret.list(
                cursor=cursor,
                limit=200,
                name=name,
            )
            for item in page.items:
                if item.name == name:
                    return item
            cursor = page.next_cursor
            if cursor is None:
                return None

    def _status(self, exc: Exception) -> int | None:
        return daytona_error_status(exc)

    def _retryable(self, exc: Exception, status: int | None) -> bool:
        return (
            status == 429
            or (status is not None and 500 <= status <= 599)
            # Transport failures (connection reset, timeout, closed client)
            # carry no status; classify them by message so a first-boot
            # network blip doesn't hard-fail reconciliation.
            or (status is None and is_transient_daytona_error(exc))
        )
