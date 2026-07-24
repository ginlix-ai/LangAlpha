"""Daytona organization-Secret reconciliation, split from the provider proper."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import structlog
from daytona import CreateSecretParams, UpdateSecretParams

from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    PlatformSecretReconciliationError,
    ReconciledPlatformSecret,
    ResolvedPlatformSecret,
)

logger = structlog.get_logger(__name__)


def is_transient_daytona_error(exc: Exception) -> bool:
    """Classify a Daytona SDK error as transient (retryable).

    Status-less transport failures (connection reset, timeouts, a closed
    client) carry no HTTP status, so 429/5xx classification misses them —
    status-BEARING errors are classified by status at the retry sites, never
    here (bare digit markers would misclassify e.g. "400 Bad Request" as
    transient). Execution errors (the command ran and the server answered)
    are not transient even when the server's message mentions a timeout.
    """

    # Transport exception types are transient even with an empty message
    # (a bare ConnectionResetError / TimeoutError carries no text).
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return True
    message = str(exc).lower()
    # A closed HTTP client means the request never reached the server. The SDK
    # wraps these as "Failed to execute command: Session is closed", so check
    # BEFORE the execution-error guard below.
    if any(marker in message for marker in ("session is closed", "client is closed")):
        return True
    if "failed to execute command" in message:
        return False
    # 5xx/429 phrases cover SDK errors stringified without a status attribute;
    # phrases, never bare digits, so "400 Bad Request" stays terminal.
    transient_markers = (
        "remote end closed connection",
        "remotedisconnected",
        "connection aborted",
        "connection reset",
        "broken pipe",
        "timed out",
        "timeout",
        "bad gateway",
        "service unavailable",
        "too many requests",
        "no ip address found",
    )
    return any(marker in message for marker in transient_markers)


class DaytonaSecretReconciler:
    """Creates or updates organization-scoped Daytona Secrets idempotently."""

    def __init__(self, client: Any) -> None:
        self._client = client

    async def reconcile(
        self, secrets: Sequence[ResolvedPlatformSecret]
    ) -> tuple[ReconciledPlatformSecret, ...]:
        reconciled: list[ReconciledPlatformSecret] = []
        for secret in secrets:
            reconciled.append(
                await self._call_with_retry(
                    secret.name,
                    lambda secret=secret: self._reconcile_one(secret),
                )
            )
        return tuple(reconciled)

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
                status = self._exception_status(exc)
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

    async def _call_with_retry(
        self,
        secret_name: str,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Retry 429/5xx and status-less transient failures without logging bodies."""

        delay_s = 0.25
        for attempt in range(1, 4):
            try:
                return await operation()
            except Exception as exc:
                status = self._exception_status(exc)
                retryable = (
                    status == 429
                    or (status is not None and 500 <= status <= 599)
                    # Transport failures (connection reset, timeout, closed
                    # client) carry no status; classify them by message so a
                    # first-boot network blip doesn't hard-fail reconciliation.
                    or (status is None and is_transient_daytona_error(exc))
                )
                logger.warning(
                    "Daytona platform Secret reconciliation failed",
                    secret_name=secret_name,
                    attempt=attempt,
                    status=status,
                    error_type=type(exc).__name__,
                    retrying=retryable and attempt < 3,
                )
                if not retryable or attempt == 3:
                    if status == 403:
                        # Operator misconfiguration, not provider availability:
                        # must fail boot loudly, never fall back to a previous
                        # rollout identity.
                        raise PlatformSecretConfigurationError(
                            "Daytona API key is missing the manage:secrets "
                            "permission required for platform Secret reconciliation"
                        ) from None
                    status_text = str(status) if status is not None else "unknown"
                    raise PlatformSecretReconciliationError(
                        "Failed to reconcile Daytona platform Secret "
                        f"{secret_name!r} (status={status_text}, "
                        f"error_type={type(exc).__name__})"
                    ) from None
                await asyncio.sleep(delay_s)
                delay_s *= 2

    @staticmethod
    def _exception_status(exc: Exception) -> int | None:
        """Extract an HTTP status through SDK wrapper exception chains."""

        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            for attr in ("status", "status_code"):
                status = getattr(current, attr, None)
                if isinstance(status, int):
                    return status
            current = current.__cause__ or current.__context__
        return None
