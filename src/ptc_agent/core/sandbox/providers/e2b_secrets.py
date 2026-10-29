"""E2B Secrets for platform credentials, split from the provider proper.

E2B binds a Secret to a sandbox through that sandbox's egress rules: on each
listed host its proxy sets one request header to the Secret's current value,
over HTTPS only, so the value never enters the sandbox and a rotation reaches
running sandboxes on their next request. The env holds a fixed marker instead,
which keeps a client that refuses an empty key working and is what
certification reads.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from typing import Any

import httpx
import structlog
from e2b import (
    AuthenticationException,
    RateLimitException,
    Secret,
    SecretNotFoundException,
    ServiceBusyException,
    TimeoutException,
)

from ptc_agent.config.core import PlatformSecretDefinition
from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    PlatformSecretReconciler,
    PlatformSecretReconciliationError,
    ReconciledPlatformSecret,
    ResolvedPlatformSecret,
)

logger = structlog.get_logger(__name__)

# E2B's per-sandbox limits: hosts with a rule, and headers in one host's rule.
_MAX_RULE_HOSTS = 10
_MAX_RULE_HEADERS = 20
# The SDK words an API error it has no class for as "<status>: <message>".
_STATUS_PREFIX = re.compile(r"^(\d{3}): ")

# E2B's proxy terminates TLS for every host with a rule, signing with a CA it
# installs into the system store of every sandbox at boot. Python's certifi and
# Node's bundled roots never read that store, so they are pointed at it. Set on
# every sandbox, so one created before a catalog existed can take rules later.
TLS_TRUST_ENV = {
    "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    "REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt",
    "NODE_EXTRA_CA_CERTS": "/usr/local/share/ca-certificates/e2b-ca.crt",
}


def secret_marker(env_var: str) -> str:
    """What a sandbox's env holds in place of one platform credential.

    Fixed per variable rather than derived from the Secret's name: E2B sets a
    sandbox's env once, at create, so a renamed Secret reaches running
    sandboxes through their egress rules, and a marker that moved with the
    name could never be certified on them again.
    """
    return f"e2b_secret_{env_var.lower()}"


def egress_rule(definition: PlatformSecretDefinition) -> str:
    """The part of a catalog entry every sandbox is told, as a stable string."""
    return json.dumps(
        {
            "header": (definition.header or "").lower(),
            "hosts": sorted(definition.hosts),
        },
        sort_keys=True,
    )


def egress_rules(
    bindings: Mapping[str, str],
    routes: Mapping[str, PlatformSecretDefinition],
) -> dict[str, list[dict[str, Any]]]:
    """E2B network rules that inject each bound Secret on its hosts.

    E2B allows one rule per host, so entries that share a host share its rule,
    one header each.
    """
    headers_by_host: dict[str, dict[str, str]] = {}
    for env_var, secret_name in sorted(bindings.items()):
        definition = routes.get(env_var)
        if definition is None:
            raise PlatformSecretConfigurationError(
                f"No platform-secret catalog entry binds {env_var}"
            )
        if not definition.header:
            raise PlatformSecretConfigurationError(
                f"Platform secret {env_var} needs a header: E2B delivers a "
                "Secret only as one named request header"
            )
        for host in definition.hosts:
            headers = headers_by_host.setdefault(host, {})
            if any(h.lower() == definition.header.lower() for h in headers):
                raise PlatformSecretConfigurationError(
                    f"Two platform secrets set header {definition.header!r} "
                    f"on {host}"
                )
            headers[definition.header] = Secret.fill(secret_name)
    if len(headers_by_host) > _MAX_RULE_HOSTS:
        raise PlatformSecretConfigurationError(
            f"Platform secrets name {len(headers_by_host)} hosts; E2B allows "
            f"{_MAX_RULE_HOSTS} per sandbox"
        )
    for host, headers in headers_by_host.items():
        if len(headers) > _MAX_RULE_HEADERS:
            raise PlatformSecretConfigurationError(
                f"Platform secrets set {len(headers)} headers on {host}; E2B "
                f"allows {_MAX_RULE_HEADERS} per host"
            )
    return {
        host: [{"transform": {"headers": headers}}]
        for host, headers in headers_by_host.items()
    }


class E2BSecretReconciler(PlatformSecretReconciler):
    """Creates or updates E2B Secrets idempotently, one per catalog entry."""

    label = "E2B"
    key_error_statuses = frozenset({401, 403})
    key_error_message = "E2B_API_KEY cannot manage Secrets (status={status})"

    def __init__(self, client: Any) -> None:
        self._secrets = client.AsyncSecret

    def _validate(self, secrets: Sequence[ResolvedPlatformSecret]) -> None:
        # A catalog E2B cannot route fails here rather than at the first
        # sandbox create.
        egress_rules(
            {s.definition.sandbox_env_var: s.name for s in secrets},
            {s.definition.sandbox_env_var: s.definition for s in secrets},
        )

    async def _reconcile_one(
        self, secret: ResolvedPlatformSecret
    ) -> ReconciledPlatformSecret:
        # The value is write-only, so every boot writes it as a new version;
        # sandboxes read the current one per request.
        metadata = {"description": secret.definition.description}
        try:
            info = await self._secrets.update(
                secret.name, secret.value, metadata=metadata
            )
        except SecretNotFoundException:
            try:
                info = await self._secrets.create(
                    secret.name, secret.value, metadata=metadata
                )
                logger.info("Created E2B platform Secret", secret_name=secret.name)
            except Exception:
                # Another worker created it first (409), or the create landed
                # and only its answer was lost: either way it exists now.
                if not await self._secrets.exists(secret.name):
                    raise
                info = await self._secrets.update(
                    secret.name, secret.value, metadata=metadata
                )
        provider_secret_id = str(getattr(info, "secret_id", "") or "")
        if not provider_secret_id:
            raise PlatformSecretReconciliationError(
                "E2B returned incomplete platform Secret metadata"
            )
        env_var = secret.definition.sandbox_env_var
        return ReconciledPlatformSecret(
            definition=secret.definition,
            name=secret.name,
            provider_secret_id=provider_secret_id,
            placeholder=secret_marker(env_var),
            egress_rule=egress_rule(secret.definition),
        )

    def _status(self, exc: Exception) -> int | None:
        if isinstance(exc, AuthenticationException):
            return 401
        status = getattr(exc, "status_code", None)
        if isinstance(status, int):
            return status
        match = _STATUS_PREFIX.match(str(exc))
        return int(match.group(1)) if match else None

    def _retryable(self, exc: Exception, status: int | None) -> bool:
        return isinstance(
            exc,
            (
                RateLimitException,
                ServiceBusyException,
                TimeoutException,
                httpx.TransportError,
                ConnectionError,
                TimeoutError,
            ),
        ) or (status is not None and status >= 500)
