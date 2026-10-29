"""Platform-secret catalog, hosted resolution, and sandbox convergence mechanics."""

from __future__ import annotations

import asyncio
import os
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import ClassVar

import structlog

from ptc_agent.config.core import CoreConfig, PlatformSecretDefinition
from ptc_agent.core.sandbox.runtime import (
    RuntimeState,
    SandboxProvider,
    SandboxRuntime,
)

logger = structlog.get_logger(__name__)


# Bounded so derived Secret names (namespace + catalog suffix) stay well under
# provider and DB VARCHAR(255) limits; mirrors the vault-secret name cap.
_SECRET_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")
_ENV_VAR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
# Env names that conventionally carry a credential. Deliberately narrower than
# mcp_sanitize's redaction heuristic, which also flags long values: a match
# here withholds a variable from the sandbox, so a non-secret must not match.
_CREDENTIAL_ENV_RE = re.compile(
    r"(?:^|_)(?:API_KEY|ACCESS_KEY|SECRET_KEY|PRIVATE_KEY|SECRET|TOKEN|PASSWORD)$",
    re.IGNORECASE,
)


class PlatformSecretError(RuntimeError):
    """Base class for failures that must fail hosted platform-secret readiness."""


class PlatformSecretConfigurationError(PlatformSecretError):
    """Hosted platform-secret configuration is missing or invalid."""


class PlatformSecretReconciliationError(PlatformSecretError):
    """A required provider-managed Secret could not be reconciled."""


class PlatformSecretVerificationError(PlatformSecretError):
    """The in-sandbox placeholder did not match the active rollout."""


class PlatformSecretReplacementRequired(PlatformSecretVerificationError):
    """A sandbox's fixed env holds something other than a catalog marker.

    Its provider sets env once, at create, so every process it starts inherits
    that value and no restart removes it: only a new sandbox is clean.
    """


@dataclass(frozen=True)
class ResolvedPlatformSecret:
    """One catalog entry resolved against the hosted deployment environment."""

    definition: PlatformSecretDefinition
    name: str
    value: str


@dataclass(frozen=True)
class ReconciledPlatformSecret:
    """Provider identity returned after one Secret is created or updated.

    ``egress_rule`` is what each sandbox must be told besides its env, on a
    provider that binds a Secret per sandbox (E2B); empty where the provider
    holds the routing itself (Daytona). It is part of the rollout identity, so
    a change to it re-pends the fleet.
    """

    definition: PlatformSecretDefinition
    name: str
    provider_secret_id: str
    placeholder: str
    egress_rule: str = ""


class PlatformSecretReconciler(ABC):
    """Creates or updates one provider's Secrets idempotently, one per catalog entry.

    Each provider supplies the write and how to read its errors. What a failure
    means is shared: a rejected key is a misconfiguration that must fail boot
    rather than continue on an earlier rollout, and anything else is an outage
    after a few retries.
    """

    label: ClassVar[str]
    # Statuses that mean the key cannot manage Secrets, never retried.
    key_error_statuses: ClassVar[frozenset[int]]
    # Formatted with ``status``.
    key_error_message: ClassVar[str]

    async def reconcile(
        self, secrets: Sequence[ResolvedPlatformSecret]
    ) -> tuple[ReconciledPlatformSecret, ...]:
        self._validate(secrets)
        reconciled: list[ReconciledPlatformSecret] = []
        for secret in secrets:
            reconciled.append(
                await self._with_retry(
                    secret.name,
                    lambda secret=secret: self._reconcile_one(secret),
                )
            )
        return tuple(reconciled)

    def _validate(self, secrets: Sequence[ResolvedPlatformSecret]) -> None:
        """Refuse a catalog the provider cannot serve, before any Secret is written."""

    @abstractmethod
    async def _reconcile_one(
        self, secret: ResolvedPlatformSecret
    ) -> ReconciledPlatformSecret: ...

    @abstractmethod
    def _status(self, exc: Exception) -> int | None:
        """The HTTP status *exc* carries, if any."""

    @abstractmethod
    def _retryable(self, exc: Exception, status: int | None) -> bool:
        """Whether *exc* may succeed on another attempt."""

    async def _with_retry(
        self,
        secret_name: str,
        operation: Callable[[], Awaitable[ReconciledPlatformSecret]],
    ) -> ReconciledPlatformSecret:
        """Retry 429/5xx and transport failures without logging bodies.

        One of our own errors already says what went wrong, so it passes
        through as raised rather than as a status-less provider failure.
        """

        delay_s = 0.25
        for attempt in range(1, 4):
            try:
                return await operation()
            except PlatformSecretError:
                raise
            except Exception as exc:
                status = self._status(exc)
                retryable = self._retryable(exc, status)
                logger.warning(
                    f"{self.label} platform Secret reconciliation failed",
                    secret_name=secret_name,
                    attempt=attempt,
                    status=status,
                    error_type=type(exc).__name__,
                    retrying=retryable and attempt < 3,
                )
                if not retryable or attempt == 3:
                    if status in self.key_error_statuses:
                        raise PlatformSecretConfigurationError(
                            self.key_error_message.format(status=status)
                        ) from None
                    status_text = str(status) if status is not None else "unknown"
                    raise PlatformSecretReconciliationError(
                        f"Failed to reconcile {self.label} platform Secret "
                        f"{secret_name!r} (status={status_text}, "
                        f"error_type={type(exc).__name__})"
                    ) from None
                await asyncio.sleep(delay_s)
                delay_s *= 2
        raise AssertionError("unreachable")


def _provider_class(kind: str | None) -> type[SandboxProvider] | None:
    """The provider class for *kind*, or None for an unknown kind or None.

    Imported on use because the providers import this module.
    """
    from ptc_agent.core.sandbox.providers import provider_class

    try:
        return provider_class(kind)  # type: ignore[arg-type]
    except ValueError:
        return None


def provider_kind_supports_platform_secrets(kind: str | None) -> bool:
    """Whether a provider kind's egress layer can substitute platform secrets.

    The kind-level form of the gate, for a caller holding a computer's ``kind``
    rather than a whole config. An unknown kind, or None, cannot.
    """
    cls = _provider_class(kind)
    return cls is not None and cls.platform_secret_namespace_env is not None


def _resolved_host_mode(host_mode: str | None) -> str:
    if host_mode is None:
        from src.config.env import HOST_MODE

        return HOST_MODE
    return host_mode


def platform_secrets_active(config: CoreConfig, *, kind: str | None = None) -> bool:
    """Whether managed secrets run for this configuration.

    Host-mode-agnostic: active whenever a capable provider has a configured
    catalog. Configuring ``sandbox.platform_secrets`` is the opt-in, so hosted
    deployments and OSS + Daytona users alike get managed-secret injection.
    With an empty catalog nothing is managed: self-hosted, built-in env stays
    plaintext; hosted, credential-named built-in env is withheld instead
    (``platform_secrets_required``).
    ``kind`` asks for one computer's provider rather than the deployment's.
    """

    return provider_kind_supports_platform_secrets(
        kind or config.sandbox.provider
    ) and bool(config.sandbox.platform_secrets)


def provider_credentials_configured(config: CoreConfig, kind: str) -> bool:
    """Whether this deployment holds an API key for a provider kind.

    A kind without one is never reconciled: its Secrets cannot be written and
    its computers cannot start, so they fail closed instead of failing boot.
    """

    try:
        settings = config.sandbox.settings_for(kind)
    except ValueError:
        return False
    return bool(getattr(settings, "api_key", ""))


def platform_secrets_required(
    config: CoreConfig, *, kind: str | None = None, host_mode: str | None = None
) -> bool:
    """Whether a platform credential may reach this sandbox only as a managed Secret.

    Hosted mode on a capable provider: a built-in env var named like a
    credential reaches the sandbox only through a catalog binding, and is
    withheld otherwise. The catalog may be empty, which just means no built-in
    needs one; the guard is the withholding, not the catalog's presence.
    ``kind`` asks for one computer's provider rather than the deployment's.
    """

    return _resolved_host_mode(
        host_mode
    ) == "platform" and provider_kind_supports_platform_secrets(
        kind or config.sandbox.provider
    )


def require_platform_secret_capable(
    config: CoreConfig, kinds: Sequence[str], *, host_mode: str | None = None
) -> None:
    """Refuse a hosted kind that would receive the catalog's credentials as plaintext.

    A provider that cannot hold Secrets resolves each catalog credential from
    the host's environment into ordinary sandbox env. Self-hosted, that is the
    documented fall-through; hosted, those credentials are the platform's.
    A machine the deployment does not trust is exempt: it is never given a
    host-resolved value at all, so it has nothing to hold.

    Raises:
        PlatformSecretConfigurationError: In hosted mode with a catalog, for
            the first of ``kinds`` that cannot hold platform Secrets.
    """
    if not config.sandbox.platform_secrets:
        return
    if _resolved_host_mode(host_mode) != "platform":
        return
    for kind in kinds:
        cls = _provider_class(kind)
        if cls is not None and not cls.trusted_host(config.sandbox):
            continue
        if not provider_kind_supports_platform_secrets(kind):
            raise PlatformSecretConfigurationError(
                f"{kind} cannot hold platform Secrets"
            )


def resolve_platform_secrets(
    config: CoreConfig,
    *,
    kind: str | None = None,
    environ: Mapping[str, str] | None = None,
    host_mode: str | None = None,
) -> tuple[ResolvedPlatformSecret, ...]:
    """Resolve the catalog, failing closed once managed secrets are in play.

    Active when a capable provider has a configured catalog (hosted, or an
    opt-in OSS + Daytona deployment). An empty catalog resolves to nothing in
    any mode. A hosted catalog on a kind that cannot hold Secrets fails closed.
    ``kind`` resolves names for that provider instead of the deployment's.
    """

    # The catalog lives in deployment config (sandbox.platform_secrets in
    # agent_config.yaml), never code: the OSS repo ships the mechanism, each
    # deployment declares its vendors. Hosted mode does not need one; a
    # credential no entry covers is withheld from the sandbox instead.
    catalog = tuple(config.sandbox.platform_secrets)
    provider = kind or config.sandbox.provider
    if not catalog:
        return ()
    provider_cls = _provider_class(provider)
    namespace_var = provider_cls.platform_secret_namespace_env if provider_cls else None
    if provider_cls is None or namespace_var is None:
        require_platform_secret_capable(config, (provider,), host_mode=host_mode)
        return ()

    env = os.environ if environ is None else environ
    provider_config = config.sandbox.settings_for(provider)
    namespace = str(getattr(provider_config, "secret_namespace", "") or "")
    if not namespace:
        raise PlatformSecretConfigurationError(
            f"{namespace_var} is required when a platform-secret catalog "
            "is configured"
        )
    if not _SECRET_NAME_RE.fullmatch(namespace):
        raise PlatformSecretConfigurationError(
            f"{namespace_var} must match ^[A-Za-z_][A-Za-z0-9_-]*$"
        )

    resolved: list[ResolvedPlatformSecret] = []
    for definition in catalog:
        value = env.get(definition.source_env_var, "")
        if not value:
            raise PlatformSecretConfigurationError(
                f"{definition.source_env_var} is required when a platform-secret "
                "catalog is configured"
            )
        name = provider_cls.platform_secret_name(
            f"{namespace}-{definition.name_suffix}"
        )
        if not _SECRET_NAME_RE.fullmatch(name):
            raise PlatformSecretConfigurationError(
                f"Derived Secret name {name!r} exceeds the provider/DB name "
                f"limit: shorten {namespace_var} or the catalog name_suffix"
            )
        if any(earlier.name == name for earlier in resolved):
            raise PlatformSecretConfigurationError(
                f"Two catalog entries resolve to the Secret name {name!r}"
            )
        resolved.append(
            ResolvedPlatformSecret(
                definition=definition,
                name=name,
                value=value,
            )
        )
    return tuple(resolved)


def is_credential_env_name(name: str) -> bool:
    """Whether an env var's name says it holds a credential."""
    return bool(_CREDENTIAL_ENV_RE.search(name))


def build_platform_secret_bindings(
    config: CoreConfig,
    *,
    kind: str | None = None,
    environ: Mapping[str, str] | None = None,
    host_mode: str | None = None,
) -> dict[str, str]:
    """Build the sandbox env-var to provider Secret-name mount map."""

    return {
        secret.definition.sandbox_env_var: secret.name
        for secret in resolve_platform_secrets(
            config, kind=kind, environ=environ, host_mode=host_mode
        )
    }


# -- Sandbox convergence mechanics (provider-neutral, via SandboxRuntime) --

_STABLE_STATES = {
    RuntimeState.RUNNING,
    RuntimeState.STOPPED,
    RuntimeState.ARCHIVED,
    RuntimeState.ERROR,
}

# Worst case is an archived sandbox restoring from cold storage.
_STABILIZE_TIMEOUT_S = 300


def _probe_env_command(env_vars: Sequence[str]) -> str:
    for env_var in env_vars:
        if not _ENV_VAR_RE.fullmatch(env_var):
            raise PlatformSecretConfigurationError(
                f"Invalid platform-secret env var name: {env_var!r}"
            )
    names = ",".join(f'"{env_var}"' for env_var in env_vars)
    # Hash in-sandbox: one exec covers the whole set and plaintext never
    # crosses the exec boundary.
    return (
        "python3 -I -c 'import hashlib,os\n"
        f"for v in [{names}]:\n"
        '    val = os.environ.get(v, "")\n'
        '    print(hashlib.sha256(val.encode()).hexdigest() if val else "")\''
    )


async def _wait_for_state(
    runtime: SandboxRuntime,
    accepted: set[RuntimeState],
    *,
    timeout: float,
) -> RuntimeState:
    deadline = time.monotonic() + timeout
    while True:
        state = await runtime.refresh_state()
        if state in accepted:
            return state
        if state == RuntimeState.ERROR:
            raise RuntimeError("Sandbox entered an error state")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for sandbox state")
        await asyncio.sleep(min(0.5, remaining))


async def _stabilize_state(runtime: SandboxRuntime) -> RuntimeState:
    state = await runtime.refresh_state()
    if state in _STABLE_STATES:
        return state
    return await _wait_for_state(
        runtime, _STABLE_STATES, timeout=_STABILIZE_TIMEOUT_S
    )


async def _ensure_running(runtime: SandboxRuntime, *, timeout: int) -> None:
    state = await _stabilize_state(runtime)
    if state == RuntimeState.ERROR:
        raise RuntimeError("Cannot operate on a sandbox in error state")
    if state != RuntimeState.RUNNING:
        try:
            await runtime.start(timeout=timeout)
        except Exception:
            # Start timeouts are observationally ambiguous. Refresh before
            # deciding whether the remote transition actually failed.
            pass
    await _wait_for_state(runtime, {RuntimeState.RUNNING}, timeout=timeout)


async def force_stop_runtime(runtime: SandboxRuntime, *, timeout: int = 120) -> None:
    """Force-stop a sandbox so no process retains plaintext env values."""

    state = await _stabilize_state(runtime)
    if state in {RuntimeState.STOPPED, RuntimeState.ARCHIVED}:
        return
    try:
        await runtime.stop(timeout=timeout, force=True)
    except Exception:
        # Stop timeouts are also ambiguous; the state poll below is authoritative.
        pass
    await _wait_for_state(runtime, {RuntimeState.STOPPED}, timeout=timeout)


async def probe_runtime_platform_secrets(
    runtime: SandboxRuntime, *, env_vars: Sequence[str]
) -> dict[str, str]:
    """Return sha256 per env var of its in-sandbox value ('' if unset)."""

    result = await runtime.exec(_probe_env_command(env_vars), timeout=60)
    if result.exit_code != 0:
        raise PlatformSecretVerificationError(
            f"Platform-secret env probe failed (exit_code={result.exit_code})"
        )
    lines = [line.strip() for line in result.stdout.splitlines()]
    # Trailing empty lines (unset vars) may be stripped by the exec transport.
    lines += [""] * (len(env_vars) - len(lines))
    if len(lines) != len(env_vars) or any(
        line and not _SHA256_HEX_RE.fullmatch(line) for line in lines
    ):
        raise PlatformSecretVerificationError(
            "Platform-secret env probe returned malformed output"
        )
    return dict(zip(env_vars, lines))


async def verify_runtime_platform_secrets(
    runtime: SandboxRuntime,
    *,
    expected: Mapping[str, str],
) -> None:
    """Require every env var to hash to its registered rollout placeholder.

    On a runtime that cannot update env (E2B) a mismatch raises
    ``PlatformSecretReplacementRequired``, since only a new sandbox changes
    that env: plaintext from before the catalog entry, or nothing at all for
    an entry added since create, which a client that refuses an empty key
    never gets past to the egress rule.
    """

    observed = await probe_runtime_platform_secrets(
        runtime, env_vars=sorted(expected)
    )
    fixed_env = not runtime.updates_env
    stale: list[str] = []
    for env_var, placeholder_sha256 in expected.items():
        if observed.get(env_var) == placeholder_sha256:
            continue
        if not fixed_env:
            raise PlatformSecretVerificationError(
                "Platform-secret placeholder verification failed"
            )
        stale.append(env_var)
    if stale:
        raise PlatformSecretReplacementRequired(
            "Sandbox env does not hold the marker for " + ", ".join(sorted(stale))
        )


async def remount_platform_secret_bindings(
    runtime: SandboxRuntime,
    *,
    expected: Mapping[str, str],
    bindings: dict[str, str],
) -> None:
    """Hot-swap the placeholder mounts on a running sandbox; never restarts.

    New processes see the new placeholders immediately (live-verified);
    existing processes keep whatever they held — opaque placeholders on a
    certified sandbox, so this is safe mid-serve. A sandbox that ever held
    plaintext needs the scrub-restart in ``converge_sandbox_platform_secrets``.

    A runtime that cannot update env (E2B) keeps the env it was created with,
    a fixed marker per variable, and takes the bindings as egress rules alone.
    """

    if runtime.updates_env:
        await runtime.update_env({}, unset=sorted(expected))
    await runtime.update_secrets(bindings)


async def converge_sandbox_platform_secrets(
    runtime: SandboxRuntime,
    *,
    expected: Mapping[str, str],
    bindings: dict[str, str],
) -> None:
    """Scrub one sandbox onto the active rollout placeholder set.

    The caller's DB row is the decision authority — this scrubs
    unconditionally and probes the sandbox only to verify the result.

    Raises:
        PlatformSecretReplacementRequired: Before touching a runtime whose env
            is fixed at create, which no restart scrubs.
    """

    if not runtime.updates_env:
        raise PlatformSecretReplacementRequired(
            f"{type(runtime).__name__} fixes its env at create, so only a new "
            "sandbox is clean"
        )
    # _STABILIZE_TIMEOUT_S, not a shorter start timeout: the first call may be
    # restoring an archived sandbox from cold storage (the DB row says running,
    # but the provider-side state can have drifted via autostop/archival).
    await _ensure_running(runtime, timeout=_STABILIZE_TIMEOUT_S)
    # update_env needs the running daemon; existing processes may retain the
    # plaintext values until the mandatory force-stop below.
    await remount_platform_secret_bindings(
        runtime, expected=expected, bindings=bindings
    )
    await force_stop_runtime(runtime)
    await _ensure_running(runtime, timeout=_STABILIZE_TIMEOUT_S)
    await verify_runtime_platform_secrets(runtime, expected=expected)
