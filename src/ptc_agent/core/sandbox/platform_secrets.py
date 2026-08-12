"""Platform-secret catalog, hosted resolution, and sandbox convergence mechanics."""

from __future__ import annotations

import asyncio
import os
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from ptc_agent.config.core import CoreConfig, PlatformSecretDefinition
from ptc_agent.core.sandbox.runtime import RuntimeState, SandboxRuntime


# Bounded so derived Secret names (namespace + catalog suffix) stay well under
# provider and DB VARCHAR(255) limits; mirrors the vault-secret name cap.
_DAYTONA_SECRET_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")
_ENV_VAR_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")


class PlatformSecretError(RuntimeError):
    """Base class for failures that must fail hosted platform-secret readiness."""


class PlatformSecretConfigurationError(PlatformSecretError):
    """Hosted platform-secret configuration is missing or invalid."""


class PlatformSecretReconciliationError(PlatformSecretError):
    """A required provider-managed Secret could not be reconciled."""


class PlatformSecretVerificationError(PlatformSecretError):
    """The in-sandbox placeholder did not match the active rollout."""


@dataclass(frozen=True)
class ResolvedPlatformSecret:
    """One catalog entry resolved against the hosted deployment environment."""

    definition: PlatformSecretDefinition
    name: str
    value: str


@dataclass(frozen=True)
class ReconciledPlatformSecret:
    """Provider identity returned after one Secret is created or updated."""

    definition: PlatformSecretDefinition
    name: str
    provider_secret_id: str
    placeholder: str


#: The runtime gate: sandbox providers whose egress layer can substitute
#: platform secrets. The single source of capability truth.
PLATFORM_SECRET_CAPABLE_PROVIDERS = frozenset({"daytona"})


def _capable_provider(config: CoreConfig) -> bool:
    return config.sandbox.provider in PLATFORM_SECRET_CAPABLE_PROVIDERS


def platform_secrets_active(config: CoreConfig) -> bool:
    """Whether managed secrets run for this configuration.

    Host-mode-agnostic: active whenever a capable provider has a configured
    catalog. Configuring ``sandbox.platform_secrets`` is the opt-in, so hosted
    deployments and OSS + Daytona users alike get managed-secret injection; an
    empty catalog leaves the sandbox on plaintext env (unchanged behavior).
    """

    return _capable_provider(config) and bool(config.sandbox.platform_secrets)


def platform_secrets_required(
    config: CoreConfig, *, host_mode: str | None = None
) -> bool:
    """Whether managed secrets are mandatory (the hosted fail-closed guard).

    Hosted platform mode on a capable provider must inject managed secrets: an
    empty catalog there is a deploy error, never a silent plaintext fallback.
    """

    if host_mode is None:
        from src.config.env import HOST_MODE

        host_mode = HOST_MODE
    return host_mode == "platform" and _capable_provider(config)


def resolve_platform_secrets(
    config: CoreConfig,
    *,
    environ: Mapping[str, str] | None = None,
    host_mode: str | None = None,
) -> tuple[ResolvedPlatformSecret, ...]:
    """Resolve the catalog, failing closed once managed secrets are in play.

    Active when a capable provider has a configured catalog (hosted, or an
    opt-in OSS + Daytona deployment). A hosted capable deployment that ships an
    empty catalog fails closed rather than silently injecting plaintext.
    """

    # The catalog lives in deployment config (sandbox.platform_secrets in
    # agent_config.yaml), never code — the OSS repo ships the mechanism, each
    # deployment declares its vendors. Empty is the opt-out (plaintext) except
    # in hosted capable mode, where it is a deploy error.
    catalog = tuple(config.sandbox.platform_secrets)
    if not catalog:
        if platform_secrets_required(config, host_mode=host_mode):
            raise PlatformSecretConfigurationError(
                "sandbox.platform_secrets is empty — hosted Daytona mode requires "
                "the platform-secret catalog in agent_config.yaml"
            )
        return ()
    if not _capable_provider(config):
        return ()

    env = os.environ if environ is None else environ
    daytona_config = config.sandbox.daytona
    namespace = str(getattr(daytona_config, "secret_namespace", "") or "")
    if not namespace:
        raise PlatformSecretConfigurationError(
            "DAYTONA_SECRET_NAMESPACE is required when a platform-secret catalog "
            "is configured"
        )
    if not _DAYTONA_SECRET_NAME_RE.fullmatch(namespace):
        raise PlatformSecretConfigurationError(
            "DAYTONA_SECRET_NAMESPACE must match ^[A-Za-z_][A-Za-z0-9_-]*$"
        )

    resolved: list[ResolvedPlatformSecret] = []
    for definition in catalog:
        value = env.get(definition.source_env_var, "")
        if not value:
            raise PlatformSecretConfigurationError(
                f"{definition.source_env_var} is required when a platform-secret "
                "catalog is configured"
            )
        name = f"{namespace}-{definition.name_suffix}"
        if not _DAYTONA_SECRET_NAME_RE.fullmatch(name):
            raise PlatformSecretConfigurationError(
                f"Derived Secret name {name!r} exceeds the provider/DB name "
                "limit — shorten DAYTONA_SECRET_NAMESPACE or the catalog "
                "name_suffix"
            )
        resolved.append(
            ResolvedPlatformSecret(
                definition=definition,
                name=name,
                value=value,
            )
        )
    return tuple(resolved)


def build_platform_secret_bindings(
    config: CoreConfig,
    *,
    environ: Mapping[str, str] | None = None,
    host_mode: str | None = None,
) -> dict[str, str]:
    """Build the sandbox env-var to provider Secret-name mount map."""

    return {
        secret.definition.sandbox_env_var: secret.name
        for secret in resolve_platform_secrets(
            config, environ=environ, host_mode=host_mode
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
        "python3 -c 'import hashlib,os\n"
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
    """Require every env var to hash to its registered rollout placeholder."""

    observed = await probe_runtime_platform_secrets(
        runtime, env_vars=sorted(expected)
    )
    for env_var, placeholder_sha256 in expected.items():
        if not observed.get(env_var) or observed[env_var] != placeholder_sha256:
            raise PlatformSecretVerificationError(
                "Platform-secret placeholder verification failed"
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
    """

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
    """

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
