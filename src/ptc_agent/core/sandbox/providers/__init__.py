"""Sandbox provider factory."""

from collections.abc import Sequence
from importlib import import_module

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    DockerConfig,
    E2BConfig,
    PlatformSecretDefinition,
    SSHConfig,
)
from ptc_agent.core.sandbox.runtime import SandboxProvider

ProviderConfig = DaytonaConfig | DockerConfig | E2BConfig | SSHConfig

# Imported on first use: each provider module loads its vendor's SDK.
_PROVIDERS: dict[str, str] = {
    "daytona": "ptc_agent.core.sandbox.providers.daytona:DaytonaProvider",
    "docker": "ptc_agent.core.sandbox.providers.docker:DockerProvider",
    "e2b": "ptc_agent.core.sandbox.providers.e2b:E2BProvider",
    "ssh": "ptc_agent.core.sandbox.providers.ssh:SSHProvider",
}

PROVIDER_KINDS = tuple(_PROVIDERS)


def provider_class(kind: str) -> type[SandboxProvider]:
    """The provider class for ``kind``, for facts a caller reads without
    building one (building a provider can set up its client).

    Raises:
        ValueError: If ``kind`` names no known provider.
    """
    target = _PROVIDERS.get(kind) if isinstance(kind, str) else None
    if target is None:
        raise ValueError(f"Unknown sandbox provider: {kind!r}")
    module, _, name = target.partition(":")
    return getattr(import_module(module), name)


def provider_runs_remote(kind: str | None) -> bool:
    """Whether sandboxes of ``kind`` run off this host; an unknown kind reads as local."""
    try:
        return provider_class(kind).runs_remote  # type: ignore[arg-type]
    except ValueError:
        return False


def may_be_primary(kind: str) -> bool:
    """Whether a computer of ``kind`` may be the user's primary. Home lives on
    the primary, which is never removed, so only a machine the deployment can
    build again may hold it: one the user brought could be wiped or gone for
    good with Home on it.

    Raises:
        ValueError: If ``kind`` names no known provider.
    """
    return provider_class(kind).recreatable


def build_provider(
    kind: str,
    provider_config: ProviderConfig,
    *,
    working_dir: str | None = None,
    platform_secrets: Sequence[PlatformSecretDefinition] = (),
) -> SandboxProvider:
    """Build a provider from one backend's own kind and settings.

    The whole identity of the returned provider arrives through these
    arguments, so two backends can be addressed from one process.
    ``platform_secrets`` is the deployment's catalog.

    Raises:
        ValueError: If ``kind`` names no known provider.
    """
    return provider_class(kind)(
        provider_config,
        working_dir=working_dir,
        platform_secrets=platform_secrets,
    )


def create_provider(config: CoreConfig) -> SandboxProvider:
    """Build the provider one CoreConfig selects.

    Raises:
        ValueError: If ``sandbox.provider`` names no known provider.
    """
    kind = config.sandbox.provider
    return build_provider(
        kind,
        config.sandbox.settings_for(kind),
        # filesystem.working_directory is the single source of truth for both
        # providers; the per-provider working_dir field is only a fallback.
        working_dir=config.filesystem.working_directory,
        platform_secrets=config.sandbox.platform_secrets,
    )
