"""Sandbox provider factory."""

from ptc_agent.config.core import CoreConfig, DaytonaConfig, DockerConfig
from ptc_agent.core.sandbox.runtime import SandboxProvider

ProviderConfig = DaytonaConfig | DockerConfig


def build_provider(
    kind: str,
    provider_config: ProviderConfig,
    *,
    working_dir: str | None = None,
) -> SandboxProvider:
    """Build a provider from one backend's own kind and settings.

    The whole identity of the returned provider arrives through these
    arguments, so two backends can be addressed from one process.

    Raises:
        ValueError: If ``kind`` names no known provider.
    """
    if kind == "daytona":
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        return DaytonaProvider(provider_config, working_dir=working_dir)

    if kind == "docker":
        from ptc_agent.core.sandbox.providers.docker import DockerProvider

        return DockerProvider(provider_config, working_dir=working_dir)

    raise ValueError(f"Unknown sandbox provider: {kind!r}")


def create_provider(config: CoreConfig) -> SandboxProvider:
    """Build the provider one CoreConfig selects.

    ``SandboxConfig`` holds each kind's settings under a field of that same
    name, which is what lets the selection stay a single lookup.

    Raises:
        ValueError: If ``sandbox.provider`` names no known provider.
    """
    kind = config.sandbox.provider
    provider_config = getattr(config.sandbox, kind, None)
    if provider_config is None:
        raise ValueError(f"Unknown sandbox provider: {kind!r}")
    return build_provider(
        kind,
        provider_config,
        # filesystem.working_directory is the single source of truth for both
        # providers; the per-provider working_dir field is only a fallback.
        working_dir=config.filesystem.working_directory,
    )
