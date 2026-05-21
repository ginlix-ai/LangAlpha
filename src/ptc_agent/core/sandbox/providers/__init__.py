"""Sandbox provider factory."""

from ptc_agent.core.sandbox.runtime import SandboxProvider


def create_provider(config) -> SandboxProvider:
    """Create a sandbox provider based on configuration.

    Args:
        config: CoreConfig (or compatible) with a ``sandbox.provider`` field.

    Returns:
        A concrete SandboxProvider instance.

    Raises:
        ValueError: If the provider name is not recognized.
    """
    provider_name = getattr(
        getattr(config, "sandbox", None), "provider", "daytona"
    )

    # Both providers use filesystem.working_directory as single source of truth
    working_dir = getattr(
        getattr(config, "filesystem", None), "working_directory", None
    )

    if provider_name == "daytona":
        from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider

        return DaytonaProvider(config.sandbox.daytona, working_dir=working_dir)

    if provider_name == "docker":
        from ptc_agent.core.sandbox.providers.docker import DockerProvider

        return DockerProvider(config.sandbox.docker, working_dir=working_dir)

    if provider_name == "memory":
        from ptc_agent.core.sandbox.providers.memory import LocalProvider

        return LocalProvider()

    raise ValueError(f"Unknown sandbox provider: {provider_name!r}")
