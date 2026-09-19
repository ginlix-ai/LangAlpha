"""Tests for the sandbox provider factory.

Locks the contract that a provider's whole identity arrives as arguments:
``build_provider`` reads no process-wide configuration, so one process can
address two backends at once.
"""

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    DockerConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.providers import build_provider, create_provider


def _core_config(sandbox: SandboxConfig, working_directory: str) -> CoreConfig:
    return CoreConfig(
        sandbox=sandbox,
        security=SecurityConfig(),
        mcp=MCPConfig(),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(working_directory=working_directory),
    )


class TestBuildProvider:
    def test_daytona_uses_the_config_it_is_given(self):
        config = DaytonaConfig(api_key="key-a", base_url="https://a.example.com/api")
        provider = build_provider("daytona", config, working_dir="/home/a")

        assert provider._config is config
        assert provider._working_dir == "/home/a"

    def test_docker_uses_the_config_it_is_given(self):
        config = DockerConfig(image="sandbox-a:latest")
        provider = build_provider("docker", config, working_dir="/home/a")

        assert provider._config is config
        assert provider._working_dir == "/home/a"

    def test_two_backends_coexist_in_one_process(self):
        first = build_provider(
            "daytona", DaytonaConfig(api_key="key-a"), working_dir="/home/a"
        )
        second = build_provider(
            "daytona", DaytonaConfig(api_key="key-b"), working_dir="/home/b"
        )

        assert first._config.api_key == "key-a"
        assert second._config.api_key == "key-b"
        assert first._working_dir != second._working_dir

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError, match="Unknown sandbox provider"):
            build_provider("memory", DaytonaConfig(), working_dir="/home/a")


class TestCreateProvider:
    def test_selects_the_sub_config_matching_the_provider_field(self):
        sandbox = SandboxConfig(
            provider="docker",
            daytona=DaytonaConfig(api_key="unused"),
            docker=DockerConfig(image="sandbox-b:latest"),
        )
        provider = create_provider(_core_config(sandbox, "/home/workspace"))

        assert provider._config is sandbox.docker
        assert provider._working_dir == "/home/workspace"

    def test_filesystem_working_directory_wins_over_the_provider_field(self):
        sandbox = SandboxConfig(
            provider="docker", docker=DockerConfig(working_dir="/ignored")
        )
        provider = create_provider(_core_config(sandbox, "/home/workspace"))

        assert provider._working_dir == "/home/workspace"

    def test_unconstructable_provider_raises(self):
        sandbox = SandboxConfig(provider="memory")

        with pytest.raises(ValueError, match="Unknown sandbox provider"):
            create_provider(_core_config(sandbox, "/home/workspace"))
