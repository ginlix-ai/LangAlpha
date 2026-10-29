"""
Tests for SandboxConfig, DockerConfig, and backward-compat config loading.

Covers:
- SandboxConfig defaults
- DockerConfig defaults
- CoreConfig.daytona property shim
- AgentConfig.daytona property shim
- validate_api_keys() conditional on provider
- create_sandbox_config() with "sandbox" and "daytona" YAML keys
- SANDBOX_PROVIDER env var override
"""

import os
from unittest.mock import patch

import pytest

from ptc_agent.config.core import (
    CoreConfig,
    DaytonaConfig,
    DockerConfig,
    E2BConfig,
    FilesystemConfig,
    LoggingConfig,
    MCPConfig,
    PlatformSecretDefinition,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.config.utils import create_sandbox_config


# ---------------------------------------------------------------------------
# SandboxConfig / DockerConfig defaults
# ---------------------------------------------------------------------------


class TestSandboxConfigDefaults:
    def test_default_provider_is_daytona(self):
        cfg = SandboxConfig()
        assert cfg.provider == "daytona"

    def test_default_daytona_config(self):
        cfg = SandboxConfig()
        assert isinstance(cfg.daytona, DaytonaConfig)
        assert cfg.daytona.api_key == ""

    def test_default_docker_config(self):
        cfg = SandboxConfig()
        assert isinstance(cfg.docker, DockerConfig)


class TestDockerConfigDefaults:
    def test_defaults(self):
        cfg = DockerConfig()
        assert cfg.image == "langalpha-sandbox:latest"
        assert cfg.working_dir == "/home/workspace"
        assert cfg.memory_limit == "4g"
        assert cfg.cpu_count == 2.0
        assert cfg.dev_mode is False
        assert cfg.host_work_dir is None
        assert cfg.network_mode == "bridge"

    def test_custom(self):
        cfg = DockerConfig(image="custom:latest", memory_limit="8g", dev_mode=True)
        assert cfg.image == "custom:latest"
        assert cfg.memory_limit == "8g"
        assert cfg.dev_mode is True


# ---------------------------------------------------------------------------
# CoreConfig.daytona property shim
# ---------------------------------------------------------------------------


class TestCoreConfigDaytonaShim:
    def _make_core(self, **overrides) -> CoreConfig:
        defaults = dict(
            sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="shim-test")),
            security=SecurityConfig(),
            mcp=MCPConfig(),
            logging=LoggingConfig(),
            filesystem=FilesystemConfig(),
        )
        defaults.update(overrides)
        return CoreConfig(**defaults)

    def test_daytona_returns_sandbox_daytona(self):
        cfg = self._make_core()
        assert cfg.daytona is cfg.sandbox.daytona
        assert cfg.daytona.api_key == "shim-test"

    def test_validate_api_keys_skips_for_docker(self):
        cfg = self._make_core(
            sandbox=SandboxConfig(provider="docker", daytona=DaytonaConfig(api_key=""))
        )
        cfg.validate_api_keys()  # Should not raise

    def test_validate_api_keys_requires_for_daytona(self):
        cfg = self._make_core(
            sandbox=SandboxConfig(provider="daytona", daytona=DaytonaConfig(api_key=""))
        )
        with pytest.raises(ValueError, match="DAYTONA_API_KEY"):
            cfg.validate_api_keys()


# ---------------------------------------------------------------------------
# create_sandbox_config — new "sandbox:" key
# ---------------------------------------------------------------------------


class TestCreateSandboxConfigNewFormat:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv("SANDBOX_PROVIDER", raising=False)
        monkeypatch.delenv("DAYTONA_SECRET_NAMESPACE", raising=False)
        monkeypatch.delenv("DOCKER_SANDBOX_IMAGE", raising=False)
        monkeypatch.delenv("DOCKER_SANDBOX_DEV_MODE", raising=False)
        monkeypatch.delenv("DOCKER_SANDBOX_HOST_DIR", raising=False)

    def test_reads_sandbox_key(self):
        config_data = {
            "sandbox": {
                "provider": "daytona",
                "daytona": {
                    "base_url": "https://app.daytona.io/api",
                    "auto_stop_interval": 3600,
                    "auto_archive_interval": 86400,
                    "auto_delete_interval": 604800,
                    "python_version": "3.12",
                },
            }
        }
        cfg = create_sandbox_config(config_data)
        assert cfg.provider == "daytona"
        assert cfg.daytona.base_url == "https://app.daytona.io/api"

    def test_reads_secret_namespace_from_environment(self, monkeypatch):
        monkeypatch.setenv("DAYTONA_SECRET_NAMESPACE", "staging")
        config_data = {
            "sandbox": {
                "provider": "daytona",
                "daytona": {
                    "base_url": "https://app.daytona.io/api",
                    "auto_stop_interval": 3600,
                    "auto_archive_interval": 86400,
                    "auto_delete_interval": 604800,
                    "python_version": "3.12",
                },
            }
        }
        cfg = create_sandbox_config(config_data)
        assert cfg.daytona.secret_namespace == "staging"

    def test_reads_platform_secrets_catalog(self):
        config_data = {
            "sandbox": {
                "provider": "daytona",
                "platform_secrets": [
                    {
                        "source_env_var": "EXAMPLE_API_KEY",
                        "sandbox_env_var": "EXAMPLE_API_KEY",
                        "name_suffix": "platform-example-api-key",
                        "description": "Platform example API key",
                        "hosts": ["api.example.com"],
                    }
                ],
            }
        }
        cfg = create_sandbox_config(config_data)
        assert cfg.platform_secrets == (
            PlatformSecretDefinition(
                source_env_var="EXAMPLE_API_KEY",
                sandbox_env_var="EXAMPLE_API_KEY",
                name_suffix="platform-example-api-key",
                description="Platform example API key",
                hosts=("api.example.com",),
            ),
        )

    def test_platform_secrets_default_empty(self):
        config_data = {"sandbox": {"provider": "daytona"}}
        assert create_sandbox_config(config_data).platform_secrets == ()

    def test_reads_docker_provider(self):
        config_data = {
            "sandbox": {
                "provider": "docker",
                "docker": {
                    "image": "my-image:latest",
                    "memory_limit": "8g",
                },
            }
        }
        cfg = create_sandbox_config(config_data)
        assert cfg.provider == "docker"
        assert cfg.docker.image == "my-image:latest"
        assert cfg.docker.memory_limit == "8g"

    def test_sandbox_key_defaults_to_daytona_provider(self):
        config_data = {
            "sandbox": {
                "daytona": {
                    "base_url": "https://test.io/api",
                    "auto_stop_interval": 3600,
                    "auto_archive_interval": 86400,
                    "auto_delete_interval": 604800,
                    "python_version": "3.12",
                },
            }
        }
        cfg = create_sandbox_config(config_data)
        assert cfg.provider == "daytona"

    def test_an_ssh_section_naming_a_computers_target_is_refused(self):
        """Each computer's settings lie over the section, so a tunnel set
        there would route every computer that lacks one through it."""
        config_data = {
            "sandbox": {"provider": "docker", "ssh": {"tunnel": "t", "port": 2222}}
        }
        with pytest.raises(ValueError, match="sandbox.ssh cannot set port, tunnel"):
            create_sandbox_config(config_data)

        tunables = {"sandbox": {"provider": "docker", "ssh": {"connect_timeout": 5}}}
        assert create_sandbox_config(tunables).ssh.connect_timeout == 5


# ---------------------------------------------------------------------------
# create_sandbox_config — backward compat "daytona:" key
# ---------------------------------------------------------------------------


class TestCreateSandboxConfigBackwardCompat:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv("SANDBOX_PROVIDER", raising=False)
        monkeypatch.delenv("DOCKER_SANDBOX_IMAGE", raising=False)
        monkeypatch.delenv("DOCKER_SANDBOX_DEV_MODE", raising=False)
        monkeypatch.delenv("DOCKER_SANDBOX_HOST_DIR", raising=False)

    def test_reads_daytona_key(self):
        """Top-level 'daytona:' key should produce SandboxConfig with provider='daytona'."""
        config_data = {
            "daytona": {
                "base_url": "https://app.daytona.io/api",
                "auto_stop_interval": 3600,
                "auto_archive_interval": 86400,
                "auto_delete_interval": 604800,
                "python_version": "3.12",
            }
        }
        cfg = create_sandbox_config(config_data)
        assert isinstance(cfg, SandboxConfig)
        assert cfg.provider == "daytona"
        assert cfg.daytona.base_url == "https://app.daytona.io/api"

    def test_missing_both_keys_raises(self):
        with pytest.raises(ValueError, match="sandbox.*daytona"):
            create_sandbox_config({})


# ---------------------------------------------------------------------------
# SANDBOX_PROVIDER env var override
# ---------------------------------------------------------------------------


class TestSandboxProviderEnvOverride:
    def test_env_overrides_provider(self):
        config_data = {
            "daytona": {
                "base_url": "https://app.daytona.io/api",
                "auto_stop_interval": 3600,
                "auto_archive_interval": 86400,
                "auto_delete_interval": 604800,
                "python_version": "3.12",
            }
        }
        with patch.dict(os.environ, {"SANDBOX_PROVIDER": "docker"}):
            cfg = create_sandbox_config(config_data)
        assert cfg.provider == "docker"

    def test_env_not_set_uses_config(self):
        config_data = {
            "sandbox": {
                "provider": "docker",
            }
        }
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SANDBOX_PROVIDER", None)
            cfg = create_sandbox_config(config_data)
        assert cfg.provider == "docker"


class TestE2BConfig:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for var in ("SANDBOX_PROVIDER", "DAYTONA_API_KEY", "E2B_API_KEY"):
            monkeypatch.delenv(var, raising=False)

    def test_env_only_config_loads_the_key_without_a_yaml_block(self, monkeypatch):
        monkeypatch.setenv("E2B_API_KEY", "e2b-key")
        monkeypatch.setenv("E2B_DOMAIN", "e2b.example.com")
        cfg = create_sandbox_config({"sandbox": {"provider": "e2b"}})
        assert cfg.provider == "e2b"
        assert cfg.e2b.api_key == "e2b-key"
        assert cfg.e2b.domain == "e2b.example.com"

    def test_daytona_key_loads_without_a_yaml_block(self, monkeypatch):
        """A deployment moved to E2B still serves its Daytona computers, so
        their credentials load even when the config names only E2B."""
        monkeypatch.setenv("DAYTONA_API_KEY", "daytona-key")
        monkeypatch.setenv("DAYTONA_SECRET_NAMESPACE", "staging")
        cfg = create_sandbox_config({"sandbox": {"provider": "e2b"}})
        assert cfg.provider == "e2b"
        assert cfg.daytona.api_key == "daytona-key"
        assert cfg.daytona.secret_namespace == "staging"

    def test_yaml_block_overrides_tunables(self):
        cfg = create_sandbox_config(
            {"sandbox": {"provider": "e2b", "e2b": {"max_timeout": 86400}}}
        )
        assert cfg.e2b.max_timeout == 86400
        assert cfg.e2b.auto_stop_interval == 3600

    def test_autodetects_e2b_only_from_its_key(self, monkeypatch):
        assert create_sandbox_config({"sandbox": {}}).provider == "docker"
        monkeypatch.setenv("E2B_API_KEY", "e2b-key")
        assert create_sandbox_config({"sandbox": {}}).provider == "e2b"

    def test_daytona_key_keeps_precedence(self, monkeypatch):
        monkeypatch.setenv("E2B_API_KEY", "e2b-key")
        monkeypatch.setenv("DAYTONA_API_KEY", "daytona-key")
        assert create_sandbox_config({"sandbox": {}}).provider == "daytona"

    def test_sandbox_provider_env_wins(self, monkeypatch):
        monkeypatch.setenv("SANDBOX_PROVIDER", "e2b")
        cfg = create_sandbox_config({"sandbox": {"provider": "docker"}})
        assert cfg.provider == "e2b"

    def test_validate_api_keys_requires_key_for_e2b(self):
        cfg = CoreConfig(
            sandbox=SandboxConfig(provider="e2b", e2b=E2BConfig(api_key="")),
            security=SecurityConfig(),
            mcp=MCPConfig(),
            logging=LoggingConfig(),
            filesystem=FilesystemConfig(),
        )
        with pytest.raises(ValueError, match="E2B_API_KEY"):
            cfg.validate_api_keys()


class TestResourceTierSpecs:
    """The sizes each provider offers, and the ceiling each one holds them to."""

    def test_daytona_offers_three_sizes(self):
        tiers = DaytonaConfig().resource_tiers
        assert {n: (t.cpu, t.memory, t.disk) for n, t in tiers.items()} == {
            "standard": (1, 2, 5),
            "performance": (2, 4, 10),
            "max": (2, 8, 10),
        }

    def test_e2b_offers_the_same_three_and_extra_large(self):
        tiers = E2BConfig().resource_tiers
        assert {n: (t.cpu, t.memory, t.disk) for n, t in tiers.items()} == {
            "standard": (1, 2, 5),
            "performance": (2, 4, 10),
            "max": (2, 8, 10),
            "xlarge": (4, 8, 20),
        }

    def test_a_shared_name_is_one_size_on_every_provider(self):
        """The tier is persisted on the row, so a name means one size."""
        daytona, e2b = DaytonaConfig().resource_tiers, E2BConfig().resource_tiers
        for name, tier in daytona.items():
            assert e2b[name] == tier

    def test_docker_keeps_the_shared_sizes(self):
        assert set(DockerConfig().resource_tiers) == {"standard", "performance", "max"}

    def test_daytona_refuses_extra_large(self):
        """Above Daytona's 4 vCPU / 8 GiB / 10 GiB per sandbox."""
        with pytest.raises(ValueError, match=r"xlarge.*disk=20 \(max 10\)"):
            DaytonaConfig(
                resource_tiers={
                    "standard": {"cpu": 1, "memory": 2, "disk": 5},
                    "xlarge": {"cpu": 4, "memory": 8, "disk": 20},
                }
            )

    def test_e2b_refuses_a_tier_above_its_own_ceiling(self):
        with pytest.raises(ValueError, match=r"memory=16 \(max 8\)"):
            E2BConfig(
                resource_tiers={"standard": {"cpu": 8, "memory": 16, "disk": 20}}
            )

    def test_docker_has_no_ceiling(self):
        cfg = DockerConfig(
            resource_tiers={"standard": {"cpu": 16, "memory": 64, "disk": 200}}
        )
        assert cfg.resource_tiers["standard"].cpu == 16


class TestPlatformSecretCatalogValidation:
    """SandboxConfig rejects a catalog that would silently mis-bind."""

    @staticmethod
    def _def(*, sandbox_env_var="FMP_API_KEY", name_suffix="platform-fmp"):
        return PlatformSecretDefinition(
            source_env_var="SRC_KEY",
            sandbox_env_var=sandbox_env_var,
            name_suffix=name_suffix,
            description="test",
            hosts=("example.com",),
        )

    def test_rejects_duplicate_sandbox_env_var(self):
        # Duplicate sandbox_env_var collapses the binding map (and collides the
        # rollout row keyed on it).
        with pytest.raises(ValueError, match="duplicate sandbox_env_var"):
            SandboxConfig(
                platform_secrets=(
                    self._def(name_suffix="a"),
                    self._def(name_suffix="b"),
                )
            )

    def test_rejects_duplicate_name_suffix(self):
        # Duplicate name_suffix resolves two entries to one provider Secret —
        # the later credential silently overwrites the former.
        with pytest.raises(ValueError, match="duplicate name_suffix"):
            SandboxConfig(
                platform_secrets=(
                    self._def(sandbox_env_var="A_KEY", name_suffix="dup"),
                    self._def(sandbox_env_var="B_KEY", name_suffix="dup"),
                )
            )

    def test_accepts_a_unique_catalog(self):
        cfg = SandboxConfig(
            platform_secrets=(
                self._def(sandbox_env_var="A_KEY", name_suffix="a"),
                self._def(sandbox_env_var="B_KEY", name_suffix="b"),
            )
        )
        assert len(cfg.platform_secrets) == 2


class TestOfferedProviders:
    """The kinds a new computer may be created on besides the deployment's own."""

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for var in (
            "SANDBOX_PROVIDER",
            "DAYTONA_API_KEY",
            "E2B_API_KEY",
            "DOCKER_SANDBOX_IMAGE",
        ):
            monkeypatch.delenv(var, raising=False)

    @staticmethod
    def _core(sandbox: SandboxConfig) -> CoreConfig:
        return CoreConfig(
            sandbox=sandbox,
            security=SecurityConfig(),
            mcp=MCPConfig(),
            logging=LoggingConfig(),
            filesystem=FilesystemConfig(),
        )

    def test_nothing_is_offered_by_default(self):
        cfg = create_sandbox_config({"sandbox": {"provider": "daytona"}})
        assert cfg.offered_providers == ()
        assert cfg.creatable_providers() == ("daytona",)

    def test_credentials_alone_offer_nothing(self, monkeypatch):
        """Development environments carry every provider's key, so a key is
        not an offer."""
        monkeypatch.setenv("DAYTONA_API_KEY", "daytona-key")
        monkeypatch.setenv("E2B_API_KEY", "e2b-key")
        cfg = create_sandbox_config({"sandbox": {"provider": "daytona"}})
        assert cfg.creatable_providers() == ("daytona",)

    def test_reads_the_offer_in_order_after_the_default(self):
        cfg = create_sandbox_config(
            {"sandbox": {"provider": "daytona", "offered_providers": ["e2b", "docker"]}}
        )
        assert cfg.offered_providers == ("e2b", "docker")
        assert cfg.creatable_providers() == ("daytona", "e2b", "docker")

    def test_an_env_override_onto_an_offered_kind_lists_it_once(self, monkeypatch):
        monkeypatch.setenv("SANDBOX_PROVIDER", "e2b")
        cfg = create_sandbox_config(
            {"sandbox": {"provider": "daytona", "offered_providers": ["e2b", "daytona"]}}
        )
        assert cfg.creatable_providers() == ("e2b", "daytona")

    def test_an_unknown_kind_is_rejected(self):
        with pytest.raises(ValueError):
            create_sandbox_config(
                {"sandbox": {"provider": "daytona", "offered_providers": ["memory"]}}
            )

    def test_an_offered_docker_kind_takes_the_docker_env_overrides(self, monkeypatch):
        monkeypatch.setenv("DOCKER_SANDBOX_IMAGE", "custom:latest")
        cfg = create_sandbox_config(
            {"sandbox": {"provider": "daytona", "offered_providers": ["docker"]}}
        )
        assert cfg.docker.image == "custom:latest"

    def test_docker_env_overrides_apply_when_docker_is_not_offered(self, monkeypatch):
        """Docker computers kept from before the offer was dropped are still
        rebuilt from these settings, mounts and image included."""
        monkeypatch.setenv("DOCKER_SANDBOX_IMAGE", "custom:latest")
        monkeypatch.setenv("DOCKER_SANDBOX_HOST_DIR", "/srv/work")
        cfg = create_sandbox_config({"sandbox": {"provider": "daytona"}})
        assert cfg.docker.image == "custom:latest"
        assert cfg.docker.host_work_dir == "/srv/work"

    def test_an_offered_kind_needs_its_credentials(self):
        cfg = self._core(
            SandboxConfig(
                provider="daytona",
                offered_providers=("e2b",),
                daytona=DaytonaConfig(api_key="daytona-key"),
                e2b=E2BConfig(api_key=""),
            )
        )
        with pytest.raises(ValueError, match="E2B_API_KEY"):
            cfg.validate_api_keys()

    def test_an_offered_kind_with_credentials_validates(self):
        cfg = self._core(
            SandboxConfig(
                provider="docker",
                offered_providers=("daytona", "e2b"),
                daytona=DaytonaConfig(api_key="daytona-key"),
                e2b=E2BConfig(api_key="e2b-key"),
            )
        )
        cfg.validate_api_keys()

    def test_an_offered_docker_kind_needs_no_credentials(self):
        cfg = self._core(
            SandboxConfig(
                provider="daytona",
                offered_providers=("docker",),
                daytona=DaytonaConfig(api_key="daytona-key"),
            )
        )
        cfg.validate_api_keys()

    def test_the_agent_config_checks_the_offer_too(self):
        from ptc_agent.config.agent import AgentConfig

        config = AgentConfig.model_construct(
            sandbox=SandboxConfig(
                provider="daytona",
                offered_providers=("e2b",),
                daytona=DaytonaConfig(api_key="daytona-key"),
            )
        )
        with pytest.raises(ValueError, match="E2B_API_KEY"):
            config.validate_api_keys()
