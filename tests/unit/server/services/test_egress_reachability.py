"""Provider-aware relay reachability: the OSS Docker default must work with
zero configuration, an explicit value must always be honored verbatim, and a
remote (Daytona) provider pointed at a local address must produce a warning —
never a silent sandbox-side connection failure."""

from __future__ import annotations

import pytest

from src.server.services.egress.reachability import (
    effective_relay_base_url,
    relay_reachability_warning,
)


@pytest.fixture
def env(monkeypatch):
    """Set the two inputs: the relay base (empty = unconfigured) and the
    server base it falls back to."""

    def _set(*, relay: str = "", server: str = "http://localhost:8000") -> None:
        monkeypatch.setattr("src.config.env.EGRESS_RELAY_BASE_URL", relay)
        monkeypatch.setattr("src.config.env.SERVER_BASE_URL", server)

    return _set


class TestEffectiveBaseUrl:
    @pytest.mark.parametrize(
        "base,expected",
        [
            ("http://localhost:8000", "http://host.docker.internal:8000"),
            ("http://127.0.0.1:8060", "http://host.docker.internal:8060"),
            ("http://0.0.0.0:8000", "http://host.docker.internal:8000"),
            ("http://localhost", "http://host.docker.internal"),
        ],
    )
    def test_docker_rewrites_a_defaulted_loopback_to_the_host_gateway(
        self, env, base, expected
    ):
        env(server=base)
        assert effective_relay_base_url("docker") == expected

    def test_docker_leaves_a_defaulted_public_base_alone(self, env):
        env(server="https://app.example.com")
        assert effective_relay_base_url("docker") == "https://app.example.com"

    def test_an_explicit_value_is_honored_verbatim_even_when_loopback(self, env):
        env(relay="http://localhost:8000", server="https://app.example.com")
        assert effective_relay_base_url("docker") == "http://localhost:8000"

    def test_daytona_never_gets_the_docker_rewrite(self, env):
        env(server="http://localhost:8000")
        assert effective_relay_base_url("daytona") == "http://localhost:8000"


class TestReachabilityWarning:
    @pytest.mark.parametrize(
        "base",
        [
            "http://localhost:8000",
            "http://127.0.0.1:8060",
            "http://wt3.localhost",
            "http://host.docker.internal:8000",
            "http://10.0.0.5:8000",
            "http://192.168.1.20:8000",
            "http://172.17.0.1:8000",
        ],
    )
    def test_daytona_plus_a_local_address_warns(self, base):
        warning = relay_reachability_warning("daytona", base)
        assert warning is not None
        assert base in warning
        assert "EGRESS_RELAY_BASE_URL" in warning

    @pytest.mark.parametrize(
        "base",
        [
            "https://api.example.com",
            "https://something.trycloudflare.com",
            "http://93.184.216.34:8000",
        ],
    )
    def test_daytona_plus_a_routable_address_is_silent(self, base):
        assert relay_reachability_warning("daytona", base) is None

    def test_local_providers_never_warn(self):
        assert relay_reachability_warning("docker", "http://localhost:8000") is None
