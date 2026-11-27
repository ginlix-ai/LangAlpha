"""Each URL setting's source is logged without its value, and a managed
deployment refuses URLs it cannot run on."""

from __future__ import annotations

import importlib
import logging
import os
from types import SimpleNamespace
from unittest import mock

import pytest

from src.config import env
from src.config.urls import check_at_boot, host_class
from src.server.services.egress.reachability import check_sandbox_urls

_PROD = {
    "SERVER_BASE_URL": "https://app.example.com",
    "PUBLIC_APP_URL": "https://app.example.com",
    "PLATFORM_SERVICE_URL": "http://host.docker.internal:9003",
    "SERVER_SANDBOX_URL": "https://api.example.com",
    "LANGALPHA_DATA_URL": "http://host.docker.internal:9005",
    "LANGALPHA_DATA_SANDBOX_URL": "https://api.example.com",
    "CHANNEL_GATEWAY_URL": "http://host.docker.internal:9002/api",
    "AUTOMATION_WEBHOOK_URL": "http://host.docker.internal:9002/hooks/automation",
}


@pytest.fixture
def boot():
    """Resolve src.config.env from exactly *values*, as a process boots on them."""

    def apply(values: dict[str, str], *, managed: bool = False) -> None:
        environ = {**values, "HOST_MODE": "platform" if managed else "oss", "APP_ENV": "production"}
        with mock.patch.dict(os.environ, environ, clear=True):
            importlib.reload(env)

    yield apply
    importlib.reload(env)


def check(provider: str, network_mode: str = "bridge") -> None:
    """Both boot checks, in the order the lifespan runs them."""
    check_at_boot()
    check_sandbox_urls(SimpleNamespace(provider=provider, docker=SimpleNamespace(network_mode=network_mode)))


@pytest.mark.parametrize(
    ("url", "kind"),
    [
        ("http://127.0.0.1:8000", "loopback"),
        ("http://127.1:8000", "loopback"),
        ("http://0x7f.1:8000", "loopback"),
        ("http://localhost:8000", "loopback"),
        ("http://localhost.:8000", "loopback"),
        ("http://[::1]:8000", "loopback"),
        ("http://10.1.2.3:8005", "private"),
        ("http://10.1:8005", "private"),
        ("http://backend:8000", "private"),
        ("http://data.internal", "private"),
        ("http://host.docker.internal:9005", "container"),
        ("https://api.example.com", "public"),
        ("https://cafe.face", "public"),
        ("http://[relay:8000", "none"),
        ("", "none"),
    ],
)
def test_host_class(url, kind):
    assert host_class(url) == kind


def test_a_managed_deployment_with_every_url_starts(boot):
    boot(_PROD, managed=True)
    check("daytona")


def test_a_managed_deployment_names_each_url_it_needs_set(boot):
    boot({**_PROD, "PLATFORM_SERVICE_URL": "", "CHANNEL_GATEWAY_URL": "  ", "PUBLIC_APP_URL": ""}, managed=True)
    with pytest.raises(
        RuntimeError, match="PUBLIC_APP_URL is not set; PLATFORM_SERVICE_URL is not set; CHANNEL_GATEWAY_URL is not set"
    ):
        check("daytona")


@pytest.mark.parametrize(
    ("name", "url", "defect"),
    [
        ("SERVER_SANDBOX_URL", "ftp://relay.example.test", "its scheme is not http or https"),
        ("SERVER_SANDBOX_URL", "https://relay.example.test:bad", "it does not parse as a URL"),
        ("AUTOMATION_WEBHOOK_URL", "relay.example.test/webhook", "its scheme is not http or https"),
        ("PLATFORM_SERVICE_URL", "relay.example.test:8443", "its scheme is not http or https"),
        ("PLATFORM_SERVICE_URL", "https://relay.example.test?x=1", "it carries a query, and paths are appended to it"),
        ("AUTOMATION_WEBHOOK_URL", "https://relay.example.test/hook?key=a#b", "it carries a fragment"),
        ("LANGALPHA_DATA_WS_URL", "https://data.example.test", "its scheme is not ws or wss"),
        ("PLATFORM_SERVICE_URL", "https://bad host.example.test", "its host is not a valid host name"),
    ],
)
def test_a_managed_deployment_refuses_an_unusable_url_without_repeating_it(boot, caplog, name, url, defect):
    boot({**_PROD, name: url}, managed=True)
    with caplog.at_level(logging.INFO), pytest.raises(RuntimeError, match=f"{name} is not usable: {defect}") as raised:
        check("daytona")
    assert "example.test" not in str(raised.value) and "example.test" not in caplog.text


def test_a_bad_parent_is_reported_once_not_under_each_setting_derived_from_it(boot):
    boot({**_PROD, "LANGALPHA_DATA_URL": "ftp://data.example.test", "LANGALPHA_DATA_SANDBOX_URL": ""}, managed=True)
    with pytest.raises(RuntimeError) as raised:
        check("docker")
    assert str(raised.value).count("is not usable") == 1


def test_a_managed_deployment_refuses_a_private_address_remote_sandboxes_would_dial(boot):
    boot({**_PROD, "LANGALPHA_DATA_SANDBOX_URL": ""}, managed=True)
    with pytest.raises(RuntimeError, match="LANGALPHA_DATA_SANDBOX_URL resolves to a container address"):
        check("daytona")


@pytest.mark.parametrize("url", ["http://127.1:8000", "http://localhost.:8000"])
def test_a_disguised_loopback_address_is_not_public(boot, url):
    boot({**_PROD, "SERVER_SANDBOX_URL": url}, managed=True)
    with pytest.raises(RuntimeError, match="SERVER_SANDBOX_URL resolves to a loopback address"):
        check("daytona")


def test_a_container_sandbox_cannot_dial_this_machine_s_loopback(boot):
    boot({**_PROD, "SERVER_SANDBOX_URL": "http://127.0.0.1:8000"}, managed=True)
    with pytest.raises(
        RuntimeError, match="loopback address, which Docker sandboxes cannot reach, since each has a loopback of its own"
    ):
        check("docker")


def test_a_container_on_the_host_network_shares_this_machine_s_loopback(boot, monkeypatch):
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    boot({**_PROD, "SERVER_SANDBOX_URL": "http://127.0.0.1:8000"}, managed=True)
    check("docker", network_mode="host")


def test_a_remote_daemon_s_host_network_is_not_this_machine_s(boot, monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "tcp://docker.example.test:2376")
    boot({**_PROD, "SERVER_SANDBOX_URL": "http://127.0.0.1:8000"}, managed=True)
    with pytest.raises(RuntimeError, match="since each has a loopback of its own"):
        check("docker", network_mode="host")


def test_a_webhook_endpoint_may_carry_a_query(boot):
    boot({**_PROD, "AUTOMATION_WEBHOOK_URL": "https://hooks.example.com/automation?key=1"}, managed=True)
    check("daytona")


def test_the_websocket_url_follows_an_uppercase_scheme(boot):
    boot({"LANGALPHA_DATA_URL": "HTTPS://data.example.com"})
    assert env.LANGALPHA_DATA_WS_URL == "wss://data.example.com"


def test_a_platform_url_of_only_a_slash_is_kept_and_reported(boot):
    boot({"PLATFORM_SERVICE_URL": "/"}, managed=True)
    assert env.PLATFORM_SERVICE_URL == "/"


def test_a_local_provider_may_dial_a_private_address(boot):
    boot({**_PROD, "LANGALPHA_DATA_SANDBOX_URL": ""}, managed=True)
    check("docker")


def test_a_self_hosted_deployment_only_warns(boot, caplog):
    boot({"SERVER_SANDBOX_URL": "http://localhost:8000", "AUTOMATION_WEBHOOK_URL": "ftp://hook.example.test"})
    with caplog.at_level(logging.WARNING):
        check("daytona")
    assert "SERVER_SANDBOX_URL resolves to a loopback address" in caplog.text
    assert "AUTOMATION_WEBHOOK_URL is not usable: its scheme is not http or https" in caplog.text
    assert "is not set" not in caplog.text


def test_a_url_of_only_whitespace_is_reported_and_read_as_unset(boot, caplog):
    boot({**_PROD, "PLATFORM_SERVICE_URL": "  "})
    assert env.PLATFORM_SERVICE_URL == ""
    with caplog.at_level(logging.WARNING):
        check("daytona")
    assert "PLATFORM_SERVICE_URL holds only whitespace, read as unset" in caplog.text


def test_a_url_with_no_host_is_reported_once(boot, caplog):
    boot({"SERVER_SANDBOX_URL": "http://[relay.example.test:8000"})
    with caplog.at_level(logging.INFO):
        check("daytona")
    assert "SERVER_SANDBOX_URL source=set unparsable" in caplog.text
    assert caplog.text.count("SERVER_SANDBOX_URL is not usable: it does not parse as a URL") == 1
    assert "none address" not in caplog.text
    assert "relay.example.test" not in caplog.text


def test_the_report_names_the_source_each_value_was_resolved_from(boot, caplog):
    boot({
        "SERVER_SANDBOX_URL": "https://api.example.com/",
        "LANGALPHA_DATA_URL": "http://10.0.0.9:8005",
        "PUBLIC_APP_URL": "",
        "SERVER_LOOPBACK_URL": " ",
        "LANGALPHA_LISTEN_URL": "http://127.0.0.1:18000",
        "CHANNEL_GATEWAY_URL": "",
    })
    with caplog.at_level(logging.INFO):
        check_at_boot()
    lines = {r.getMessage().split()[2]: r.getMessage() for r in caplog.records if "source=" in r.getMessage()}
    assert "source=set scheme=https host=public path=no slash=yes" in lines["SERVER_SANDBOX_URL"]
    assert "source=from:LANGALPHA_DATA_URL scheme=ws host=private" in lines["LANGALPHA_DATA_WS_URL"]
    assert "source=from:LANGALPHA_DATA_URL scheme=http host=private" in lines["LANGALPHA_DATA_SANDBOX_URL"]
    assert "source=default scheme=http host=loopback" in lines["SERVER_BASE_URL"]
    assert "source=blank,from:SERVER_BASE_URL scheme=http host=loopback" in lines["PUBLIC_APP_URL"]
    assert "source=whitespace,listener scheme=http host=loopback" in lines["SERVER_LOOPBACK_URL"]
    assert "SERVER_LOOPBACK_URL holds only whitespace, read as unset" in caplog.text
    assert "source=blank,unset empty" in lines["CHANNEL_GATEWAY_URL"]
    assert "source=unset empty" in lines["PLATFORM_SERVICE_URL"]
    assert env.SERVER_LOOPBACK_URL == "http://127.0.0.1:18000"
    assert env.PUBLIC_APP_URL == env.SERVER_BASE_URL
    assert not any("example.com" in r.getMessage() or "10.0.0.9" in r.getMessage() for r in caplog.records)


def test_an_unset_sandbox_url_reports_the_server_base_it_falls_back_to(boot, caplog):
    boot({"SERVER_BASE_URL": "https://api.example.com"})
    with caplog.at_level(logging.INFO):
        check_at_boot()
    assert "SERVER_SANDBOX_URL source=from:SERVER_BASE_URL scheme=https host=public" in caplog.text
    assert env.SERVER_SANDBOX_URL == ""


def test_the_platform_url_loses_its_trailing_slash_once_at_the_source(boot):
    boot({"PLATFORM_SERVICE_URL": "https://platform.example.com/"})
    assert env.PLATFORM_SERVICE_URL == "https://platform.example.com"
