"""The preview base: where a deployment serves previews, and when it may not boot.

Pins that a base parses only as a scheme, a host and a port, that a capability
host parses back to the port and token minted, and that a deployment whose
previews could not be served, or would share the app's cookies, refuses to boot.
"""

from __future__ import annotations

import pytest

from ptc_agent.config.core import E2BConfig, SandboxConfig
from src.config import env
from src.server.services.preview_base import (
    PreviewBase,
    PreviewProxyConfigurationError,
    check_preview_proxy_config,
    parse_preview_base,
    preview_proxy_requirements,
)

_TOKEN = "abcdefghijklmnopqrstuvwxyz234567"
_PRIVATE_E2B = ("sandbox.e2b.public_traffic is false",)


class TestBase:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("", None),
            ("https://preview.example.com", PreviewBase("https", "preview.example.com")),
            ("https://preview.example.com/", PreviewBase("https", "preview.example.com")),
            ("https://Preview.Example.com:443", PreviewBase("https", "preview.example.com")),
            ("http://preview.localhost:8000", PreviewBase("http", "preview.localhost", 8000)),
        ],
    )
    def test_a_base_is_a_scheme_a_host_and_a_port(self, value, expected):
        assert parse_preview_base(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "preview.example.com",
            "ftp://preview.example.com",
            "https://preview.example.com/previews",
            "https://preview.example.com?x=1",
            "https://user:pw@preview.example.com",
            "https://preview.example.com:notaport",
            # The capability is the hostname: plain http shows it to every hop.
            "http://preview.example.com",
            # A capability is a subdomain of the base.
            "https://127.0.0.1",
        ],
    )
    def test_anything_else_refuses_to_boot(self, value):
        with pytest.raises(PreviewProxyConfigurationError):
            parse_preview_base(value)

    def test_a_minted_host_parses_back_to_its_port_and_token(self):
        base = PreviewBase("http", "preview.localhost", 8000)
        url = base.url(3000, _TOKEN)

        assert url == f"http://3000-{_TOKEN}.preview.localhost:8000/"
        assert base.parse(f"3000-{_TOKEN}.preview.localhost:8000") == (3000, _TOKEN)

    @pytest.mark.parametrize(
        "host",
        [
            f"8080-{_TOKEN.upper()}.PREVIEW.EXAMPLE.COM",
            f"8080-{_TOKEN}.preview.example.com.",
            f"8080-{_TOKEN}.preview.example.com:443",
        ],
    )
    def test_case_a_trailing_dot_and_a_port_do_not_change_the_name(self, host):
        assert PreviewBase("https", "preview.example.com").parse(host) == (8080, _TOKEN)

    @pytest.mark.parametrize(
        "host",
        [
            None,
            "",
            "preview.example.com",
            f"x.8080-{_TOKEN}.preview.example.com",
            f"8080-{_TOKEN}.evilpreview.example.com",
            "8080-" + "1" * 32 + ".preview.example.com",
            f"8080-{_TOKEN[:-1]}.preview.example.com",
            f"[::1]:{8080}",
        ],
    )
    def test_no_other_host_names_a_capability(self, host):
        assert PreviewBase("https", "preview.example.com").parse(host) is None

    def test_the_proxy_claims_the_whole_base_and_nothing_beside_it(self):
        base = PreviewBase("https", "preview.example.com")

        assert base.claims("preview.example.com")
        assert base.claims("junk.preview.example.com:443")
        assert not base.claims("evilpreview.example.com")
        assert not base.claims("app.example.com")


class TestBootCheck:
    @pytest.mark.parametrize(
        ("sandbox", "named"),
        [
            (SandboxConfig(e2b=E2BConfig(public_traffic=False)), "e2b.public_traffic"),
            (SandboxConfig(offered_providers=("ssh",)), "includes ssh"),
        ],
    )
    def test_a_proxied_provider_without_a_base_refuses_to_boot(
        self, monkeypatch, sandbox, named
    ):
        """An SSH preview is never browser-ready, the same as a private E2B one."""
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "")

        with pytest.raises(PreviewProxyConfigurationError, match=named):
            check_preview_proxy_config(
                proxy_required_by=preview_proxy_requirements(sandbox), app_urls=()
            )

    def test_public_e2b_needs_no_base(self, monkeypatch):
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "")

        check_preview_proxy_config(
            proxy_required_by=preview_proxy_requirements(SandboxConfig()), app_urls=()
        )

    def test_a_base_that_holds_the_app_host_refuses_to_boot(self, monkeypatch):
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "https://example.com")

        with pytest.raises(PreviewProxyConfigurationError):
            check_preview_proxy_config(
                proxy_required_by=_PRIVATE_E2B,
                app_urls=("https://app.example.com",),
            )

    @pytest.mark.parametrize(
        ("base", "app"),
        [
            ("https://preview.example.com", "https://app.example.com"),
            ("https://p.app.example.com", "https://app.example.com"),
            ("https://preview.example.co.uk", "https://example.co.uk"),
            ("https://preview.example.com", "https://*.example.com"),
        ],
    )
    def test_a_base_on_the_apps_site_refuses_to_boot(self, monkeypatch, base, app):
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", base)

        with pytest.raises(PreviewProxyConfigurationError, match="same site"):
            check_preview_proxy_config(proxy_required_by=_PRIVATE_E2B, app_urls=("*", app))

    def test_a_base_on_a_site_of_its_own_boots(self, monkeypatch):
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "https://preview.example.net")

        check_preview_proxy_config(
            proxy_required_by=_PRIVATE_E2B,
            app_urls=("https://app.example.com", "http://localhost:8000", "*"),
        )

    def test_a_local_base_beside_a_local_app_boots(self, monkeypatch):
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "http://preview.localhost:8000")

        check_preview_proxy_config(
            proxy_required_by=_PRIVATE_E2B,
            app_urls=("http://localhost:8000", "http://app.localhost:5173"),
        )
