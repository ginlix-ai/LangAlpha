"""Contracts of the E2B provider that the rest of the system leans on.

E2B's SDK connects by resuming, sets deadlines outright, and raises on any
nonzero exit; each test here pins the translation that keeps those from
leaking: a lookup never wakes a stopped computer, a renewal never shortens a
longer deadline set elsewhere, and a command's exit code is a result rather
than an error.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from e2b import (
    AuthenticationException,
    BuildException,
    CommandExitException,
    FileNotFoundException,
    RateLimitException,
    SandboxException,
    SandboxNotFoundException,
    SandboxNotRunningException,
    Secret,
    SecretException,
    SecretNotFoundException,
)

from ptc_agent.config.core import E2BConfig, PlatformSecretDefinition, ResourceTier
from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    ResolvedPlatformSecret,
)
from ptc_agent.core.sandbox.providers import e2b as e2b_module
from ptc_agent.core.sandbox.providers.e2b import E2BProvider
from ptc_agent.core.sandbox.providers.e2b_runtime import (
    E2BRuntime,
    _exit_status,
    _window,
)
from ptc_agent.core.sandbox.providers.e2b_secrets import (
    E2BSecretReconciler,
    egress_rules,
)
from ptc_agent.core.sandbox.runtime import (
    RuntimeState,
    SandboxFailureKind,
    SandboxRateLimitedError,
    SandboxTransientError,
)


def _info(state: str = "running", left_s: float = 3600) -> SimpleNamespace:
    now = datetime.now(UTC)
    return SimpleNamespace(
        sandbox_id="sbx-1",
        name="langalpha-standard-abc",
        state=SimpleNamespace(value=state),
        started_at=now,
        end_at=now + timedelta(seconds=left_s),
        cpu_count=1,
        memory_mb=1024,
    )


def _client(info: SimpleNamespace | None = None, *, template: str = "ready") -> SimpleNamespace:
    sandbox_api = SimpleNamespace(
        get_info=AsyncMock(return_value=info or _info()),
        connect=AsyncMock(),
        set_timeout=AsyncMock(),
        pause=AsyncMock(return_value=True),
        kill=AsyncMock(return_value=True),
        create=AsyncMock(),
    )
    template_api = SimpleNamespace(
        get_tags=AsyncMock(return_value=[SimpleNamespace(tag="default", build_id="b1")]),
        get_build_status=AsyncMock(
            return_value=SimpleNamespace(status=SimpleNamespace(value=template))
        ),
        build=AsyncMock(),
    )
    return SimpleNamespace(AsyncSandbox=sandbox_api, AsyncTemplate=template_api)


def _provider(
    client: SimpleNamespace | None = None,
    *,
    template_id: str | None = "tpl-1",
    platform_secrets: tuple[PlatformSecretDefinition, ...] = (),
    **config: object,
) -> E2BProvider:
    provider = E2BProvider(
        E2BConfig(api_key="test", **config),
        working_dir="/home/workspace",
        platform_secrets=platform_secrets,
    )
    provider._client = client or _client()
    # The name-to-id lookup goes through E2B's raw API, outside the bound client.
    provider._template_id = AsyncMock(return_value=template_id)
    return provider


class TestClassifyError:
    @pytest.mark.parametrize(
        ("exc", "kind"),
        [
            (FileNotFoundException("no such file"), SandboxFailureKind.PATH_ABSENT),
            (FileNotFoundError("/home/workspace/x"), SandboxFailureKind.PATH_ABSENT),
            (SandboxNotFoundException("gone"), SandboxFailureKind.SANDBOX_GONE),
            # Paused or dropped mid-call: the reconnect decides, never this.
            (SandboxNotRunningException("was not found"), SandboxFailureKind.TRANSIENT),
            (SandboxException("502: bad gateway", status_code=502), SandboxFailureKind.TRANSIENT),
            (SandboxException("400: bad request", status_code=400), SandboxFailureKind.UNKNOWN),
            # A bad key must never authorize replacing a sandbox.
            (AuthenticationException("401"), SandboxFailureKind.UNKNOWN),
        ],
    )
    def test_kind_follows_the_exception_type(self, exc, kind):
        assert _provider().classify_error(exc) is kind


class TestNoImplicitResume:
    @pytest.mark.asyncio
    async def test_get_looks_up_without_connecting(self):
        client = _client(_info("paused"))
        runtime = await _provider(client).get("sbx-1")

        assert await runtime.get_state() is RuntimeState.STOPPED
        client.AsyncSandbox.connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_work_on_a_paused_sandbox_is_transient_and_does_not_connect(self):
        client = _client(_info("paused"))
        runtime = await _provider(client).get("sbx-1")

        with pytest.raises(SandboxTransientError):
            await runtime.exec("true")
        client.AsyncSandbox.connect.assert_not_awaited()


class TestKeepAlive:
    def _runtime(self, client: SimpleNamespace, window_s: int = 3600) -> E2BRuntime:
        return E2BRuntime(
            client,
            "sbx-1",
            sandbox=MagicMock(),
            window_s=window_s,
            max_timeout_s=86400,
        )

    @pytest.mark.asyncio
    async def test_a_longer_deadline_set_elsewhere_is_kept(self):
        client = _client(_info(left_s=20 * 3600))
        runtime = self._runtime(client)
        runtime._alive_until = 0.0

        await runtime._keep_alive()

        client.AsyncSandbox.set_timeout.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_slipping_deadline_is_renewed_to_the_window(self):
        client = _client(_info(left_s=600))
        runtime = self._runtime(client)
        runtime._alive_until = 0.0

        await runtime._keep_alive()

        client.AsyncSandbox.set_timeout.assert_awaited_once_with("sbx-1", 3600)

    @pytest.mark.asyncio
    async def test_a_long_command_gets_a_deadline_past_its_own_timeout(self):
        client = _client(_info(left_s=3000))
        runtime = self._runtime(client)
        runtime._alive_until = 0.0

        await runtime._keep_alive(busy_for_s=7200)

        (_, seconds), _ = client.AsyncSandbox.set_timeout.await_args
        assert seconds > 7200

    @pytest.mark.asyncio
    async def test_a_sandbox_paused_under_a_held_handle_is_not_renewed(self):
        client = _client(_info("paused", left_s=0))
        runtime = self._runtime(client)
        runtime._alive_until = 0.0

        with pytest.raises(SandboxTransientError):
            await runtime._keep_alive()

        client.AsyncSandbox.set_timeout.assert_not_awaited()
        assert await runtime.get_state() is RuntimeState.STOPPED


class TestStopEndsJobs:
    """A pause keeps every process, so the stop ends the computer's jobs first."""

    def _runtime(self, client: SimpleNamespace, **kw) -> E2BRuntime:
        return E2BRuntime(
            client, "sbx-1", window_s=3600, max_timeout_s=86400,
            default_working_dir="/home/workspace", **kw,
        )

    @pytest.mark.asyncio
    async def test_the_jobs_end_before_the_pause(self):
        order: list[str] = []
        client = _client()
        client.AsyncSandbox.pause.side_effect = lambda *a, **k: order.append("pause")

        async def end_jobs(exec_fn, jobs_dir, *a, **k):
            order.append(f"end {jobs_dir}")
            return 2

        runtime = self._runtime(client, sandbox=MagicMock())
        with patch("ptc_agent.core.sandbox.jobs.end_jobs", new=end_jobs):
            await runtime.stop()

        assert order == ["end /home/workspace/.system/jobs", "pause"]

    @pytest.mark.asyncio
    async def test_a_failed_end_still_pauses(self):
        client = _client()
        runtime = self._runtime(client, sandbox=MagicMock())
        with patch(
            "ptc_agent.core.sandbox.jobs.end_jobs", new=AsyncMock(side_effect=TimeoutError())
        ):
            await runtime.stop()
        client.AsyncSandbox.pause.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_paused_sandbox_is_not_resumed_to_end_its_jobs(self):
        client = _client(_info("paused"))
        runtime = await _provider(client).get("sbx-1")
        end = AsyncMock()
        with patch("ptc_agent.core.sandbox.jobs.end_jobs", new=end):
            await runtime.stop()
        end.assert_not_awaited()
        client.AsyncSandbox.connect.assert_not_awaited()


class TestExec:
    @pytest.mark.asyncio
    async def test_nonzero_exit_is_a_result_with_stderr_folded_in(self):
        sandbox = MagicMock()
        sandbox.commands.run = AsyncMock(
            side_effect=CommandExitException(
                stderr="", stdout="boom\n", exit_code=3, error=None
            )
        )
        runtime = E2BRuntime(
            _client(), "sbx-1", sandbox=sandbox, window_s=3600, max_timeout_s=3600
        )

        result = await runtime.exec("false", timeout=30)

        assert (result.stdout, result.stderr, result.exit_code) == ("boom\n", "", 3)
        command = sandbox.commands.run.await_args.args[0]
        assert command.startswith("exec 2>&1\n")
        assert sandbox.commands.run.await_args.kwargs["user"] == "root"


class TestExitStatus:
    """envd reports a signal death as exit -1; the shell's status is 128 + signal."""

    @pytest.mark.parametrize(
        ("exit_code", "error", "expected"),
        [
            (-1, "signal: hangup", 129),
            (-1, "signal: interrupt", 130),
            (-1, "signal: aborted", 134),
            (-1, "signal: bus error", 135),
            (-1, "signal: killed", 137),
            (-1, "signal: segmentation fault", 139),
            (-1, "signal: terminated", 143),
            (-1, "signal: segmentation fault (core dumped)", 139),
            (-1, "signal: weird", -1),
            (-1, None, -1),
            (3, "exit status 3", 3),
            (3, "signal: killed", 3),
        ],
    )
    def test_maps_a_named_signal_to_the_shell_status(self, exit_code, error, expected):
        assert _exit_status(exit_code, error) == expected

    @pytest.mark.asyncio
    async def test_a_command_killed_by_a_signal_returns_the_shell_status(self):
        sandbox = MagicMock()
        sandbox.commands.run = AsyncMock(
            side_effect=CommandExitException(
                stderr="", stdout="", exit_code=-1, error="signal: killed"
            )
        )

        result = await E2BRuntime._run(sandbox, "sleep 60", timeout=30)

        assert result.exit_code == 137


class TestLifetimeAndTiers:
    def test_no_idle_limit_runs_to_the_plan_limit_and_windows_are_capped(self):
        assert _window(0, default_s=7200, max_s=3600) == 3600
        assert _window(None, default_s=7200, max_s=3600) == 3600
        assert _window(15, default_s=7200, max_s=3600) == 900

    @pytest.mark.asyncio
    async def test_create_pauses_on_timeout_and_never_auto_resumes(self):
        client = _client()
        client.AsyncSandbox.create.return_value = SimpleNamespace(sandbox_id="sbx-new")
        await _provider(client).create(tier="standard")

        lifecycle = client.AsyncSandbox.create.await_args.kwargs["lifecycle"]
        assert lifecycle == {"on_timeout": "pause", "auto_resume": False}

    def test_template_names_are_valid_and_follow_the_size(self):
        provider = _provider(template_name="My Prefix")
        small = provider.template_name([], tier="std", preset=ResourceTier(cpu=1, memory=1, disk=3))
        big = provider.template_name([], tier="std", preset=ResourceTier(cpu=2, memory=1, disk=3))

        assert small != big
        assert small.startswith("my-prefix-std-")
        assert all(c.isalnum() or c in "-_" for c in small)


class TestTemplateReadiness:
    """A sandbox starts only from a template whose build is ready.

    A template's name and default tag exist from the moment its first build
    starts, and E2B answers a create from it with 404 until the build is done.
    Nor is there a bare-image fallback: E2B's base image lacks what the
    dependency install needs, so that computer would stay broken.
    """

    @pytest.fixture(autouse=True)
    def _no_wait(self, monkeypatch):
        monkeypatch.setattr(e2b_module, "_TEMPLATE_WAIT_S", 0)

    async def _create(
        self, client: SimpleNamespace, *, template_id: str | None = "tpl-1", **config: object
    ) -> None:
        build = client.AsyncTemplate.get_build_status.return_value

        async def create(**kwargs: object) -> SimpleNamespace:
            ready = template_id is not None and build.status.value == "ready"
            if not ready and not client.AsyncTemplate.build.await_count:
                raise SandboxException(
                    f"404: template '{kwargs['template']}' not found", status_code=404
                )
            return SimpleNamespace(sandbox_id="sbx-new")

        client.AsyncSandbox.create.side_effect = create
        provider = _provider(client, template_id=template_id, **config)
        provider.template_definition = MagicMock()  # the builder DSL is E2B's
        await provider.create(tier="standard")

    @pytest.mark.asyncio
    async def test_a_template_still_building_fails_transient_without_a_second_build(self):
        client = _client(template="building")

        with pytest.raises(SandboxTransientError, match="still building"):
            await self._create(client)
        client.AsyncTemplate.build.assert_not_awaited()
        assert client.AsyncSandbox.create.await_count == 1  # the 404 that sent it to check

    @pytest.mark.asyncio
    async def test_a_missing_template_with_auto_build_off_refuses(self):
        client = _client()

        with pytest.raises(RuntimeError, match="not built"):
            await self._create(client, template_id=None, template_auto_create=False)
        assert client.AsyncSandbox.create.await_count == 1

    @pytest.mark.asyncio
    async def test_a_failed_build_is_built_again(self):
        client = _client(template="error")
        await self._create(client)

        client.AsyncTemplate.build.assert_awaited_once()
        assert client.AsyncSandbox.create.await_args.kwargs["template"].startswith(
            "langalpha-standard-"
        )

    @pytest.mark.asyncio
    async def test_a_ready_template_is_used_without_a_status_check(self):
        client = _client(template="ready")
        await self._create(client)

        client.AsyncTemplate.build.assert_not_awaited()
        client.AsyncTemplate.get_build_status.assert_not_awaited()
        assert client.AsyncSandbox.create.await_count == 1
        assert client.AsyncSandbox.create.await_args.kwargs["template"].startswith(
            "langalpha-standard-"
        )

    @pytest.mark.asyncio
    async def test_a_failed_lookup_is_left_to_the_create_retry(self):
        # Returning the unchecked name would only meet the same 404 again.
        client = _client(template="building")
        client.AsyncTemplate.get_tags.side_effect = SandboxException(
            "503: unavailable", status_code=503
        )

        with pytest.raises(SandboxException) as raised:
            await self._create(client)
        assert _provider().is_transient_error(raised.value)
        client.AsyncTemplate.build.assert_not_awaited()
        assert client.AsyncSandbox.create.await_count == 1

    @pytest.mark.parametrize(
        ("after", "raised"),
        [("building", SandboxTransientError), (None, BuildException)],
        ids=["another-build-won", "no-build-under-way"],
    )
    @pytest.mark.asyncio
    async def test_a_refused_build_waits_only_on_one_under_way(self, after, raised):
        # Both checks found no template, and E2B took the other process's build.
        client = _client(template="building")
        client.AsyncTemplate.build.side_effect = BuildException(
            "400: build is not in waiting state"
        )
        client.AsyncSandbox.create.side_effect = SandboxException(
            "404: template not found", status_code=404
        )
        provider = _provider(client)
        provider._template_id = AsyncMock(side_effect=[None, after and "tpl-1"])
        provider.template_definition = MagicMock()

        with pytest.raises(raised):
            await provider.create(tier="standard")
        client.AsyncTemplate.build.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_create_error_other_than_404_is_not_a_template_check(self):
        client = _client(template="ready")
        client.AsyncSandbox.create.side_effect = SandboxException(
            "502: bad gateway", status_code=502
        )
        provider = _provider(client)

        with pytest.raises(SandboxException, match="502"):
            await provider.create(tier="standard")
        client.AsyncTemplate.get_build_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_build_status_is_read_by_template_id(self):
        """By name, the status endpoint resolves only while the build runs."""
        client = _client(template="building")
        with pytest.raises(SandboxTransientError):
            await self._create(client, template_id="tpl-1")

        build_info = client.AsyncTemplate.get_build_status.await_args.args[0]
        assert build_info.template_id == "tpl-1"


_FMP = PlatformSecretDefinition(
    source_env_var="FMP_API_KEY",
    sandbox_env_var="FMP_API_KEY",
    name_suffix="platform-fmp-api-key",
    description="Platform FMP API key",
    hosts=("financialmodelingprep.com",),
    header="apikey",
)
_FMP_RULES = {
    "financialmodelingprep.com": [
        {"transform": {"headers": {"apikey": Secret.fill("prod-fmp")}}}
    ]
}


class TestPlatformSecrets:
    """A bound Secret reaches a sandbox only as an egress rule.

    E2B sets a sandbox's env once, at create, so the env holds a fixed marker
    that certification reads, and everything that can change later (the
    Secret's name, its hosts, its header) lives in the rules.
    """

    @pytest.mark.asyncio
    async def test_create_binds_the_secret_by_rule_and_keeps_its_value_out_of_env(self):
        client = _client()
        client.AsyncSandbox.create.return_value = SimpleNamespace(sandbox_id="sbx-new")
        await _provider(client, platform_secrets=(_FMP,)).create(
            env_vars={"OTHER": "x"},
            platform_secret_bindings={"FMP_API_KEY": "prod-fmp"},
            tier="standard",
        )

        kwargs = client.AsyncSandbox.create.await_args.kwargs
        assert kwargs["envs"]["FMP_API_KEY"] == "e2b_secret_fmp_api_key"
        assert kwargs["envs"]["OTHER"] == "x"
        assert kwargs["network"] == {"rules": _FMP_RULES}

    @pytest.mark.asyncio
    async def test_every_sandbox_trusts_the_egress_proxy_ca(self):
        """Without it, certifi and Node refuse a host the moment it has a rule."""
        client = _client()
        client.AsyncSandbox.create.return_value = SimpleNamespace(sandbox_id="sbx-new")
        await _provider(client).create(tier="standard")

        kwargs = client.AsyncSandbox.create.await_args.kwargs
        assert kwargs["network"] is None
        assert kwargs["envs"]["SSL_CERT_FILE"] == "/etc/ssl/certs/ca-certificates.crt"
        assert kwargs["envs"]["REQUESTS_CA_BUNDLE"] == "/etc/ssl/certs/ca-certificates.crt"
        assert kwargs["envs"]["NODE_EXTRA_CA_CERTS"].endswith("e2b-ca.crt")

    @pytest.mark.asyncio
    async def test_a_remount_replaces_the_rules_and_never_touches_env(self):
        client = _client()
        client.AsyncSandbox.update_network = AsyncMock()
        runtime = E2BRuntime(
            client,
            "sbx-1",
            sandbox=MagicMock(),
            window_s=3600,
            max_timeout_s=3600,
            secret_routes={"FMP_API_KEY": _FMP},
        )

        await runtime.update_secrets({"FMP_API_KEY": "prod-fmp"})

        client.AsyncSandbox.update_network.assert_awaited_once_with(
            "sbx-1", {"rules": _FMP_RULES}
        )
        assert not runtime.updates_env

    def test_entries_sharing_a_host_share_its_one_rule(self):
        other = _FMP.model_copy(
            update={"sandbox_env_var": "OTHER_KEY", "header": "x-other"}
        )
        rules = egress_rules(
            {"FMP_API_KEY": "prod-fmp", "OTHER_KEY": "prod-other"},
            {"FMP_API_KEY": _FMP, "OTHER_KEY": other},
        )
        assert rules == {
            "financialmodelingprep.com": [
                {
                    "transform": {
                        "headers": {
                            "apikey": Secret.fill("prod-fmp"),
                            "x-other": Secret.fill("prod-other"),
                        }
                    }
                }
            ]
        }

    @pytest.mark.parametrize(
        ("other_header", "match"),
        [(None, "needs a header"), ("APIKEY", "Two platform secrets")],
    )
    def test_a_catalog_e2b_cannot_route_is_refused(self, other_header, match):
        other = _FMP.model_copy(
            update={"sandbox_env_var": "OTHER_KEY", "header": other_header}
        )
        with pytest.raises(PlatformSecretConfigurationError, match=match):
            egress_rules(
                {"FMP_API_KEY": "a", "OTHER_KEY": "b"},
                {"FMP_API_KEY": _FMP, "OTHER_KEY": other},
            )


class TestPrivatePorts:
    """With public_traffic off, a port answers only E2B's traffic token.

    A browser cannot add that header, so no raw sandbox host may be handed to
    one: the preview says so, and the server proxies it.
    """

    @staticmethod
    def _handle(token: str | None) -> SimpleNamespace:
        return SimpleNamespace(
            get_host=lambda port: f"{port}-sbx-1.e2b.app", traffic_access_token=token
        )

    @pytest.mark.asyncio
    async def test_a_private_deployment_creates_without_public_traffic(self):
        client = _client()
        client.AsyncSandbox.create.return_value = SimpleNamespace(sandbox_id="sbx-new")
        await _provider(client, public_traffic=False).create(tier="standard")

        kwargs = client.AsyncSandbox.create.await_args.kwargs
        assert kwargs["network"] == {"allow_public_traffic": False}

    @pytest.mark.asyncio
    async def test_privacy_rides_beside_the_secret_rules(self):
        client = _client()
        client.AsyncSandbox.create.return_value = SimpleNamespace(sandbox_id="sbx-new")
        await _provider(client, public_traffic=False, platform_secrets=(_FMP,)).create(
            platform_secret_bindings={"FMP_API_KEY": "prod-fmp"}, tier="standard"
        )

        kwargs = client.AsyncSandbox.create.await_args.kwargs
        assert kwargs["network"] == {"rules": _FMP_RULES, "allow_public_traffic": False}

    @pytest.mark.asyncio
    async def test_a_private_port_carries_its_token_and_is_not_browser_ready(self):
        runtime = E2BRuntime(
            _client(),
            "sbx-1",
            sandbox=self._handle("tok-1"),
            window_s=3600,
            max_timeout_s=3600,
            public_traffic=False,
        )

        info = await runtime.get_preview_url(8080)

        assert info.url == "https://8080-sbx-1.e2b.app"
        assert info.auth_headers == {"e2b-traffic-access-token": "tok-1"}
        assert not info.browser_ready
        assert "tok-1" not in repr(info)
        assert await runtime.get_preview_link(8080) == info

    @pytest.mark.asyncio
    async def test_a_public_port_on_a_public_deployment_goes_to_the_browser(self):
        runtime = E2BRuntime(
            _client(), "sbx-1", sandbox=self._handle(None), window_s=3600, max_timeout_s=3600
        )

        info = await runtime.get_preview_url(8080)

        assert info.browser_ready
        assert info.auth_headers == {}

    @pytest.mark.asyncio
    async def test_a_public_sandbox_on_a_private_deployment_is_still_proxied(self):
        client = _client()
        client.AsyncSandbox.connect.return_value = self._handle(None)
        runtime = await _provider(client, public_traffic=False).get("sbx-1")

        info = await runtime.get_preview_url(8080)

        assert not info.browser_ready
        assert info.auth_headers == {}


class TestSecretReconciler:
    @staticmethod
    def _reconciler(**api: AsyncMock) -> E2BSecretReconciler:
        defaults = {"update": AsyncMock(), "create": AsyncMock(), "exists": AsyncMock()}
        return E2BSecretReconciler(
            SimpleNamespace(AsyncSecret=SimpleNamespace(**{**defaults, **api}))
        )

    _SECRET = ResolvedPlatformSecret(definition=_FMP, name="prod-fmp", value="v")

    @pytest.mark.asyncio
    async def test_a_missing_secret_is_created_and_the_sandbox_marker_returned(self):
        create = AsyncMock(return_value=SimpleNamespace(secret_id="sec_1"))
        reconciled = await self._reconciler(
            update=AsyncMock(side_effect=SecretNotFoundException("gone")),
            create=create,
        ).reconcile([self._SECRET])

        create.assert_awaited_once()
        assert reconciled[0].provider_secret_id == "sec_1"
        assert reconciled[0].placeholder == "e2b_secret_fmp_api_key"
        assert "financialmodelingprep.com" in reconciled[0].egress_rule

    @pytest.mark.asyncio
    async def test_losing_a_create_race_updates_the_winner(self):
        update = AsyncMock(
            side_effect=[
                SecretNotFoundException("gone"),
                SimpleNamespace(secret_id="sec_2"),
            ]
        )
        reconciled = await self._reconciler(
            update=update,
            create=AsyncMock(side_effect=SecretException("409: already exists")),
            exists=AsyncMock(return_value=True),
        ).reconcile([self._SECRET])

        assert reconciled[0].provider_secret_id == "sec_2"
        assert update.await_count == 2

    @pytest.mark.asyncio
    async def test_a_rejected_key_fails_boot_as_configuration(self):
        with pytest.raises(PlatformSecretConfigurationError, match="cannot manage"):
            await self._reconciler(
                update=AsyncMock(side_effect=AuthenticationException("401: no")),
            ).reconcile([self._SECRET])

    @pytest.mark.asyncio
    async def test_a_catalog_it_cannot_route_writes_no_secret(self):
        update = AsyncMock()
        headerless = ResolvedPlatformSecret(
            definition=_FMP.model_copy(update={"header": None}), name="prod-fmp", value="v"
        )
        with pytest.raises(PlatformSecretConfigurationError, match="needs a header"):
            await self._reconciler(update=update).reconcile([headerless])
        update.assert_not_awaited()


class TestMetricsSince:
    """E2B's metrics read is a class-level API call: no connect, so no resume."""

    @staticmethod
    def _sample(ts: datetime, **fields) -> SimpleNamespace:
        values = {
            "cpu_count": 2,
            "cpu_used_pct": 12.5,
            "disk_total": 10 * 2**30,
            "disk_used": 2**30,
            "mem_total": 4 * 2**30,
            "mem_used": 2**29,
            "mem_cache": 2**28,
        }
        return SimpleNamespace(timestamp=ts, **{**values, **fields})

    @staticmethod
    def _provider_with(samples) -> tuple[E2BProvider, SimpleNamespace]:
        client = _client()
        client.AsyncSandbox.get_metrics = AsyncMock(return_value=samples)
        return _provider(client), client

    T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

    def test_reports_metrics(self):
        assert E2BProvider.reports_metrics is True

    @pytest.mark.asyncio
    async def test_maps_fields_through_the_class_level_read(self):
        T0 = self.T0
        until = T0 + timedelta(hours=1)
        provider, client = self._provider_with([self._sample(T0 + timedelta(minutes=1))])

        [sample] = await provider.metrics_since("sbx-1", T0, until=until)

        client.AsyncSandbox.get_metrics.assert_awaited_once_with(
            "sbx-1", start=T0, end=until
        )
        client.AsyncSandbox.connect.assert_not_awaited()
        assert sample.timestamp == T0 + timedelta(minutes=1)
        assert sample.cpu_used_pct == 12.5
        assert sample.cpu_count == 2
        assert sample.mem_used_bytes == 2**29 + 2**28
        assert sample.mem_cache_bytes == 2**28
        assert sample.mem_total_bytes == 4 * 2**30
        assert sample.disk_used_bytes == 2**30
        assert sample.disk_total_bytes == 10 * 2**30

    @pytest.mark.asyncio
    async def test_a_rate_limit_refusal_is_a_rate_limited_error(self):
        provider, client = self._provider_with([])
        client.AsyncSandbox.get_metrics.side_effect = RateLimitException("slow down")

        with pytest.raises(SandboxRateLimitedError, match="slow down"):
            await provider.metrics_since("sbx-1", self.T0)

    @pytest.mark.asyncio
    async def test_a_429_status_is_a_rate_limited_error(self):
        provider, client = self._provider_with([])
        client.AsyncSandbox.get_metrics.side_effect = SandboxException(
            "too many requests", status_code=429
        )

        with pytest.raises(SandboxRateLimitedError):
            await provider.metrics_since("sbx-1", self.T0)

    @pytest.mark.asyncio
    async def test_another_sandbox_error_propagates_as_itself(self):
        provider, client = self._provider_with([])
        error = SandboxException("boom", status_code=500)
        client.AsyncSandbox.get_metrics.side_effect = error

        with pytest.raises(SandboxException) as raised:
            await provider.metrics_since("sbx-1", self.T0)

        assert raised.value is error
        assert not isinstance(raised.value, SandboxRateLimitedError)

    @pytest.mark.asyncio
    async def test_a_template_before_envd_0_2_4_has_no_disk(self):
        provider, _ = self._provider_with(
            [self._sample(self.T0 + timedelta(minutes=1), disk_total=0, disk_used=0)]
        )

        [sample] = await provider.metrics_since("sbx-1", self.T0)

        assert sample.disk_total_bytes is None
        assert sample.disk_used_bytes is None
        assert sample.mem_total_bytes == 4 * 2**30

    @pytest.mark.asyncio
    async def test_zero_memory_total_and_cpu_count_are_none(self):
        provider, _ = self._provider_with(
            [
                self._sample(
                    self.T0 + timedelta(minutes=1),
                    mem_total=0,
                    cpu_count=0,
                    cpu_used_pct=0.0,
                )
            ]
        )

        [sample] = await provider.metrics_since("sbx-1", self.T0)

        assert sample.mem_total_bytes is None
        assert sample.cpu_count is None
        assert sample.cpu_used_pct == 0.0

    @pytest.mark.asyncio
    async def test_the_sample_at_since_is_dropped_though_e2b_floors_start(self):
        since = self.T0 + timedelta(milliseconds=400)
        floored = self.T0
        provider, _ = self._provider_with(
            [
                self._sample(floored),
                self._sample(since),
                self._sample(since + timedelta(milliseconds=1)),
                self._sample(self.T0 + timedelta(seconds=5)),
            ]
        )

        samples = await provider.metrics_since("sbx-1", since)

        assert [s.timestamp for s in samples] == [
            since + timedelta(milliseconds=1),
            self.T0 + timedelta(seconds=5),
        ]

    @pytest.mark.asyncio
    async def test_until_bounds_the_window_inclusively(self):
        until = self.T0 + timedelta(minutes=10)
        provider, _ = self._provider_with(
            [
                self._sample(until + timedelta(seconds=1)),
                self._sample(until),
                self._sample(self.T0 + timedelta(minutes=5)),
            ]
        )

        samples = await provider.metrics_since("sbx-1", self.T0, until=until)

        assert [s.timestamp for s in samples] == [self.T0 + timedelta(minutes=5), until]

    @pytest.mark.asyncio
    async def test_samples_are_oldest_first_and_utc(self):
        naive = datetime(2026, 1, 1, 12, 5)
        provider, _ = self._provider_with(
            [
                self._sample(self.T0 + timedelta(minutes=9)),
                self._sample(naive),
                self._sample(self.T0 + timedelta(minutes=7)),
            ]
        )

        samples = await provider.metrics_since("sbx-1", self.T0)

        assert [s.timestamp for s in samples] == [
            self.T0 + timedelta(minutes=5),
            self.T0 + timedelta(minutes=7),
            self.T0 + timedelta(minutes=9),
        ]
        assert all(s.timestamp.utcoffset() == timedelta(0) for s in samples)

    @pytest.mark.asyncio
    async def test_sdk_errors_propagate(self):
        provider, client = self._provider_with([])
        client.AsyncSandbox.get_metrics.side_effect = SandboxNotFoundException("gone")

        with pytest.raises(SandboxNotFoundException):
            await provider.metrics_since("sbx-1", self.T0)
        client.AsyncSandbox.connect.assert_not_awaited()
