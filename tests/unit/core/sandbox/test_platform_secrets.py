"""Platform-secret catalog, classification, hosted env, and convergence tests."""

import hashlib
import re
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
    MCPServerConfig,
    PlatformSecretDefinition,
    SandboxConfig,
    SecurityConfig,
)
from ptc_agent.core.sandbox.platform_secrets import (
    PlatformSecretConfigurationError,
    build_platform_secret_bindings,
    platform_secrets_active,
    platform_secrets_required,
    require_platform_secret_capable,
    resolve_platform_secrets,
)
from tests.unit.core.sandbox.fake_provider import fake_provider


_FMP_DEFINITION = PlatformSecretDefinition(
    source_env_var="FMP_API_KEY",
    sandbox_env_var="FMP_API_KEY",
    name_suffix="platform-fmp-api-key",
    description="Platform FMP API key",
    hosts=("financialmodelingprep.com",),
)


def _config(
    *,
    provider: str = "daytona",
    namespace: str = "prod",
    platform_secrets: tuple[PlatformSecretDefinition, ...] = (_FMP_DEFINITION,),
) -> CoreConfig:
    return CoreConfig(
        sandbox=SandboxConfig(
            provider=provider,
            daytona=DaytonaConfig(
                api_key="daytona-key",
                secret_namespace=namespace,
            ),
            e2b=E2BConfig(api_key="e2b-key", secret_namespace=namespace),
            docker=DockerConfig(),
            platform_secrets=platform_secrets,
        ),
        security=SecurityConfig(),
        mcp=MCPConfig(
            servers=[
                MCPServerConfig(
                    name="price_data",
                    env={"FMP_API_KEY": "${FMP_API_KEY}"},
                )
            ]
        ),
        logging=LoggingConfig(),
        filesystem=FilesystemConfig(),
    )


def test_resolve_hosted_platform_secret(monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    resolved = resolve_platform_secrets(
        _config(), environ={"FMP_API_KEY": "real-fmp-value"}
    )

    assert len(resolved) == 1
    assert resolved[0].name == "prod-platform-fmp-api-key"
    assert resolved[0].definition.hosts == ("financialmodelingprep.com",)
    assert build_platform_secret_bindings(
        _config(), environ={"FMP_API_KEY": "real-fmp-value"}
    ) == {"FMP_API_KEY": "prod-platform-fmp-api-key"}


@pytest.mark.parametrize(
    ("namespace", "environ", "missing_name"),
    [
        ("", {"FMP_API_KEY": "value"}, "DAYTONA_SECRET_NAMESPACE"),
        ("bad namespace", {"FMP_API_KEY": "value"}, "must match"),
        ("prod", {}, "FMP_API_KEY"),
    ],
)
def test_resolve_hosted_configuration_fails_closed(
    monkeypatch, namespace, environ, missing_name
):
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    with pytest.raises(PlatformSecretConfigurationError, match=missing_name):
        resolve_platform_secrets(_config(namespace=namespace), environ=environ)


def test_resolve_on_e2b_reads_its_namespace_and_lowercases_the_name():
    # E2B stores Secret names lowercased, and a sandbox names the Secret it
    # wants by that stored form.
    resolved = resolve_platform_secrets(
        _config(provider="e2b", namespace="Prod"),
        environ={"FMP_API_KEY": "value"},
        host_mode="oss",
    )
    assert resolved[0].name == "prod-platform-fmp-api-key"

    with pytest.raises(PlatformSecretConfigurationError, match="E2B_SECRET_NAMESPACE"):
        resolve_platform_secrets(
            _config(provider="e2b", namespace=""),
            environ={"FMP_API_KEY": "value"},
            host_mode="oss",
        )


def test_resolve_for_a_kind_uses_that_providers_namespace_and_naming():
    """A computer kept from before a provider switch still gets its own kind's
    Secret names, whatever the deployment's provider is now."""
    config = _config(provider="daytona")
    config.sandbox.daytona.secret_namespace = "Dtn"
    config.sandbox.e2b.secret_namespace = "E2b"
    environ = {"FMP_API_KEY": "value"}

    assert [s.name for s in resolve_platform_secrets(
        config, kind="e2b", environ=environ, host_mode="oss"
    )] == ["e2b-platform-fmp-api-key"]
    assert [s.name for s in resolve_platform_secrets(
        config, environ=environ, host_mode="oss"
    )] == ["Dtn-platform-fmp-api-key"]
    assert resolve_platform_secrets(
        config, kind="docker", environ=environ, host_mode="oss"
    ) == ()
    assert platform_secrets_active(_config(provider="docker"), kind="e2b")
    assert not platform_secrets_active(_config(), kind="docker")

    config.sandbox.e2b.secret_namespace = ""
    with pytest.raises(PlatformSecretConfigurationError, match="E2B_SECRET_NAMESPACE"):
        resolve_platform_secrets(config, kind="e2b", environ=environ, host_mode="oss")


@pytest.mark.parametrize("provider", ["daytona", "e2b"])
def test_resolve_hosted_empty_catalog_resolves_to_nothing(provider):
    # Hosted mode needs no catalog once no built-in holds a vendor credential;
    # a credential that is still configured is withheld at sandbox build.
    config = _config(provider=provider, platform_secrets=())
    environ = {"FMP_API_KEY": "value"}

    assert resolve_platform_secrets(
        config, environ=environ, host_mode="platform"
    ) == ()
    assert build_platform_secret_bindings(
        config, environ=environ, host_mode="platform"
    ) == {}

def test_resolve_rejects_an_overlong_derived_name():
    # namespace + '-' + suffix must fit the provider/DB name bound; a suffix
    # that pushes the derived name past it fails closed at resolution, matching
    # the invariant the module comment claims.
    definition = PlatformSecretDefinition(
        source_env_var="FMP_API_KEY",
        sandbox_env_var="FMP_API_KEY",
        name_suffix="x" * 62,
        description="Platform FMP API key",
        hosts=("financialmodelingprep.com",),
    )
    with pytest.raises(PlatformSecretConfigurationError, match="Derived Secret name"):
        resolve_platform_secrets(
            _config(namespace="prod", platform_secrets=(definition,)),
            environ={"FMP_API_KEY": "real-fmp-value"},
        )


def test_oss_daytona_with_catalog_opts_in(monkeypatch):
    # Configuring a catalog on a capable provider is the opt-in — it activates
    # in OSS mode too, no HOST_MODE=platform required.
    monkeypatch.setattr("src.config.env.HOST_MODE", "oss")
    resolved = resolve_platform_secrets(
        _config(), environ={"FMP_API_KEY": "real-fmp-value"}
    )
    assert len(resolved) == 1
    assert resolved[0].name == "prod-platform-fmp-api-key"


def test_empty_catalog_is_the_opt_out(monkeypatch):
    # No catalog on a capable provider stays on plaintext env (unchanged
    # behavior) — but only when not hosted-required.
    monkeypatch.setattr("src.config.env.HOST_MODE", "oss")
    assert resolve_platform_secrets(_config(platform_secrets=()), environ={}) == ()


def test_incapable_provider_never_activates():
    # Docker/memory can't substitute placeholders, so a catalog is inert there.
    assert resolve_platform_secrets(_config(provider="docker"), environ={}) == ()
    assert not platform_secrets_active(_config(provider="docker"))
    assert not platform_secrets_active(_config(provider="memory"))


def test_active_gate_is_host_agnostic():
    assert platform_secrets_active(_config())
    assert not platform_secrets_active(_config(platform_secrets=()))
    assert not platform_secrets_active(_config(provider="docker"))


def test_required_gate_is_the_hosted_fail_closed_guard():
    assert platform_secrets_required(_config(), host_mode="platform")
    assert not platform_secrets_required(_config(), host_mode="oss")
    assert not platform_secrets_required(
        _config(provider="docker"), host_mode="platform"
    )
    # Required is capability-only — an empty catalog is still "required" so the
    # hosted deploy fails closed rather than silently running on plaintext.
    assert platform_secrets_required(
        _config(platform_secrets=()), host_mode="platform"
    )


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _FakeRuntime:
    """Emulates the in-sandbox hashing probe; env flips to the mount on restart."""

    updates_env = True

    def __init__(self, *, env_value: str, post_scrub_value: str = "dtn_secret_new"):
        from ptc_agent.core.sandbox.runtime import RuntimeState

        self.env_value = env_value
        self.post_scrub_value = post_scrub_value
        self.calls: list[tuple] = []
        self._state = RuntimeState.RUNNING
        self._states = RuntimeState

    def _env_lookup(self, name):
        return self.env_value

    async def exec(self, command, timeout=60):
        from ptc_agent.core.sandbox.runtime import ExecResult

        self.calls.append(("exec", command))
        names = re.findall(r'"([A-Za-z_][A-Za-z0-9_]*)"', command)
        lines = [
            _hash(value) if (value := self._env_lookup(name)) else ""
            for name in names
        ]
        return ExecResult(stdout="\n".join(lines) + "\n", stderr="", exit_code=0)

    async def update_env(self, env, *, unset=()):
        self.calls.append(("update_env", dict(env), tuple(unset)))
        self.env_value = ""

    async def update_secrets(self, secrets):
        self.calls.append(("update_secrets", dict(secrets)))

    async def stop(self, timeout=60, *, force=False):
        self.calls.append(("stop", force))
        self._state = self._states.STOPPED

    async def start(self, timeout=120):
        self.calls.append(("start",))
        self._state = self._states.RUNNING
        self.env_value = self.post_scrub_value

    async def get_state(self):
        return self._state

    async def refresh_state(self):
        return await self.get_state()


@pytest.mark.asyncio
async def test_probe_hashes_the_whole_set_in_one_exec():
    from ptc_agent.core.sandbox.platform_secrets import (
        probe_runtime_platform_secrets,
    )

    class _TwoVarRuntime(_FakeRuntime):
        env = {"FMP_API_KEY": "value-a", "OTHER_KEY": ""}

        def _env_lookup(self, name):
            return self.env.get(name, "")

    runtime = _TwoVarRuntime(env_value="")
    observed = await probe_runtime_platform_secrets(
        runtime, env_vars=["FMP_API_KEY", "OTHER_KEY"]
    )

    assert observed == {"FMP_API_KEY": _hash("value-a"), "OTHER_KEY": ""}
    assert [call[0] for call in runtime.calls] == ["exec"]


@pytest.mark.asyncio
async def test_probe_rejects_invalid_env_var_name():
    from ptc_agent.core.sandbox.platform_secrets import (
        probe_runtime_platform_secrets,
    )

    with pytest.raises(PlatformSecretConfigurationError, match="env var"):
        await probe_runtime_platform_secrets(
            _FakeRuntime(env_value="x"), env_vars=['FMP"; rm -rf /']
        )


@pytest.mark.asyncio
async def test_probe_raises_on_nonzero_exit():
    from ptc_agent.core.sandbox.platform_secrets import (
        PlatformSecretVerificationError,
        probe_runtime_platform_secrets,
    )
    from ptc_agent.core.sandbox.runtime import ExecResult

    class _BrokenExecRuntime(_FakeRuntime):
        async def exec(self, command, timeout=60):
            return ExecResult(stdout="", stderr="", exit_code=127)

    with pytest.raises(PlatformSecretVerificationError, match="exit_code=127"):
        await probe_runtime_platform_secrets(
            _BrokenExecRuntime(env_value=""), env_vars=["FMP_API_KEY"]
        )


@pytest.mark.asyncio
async def test_probe_rejects_malformed_output():
    from ptc_agent.core.sandbox.platform_secrets import (
        PlatformSecretVerificationError,
        probe_runtime_platform_secrets,
    )
    from ptc_agent.core.sandbox.runtime import ExecResult

    class _GarbageRuntime(_FakeRuntime):
        async def exec(self, command, timeout=60):
            return ExecResult(stdout="not-a-hash\n", stderr="", exit_code=0)

    with pytest.raises(PlatformSecretVerificationError, match="malformed"):
        await probe_runtime_platform_secrets(
            _GarbageRuntime(env_value="x"), env_vars=["FMP_API_KEY"]
        )


@pytest.mark.asyncio
async def test_wait_for_state_times_out_and_rejects_error_state():
    from ptc_agent.core.sandbox.platform_secrets import _wait_for_state
    from ptc_agent.core.sandbox.runtime import RuntimeState

    stuck = _FakeRuntime(env_value="x")
    stuck._state = RuntimeState.STARTING
    with pytest.raises(TimeoutError):
        await _wait_for_state(stuck, {RuntimeState.RUNNING}, timeout=0.05)

    errored = _FakeRuntime(env_value="x")
    errored._state = RuntimeState.ERROR
    with pytest.raises(RuntimeError, match="error state"):
        await _wait_for_state(errored, {RuntimeState.RUNNING}, timeout=0.05)


@pytest.mark.asyncio
async def test_ensure_running_swallows_start_error_when_state_recovers():
    from ptc_agent.core.sandbox.platform_secrets import _ensure_running

    class _AmbiguousStartRuntime(_FakeRuntime):
        async def start(self, timeout=120):
            # The remote transition succeeds even though the call errors.
            await super().start(timeout=timeout)
            raise TimeoutError("start call timed out")

    runtime = _AmbiguousStartRuntime(env_value="x")
    runtime._state = runtime._states.STOPPED
    await _ensure_running(runtime, timeout=1)

    assert runtime._state == runtime._states.RUNNING


@pytest.mark.asyncio
async def test_converge_scrubs_unconditionally_in_order():
    from ptc_agent.core.sandbox.platform_secrets import (
        converge_sandbox_platform_secrets,
    )

    runtime = _FakeRuntime(
        env_value="old-plaintext", post_scrub_value="dtn_secret_new"
    )
    await converge_sandbox_platform_secrets(
        runtime,
        expected={"FMP_API_KEY": _hash("dtn_secret_new")},
        bindings={"FMP_API_KEY": "prod-platform-fmp-api-key"},
    )

    assert [call[0] for call in runtime.calls] == [
        "update_env",
        "update_secrets",
        "stop",
        "start",
        "exec",  # verify
    ]
    assert runtime.calls[0] == ("update_env", {}, ("FMP_API_KEY",))
    assert runtime.calls[1] == (
        "update_secrets",
        {"FMP_API_KEY": "prod-platform-fmp-api-key"},
    )
    assert runtime.calls[2] == ("stop", True)


@pytest.mark.asyncio
async def test_converge_raises_when_verification_fails():
    from ptc_agent.core.sandbox.platform_secrets import (
        PlatformSecretVerificationError,
        converge_sandbox_platform_secrets,
    )

    runtime = _FakeRuntime(env_value="old-plaintext", post_scrub_value="wrong")
    with pytest.raises(PlatformSecretVerificationError):
        await converge_sandbox_platform_secrets(
            runtime,
            expected={"FMP_API_KEY": _hash("dtn_secret_new")},
            bindings={"FMP_API_KEY": "prod-platform-fmp-api-key"},
        )


@pytest.mark.asyncio
async def test_capability_absent_defaults_raise():
    from ptc_agent.core.sandbox.runtime import SandboxProvider, SandboxRuntime

    class _Stub:
        pass

    with pytest.raises(NotImplementedError, match="env updates"):
        await SandboxRuntime.update_env(_Stub(), {"A": "b"})
    with pytest.raises(NotImplementedError, match="platform secrets"):
        await SandboxRuntime.update_secrets(_Stub(), {"A": "b"})
    with pytest.raises(NotImplementedError, match="platform secrets"):
        await SandboxProvider.reconcile_platform_secrets(_Stub(), [])


def test_hosted_daytona_env_excludes_plaintext_and_mounts_secret(monkeypatch):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=fake_provider(),
    ):
        sandbox = PTCSandbox(_config())

    bindings = build_platform_secret_bindings(sandbox.config)
    env_vars = sandbox._build_sandbox_env_vars(bindings)

    assert bindings == {"FMP_API_KEY": "prod-platform-fmp-api-key"}
    assert "FMP_API_KEY" not in env_vars


@pytest.mark.parametrize(
    ("name", "credential"),
    [
        ("FMP_API_KEY", True),
        ("CLIENT_SECRET", True),
        ("AWS_SECRET_ACCESS_KEY", True),
        ("GITHUB_TOKEN", True),
        ("DB_PASSWORD", True),
        ("GINLIX_DATA_URL", False),
        ("GINLIX_TOKEN_FILE", False),
        ("MAX_TOKENS", False),
        ("PUBLIC_KEY_PATH", False),
    ],
)
def test_credential_env_name(name, credential):
    from ptc_agent.core.sandbox.platform_secrets import is_credential_env_name

    assert is_credential_env_name(name) is credential


def _hosted_sandbox(monkeypatch, config: CoreConfig):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=fake_provider(),
    ):
        return PTCSandbox(config)


def test_hosted_env_withholds_an_unbound_credential(monkeypatch):
    # With no catalog entry for it, a vendor key never reaches a hosted
    # sandbox as plaintext; non-secret env is untouched.
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    monkeypatch.setenv("GINLIX_DATA_URL", "http://data.test")
    config = _config(platform_secrets=())
    config.mcp.servers[0].env["GINLIX_DATA_URL"] = "${GINLIX_DATA_URL}"
    sandbox = _hosted_sandbox(monkeypatch, config)

    bindings = build_platform_secret_bindings(sandbox.config)
    env_vars = sandbox._build_sandbox_env_vars(bindings)

    assert bindings == {}
    assert "FMP_API_KEY" not in env_vars
    assert "real-fmp-value" not in env_vars.values()
    assert env_vars["GINLIX_DATA_URL"] == "http://data.test"


def test_hosted_env_withholds_a_credential_the_catalog_does_not_cover(
    monkeypatch,
):
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    config = _config()
    config.mcp.servers[0].env["OTHER_API_KEY"] = "literal-other-value"
    sandbox = _hosted_sandbox(monkeypatch, config)

    env_vars = sandbox._build_sandbox_env_vars(
        build_platform_secret_bindings(sandbox.config)
    )

    assert "FMP_API_KEY" not in env_vars
    assert "OTHER_API_KEY" not in env_vars


@pytest.mark.parametrize(
    "value",
    [
        # As config loading leaves it: ${FMP_API_KEY} already expanded.
        "real-fmp-value",
        # A reference the loader could not expand, resolved here instead.
        "${FMP_API_KEY}",
    ],
)
def test_hosted_env_withholds_a_credential_under_another_name(monkeypatch, value):
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    config = _config(platform_secrets=())
    config.mcp.servers[0].env = {"FMP_KEY": value}
    sandbox = _hosted_sandbox(monkeypatch, config)

    env_vars = sandbox._build_sandbox_env_vars(
        build_platform_secret_bindings(sandbox.config)
    )

    assert "FMP_KEY" not in env_vars
    assert "real-fmp-value" not in env_vars.values()


@pytest.mark.parametrize(
    ("host_mode", "provider", "platform_secrets"),
    [
        # Self-hosted, a provider that cannot hold Secrets falls through to env.
        ("oss", "docker", (_FMP_DEFINITION,)),
        ("oss", "memory", (_FMP_DEFINITION,)),
        # Self-hosted Daytona with no catalog stays on plaintext env.
        ("oss", "daytona", ()),
    ],
)
def test_ungated_modes_keep_current_plaintext_behavior(
    monkeypatch, host_mode, provider, platform_secrets
):
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    monkeypatch.setattr("src.config.env.HOST_MODE", host_mode)
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=fake_provider("docker" if provider == "memory" else provider),
    ):
        sandbox = PTCSandbox(
            _config(provider=provider, namespace="", platform_secrets=platform_secrets)
        )

    bindings = build_platform_secret_bindings(sandbox.config)
    env_vars = sandbox._build_sandbox_env_vars(bindings)

    assert bindings == {}
    assert env_vars["FMP_API_KEY"] == "real-fmp-value"


@pytest.mark.parametrize("trusted", [False, True])
def test_an_ssh_machine_gets_server_env_only_when_the_deployment_trusts_it(
    monkeypatch, trusted
):
    # Config load replaces ${FMP_API_KEY} with the host's value, so the server
    # env arrives here as a literal no placeholder check can tell from a constant.
    from ptc_agent.core.sandbox.providers.ssh import SSHProvider
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

    monkeypatch.setattr("src.config.env.HOST_MODE", "oss")
    config = _config(namespace="", platform_secrets=())
    config.sandbox.ssh.trusted_host = trusted
    config.mcp.servers[0].env = {"FMP_API_KEY": "real-fmp-value"}
    with patch(
        "ptc_agent.core.sandbox.ptc_sandbox.create_provider",
        return_value=SSHProvider.__new__(SSHProvider),
    ):
        sandbox = PTCSandbox(config)

    env_vars = sandbox._build_sandbox_env_vars({})

    assert ("real-fmp-value" in env_vars.values()) is trusted
    assert (env_vars.get("FMP_API_KEY") == "real-fmp-value") is trusted


@pytest.mark.parametrize("trusted", [False, True])
def test_a_hosted_catalog_refuses_ssh_machines_only_once_they_are_trusted(trusted):
    # Untrusted, a machine is sent nothing and has nothing to hold; trusted,
    # it would get the catalog's credentials as plaintext.
    config = _config()
    config.sandbox.ssh.trusted_host = trusted

    if trusted:
        with pytest.raises(
            PlatformSecretConfigurationError, match="ssh cannot hold platform Secrets"
        ):
            require_platform_secret_capable(config, ("ssh",), host_mode="platform")
    else:
        require_platform_secret_capable(config, ("ssh",), host_mode="platform")


@pytest.mark.parametrize("provider", ["docker", "memory"])
def test_hosted_catalog_refuses_a_provider_that_cannot_hold_secrets(
    monkeypatch, provider
):
    # Falling through here would put the platform's own credentials into the
    # sandbox's env as plaintext.
    monkeypatch.setattr("src.config.env.HOST_MODE", "platform")
    monkeypatch.setenv("FMP_API_KEY", "real-fmp-value")
    config = _config(provider=provider, namespace="", platform_secrets=(_FMP_DEFINITION,))

    with pytest.raises(
        PlatformSecretConfigurationError,
        match=f"{provider} cannot hold platform Secrets",
    ):
        build_platform_secret_bindings(config)


class _FixedEnvRuntime(_FakeRuntime):
    """E2B's shape: the env is what create set, and nothing can update it."""

    updates_env = False


@pytest.mark.asyncio
async def test_remount_on_a_fixed_env_runtime_sends_only_the_bindings():
    from ptc_agent.core.sandbox.platform_secrets import (
        remount_platform_secret_bindings,
    )

    runtime = _FixedEnvRuntime(env_value="e2b_secret_fmp_api_key")
    await remount_platform_secret_bindings(
        runtime,
        expected={"FMP_API_KEY": _hash("e2b_secret_fmp_api_key")},
        bindings={"FMP_API_KEY": "prod-platform-fmp-api-key"},
    )

    assert runtime.calls == [
        ("update_secrets", {"FMP_API_KEY": "prod-platform-fmp-api-key"})
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runtime_cls", "env_value", "passes"),
    [
        (_FixedEnvRuntime, "e2b_secret_fmp_api_key", True),
        (_FixedEnvRuntime, "old-plaintext", False),
        # A catalog entry added after create: a client that refuses an empty
        # key never sends the request its egress rule would fill in.
        (_FixedEnvRuntime, "", False),
        # A runtime that remounts env has to show the placeholder.
        (_FakeRuntime, "", False),
    ],
)
async def test_verify_requires_the_marker_and_a_fixed_env_a_new_sandbox(
    runtime_cls, env_value, passes
):
    from ptc_agent.core.sandbox.platform_secrets import (
        PlatformSecretReplacementRequired,
        PlatformSecretVerificationError,
        verify_runtime_platform_secrets,
    )

    runtime = runtime_cls(env_value=env_value)
    expected = {"FMP_API_KEY": _hash("e2b_secret_fmp_api_key")}
    if passes:
        await verify_runtime_platform_secrets(runtime, expected=expected)
        return
    with pytest.raises(PlatformSecretVerificationError) as raised:
        await verify_runtime_platform_secrets(runtime, expected=expected)
    # Only a fixed env asks for a new sandbox; Daytona's scrub can clear its own.
    assert isinstance(raised.value, PlatformSecretReplacementRequired) is (
        runtime_cls is _FixedEnvRuntime
    )
