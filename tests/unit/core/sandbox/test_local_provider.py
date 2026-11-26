"""A computer on the server's own machine: the gate, the environment it gets, and
what an SSH machine must keep seeing of the install groups beside it."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import subprocess
import sys

import pytest

from ptc_agent.config.core import LocalConfig, SandboxConfig
from ptc_agent.core.sandbox._defaults import DEFAULT_DEPENDENCIES
from ptc_agent.core.sandbox.providers import (
    build_provider,
    builds_computers,
    is_machine_kind,
    provider_class,
)
from ptc_agent.core.sandbox.providers import _ssh_bootstrap as bootstrap
from ptc_agent.core.sandbox.providers._ssh_shell import layout_script
from ptc_agent.core.sandbox.providers.local import (
    LocalComputerNotAllowed,
    LocalProvider,
    LocalRuntime,
    local_computer_allowed,
)
from ptc_agent.core.sandbox.providers.local_runtime import (
    _write_atomic,
    install_groups,
    machine_env,
)
from src.tools.secretary.utils import computer_machine, computer_state


@pytest.fixture
def allowed(monkeypatch):
    monkeypatch.setenv("HOST_MODE", "oss")
    monkeypatch.setenv("LANGALPHA_LOCAL_COMPUTER", "1")
    monkeypatch.setenv("LANGALPHA_OWNER_SECRET", "s3cret")


class TestTheGate:
    @pytest.mark.parametrize(
        "host_mode,flag",
        [
            ("platform", "1"),
            ("", "1"),
            ("oss", None),
            ("oss", "0"),
            (None, None),
            ("platform", None),
        ],
    )
    def test_refused_unless_both_say_so(self, monkeypatch, host_mode, flag):
        for name, value in (("HOST_MODE", host_mode), ("LANGALPHA_LOCAL_COMPUTER", flag)):
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)

        assert local_computer_allowed() is False
        with pytest.raises(LocalComputerNotAllowed):
            LocalProvider(LocalConfig())
        with pytest.raises(LocalComputerNotAllowed):
            build_provider("local", LocalConfig(), working_dir="/anywhere")

    def test_refused_without_the_owner_secret(self, allowed, monkeypatch):
        monkeypatch.delenv("LANGALPHA_OWNER_SECRET")
        assert local_computer_allowed() is False

    def test_allowed_with_both(self, allowed):
        assert local_computer_allowed() is True
        assert isinstance(LocalProvider(LocalConfig()), LocalProvider)

    def test_unset_host_mode_reads_as_the_server_reads_it(self, allowed, monkeypatch):
        monkeypatch.delenv("HOST_MODE")
        assert local_computer_allowed() is True

    def test_no_config_offers_it_where_it_is_refused(self, monkeypatch):
        monkeypatch.setenv("HOST_MODE", "platform")
        monkeypatch.setenv("LANGALPHA_LOCAL_COMPUTER", "1")
        monkeypatch.setenv("LANGALPHA_OWNER_SECRET", "s3cret")
        offered = SandboxConfig(provider="docker", offered_providers=("local",))

        assert offered.creatable_providers() == ("docker",)
        assert SandboxConfig(provider="local").creatable_providers() == ()

    def test_offered_where_it_is_allowed(self, allowed):
        offered = SandboxConfig(provider="docker", offered_providers=("local",))
        assert offered.creatable_providers() == ("docker", "local")

    def test_the_gate_imports_where_there_are_no_posix_accounts(self):
        """The server imports it on every platform, and Windows has neither module."""
        code = (
            "import sys\n"
            "sys.modules['pwd'] = None\n"
            "sys.modules['grp'] = None\n"
            "from ptc_agent.core.sandbox.providers.local import local_computer_allowed\n"
            "print(local_computer_allowed())\n"
        )
        env = {"PATH": os.environ["PATH"], "HOST_MODE": "platform", "PYTHON_DOTENV_DISABLED": "1"}
        done = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120
        )
        assert done.returncode == 0, done.stderr
        assert done.stdout.splitlines()[-1] == "False"


class TestReplacingAFile:
    @pytest.mark.parametrize("mode", [0o600, 0o755, 0o664])
    def test_it_keeps_its_mode_when_the_caller_names_none(self, tmp_path, mode):
        path = tmp_path / "file"
        path.write_text("old")
        path.chmod(mode)
        _write_atomic(str(path), b"new")
        assert path.read_text() == "new"
        assert stat.S_IMODE(path.stat().st_mode) == mode

    def test_a_new_file_gets_the_default(self, tmp_path):
        old = os.umask(0o022)
        try:
            _write_atomic(str(tmp_path / "file"), b"new")
        finally:
            os.umask(old)
        assert stat.S_IMODE((tmp_path / "file").stat().st_mode) == 0o644

    def test_a_named_mode_wins(self, tmp_path):
        path = tmp_path / "credentials"
        path.write_text("old")
        path.chmod(0o644)
        _write_atomic(str(path), b"new", 0o600)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    @pytest.mark.asyncio
    async def test_an_upload_keeps_a_scripts_bit_and_credentials_stay_private(
        self, allowed, monkeypatch, tmp_path
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        root = tmp_path / "Root"
        runtime = LocalRuntime(sandbox_id="a" * 32, working_dir=str(root), bootstrap_timeout=60)
        monkeypatch.setattr(runtime, "_ensure_running", _nothing_to_start)
        script = root / "run.sh"
        script.parent.mkdir()
        script.write_text("old")
        script.chmod(0o755)

        await runtime.upload_file(b"new", str(script))
        await runtime.upload_file(b"token", str(root / "_internal" / "token"))

        assert stat.S_IMODE(script.stat().st_mode) == 0o755
        assert stat.S_IMODE((root / "_internal" / "token").stat().st_mode) == 0o600


async def _nothing_to_start() -> None:
    return None


class TestEnvironment:
    def test_a_proxy_reaches_a_command(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path))
        names = ("HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy")
        for name in names:
            monkeypatch.setenv(name, f"value-of-{name}")
        env = machine_env()
        for name in names:
            assert env[name] == f"value-of-{name}"

    def test_the_apps_node_reaches_a_command(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("LANGALPHA_NODE", "/Applications/LangAlpha.app/Contents/MacOS/LangAlpha")
        assert machine_env()["LANGALPHA_NODE"] == "/Applications/LangAlpha.app/Contents/MacOS/LangAlpha"


class TestARealCommand:
    @pytest.fixture(params=["asyncio", "uvloop"])
    def event_loop_policy(self, request, event_loop_policy):
        """These run for real, on each loop a server may run."""
        if request.param == "uvloop":
            return pytest.importorskip("uvloop").EventLoopPolicy()
        return event_loop_policy

    @pytest.mark.asyncio
    async def test_a_command_sees_none_of_the_servers_environment(
        self, allowed, monkeypatch, tmp_path
    ):
        """Run for real: what reaches a process is what its own ``env`` prints."""
        monkeypatch.setenv("HOME", str(tmp_path))
        secrets = (
            "LA_TEST_DATABASE_PASSWORD",
            "ANTHROPIC_API_KEY",
            "DAYTONA_API_KEY",
            "EGRESS_RELAY_SECRET",
        )
        for name in secrets:
            monkeypatch.setenv(name, "must-not-reach-a-command")
        monkeypatch.setenv("VIRTUAL_ENV", "/server/venv")
        monkeypatch.setenv("PATH", f"/server/venv/bin:{'/usr/bin:/bin'}")

        runtime = LocalRuntime(
            sandbox_id="a" * 32, working_dir=str(tmp_path / "Root"), bootstrap_timeout=60
        )
        await runtime._run(None, layout_script(runtime._dir(None)))
        result = await runtime._framed(None, "env", timeout=30, computer=False)

        assert result.exit_code == 0
        lines = result.stdout.decode().splitlines()
        names = {line.partition("=")[0] for line in lines}
        assert not names & set(secrets)
        assert "VIRTUAL_ENV" not in names
        assert b"must-not-reach-a-command" not in result.stdout
        path = next(line for line in lines if line.startswith("PATH="))
        assert "/server/venv" not in path
        assert {"HOME", "USER", "LOGNAME", "SHELL", "PATH"} <= names

    @pytest.mark.asyncio
    async def test_a_kill_ends_what_the_command_started(
        self, allowed, monkeypatch, tmp_path
    ):
        monkeypatch.setenv("HOME", str(tmp_path))
        runtime = LocalRuntime(
            sandbox_id="b" * 32, working_dir=str(tmp_path), bootstrap_timeout=60
        )
        await runtime._run(None, layout_script(runtime._dir(None)))
        alive = tmp_path / "alive"
        result = await runtime._framed(
            None,
            f"(while :; do date > {alive}; sleep 0.1; done) & wait",
            timeout=1,
            computer=False,
        )
        assert result.timed_out and result.started

        await asyncio.sleep(0.5)
        written = alive.stat().st_mtime_ns
        await asyncio.sleep(0.5)
        assert alive.stat().st_mtime_ns == written


class TestInstallGroups:
    def test_naming_no_groups_hashes_as_it_always_has(self):
        """The stamp every SSH machine holds, rebuilt as it was written: a change
        here reinstalls the runtime on all of them."""

        def digest(*parts: object) -> str:
            return hashlib.sha256(json.dumps(parts, sort_keys=True).encode()).hexdigest()[:16]

        packages = (*DEFAULT_DEPENDENCIES, "tzdata")
        recipe = {
            "uv": [bootstrap.UV_VERSION, bootstrap._UV_SHA256],
            "python": bootstrap.PYTHON_VERSION,
            "packages": packages,
            "ripgrep": [bootstrap.RIPGREP_VERSION, bootstrap._RIPGREP_SHA256],
            "browsers": digest(
                bootstrap.SANDBOX_PLAYWRIGHT_VERSION,
                bootstrap._LAUNCHER,
                bootstrap._MISSING_BROWSERS,
            ),
            "revision": 2,
        }
        whole = hashlib.sha256(json.dumps(recipe, sort_keys=True).encode()).hexdigest()[:16]

        assert bootstrap.recipe_hash() == whole
        assert bootstrap.layer_hashes() == {
            "packages": digest(packages),
            "browsers": recipe["browsers"],
        }

    def test_the_defaults_are_the_full_install(self):
        assert bootstrap.recipe_hash(bootstrap.default_groups()) == bootstrap.recipe_hash()

    def test_every_group_names_a_package_the_list_holds(self):
        listed = {bootstrap._distribution(p) for p in DEFAULT_DEPENDENCIES}
        for names in bootstrap._GROUP_DISTRIBUTIONS.values():
            assert names <= listed

    def test_core_alone_leaves_the_groups_out_and_keeps_the_rest(self):
        kept = bootstrap._packages(bootstrap.selected_groups([]))
        names = {bootstrap._distribution(p) for p in kept}

        assert not names & bootstrap._GROUP_DISTRIBUTIONS["browsing"]
        assert not names & bootstrap._GROUP_DISTRIBUTIONS["documents"]
        assert {"pandas", "mcp", "openpyxl", "tzdata"} <= names
        script = bootstrap.bootstrap_script(
            lock_wait_s=1, groups=bootstrap.selected_groups([])
        )
        assert "phase browsers" not in script

    def test_chromium_brings_playwright_and_replaces_the_launcher(self):
        groups = bootstrap.selected_groups(["chromium"])
        script = bootstrap.bootstrap_script(lock_wait_s=1, groups=groups)

        assert "browsing" in groups
        assert "-m playwright install chromium" in script
        assert "langalpha-launcher\" &&" not in script
        assert bootstrap.recipe_hash(groups) != bootstrap.recipe_hash()

    def test_an_unknown_name_is_refused_by_the_selection_and_dropped_at_a_start(self):
        with pytest.raises(ValueError):
            bootstrap.selected_groups(["nonsense"])

        chosen = install_groups({"LOCAL_INSTALL_GROUPS": "documents,nonsense"})
        assert chosen == {"core", "documents"}

    def test_unset_means_the_defaults_and_empty_means_core(self):
        assert install_groups({}) == bootstrap.default_groups()
        assert install_groups({"LOCAL_INSTALL_GROUPS": ""}) == {"core"}


class TestWhatTheKindIs:
    def test_the_server_provides_it_though_it_cannot_rebuild_it(self, monkeypatch):
        monkeypatch.setenv("HOST_MODE", "oss")
        monkeypatch.setenv("LANGALPHA_LOCAL_COMPUTER", "1")
        monkeypatch.setenv("LANGALPHA_OWNER_SECRET", "s3cret")
        assert provider_class("local").recreatable is False
        assert is_machine_kind("local") is True
        # A deployment offering only this computer still makes one for a user.
        assert builds_computers(SandboxConfig(provider="local")) is True
        assert builds_computers(SandboxConfig(provider="memory")) is False

    def test_the_agent_is_told_it_is_the_users_own_machine(self, monkeypatch):
        monkeypatch.setattr("sys.platform", "darwin")
        assert computer_machine("local", None) == "personal_mac"
        monkeypatch.setattr("sys.platform", "linux")
        assert computer_machine("local", None) == "personal"

    def test_reaching_a_stopped_one_is_not_a_hosted_start(self):
        assert computer_state("local", "stopped") == ""
        assert computer_state("docker", "stopped") == "stopped"

    @pytest.mark.asyncio
    async def test_a_preview_is_the_port_itself(self, allowed):
        runtime = await LocalProvider(LocalConfig()).get("c" * 32)
        info = await runtime.get_preview_url(8123)

        assert info.url == "http://127.0.0.1:8123"
        assert info.browser_ready and not info.process_local


class TestTheDefaultRoot:
    @pytest.mark.asyncio
    async def test_the_next_free_folder_is_picked(self, monkeypatch, tmp_path):
        import ptc_agent.core.sandbox.providers.local as local

        base = str(tmp_path / "LangAlpha")
        monkeypatch.setattr(local, "machine_home", lambda: str(tmp_path))

        async def free(path):
            return path != f"{base}-2"

        async def taken():
            return [base]

        monkeypatch.setattr(local, "root_is_free", free)
        assert await LocalProvider.default_root_dir(taken) == f"{base}-3"

    @pytest.mark.asyncio
    async def test_with_none_free_the_create_is_refused(self, monkeypatch, tmp_path):
        import ptc_agent.core.sandbox.providers.local as local

        monkeypatch.setattr(local, "machine_home", lambda: str(tmp_path))

        async def free(path):
            return False

        async def taken():
            return []

        monkeypatch.setattr(local, "root_is_free", free)
        with pytest.raises(local.NoFreeRootFolder):
            await LocalProvider.default_root_dir(taken)
