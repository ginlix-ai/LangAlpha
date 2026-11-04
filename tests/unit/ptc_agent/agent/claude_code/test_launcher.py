from __future__ import annotations

import hashlib
import inspect
import json
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

import anyio
import pytest
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

from ptc_agent.agent.claude_code.launcher import (
    API_KEY_FD_ENV,
    SDK_CONNECT_SHA256,
    TOKEN_FD_ENV,
    CliSlots,
    Credential,
    LocalLauncher,
    ScopedEnvTransport,
    _token_socket,
    session_file_path,
    thread_dir,
)


def test_the_transport_is_a_copy_of_the_pinned_sdks():
    source = inspect.getsource(SubprocessCLITransport.connect)
    assert hashlib.sha256(source.encode()).hexdigest() == SDK_CONNECT_SHA256, (
        "the SDK's connect changed: carry the change into ScopedEnvTransport.connect, then update SDK_CONNECT_SHA256"
    )


async def launch(launcher: LocalLauncher, tmp_path: Path, fd_env: str = TOKEN_FD_ENV) -> tuple[dict[str, str], str, str]:
    """Start a stand-in CLI that records its environment, its argv and what the descriptor ``fd_env`` names holds."""
    cli, seen = tmp_path / "claude", tmp_path / "seen.json"
    cli.write_text(
        f"#!{sys.executable}\nimport json, os, socket, sys\n"
        f"fd = os.environ.get({fd_env!r})\n"
        "token = socket.socket(fileno=int(fd)).recv(4096).decode().strip() if fd else ''\n"
        f"json.dump({{'env': dict(os.environ), 'argv': sys.argv[1:], 'token': token}}, open({str(seen)!r}, 'w'))\n"
    )
    cli.chmod(0o755)
    launcher = replace(launcher, cli_path=str(cli))
    prepared = await launcher.scratch()
    transport = launcher.transport(ClaudeAgentOptions(env={"CLAUDE_CONFIG_DIR": "/c"}, cwd=str(tmp_path)))
    await transport.connect()
    await transport._process.wait()
    await transport.close()
    await launcher.release(prepared)
    got = json.loads(seen.read_text())
    return got["env"], " ".join(got["argv"]), got["token"]


async def child_env(launcher: LocalLauncher, tmp_path: Path) -> dict[str, str]:
    return (await launch(launcher, tmp_path))[0]


@pytest.mark.asyncio
async def test_the_child_gets_the_allowlist_and_the_runtimes_variables_only(monkeypatch, tmp_path):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-platform-key")
    monkeypatch.setenv("DATABASE_URL", "postgres://secret")
    monkeypatch.setenv("USER", "alan")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:3128")
    env = await child_env(LocalLauncher(env={"ANTHROPIC_BASE_URL": "http://127.0.0.1:1"}), tmp_path)
    assert "ANTHROPIC_API_KEY" not in env and "DATABASE_URL" not in env
    assert env["USER"] == "alan" and env["ANTHROPIC_BASE_URL"] == "http://127.0.0.1:1"
    assert env["HTTPS_PROXY"] == "http://proxy.example:3128"
    assert env["CLAUDE_CONFIG_DIR"] == "/c" and env["PWD"] == str(tmp_path)
    assert TOKEN_FD_ENV not in env


@pytest.mark.asyncio
async def test_the_cli_looks_for_its_credential_in_the_turns_dir_only(monkeypatch, tmp_path):
    monkeypatch.setenv("CLAUDE_SECURESTORAGE_CONFIG_DIR", "")
    env = await child_env(LocalLauncher(), tmp_path)
    assert env["CLAUDE_CONFIG_DIR"] == "/c" and "CLAUDE_SECURESTORAGE_CONFIG_DIR" not in env


@pytest.mark.asyncio
async def test_the_token_reaches_the_cli_through_a_socket_never_its_environment_or_argv(tmp_path):
    token = "sk-ant-oat01-" + "7" * 80
    env, argv, handed = await launch(LocalLauncher(token=token), tmp_path)
    assert handed == token
    assert token not in argv and not any(token in v for v in env.values())


class _Store:
    def __init__(self, credential: Credential) -> None:
        self.credential = credential

    async def read(self) -> Credential:
        return self.credential

    async def refused(self, credential: Credential) -> None: ...


@pytest.mark.asyncio
async def test_an_api_key_reaches_the_cli_on_its_own_descriptor_never_its_environment_or_argv(tmp_path):
    key = "sk-ant-api03-" + "9" * 80
    launcher = LocalLauncher(saved=_Store(Credential(key, kind="api_key")))
    env, argv, handed = await launch(launcher, tmp_path, fd_env=API_KEY_FD_ENV)
    assert handed == key and TOKEN_FD_ENV not in env
    assert key not in argv and not any(key in v for v in env.values())


@pytest.mark.asyncio
async def test_a_cli_waits_for_a_slot_is_busy_once_none_frees_in_time_and_gives_it_back_once_closed(monkeypatch):
    from src.llms.claude_code import ClaudeTurnFailed

    monkeypatch.setattr(CliSlots, "WAIT_S", 0.2)
    monkeypatch.setattr(ScopedEnvTransport, "_start", lambda self: anyio.sleep(0))
    slots = CliSlots(1)
    first = LocalLauncher(slots=slots).transport(ClaudeAgentOptions())
    await first.connect()
    with pytest.raises(ClaudeTurnFailed) as busy:
        await LocalLauncher(slots=slots).transport(ClaudeAgentOptions()).connect()
    assert busy.value.kind == "busy"
    await first.close()
    await first.close()
    after = LocalLauncher(slots=slots).transport(ClaudeAgentOptions())
    await after.connect()
    with pytest.raises(ClaudeTurnFailed):
        await LocalLauncher(slots=slots).transport(ClaudeAgentOptions()).connect()
    await after.close()


@pytest.mark.asyncio
async def test_a_cli_that_fails_to_start_gives_its_slot_back(monkeypatch):
    async def fails(self):
        raise RuntimeError("no cli")

    slots = CliSlots(1)
    monkeypatch.setattr(ScopedEnvTransport, "_start", fails)
    with pytest.raises(RuntimeError):
        await LocalLauncher(slots=slots).transport(ClaudeAgentOptions()).connect()
    monkeypatch.setattr(CliSlots, "WAIT_S", 0.2)
    monkeypatch.setattr(ScopedEnvTransport, "_start", lambda self: anyio.sleep(0))
    await LocalLauncher(slots=slots).transport(ClaudeAgentOptions()).connect()


@pytest.mark.skipif(not os.path.isdir("/proc"), reason="reads open descriptors from /proc")
@pytest.mark.parametrize("uvloop", [False, True])
def test_no_other_process_spawned_while_the_token_is_held_gets_its_socket(uvloop):
    if uvloop:
        pytest.importorskip("uvloop")

    async def spawn_meanwhile() -> tuple[int, list[str]]:
        held = _token_socket("sk-ant-oat01-held")
        try:
            done = await anyio.run_process(["ls", "/proc/self/fd"])
            return held.fileno(), done.stdout.decode().split()
        finally:
            held.close()

    fd, seen = anyio.run(spawn_meanwhile, backend_options={"use_uvloop": uvloop})
    assert str(fd) not in seen


@pytest.mark.asyncio
async def test_prepare_lays_out_the_session_and_prompt_and_release_keeps_only_the_threads_dir():
    launcher = LocalLauncher()
    prepared = await launcher.prepare("t-launch", "sid", [{"uuid": "a"}], identity="u", system_prompt="be brief")
    assert prepared.cwd == os.path.realpath(prepared.cwd) == thread_dir("t-launch")
    session = session_file_path(prepared.config_dir, prepared.cwd, "sid")
    assert session.parent.parent == Path(prepared.config_dir) / "projects" and session.name == "sid.jsonl"
    assert session.read_text() == '{"uuid":"a"}\n'
    assert Path(prepared.system_prompt_file).read_text() == "be brief"
    await launcher.release(prepared)
    assert not Path(prepared.config_dir).exists() and Path(prepared.cwd).is_dir()


@pytest.mark.parametrize("key", ["..", "", "a/b", "/etc", "t.1"])
def test_a_thread_key_that_is_not_a_plain_id_is_refused(key):
    with pytest.raises(ValueError):
        thread_dir(key)


def test_a_thread_dir_someone_planted_as_a_symlink_is_refused(tmp_path):
    base = Path(os.path.realpath("/tmp")) / "langalpha-claude"
    base.mkdir(mode=0o700, exist_ok=True)
    link = base / "t-planted"
    link.unlink(missing_ok=True)
    link.symlink_to(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="not a directory this user owns"):
            thread_dir("t-planted")
    finally:
        link.unlink()


@pytest.mark.asyncio
async def test_without_the_at_rest_key_a_turn_does_not_start_and_leaves_nothing(monkeypatch, tmp_path):
    monkeypatch.delenv("BYOK_ENCRYPTION_KEY")
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    with pytest.raises(RuntimeError, match="BYOK_ENCRYPTION_KEY"):
        await LocalLauncher().prepare("t-nokey", None, [], identity="u", system_prompt="s")
    assert not any(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_a_prepare_that_fails_midway_removes_its_config_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

    def full(*args):
        raise OSError("no space left on device")

    monkeypatch.setattr("ptc_agent.agent.claude_code.launcher.write_session_file", full)
    with pytest.raises(OSError, match="no space"):
        await LocalLauncher().prepare("t-full", "sid", [{"uuid": "a"}], identity="u", system_prompt="s")
    assert not any(tmp_path.iterdir())
