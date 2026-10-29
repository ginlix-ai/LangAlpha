from __future__ import annotations

import hashlib
import inspect
import os
import tempfile
from pathlib import Path

import pytest
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport

from ptc_agent.agent.claude_code.launcher import SDK_CONNECT_SHA256, LocalLauncher, session_file_path, thread_dir


def test_the_transport_is_a_copy_of_the_pinned_sdks():
    source = inspect.getsource(SubprocessCLITransport.connect)
    assert hashlib.sha256(source.encode()).hexdigest() == SDK_CONNECT_SHA256, (
        "the SDK's connect changed: carry the change into ScopedEnvTransport.connect, then update SDK_CONNECT_SHA256"
    )


async def child_env(launcher: LocalLauncher, tmp_path: Path) -> dict[str, str]:
    dump = tmp_path / "env"
    cli = tmp_path / "claude"
    cli.write_text(f"#!/bin/sh\nenv > {dump}\n")
    cli.chmod(0o755)
    transport = LocalLauncher(env=launcher.env, cli_path=str(cli), machine_login=launcher.machine_login).transport(
        ClaudeAgentOptions(env={"CLAUDE_CONFIG_DIR": "/c"}, cwd=str(tmp_path))
    )
    await transport.connect()
    await transport._process.wait()
    await transport.close()
    return dict(line.split("=", 1) for line in dump.read_text().splitlines() if "=" in line)


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
    assert "CLAUDE_SECURESTORAGE_CONFIG_DIR" not in env


@pytest.mark.asyncio
async def test_only_a_machine_login_launcher_points_the_cli_at_the_machines_login(tmp_path):
    env = await child_env(LocalLauncher(machine_login=True), tmp_path)
    assert env["CLAUDE_SECURESTORAGE_CONFIG_DIR"] == ""


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
