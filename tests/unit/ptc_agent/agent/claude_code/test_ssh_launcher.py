"""SSHLauncher's remote command, run here: the login shell's boot, the shim and a stand-in CLI.

The script the launcher sends is the one a machine runs; here ``sh`` stands
in for the login shell and ``HOME`` for the machine's home, so the token
hand-off, the environment and the relay handshake are checked without an
sshd (``tests/integration/claude_code`` runs the same over SSH).
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from claude_agent_sdk import ClaudeAgentOptions, CLIConnectionError

from ptc_agent.agent.claude_code import ssh_launcher
from ptc_agent.agent.claude_code.launcher import _INHERITED, TOKEN_FD_ENV, cli_env
from ptc_agent.agent.claude_code.machine_token import NO_PERL, STREAM_BOOT
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher, kill_if_cli, remote_session_path
from ptc_agent.core.sandbox.providers._ssh_transport import _CLI_SLOTS, _COMMAND_SLOTS, Machine
from src.llms.claude_code import ClaudeTurnFailed
from tests.unit.ptc_agent.agent.claude_code.bsd_userland import bsd_path, on_linux
from tests.unit.ptc_agent.agent.claude_code.local_link import local_link

pytestmark = [
    pytest.mark.macos_scripts,
    pytest.mark.skipif(not (shutil.which("perl") and shutil.which("bash")), reason="the machine side needs perl and bash"),
]

TOKEN = "sk-ant-oat01-" + "7" * 80
CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"
STORED = "sk-ant-oat01-" + "5" * 80


async def _no_link():  # pragma: no cover - never called: nothing here connects
    raise AssertionError


def _machine_at(root) -> Machine:
    return Machine(_no_link, root if callable(root) else lambda: root)


def _launcher(relay: bool) -> SSHLauncher:
    async def stored() -> str:  # pragma: no cover - the transport calls it, not the script
        return STORED

    return SSHLauncher(_machine_at("/m/root"), identity="u", token_file=CID, relay=stored if relay else None,
                       env={"ANTHROPIC_BASE_URL": "http://api.test"})


def _machine(tmp_path: Path, token: str | None = TOKEN, mode: int = 0o600) -> dict[str, str]:
    """A home holding the machine's token file, and the environment sshd would give a command there."""
    home = tmp_path / "home"
    (home / ".config" / "langalpha" / "claude-code").mkdir(parents=True)
    if token is not None:
        path = home / ".config" / "langalpha" / "claude-code" / CID
        path.write_text(token + "\n")
        path.chmod(mode)
    return {"HOME": str(home), "PATH": os.environ["PATH"], "USER": "dev", "LANG": "C.UTF-8",
            "SSH_CONNECTION": "10.0.0.1 1 10.0.0.2 22", "MACHINE_SECRET": "machine-only"}


def _run(tmp_path: Path, machine: dict[str, str], *, relay: bool, after: bytes = b"") -> tuple[str, dict]:
    """Run the launcher's command as sshd would; what the shim announced and what the stand-in CLI saw."""
    seen = tmp_path / "seen.json"
    cli = tmp_path / "claude"
    cli.write_text(
        f"#!{sys.executable}\nimport json, os, socket, sys\n"
        f"fd = os.environ.get({TOKEN_FD_ENV!r})\n"
        "token = socket.socket(fileno=int(fd)).recv(4096).decode().strip() if fd else None\n"
        f"json.dump({{'env': dict(os.environ), 'argv': sys.argv, 'token': token, 'stdin': sys.stdin.read()}}, open({str(seen)!r}, 'w'))\n"
    )
    cli.chmod(0o755)
    options = ClaudeAgentOptions(cli_path=str(cli), cwd=str(tmp_path),
                                 env={"CLAUDE_CONFIG_DIR": str(tmp_path / "cfg"), "ODD": "it's\na \"value\" $HOME"})
    launcher = _launcher(relay)
    script = launcher.command(options)
    assert "\n" not in script and TOKEN not in script and STORED not in script
    done = subprocess.run(["sh", "-c", STREAM_BOOT], input=script.encode() + b"\n" + after, env=machine,
                          capture_output=True, timeout=30)
    assert done.returncode == 0, done.stderr.decode()
    first, _, _ = done.stdout.decode().partition("\n")
    seen_by_cli = json.loads(seen.read_text())
    assert cli_env(options, launcher.env).items() <= seen_by_cli["env"].items(), "the local launcher's environment, every key"
    return first, seen_by_cli


def test_the_cli_gets_the_machines_token_through_a_socket_and_a_clean_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-platform-key")
    announced, seen = _run(tmp_path, _machine(tmp_path), relay=True, after=b'{"type":"user"}\n')
    assert announced == "langalpha-token file"
    assert seen["token"] == TOKEN
    assert seen["stdin"] == '{"type":"user"}\n'
    assert not any(TOKEN in v for v in [*seen["env"].values(), *seen["argv"]])
    runtime = {"CLAUDE_CONFIG_DIR", "ODD", "ANTHROPIC_BASE_URL", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_AGENT_SDK_VERSION",
               "CLAUDE_CODE_SDK_READS_SESSION_STATE", "PWD", TOKEN_FD_ENV}
    assert {k for k in seen["env"] if not k.startswith("__CF_")} <= {*_INHERITED, *runtime}, "macOS's own __CF_ keys aside"
    assert seen["env"]["ODD"] == "it's\na \"value\" $HOME" and seen["env"]["HOME"].endswith("/home")


def test_a_machine_without_a_token_is_relayed_one_line_and_the_cli_gets_the_rest_of_the_stream(tmp_path):
    announced, seen = _run(tmp_path, _machine(tmp_path, token=None), relay=True, after=STORED.encode() + b"\n{\"a\":1}\n")
    assert announced == "langalpha-token relay"
    assert seen["token"] == STORED and seen["stdin"] == '{"a":1}\n'
    assert not any(STORED in v for v in seen["env"].values())


def test_a_relayed_empty_line_or_no_relay_starts_the_cli_with_no_token(tmp_path):
    relayed_nothing = _run(tmp_path, _machine(tmp_path, token=None), relay=True, after=b"\n")
    off = _run(tmp_path / "off", _machine(tmp_path / "off", token=None), relay=False)
    assert relayed_nothing[0] == "langalpha-token relay" and off[0] == "langalpha-token none"
    for _, seen in (relayed_nothing, off):
        assert seen["token"] is None and TOKEN_FD_ENV not in seen["env"]


@pytest.mark.parametrize("how", ["symlink", "group-readable", "empty", "whitespace"])
def test_a_token_file_not_kept_privately_is_ignored(tmp_path, how):
    text = {"empty": "", "whitespace": " \t "}.get(how, TOKEN)
    machine = _machine(tmp_path, token=text, mode=0o640 if how == "group-readable" else 0o600)
    if how == "symlink":
        path = Path(machine["HOME"]) / ".config" / "langalpha" / "claude-code" / CID
        target = tmp_path / "elsewhere"
        path.rename(target)
        path.symlink_to(target)
    announced, seen = _run(tmp_path, machine, relay=False)
    assert announced == "langalpha-token none" and seen["token"] is None
    relayed, seen = _run(tmp_path, machine, relay=True, after=STORED.encode() + b"\n")
    assert relayed == "langalpha-token relay" and seen["token"] == STORED


def test_the_session_file_is_keyed_by_the_machines_path_not_this_workers():
    path = remote_session_path("/m/cfg", "/home/dev/langalpha/_internal/claude-code/threads/t1", "s1")
    assert path == "/m/cfg/projects/-home-dev-langalpha--internal-claude-code-threads-t1/s1.jsonl"



def test_the_base_is_under_the_root_read_once_the_connection_is_resolved():
    launcher = SSHLauncher(_machine_at("/m/root"), identity="u", token_file=CID)
    with pytest.raises(RuntimeError):
        launcher.base


class _ManagedLink:
    """A link whose machine answers the managed-sources test, naming ``managed``."""

    target = SimpleNamespace(host="studio")

    def __init__(self, order: list[str], *, managed: str = "", perl: bool = True, token: bool = False) -> None:
        self.order = order
        self.managed = managed
        self.perl = perl
        self.token = token

    async def run(self, script: str, timeout: float) -> SimpleNamespace:
        self.order.append("managed")
        out = f"{self.managed}\n{int(self.token)}\n{'perl' if self.perl else 'none'}\n"
        return SimpleNamespace(exit_code=0, stdout=out.encode(), stderr=b"")

    def __str__(self) -> str:
        return "the-link"


@pytest.mark.asyncio
@pytest.mark.parametrize(("managed", "sources"), [
    ("settings", ("settings",)), ("dropins", ("dropins",)), ("mcp", ("mcp",)), ("profile", ("profile",)),
    ("settings dropins mcp profile", ("settings", "dropins", "mcp", "profile")),
])
async def test_a_machine_with_any_managed_source_is_refused_before_the_cli_is_looked_for(monkeypatch, managed, sources):
    order: list[str] = []
    the_link = _ManagedLink(order, managed=managed, perl=False)

    async def link():
        return the_link

    async def ensure_cli(conn, at, *, install):
        order.append("cli")
        return f"{at}/_internal/claude-code/cli/claude"

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)
    on_blocked = AsyncMock()
    launcher = SSHLauncher(Machine(link, lambda: "/m/ready"), identity="u", token_file=CID, on_blocked=on_blocked)
    with pytest.raises(ClaudeTurnFailed) as failed:
        await launcher.ready()
    assert failed.value.kind == "policy_blocked" and str(list(sources)) in failed.value.detail
    assert order == ["managed"]
    on_blocked.assert_awaited_once_with(False)


@pytest.mark.asyncio
async def test_a_machine_without_perl_is_refused_before_the_cli_is_looked_for(monkeypatch):
    order: list[str] = []
    the_link = _ManagedLink(order, perl=False, token=True)

    async def link():
        return the_link

    async def ensure_cli(conn, at, *, install):
        order.append("cli")
        return f"{at}/_internal/claude-code/cli/claude"

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)
    on_no_perl = AsyncMock()
    launcher = SSHLauncher(Machine(link, lambda: "/m/ready"), identity="u", token_file=CID, on_no_perl=on_no_perl)
    with pytest.raises(ClaudeTurnFailed, match=re.escape(NO_PERL)) as refused:
        await launcher.ready()
    assert refused.value.kind == "needs_perl", "refused as admission refuses it"
    assert order == ["managed"]
    assert on_no_perl.await_args_list == [((True,),)], "so a check that found perl there stops admitting turns"


@pytest.mark.asyncio
async def test_where_a_runs_token_came_from_is_told(monkeypatch):
    the_link = _ManagedLink([])

    async def link():
        return the_link

    async def ensure_cli(conn, at, *, install):
        return f"{at}/_internal/claude-code/cli/claude"

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)
    on_announced = AsyncMock()
    launcher = SSHLauncher(Machine(link, lambda: "/m/ready"), identity="u", token_file=CID, on_announced=on_announced)
    await launcher.ready()
    await launcher.started("none")
    on_announced.assert_awaited_once_with("none")
    assert launcher.source == "none"


@pytest.mark.asyncio
async def test_the_root_is_read_only_once_the_connection_is_resolved_and_the_cli_found_as_asked(monkeypatch):
    order: list[str] = []
    root = {"now": "pending"}

    the_link = _ManagedLink(order)

    async def link():
        order.append("link")
        root["now"] = "/m/ready"
        return the_link

    def read_root() -> str:
        order.append("root")
        return root["now"]

    async def ensure_cli(conn, at, *, install):
        order.append(f"cli {conn} {at} install={install}")
        return f"{at}/_internal/claude-code/cli/claude"

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)
    launcher = SSHLauncher(Machine(link, read_root), identity="u", token_file=CID, install=False)
    assert order == []
    assert await launcher.ready() is the_link
    assert order == ["link", "root", "managed", "cli the-link /m/ready install=False"]
    assert launcher.base == "/m/ready/_internal/claude-code"


@pytest.mark.asyncio
async def test_a_root_that_is_not_absolute_is_refused_when_read():
    async def link():
        return "the-link"

    launcher = SSHLauncher(Machine(link, lambda: "~/langalpha"), identity="u", token_file=CID)
    with pytest.raises(ValueError, match="not an absolute path"):
        await launcher.ready()


def _local(tmp_path, monkeypatch, cli: str = "/bin/true", path: str | None = None):
    """A launcher whose machine is this one, over a local stand-in connection."""
    link = local_link(path, HOME=str(tmp_path))

    async def connect():
        return link

    async def ensure_cli(conn, at, *, install):
        return cli

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)
    return link, lambda **kw: SSHLauncher(Machine(connect, lambda: str(tmp_path / "root")), identity="u", token_file=CID, **kw)


@pytest.mark.asyncio
async def test_release_removes_the_turns_config_dir_and_keeps_the_threads(tmp_path, monkeypatch):
    """The thread's dir stays, empty or not: the thread's next turn may be in it already."""
    link, launcher = _local(tmp_path, monkeypatch)
    one = launcher()
    prepared = await one._lay_out(await one.ready(), "t1")
    await one.release(prepared)
    assert not Path(prepared.config_dir).exists() and Path(prepared.cwd).is_dir()


async def _started(launcher: SSHLauncher) -> Any:
    prepared = await launcher._lay_out(await launcher.ready(), None)
    transport = launcher.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    await transport.connect()
    return transport


@pytest.mark.asyncio
async def test_clis_at_their_cap_still_leave_commands_a_slot_and_keep_the_link_until_they_end(tmp_path, monkeypatch):
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    link, launcher = _local(tmp_path, monkeypatch, str(cli))
    running = [await _started(launcher()) for _ in range(_CLI_SLOTS)]
    with pytest.raises(CLIConnectionError, match=f"^{re.escape(ssh_launcher.CLI_SLOTS_FULL)}$"):
        await _started(launcher(slot_wait_s=1))
    done = await link.run("echo free", timeout=10, slot=link.slots)
    assert done.stdout == b"free\n"
    link.drain()
    assert not link.conn.closed, "a link shed from the pool waits for its turns to end"
    for transport in running:
        await transport.close()
    assert link.active == 0 and link.conn.closed


@pytest.mark.asyncio
async def test_a_turn_waits_for_a_cli_slot_past_its_start_time_and_starts_once_one_frees(tmp_path, monkeypatch):
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    link, launcher = _local(tmp_path, monkeypatch, str(cli))
    monkeypatch.setattr(ssh_launcher, "START_TIMEOUT_S", 1)
    running = [await _started(launcher()) for _ in range(_CLI_SLOTS)]
    waiting = asyncio.ensure_future(_started(launcher()))
    await asyncio.sleep(2)
    assert not waiting.done(), "still waiting for a slot, past the start's own time"
    await running.pop().close()
    running.append(await asyncio.wait_for(waiting, 10))
    for transport in running:
        await transport.close()


@pytest.mark.asyncio
async def test_a_turn_waits_out_commands_holding_every_command_slot_past_its_start_time(tmp_path, monkeypatch):
    """Commands filling the command slots are a wait under the turn's slot
    budget: it starts once one frees, and one that never frees refuses the
    turn as busy, giving back the CLI slot it took."""
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    link, launcher = _local(tmp_path, monkeypatch, str(cli))
    monkeypatch.setattr(ssh_launcher, "START_TIMEOUT_S", 1)
    first = await _started(launcher(slot_wait_s=1))
    for _ in range(_COMMAND_SLOTS - 1):
        await link.slots.acquire()

    with pytest.raises(CLIConnectionError, match=f"^{re.escape(ssh_launcher.COMMAND_SLOTS_FULL)}$"):
        await _started(launcher(slot_wait_s=1))
    assert link.cli_slots._value == _CLI_SLOTS - 1

    waiting = asyncio.ensure_future(_started(launcher(slot_wait_s=10)))
    await asyncio.sleep(2)
    assert not waiting.done(), "still waiting for a command slot, past the start's own time"
    link.slots.release()
    transport = await asyncio.wait_for(waiting, 10)
    await transport.close()
    await asyncio.sleep(0.1)
    assert link.slots._value == 1 and link.cli_slots._value == _CLI_SLOTS - 1
    await first.close()


@pytest.mark.parametrize("named", ["cli", "other"])
def test_the_cli_is_signalled_only_while_its_pid_still_runs_it(tmp_path, named):
    cli = tmp_path / "bin dir" / "claude"
    cli.parent.mkdir()
    shutil.copy(shutil.which("sleep"), cli)
    proc = subprocess.Popen([str(cli), "30"])
    try:
        asked = str(cli) if named == "cli" else str(tmp_path / "claude")
        subprocess.run(["sh", "-c", kill_if_cli(proc.pid, asked, "KILL")], check=True, timeout=10)
        if named == "cli":
            assert proc.wait(timeout=5) == -9
        else:
            with pytest.raises(subprocess.TimeoutExpired):
                proc.wait(timeout=0.5)
    finally:
        proc.kill()
        proc.wait()


@on_linux
@pytest.mark.asyncio
async def test_a_turns_layout_and_its_release_run_on_a_macs_userland(tmp_path, monkeypatch):
    link, launcher = _local(tmp_path, monkeypatch, path=bsd_path(tmp_path))
    one = launcher()
    prepared = await one._lay_out(await one.ready(), "t1")
    assert Path(prepared.config_dir).is_dir() and prepared.cwd == str(Path(tmp_path / "root/_internal/claude-code/threads/t1").resolve())
    stale = Path(tmp_path / "root/_internal/claude-code/config/old")
    stale.mkdir()
    os.utime(stale, (0, 0))
    again = await one._lay_out(link, "t1")
    assert not stale.exists(), "a config dir a lost connection left is swept"
    for each in (prepared, again):
        await one.release(each)
        assert not Path(each.config_dir).exists()


@on_linux
@pytest.mark.parametrize("named", ["cli", "other"])
def test_the_cli_is_signalled_by_ps_where_there_is_no_proc(tmp_path, named):
    """A Mac has no /proc, so the pid's command line comes from its ps."""
    cli = tmp_path / "bin dir" / "claude"
    cli.parent.mkdir()
    shutil.copy(shutil.which("sleep"), cli)
    proc = subprocess.Popen([str(cli), "30"])
    try:
        asked = str(cli) if named == "cli" else str(tmp_path / "claude")
        script = kill_if_cli(proc.pid, asked, "KILL").replace("/proc/", "/no-such-proc/")
        subprocess.run(["sh", "-c", script], check=True, timeout=10, env={"PATH": bsd_path(tmp_path)})
        if named == "cli":
            assert proc.wait(timeout=5) == -9
        else:
            with pytest.raises(subprocess.TimeoutExpired):
                proc.wait(timeout=0.5)
    finally:
        proc.kill()
        proc.wait()
