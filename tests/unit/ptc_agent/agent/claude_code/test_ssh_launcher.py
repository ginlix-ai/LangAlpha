"""SSHLauncher's remote command, run here: the login shell's boot, the shim and a stand-in CLI.

The script the launcher sends is the one a machine runs; here ``sh`` stands
in for the login shell and ``HOME`` for the machine's home, so the token
hand-off, the environment and the relay handshake are checked without an
sshd (``tests/integration/claude_code`` runs the same over SSH).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from claude_agent_sdk import ClaudeAgentOptions

from ptc_agent.agent.claude_code import detach, ssh_launcher
from ptc_agent.agent.claude_code.launcher import _INHERITED, cli_env
from ptc_agent.agent.claude_code.machine_token import TOKEN_FD_ENV
from ptc_agent.agent.claude_code.machine_token import NO_PERL, STREAM_BOOT
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher, SSHTransport, kill_if_cli, remote_session_path
from ptc_agent.core.sandbox.providers._ssh_transport import _COMMAND_SLOTS, CLI_SLOTS, Machine, SlotWaitTimeout
from claude_agent_sdk import CLIConnectionError

from src.llms.claude_code import COMPUTER_LOST, ClaudeTurnFailed
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
    store = SimpleNamespace(read=AsyncMock(), refused=AsyncMock())  # the transport reads it, not the script
    return SSHLauncher(_machine_at("/m/root"), identity="u", token_file=CID, credential=store if relay else None,
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


def _relayed(secret: str) -> bytes:
    """The line the transport relays a token on."""
    return f"{secret}\n".encode()


def _run(tmp_path: Path, machine: dict[str, str], *, relay: bool, after: bytes = b"") -> tuple[str, dict]:
    """Run the launcher's command as sshd would, ending the CLI's input; what
    the shim announced first in the CLI's stdout and what the stand-in CLI saw."""
    seen = tmp_path / "seen.json"
    shutil.rmtree(tmp_path / "cfg", ignore_errors=True)
    (tmp_path / "cfg").mkdir()
    cli = tmp_path / "claude"
    cli.write_text(
        f"#!{sys.executable}\nimport json, os, socket, sys\n"
        f"var = {TOKEN_FD_ENV!r} if {TOKEN_FD_ENV!r} in os.environ else None\n"
        "token = socket.socket(fileno=int(os.environ[var])).recv(4096).decode().strip() if var else None\n"
        f"json.dump({{'env': dict(os.environ), 'argv': sys.argv, 'token': token, 'var': var, 'stdin': sys.stdin.read()}}, open({str(seen)!r}, 'w'))\n"
    )
    cli.chmod(0o755)
    options = ClaudeAgentOptions(cli_path=str(cli), cwd=str(tmp_path),
                                 env={"CLAUDE_CONFIG_DIR": str(tmp_path / "cfg"), "ODD": "it's\na \"value\" $HOME"})
    launcher = _launcher(relay)
    script = launcher.command(options)
    assert "\n" not in script and TOKEN not in script and STORED not in script
    done = subprocess.run(["sh", "-c", STREAM_BOOT], input=script.encode() + b"\n" + after + b"langalpha-eof\n",
                          env=machine, capture_output=True, timeout=30)
    assert done.returncode == 0, done.stderr.decode()
    assert done.stdout.startswith(b"langalpha-attach 0\n")
    assert _ended(tmp_path / "cfg") == "x 0"
    first, _, _ = (tmp_path / "cfg" / "attach" / "out").read_text().partition("\n")
    seen_by_cli = json.loads(seen.read_text())
    assert cli_env(options, launcher.env).items() <= seen_by_cli["env"].items(), "the local launcher's environment, every key"
    return first, seen_by_cli


def _ended(config_dir: Path, wait_s: float = 30) -> str:
    """How the CLI under the supervisor in ``config_dir`` ended, once it has."""
    rc = config_dir / "attach" / "rc"
    deadline = time.monotonic() + wait_s
    while not rc.exists():
        assert time.monotonic() < deadline, "the CLI never ended"
        time.sleep(0.05)
    return rc.read_text().strip()


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
    announced, seen = _run(tmp_path, _machine(tmp_path, token=None), relay=True, after=_relayed(STORED) + b"{\"a\":1}\n")
    assert announced == "langalpha-token relay"
    assert (seen["token"], seen["var"], seen["stdin"]) == (STORED, TOKEN_FD_ENV, '{"a":1}\n')
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
    relayed, seen = _run(tmp_path, machine, relay=True, after=_relayed(STORED))
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

    host = "studio"

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


def _clis_full(n: int) -> str:
    return ClaudeTurnFailed.TEMPLATES["clis_full"].format(n=n)


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
    running = [await _started(launcher()) for _ in range(CLI_SLOTS)]
    with pytest.raises(ClaudeTurnFailed, match=f"^{re.escape(_clis_full(CLI_SLOTS))}$"):
        await _started(launcher(slot_wait_s=0.2))
    done = await link.run("echo free", timeout=10, slot=link.slots)
    assert done.stdout == b"free\n"
    link.drain()
    assert not link.conn.closed, "a link shed from the pool waits for its turns to end"
    for transport in running:
        await transport.close()
    assert link.active == 0 and link.conn.closed


@pytest.mark.asyncio
async def test_clis_past_one_connections_slots_run_on_the_second_and_are_signalled_there(tmp_path, monkeypatch):
    """The first connection keeps its two spare slots for commands; once
    both connections' CLI slots are taken the next CLI is refused naming
    them. A CLI on the second connection is signalled over it, so losing the
    first never leaves it running."""
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    link, launcher = _local(tmp_path, monkeypatch, str(cli))
    second = local_link(HOME=str(tmp_path))
    dials = []

    async def spill(want):
        dials.append(want)
        return second

    link.spill = spill
    link.peers = lambda: [second] if dials else []
    running = [await _started(launcher(slot_wait_s=5)) for _ in range(2 * CLI_SLOTS)]
    assert (link.cli_slots._value, second.cli_slots._value) == (0, 0)
    assert dials == ["cli"] and link.slots._value == 2
    with pytest.raises(ClaudeTurnFailed, match=f"^{re.escape(_clis_full(2 * CLI_SLOTS))}$"):
        await _started(launcher(slot_wait_s=0.2))
    done = await link.run("echo free", timeout=10, slot=link.slots)
    assert done.stdout == b"free\n"

    spilled = running[-1]
    kills = []
    real_run = second.run

    async def run(script, **kw):
        if kw.get("kill"):
            kills.append(script)
        return await real_run(script, **kw)

    monkeypatch.setattr(second, "run", run)
    link.retired = True
    spilled._pid = 4242
    await spilled._signal("TERM")
    assert len(kills) == 1 and "4242" in kills[0]
    link.retired = False
    for transport in running:
        await transport.close()
    await asyncio.sleep(0.1)
    assert (link.cli_slots._value, second.cli_slots._value) == (CLI_SLOTS, CLI_SLOTS)


@pytest.mark.asyncio
async def test_a_run_holds_one_of_the_computers_clis_from_its_layout_to_its_release(tmp_path, monkeypatch):
    """Taken before anything runs on the machine, with the whole wait, so a
    turn on a full computer waits where its queue is told, not behind the
    running turns' commands; one none frees in time refuses the run naming
    the computer's count, and a layout that fails gives it back."""
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    link, launcher = _local(tmp_path, monkeypatch, str(cli))
    held = []

    @contextlib.asynccontextmanager
    async def slot(wait_s):
        if held:
            raise SlotWaitTimeout("cli", 3)
        held.append(wait_s)
        try:
            yield
        finally:
            held.clear()

    async def written(self, link, prepared, files):
        pass

    # The stand-in connection has no SFTP; the layout's files are not what this is about.
    monkeypatch.setattr(SSHLauncher, "_write", written)
    one = launcher(cli_slot=slot, slot_wait_s=7)
    prepared = await one.scratch()
    assert held == [7] and link.cli_slots._value == CLI_SLOTS
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    await transport.connect()
    assert link.cli_slots._value == CLI_SLOTS - 1
    with pytest.raises(ClaudeTurnFailed, match=f"^{re.escape(_clis_full(3))}$"):
        await launcher(cli_slot=slot).scratch()
    await transport.close()
    assert held == [7] and link.cli_slots._value == CLI_SLOTS
    await one.release(prepared)
    assert held == []

    async def unreachable(conn, at, *, install):
        raise RuntimeError("no CLI")

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", unreachable)
    with pytest.raises(RuntimeError, match="no CLI"):
        await launcher(cli_slot=slot).scratch()
    assert held == []


@pytest.mark.asyncio
async def test_a_turn_waits_for_a_cli_slot_past_its_start_time_and_starts_once_one_frees(tmp_path, monkeypatch):
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    link, launcher = _local(tmp_path, monkeypatch, str(cli))
    monkeypatch.setattr(ssh_launcher, "START_TIMEOUT_S", 0.5)
    running = [await _started(launcher()) for _ in range(CLI_SLOTS)]
    waiting = asyncio.ensure_future(_started(launcher()))
    await asyncio.sleep(1)
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
    monkeypatch.setattr(ssh_launcher, "START_TIMEOUT_S", 0.5)
    first = await _started(launcher(slot_wait_s=1))
    for _ in range(_COMMAND_SLOTS - 1):
        await link.slots.acquire()

    with pytest.raises(ClaudeTurnFailed, match=f"^{re.escape(ClaudeTurnFailed.TEMPLATES["commands_full"])}$"):
        await _started(launcher(slot_wait_s=0.2))
    assert link.cli_slots._value == CLI_SLOTS - 1

    waiting = asyncio.ensure_future(_started(launcher(slot_wait_s=10)))
    await asyncio.sleep(1)
    assert not waiting.done(), "still waiting for a command slot, past the start's own time"
    link.slots.release()
    transport = await asyncio.wait_for(waiting, 10)
    await transport.close()
    await asyncio.sleep(0.1)
    assert link.slots._value == 1 and link.cli_slots._value == CLI_SLOTS - 1
    await first.close()


def _running_cli(tmp_path) -> tuple[Path, subprocess.Popen]:
    """A process whose command line starts with a path named ``claude``.

    A copy of ``sleep`` would not do: a multi-call coreutils (uutils) picks
    its program from that name and exits, and macOS kills a copied system
    binary as it starts. Nor would a link to a framework Python (Homebrew's),
    which execs its app bundle and is listed by that name. A link to bash
    runs under the link's name, and waits in a builtin ``read`` with no child
    to exec into; the line it prints says it got that far."""
    cli = tmp_path / "bin dir" / "claude"
    cli.parent.mkdir()
    cli.symlink_to(shutil.which("bash"))
    proc = subprocess.Popen(
        [str(cli), "-c", "echo; read -r x"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        # A startup file it names would run before the command.
        env={k: v for k, v in os.environ.items() if k != "BASH_ENV"},
    )
    assert proc.stdout.readline() == b"\n"
    return cli, proc


@pytest.mark.parametrize("named", ["cli", "other"])
def test_the_cli_is_signalled_only_while_its_pid_still_runs_it(tmp_path, named):
    cli, proc = _running_cli(tmp_path)
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
        proc.communicate()


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
    cli, proc = _running_cli(tmp_path)
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
        proc.communicate()


@pytest.mark.asyncio
async def test_a_cli_starting_fences_the_computers_reports_before_it_reads_the_token_file(tmp_path, monkeypatch):
    cli = tmp_path / "claude"
    cli.write_text(f"#!{sys.executable}\nimport sys\nsys.stdin.read()\n")
    cli.chmod(0o755)
    _, launcher = _local(tmp_path, monkeypatch, str(cli))
    plan = SimpleNamespace(launching=AsyncMock())
    transport = await _started(launcher(plan=plan))
    await transport.close()
    plan.launching.assert_awaited_once()


def _echo_cli(tmp_path: Path) -> Path:
    """A stand-in CLI that answers each line of input with a message carrying it, and ends with its input."""
    cli = tmp_path / "claude"
    cli.write_text(
        f"#!{sys.executable}\nimport json, sys\n"
        "for line in sys.stdin:\n"
        "    print(json.dumps({'type': 'echo', 'line': json.loads(line)}), flush=True)\n"
    )
    cli.chmod(0o755)
    return cli


def _drop(transport: Any) -> None:
    """Lose the channel as a dropped connection does. Its process ends with a
    status of its own, as a cloud stream's does (-1), but never says the CLI
    ended, which only its last line does."""
    transport._process.proc.kill()


async def _next(messages: Any) -> dict[str, Any]:
    return await asyncio.wait_for(anext(messages), 20)


@pytest.mark.asyncio
@pytest.mark.parametrize("acks", [True, False], ids=["acked", "unacked"])
async def test_a_lost_connection_is_reattached_and_the_cli_misses_nothing(tmp_path, monkeypatch, acks):
    """The CLI runs on under its supervisor; a new channel attaches where the
    last one stopped reading and is sent again only the input the supervisor
    did not take, whether or not its acks reached the transport, so the SDK
    reads one unbroken stream and the CLI each line once."""
    if not acks:
        heard = SSHTransport._stderr_line
        monkeypatch.setattr(
            SSHTransport, "_stderr_line",
            lambda self, line, **kw: None if line.startswith(detach.TAKEN_MARK) else heard(self, line, **kw),
        )
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    transport = await _started(launcher())
    messages = transport.read_messages()
    await transport.write('{"n": 1}\n')
    assert (await _next(messages))["line"] == {"n": 1}
    _drop(transport)
    await transport.write('{"n": 2}\n')
    assert (await _next(messages))["line"] == {"n": 2}
    _drop(transport)
    await asyncio.sleep(0.2)
    await transport.write('{"n": 3}\n')
    assert (await _next(messages))["line"] == {"n": 3}
    await transport.end_input()
    with pytest.raises(StopAsyncIteration):
        await _next(messages)
    await transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("resumes", [True, False], ids=["resumed", "failed"])
async def test_a_machine_that_does_not_answer_again_ends_the_read_and_its_cli_ends_on_its_own(tmp_path, monkeypatch, resumes):
    """Past the wait the read ends the turn, stopped to be resumed where the
    launcher says, else as a lost connection, and the supervisor ends the
    CLI once nothing has attached for its grace, saying so."""
    monkeypatch.setattr(ssh_launcher, "RECONNECT_WAIT_S", 1)
    monkeypatch.setattr(ssh_launcher, "DETACHED_GRACE_S", 1)
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    one = launcher(resumes=resumes)
    prepared = await one._lay_out(await one.ready(), None)
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    await transport.connect()
    messages = transport.read_messages()

    async def silent():
        raise ConnectionRefusedError("the tunnel is down")

    one.machine = Machine(silent, one.machine.root)
    _drop(transport)
    with pytest.raises(ClaudeTurnFailed if resumes else CLIConnectionError, match="Lost the connection to local") as lost:
        await _next(messages)
    assert getattr(lost.value, "kind", None) == (COMPUTER_LOST if resumes else None)
    assert await asyncio.to_thread(_ended, Path(prepared.config_dir)) == detach.GRACE_KILLED
    await transport.close()


@pytest.mark.asyncio
async def test_a_cli_gone_from_the_machine_fails_the_read_at_once(tmp_path, monkeypatch):
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    one = launcher(resumes=True)
    prepared = await one._lay_out(await one.ready(), None)
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    await transport.connect()
    messages = transport.read_messages()
    shutil.rmtree(Path(prepared.config_dir) / "attach")
    _drop(transport)
    started = time.monotonic()
    with pytest.raises(ClaudeTurnFailed, match="no longer running") as gone:
        await _next(messages)
    assert gone.value.kind == COMPUTER_LOST
    assert time.monotonic() - started < ssh_launcher.RECONNECT_WAIT_S
    closing = time.monotonic()
    await transport.close()
    assert time.monotonic() - closing < ssh_launcher.EXIT_GRACE_S, "a pid no longer the CLI's is never signalled"


def _noisy_cli(tmp_path: Path) -> Path:
    """A stand-in CLI that answers each line of input on stdout, with a line
    of stderr before the answer and another a moment after it."""
    cli = tmp_path / "claude"
    cli.write_text(
        f"#!{sys.executable}\nimport json, sys, time\n"
        "for line in sys.stdin:\n"
        "    n = json.loads(line)['n']\n"
        "    print(f'said {n}', file=sys.stderr, flush=True)\n"
        "    print(json.dumps({'type': 'echo', 'n': n}), flush=True)\n"
        "    time.sleep(0.2)\n"
        "    print(f'after {n}', file=sys.stderr, flush=True)\n"
    )
    cli.chmod(0o755)
    return cli


@pytest.mark.asyncio
async def test_a_reattach_relays_each_line_of_the_clis_stderr_once(tmp_path, monkeypatch):
    """The stderr a new channel replays starts at the first line the last one
    did not finish, however that channel ended, including the lines the CLI
    wrote with no channel attached: no line is skipped or seen twice."""
    heard: list[str] = []
    link, launcher = _local(tmp_path, monkeypatch, str(_noisy_cli(tmp_path)))
    one = launcher()
    prepared = await one._lay_out(await one.ready(), None)

    async def slow():
        await asyncio.sleep(0.5)
        return link

    one.machine = Machine(slow, one.machine.root)
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir},
                                                 stderr=heard.append))
    await transport.connect()
    messages = transport.read_messages()
    for n in range(1, 4):
        await transport.write(json.dumps({"n": n}) + "\n")
        assert (await _next(messages))["n"] == n
        _drop(transport)
    await transport.write(json.dumps({"n": 4}) + "\n")
    assert (await _next(messages))["n"] == 4
    await transport.end_input()
    with pytest.raises(StopAsyncIteration):
        await _next(messages)
    await transport.close()
    assert [line for line in heard if line.startswith(("said", "after"))] == [
        f"{word} {n}" for n in range(1, 5) for word in ("said", "after")
    ]


@pytest.mark.asyncio
async def test_closing_a_turn_with_no_channel_attached_ends_its_cli(tmp_path, monkeypatch):
    """A stop that lands while the transport reattaches still ends the CLI,
    by its pid over a new connection, rather than leaving it to the
    supervisor's grace. The stand-in runs again under its own name, since
    the CLI is signalled only while its command line starts with its path;
    a framework Python would be listed as its app bundle instead."""
    monkeypatch.setattr(ssh_launcher, "EXIT_GRACE_S", 1)
    cli = tmp_path / "claude"
    bash = shutil.which("bash")
    cli.write_text(
        f"#!{bash}\n"
        f'if [ -z "${{NAMED:-}}" ]; then NAMED=1 exec -a "$0" {bash} "$0" "$@"; fi\n'
        "while read -r _; do :; done\n"
    )
    cli.chmod(0o755)
    _, launcher = _local(tmp_path, monkeypatch, str(cli))
    one = launcher()
    prepared = await one._lay_out(await one.ready(), None)
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    await transport.connect()
    deadline = time.monotonic() + 15

    def named() -> bool:
        ps = subprocess.run(["ps", "-ww", "-o", "args=", "-p", str(transport._pid)], capture_output=True, text=True)
        return ps.stdout.startswith(str(cli))

    while transport._pid is None or not await asyncio.to_thread(named):
        assert time.monotonic() < deadline, "the CLI never named itself"
        await asyncio.sleep(0.1)
    await transport._detach()
    transport._cli_link.retired = True
    fresh, dials = local_link(None, HOME=str(tmp_path)), []

    async def dial():
        dials.append(fresh)
        return fresh

    one.machine = Machine(dial, one.machine.root)
    await transport.close()
    assert await asyncio.to_thread(_ended, Path(prepared.config_dir), 10) == "s 15"
    assert dials and transport._cli_link is fresh, "the signal went over a new connection"


class _SentOnDrain:
    """A channel's stdin that sends only on a drain, as a cloud stream's does (``sandbox_launcher._Input``)."""

    def __init__(self, writer: Any) -> None:
        self._writer = writer
        self._queued = bytearray()

    def write(self, data: bytes) -> None:
        self._queued += data

    async def drain(self) -> None:
        data, self._queued = bytes(self._queued), bytearray()
        self._writer.write(data)
        await self._writer.drain()


@pytest.mark.asyncio
async def test_a_reattach_sends_what_it_queued_on_a_channel_that_sends_only_on_a_drain(tmp_path, monkeypatch):
    """Input the supervisor never took goes again on the new channel even
    where nothing leaves without a drain, rather than waiting on it forever."""
    started = SSHTransport._start

    async def start(self, hold, script):
        await started(self, hold, script)
        self._process.stdin = _SentOnDrain(self._process.stdin)

    monkeypatch.setattr(SSHTransport, "_start", start)
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    transport = await _started(launcher())
    messages = transport.read_messages()
    await transport.write('{"n": 1}\n')
    assert (await _next(messages))["line"] == {"n": 1}
    transport._live = False
    _drop(transport)
    await transport.write('{"n": 2}\n')
    assert (await _next(messages))["line"] == {"n": 2}
    await transport.end_input()
    with pytest.raises(StopAsyncIteration):
        await _next(messages)
    await transport.close()


@pytest.mark.asyncio
async def test_a_wait_asks_the_machine_on_the_clis_connection_and_closes_one_that_does_not_answer(tmp_path, monkeypatch):
    """A channel let go is not answering; a connection busy with kills is,
    however long they hold it past the probe's own time; one that fails to
    answer is closed with its channel, so the reattach dials a new one
    rather than the stale one."""
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    transport = await _started(launcher())
    link = transport._cli_link
    closed = []
    monkeypatch.setattr(link, "close", lambda: closed.append(True), raising=False)
    assert await transport._answers() is True
    grace = ssh_launcher.EXIT_GRACE_S
    monkeypatch.setattr(ssh_launcher, "EXIT_GRACE_S", 0.1)

    async def kill_running(s: float) -> None:
        async with link.kill_slot:
            await asyncio.sleep(s)

    killing = asyncio.ensure_future(kill_running(0.5))
    await asyncio.sleep(0)
    assert await asyncio.wait_for(transport._answers(), 5) is True, "a kill holding the slot past the probe's time is waited out"
    await killing
    monkeypatch.setattr(ssh_launcher, "EXIT_GRACE_S", grace)
    monkeypatch.setattr(ssh_launcher, "RECONNECT_WAIT_S", 0.2)
    async with link.kill_slot:
        assert await transport._answers() is True, "a connection its kills keep busy is one that answers"
    assert closed == [] and transport._reach.up.is_set()
    monkeypatch.setattr(link, "run", AsyncMock(side_effect=TimeoutError()))
    assert await transport._answers() is False
    assert closed == [True] and not transport._reach.up.is_set()
    monkeypatch.setattr(link, "run", AsyncMock())
    transport._live = False
    assert await transport._answers() is False, "a channel let go is about to reattach, however the machine answers"
    await transport.close()


def test_a_reattach_sends_again_only_the_input_the_supervisor_has_not_taken():
    ledger = detach.InputLedger()
    ledger.add(b"abc")
    ledger.add(b"def")
    assert not ledger.takes(7), "a count past what was sent is the protocol broken"
    ledger.ack(2)
    assert (bytes(ledger.unsent), ledger.start) == (b"cdef", 2)
    ledger.ack(1)
    assert (bytes(ledger.unsent), ledger.start) == (b"cdef", 2), "an older count takes nothing back"
    ledger.ack(6)
    assert (bytes(ledger.unsent), ledger.start) == (b"", 6)


def test_a_machine_not_answering_is_told_however_deep_its_cause():
    try:
        try:
            try:
                raise ConnectionResetError("reset")
            except OSError as e:
                raise RuntimeError("the channel failed") from e
        except RuntimeError as e:
            raise CLIConnectionError("lost") from e
    except CLIConnectionError as e:
        assert detach.unanswered(e)
    try:
        try:
            raise ConnectionResetError("reset")
        except OSError:
            raise ValueError("a refusal raised while handling it")
    except ValueError as e:
        assert not detach.unanswered(e), "only what it was raised from counts, not what was being handled"


@pytest.mark.asyncio
async def test_a_reattach_whose_channel_ends_before_it_attaches_dials_again(tmp_path, monkeypatch):
    """A channel the machine ends before the supervisor says it attached, as
    a login cut short does, is dialed again within the wait."""
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    transport = await _started(launcher())
    messages = transport.read_messages()
    attach, tries = detach.attach_command, []

    def cut_short_once(*args: Any) -> str:
        tries.append(args)
        return "exit 3" if len(tries) == 1 else attach(*args)

    monkeypatch.setattr(detach, "attach_command", cut_short_once)
    _drop(transport)
    await transport.write('{"n": 1}\n')
    assert (await _next(messages))["line"] == {"n": 1}
    assert len(tries) == 2
    await transport.end_input()
    with pytest.raises(StopAsyncIteration):
        await _next(messages)
    await transport.close()


@pytest.mark.asyncio
async def test_a_protocol_line_cut_short_changes_nothing(tmp_path, monkeypatch):
    """A channel lost mid-line leaves a fragment, which must not read as the
    CLI attached or ended."""
    _, launcher = _local(tmp_path, monkeypatch)
    one = launcher()
    prepared = await one._lay_out(await one.ready(), None)
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    transport._stderr_line(detach.ATTACH_MARK, whole=False)
    assert not transport._err_attached
    transport._stderr_line(detach.ATTACH_MARK)
    transport._stderr_line(detach.EXIT_MARK + "x 0", whole=False)
    assert transport._exit is None and transport._err_lines == 0
    transport._stderr_line(detach.EXIT_MARK + "x 0")
    assert transport._exit == "x 0"


@pytest.mark.asyncio
async def test_a_reattach_that_finds_no_slot_fails_as_a_busy_computer_not_a_lost_one(tmp_path, monkeypatch):
    monkeypatch.setattr(ssh_launcher, "RECONNECT_WAIT_S", 2)
    link, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    one = launcher(resumes=True)
    prepared = await one._lay_out(await one.ready(), None)
    transport = one.transport(ClaudeAgentOptions(cwd=prepared.cwd, env={"CLAUDE_CONFIG_DIR": prepared.config_dir}))
    await transport.connect()
    messages = transport.read_messages()

    @contextlib.asynccontextmanager
    async def full(wait_s: float):
        await asyncio.sleep(wait_s)
        raise SlotWaitTimeout("command")
        yield

    monkeypatch.setattr(link, "cli_hold", full)
    _drop(transport)
    with pytest.raises(ClaudeTurnFailed) as busy:
        await _next(messages)
    assert busy.value.kind == "commands_full"
    await transport.close()


@pytest.mark.asyncio
async def test_a_cloud_probe_that_does_not_finish_is_no_answer(tmp_path, monkeypatch):
    _, launcher = _local(tmp_path, monkeypatch, str(_echo_cli(tmp_path)))
    transport = await _started(launcher())
    link = transport._cli_link
    transport._cli_link = SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(exit_code=None)))
    assert await transport._answers() is False
    assert not transport._reach.up.is_set()
    transport._cli_link = link
    await transport.close()
