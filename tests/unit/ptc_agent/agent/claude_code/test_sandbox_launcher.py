"""A cloud computer's launcher here: a cloud computer's runtime stood in for by this machine, the bundled CLI, the loopback API.

``local_sandbox`` runs the sandbox's side locally through the same code a
cloud runtime takes (exec under sh with its streams merged, uploads, a
streamed process), so the shim, the relay, the lease and the session's
round trip through the checkpoint are checked with no provider
(``tests/integration/sandbox/test_claude_code_sandbox.py`` runs them on
Daytona, E2B and Docker).
"""

from __future__ import annotations

import asyncio
import gzip
import os
import platform
import shutil
import stat
import subprocess
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import claude_agent_sdk
import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.claude_code import cli_install, ssh_launcher
from ptc_agent.agent.claude_code.launcher import Credential
from ptc_agent.agent.claude_code.sandbox_launcher import SandboxLink, sandbox_launcher
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.paths import CLAUDE_CODE_TOKEN_DIR, LANGALPHA_CONFIG_DIR
from src.llms.claude_code import ClaudeTurnFailed

from .fake_api import TOKEN, FakeMessagesAPI
from .harness import new_graph, run_turn, spec
from .local_sandbox import LocalSandboxRuntime

pytestmark = [
    pytest.mark.enable_socket,
    pytest.mark.asyncio,
    pytest.mark.skipif(sys.platform != "linux" or not (shutil.which("perl") and shutil.which("bash")),
                       reason="a cloud computer is Linux with perl and bash"),
]

SPEC = spec("claude-haiku-5-5")
KEY = "sk-ant-api03-" + "4" * 80
BUNDLED = Path(claude_agent_sdk.__file__).parent / "_bundled" / "claude"
PLATFORM = {"x86_64": "linux-x64", "aarch64": "linux-arm64"}.get(platform.machine(), "")


class Store:
    """The credential a user's turns on cloud computers run on, as the launcher reads it."""

    def __init__(self, credential: Credential | None) -> None:
        self.credential, self.reads, self.refusals = credential, 0, []

    async def read(self) -> Credential | None:
        self.reads += 1
        return self.credential

    async def refused(self) -> None:
        self.refusals.append(self.credential.kind)


def _box(runtime: LocalSandboxRuntime) -> SimpleNamespace:
    return SimpleNamespace(wait_ready=AsyncMock(), runtime=runtime, working_dir=runtime.working_dir)


@pytest.fixture
def runtime(tmp_path) -> LocalSandboxRuntime:
    root = tmp_path / "home" / "langalpha"
    root.mkdir(parents=True)
    return LocalSandboxRuntime(root)


@pytest.fixture
def placed(monkeypatch):
    """The installer replaced by one placing the bundled CLI where it would put the pinned one."""

    async def ensure_cli(link, root, *, install=True, **_):
        path = Path(cli_install.cli_dir(root)) / cli_install.pinned().version / PLATFORM / "claude"
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(BUNDLED, path)
        path.chmod(0o755)
        return str(path)

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)
    if not PLATFORM or cli_install.pinned().platforms[PLATFORM].size != BUNDLED.stat().st_size:
        pytest.skip("the bundled CLI is not the pinned build for this machine")


def _files_holding(root: Path, secret: str) -> list[Path]:
    return [p for p in root.rglob("*") if p.is_file() and not p.is_symlink() and p.stat().st_size < 50_000_000
            and secret.encode() in p.read_bytes()]


async def test_a_script_runs_under_sh_with_its_stderr_apart_from_its_stdout(runtime):
    result = await SandboxLink(runtime).run("echo out; echo \"it's\" >&2\nprintf 'a\\nb'; exit 3", timeout=30)
    assert (result.exit_code, result.stdout, result.stderr) == (3, b"out\na\nb", b"it's\n")


async def test_a_script_past_its_timeout_raises():
    class Slow(LocalSandboxRuntime):
        async def exec(self, command, timeout=60, *, env=None):
            await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        await SandboxLink(Slow(Path("/"))).run("true", timeout=0.2)


async def test_files_land_whole_and_private_with_nothing_beside_them(runtime):
    first, second = runtime.root / "a.txt", runtime.root / "deep" / "er" / "b.jsonl"
    await SandboxLink(runtime).write_files([(str(first), b"one"), (str(second), b"two\n")])
    assert (first.read_bytes(), second.read_bytes()) == (b"one", b"two\n")
    assert {stat.S_IMODE(p.stat().st_mode) for p in (first, second)} == {0o600}
    assert sorted(p.name for p in runtime.root.rglob("*") if p.is_file()) == ["a.txt", "b.jsonl"]


async def test_an_upload_arrives_unpacked_from_its_parts_and_a_failed_one_leaves_nothing(runtime, tmp_path, monkeypatch):
    monkeypatch.setattr("ptc_agent.agent.claude_code.sandbox_launcher._PART_BYTES", 1 << 17)
    local = tmp_path / "big.bin"
    local.write_bytes(os.urandom(1 << 19))
    remote = runtime.root / ".upload.x"
    link = SandboxLink(runtime)
    await link.upload(str(local), str(remote), timeout=60)
    assert remote.read_bytes() == local.read_bytes()
    assert gzip.decompress(Path(f"{local}.gz").read_bytes()) == local.read_bytes()
    assert len(runtime.uploads) == 5 and runtime.uploads == [f"{remote}.gz.{i}" for i in range(5)]
    assert sorted(p.name for p in runtime.root.iterdir()) == [".upload.x"]

    remote.unlink()
    sent = runtime.upload_files

    async def broken(files):
        if files[0][1].endswith(".1"):
            Path(files[0][1]).write_bytes(b"half")
            raise ConnectionError("lost")
        await sent(files)

    runtime.upload_files = broken
    with pytest.raises(ConnectionError):
        await link.upload(str(local), str(remote), timeout=60)
    assert list(runtime.root.iterdir()) == []


async def test_first_uploads_racing_in_one_worker_each_send_a_whole_gzip(tmp_path):
    local = tmp_path / "claude"
    local.write_bytes(os.urandom(1 << 20) * 16)
    packed = await asyncio.gather(*(asyncio.to_thread(cli_install.gzipped, local) for _ in range(4)))
    assert {p.read_bytes() for p in packed} == {packed[0].read_bytes()}
    assert gzip.decompress(packed[0].read_bytes()) == local.read_bytes()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["claude", "claude.gz"]


def test_a_gzip_whose_source_is_gone_leaves_no_descriptor_or_temp_file(tmp_path):
    before = len(os.listdir("/dev/fd"))
    with pytest.raises(FileNotFoundError):
        cli_install.gzipped(tmp_path / "claude")
    assert len(os.listdir("/dev/fd")) == before
    assert list(tmp_path.iterdir()) == []


def _gone(pid: int) -> bool:
    """No such process, or a zombie a container without a reaper never reaps."""
    state = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return state == "" or state.startswith("Z")


def _own_lease(monkeypatch, tmp_path) -> Path:
    """This test's lease, apart from the ones tests on other workers make in /tmp."""
    lease = tmp_path / "lease"
    monkeypatch.setattr(sys.modules[SandboxLink.__module__], "new_lease", lambda: str(lease))
    return lease


async def _started(link: SandboxLink, command: str):
    async with link.cli_hold(1) as start:
        return await start(command)


@pytest.mark.parametrize("eof", [True, False], ids=["closes-stdin", "cannot-close-stdin"])
async def test_ending_the_input_ends_the_process(runtime, eof):
    runtime.eof = eof
    link = SandboxLink(runtime)
    channel = await _started(link, "cat")
    channel.stdin.write(b"hi\n")
    await channel.stdin.drain()
    assert await channel.stdout.readline() == b"hi\n"
    channel.stdin.write_eof()
    await asyncio.wait_for(channel.wait_closed(), 5)
    assert (channel.exit_status, channel.exit_signal) == ((0, None) if eof else (143, None))


async def _ended_within(channel, seconds: float) -> bool:
    try:
        await asyncio.wait_for(channel.wait_closed(), seconds)
    except TimeoutError:
        return False
    return True


async def test_a_process_whose_lease_the_backend_stops_renewing_is_ended_with_what_it_started(runtime):
    """A worker that dies leaves the sandbox's end of the stream open, so the
    lease ends the process, and a child it started is not left running."""
    kid = runtime.root / "kid"
    link = SandboxLink(runtime, lease_s=2, renew_s=3600)
    channel = await _started(link, f"sleep 600 & echo $! > {kid}; exec cat")
    assert await _ended_within(channel, 15)
    assert channel.exit_status == 143
    await asyncio.sleep(0.5)
    assert _gone(int(kid.read_text()))


async def test_a_renewed_lease_keeps_the_process_and_ending_it_takes_the_lease(runtime, monkeypatch, tmp_path):
    lease = _own_lease(monkeypatch, tmp_path)
    link = SandboxLink(runtime, lease_s=2, renew_s=0.5)
    channel = await _started(link, "exec cat")
    assert not await _ended_within(channel, 6)
    assert lease.exists()
    channel.stdin.write_eof()
    assert await _ended_within(channel, 5) and channel.exit_status == 0
    await asyncio.sleep(2)
    assert not lease.exists()


async def test_a_process_killed_with_its_group_leaves_no_lease(runtime, monkeypatch, tmp_path):
    """The watchdog dies with the group it watches, so the backend removes the lease."""
    lease = _own_lease(monkeypatch, tmp_path)
    link = SandboxLink(runtime, lease_s=30, renew_s=0.5)
    channel = await _started(link, "exec cat")
    for _ in range(50):
        if lease.exists():
            break
        await asyncio.sleep(0.1)
    assert lease.exists()
    channel.send_signal("KILL")
    assert await _ended_within(channel, 10)
    await asyncio.sleep(1.5)
    assert not lease.exists()


async def test_a_lease_that_cannot_be_renewed_says_so(runtime, caplog):
    """A watchdog ending the CLI then has a reason in the logs."""
    exec_ = runtime.exec

    async def failing(command, timeout=60, *, env=None):
        if command.startswith("touch -c"):
            raise ConnectionError("the provider's exec is down")
        return await exec_(command, timeout, env=env)

    runtime.exec = failing
    link = SandboxLink(runtime, lease_s=30, renew_s=0.2)
    channel = await _started(link, "exec cat")
    await asyncio.sleep(1)
    assert any("could not renew" in r.getMessage() and "ConnectionError" in r.getMessage() for r in caplog.records)
    channel.stdin.write_eof()
    assert await _ended_within(channel, 5)


def _launcher(runtime: LocalSandboxRuntime, api: FakeMessagesAPI, store: Store) -> SSHLauncher:
    env = {k: v for k, v in api.cli_env().items() if k != "ANTHROPIC_API_KEY"}
    return sandbox_launcher(_box(runtime), identity="test-user", credential=store, env={**env, "CLAUDE_CODE_MAX_RETRIES": "0"})


def _left(runtime: LocalSandboxRuntime) -> tuple[list, list]:
    config = runtime.root / "_internal" / "claude-code" / "config"
    return [s for s in runtime.streams if s.returncode is None], sorted(config.iterdir()) if config.exists() else []


@pytest.mark.parametrize("eof", [True, False], ids=["closes-stdin", "cannot-close-stdin"])
async def test_a_turn_runs_on_the_relayed_api_key_held_nowhere_and_the_next_resumes_from_the_checkpoint(
    runtime, placed, eof,
):
    runtime.eof = eof
    store = Store(Credential(KEY, kind="api_key"))
    graph = new_graph()
    async with FakeMessagesAPI() as api:
        launcher = _launcher(runtime, api, store)
        first = await run_turn(SPEC, launcher, graph, "t-cloud", [HumanMessage("FACT: the code word is heron. RUN_TOOL please")])
        assert launcher.source == "relay"
        ai_call, result, final = first["messages"][1:]
        assert isinstance(ai_call, AIMessage) and isinstance(result, ToolMessage) and result.content.startswith("ran: ")
        assert result.content in final.text
        assert _left(runtime) == ([], [])
        second = await run_turn(SPEC, _launcher(runtime, api, store), graph, "t-cloud", [HumanMessage("RECALL it")])
        assert second["claude_session"]["session_id"] == first["claude_session"]["session_id"]
        assert "FACT: the code word is heron" in second["messages"][-1].text
        assert set(api.api_keys) == {KEY} and store.reads == 2
    assert _left(runtime) == ([], [])
    assert _files_holding(runtime.root.parent, KEY) == []
    assert all(KEY.encode() not in s.encode() for s in runtime.scripts)


async def test_a_token_file_in_the_sandbox_is_never_read(runtime, placed):
    """Code the agent runs can write there, so only the relayed token signs the turn in."""
    planted = "sk-ant-oat01-planted-" + uuid.uuid4().hex
    folder = runtime.root.parent / ".config" / LANGALPHA_CONFIG_DIR.split("/")[-1] / CLAUDE_CODE_TOKEN_DIR.split("/")[-1]
    folder.mkdir(parents=True)
    for name in (str(uuid.uuid4()), "token"):
        (folder / name).write_text(planted)
        (folder / name).chmod(0o600)
    store = Store(Credential(TOKEN))
    async with FakeMessagesAPI(first_party=True) as api:
        launcher = _launcher(runtime, api, store)
        await run_turn(SPEC, launcher, new_graph(), "t-planted", [HumanMessage("ECHO hi")])
        assert (launcher.source, set(api.credentials)) == ("relay", {TOKEN})


async def test_a_refused_setup_token_is_reported_to_its_store_by_kind(runtime, placed):
    revoked = "sk-ant-oat01-revoked"
    store = Store(Credential(revoked))
    async with FakeMessagesAPI(first_party=True, refused=frozenset({revoked})) as api:
        with pytest.raises(ClaudeTurnFailed):
            await run_turn(SPEC, _launcher(runtime, api, store), new_graph(), "t-refused", [HumanMessage("ECHO hi")])
    assert store.refusals == ["setup_token"]


async def test_no_credential_runs_signed_out(runtime, placed):
    async with FakeMessagesAPI(first_party=True) as api:
        with pytest.raises(ClaudeTurnFailed) as failed:
            await run_turn(SPEC, _launcher(runtime, api, Store(None)), new_graph(), "t-none", [HumanMessage("ECHO hi")])
        assert failed.value.kind == "auth_failed" and api.credentials == []

