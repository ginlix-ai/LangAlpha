"""SSHLauncher end to end: the bundled CLI on a real sshd, against the loopback Messages API.

The machine is a container of ``tests/integration/sandbox/ssh_target`` on the
host's network, so the CLI there reaches the fake API on this host's
loopback. The CLI is the bundled one, placed by the test where the
installer would put it. Needs Docker; Linux only (host networking).
"""

from __future__ import annotations

import asyncio
import json
import shutil
import socket
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import asyncssh
import pytest
import pytest_asyncio
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import ToolNode

from ptc_agent.agent.claude_code.runtime import ClaudeTurnFailed, build_claude_code_graph, token_works
from ptc_agent.agent.claude_code import ssh_launcher
from ptc_agent.agent.claude_code.machine_token import (
    machine_token_state,
    promote_machine_token,
    remove_machine_token,
    stage_machine_token,
)
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers import _ssh_transport
from ptc_agent.core.sandbox.providers._ssh_connect import SSHTarget
from ptc_agent.core.sandbox.providers._ssh_transport import Link, pool
from tests.unit.ptc_agent.agent.claude_code.fake_api import TOKEN, FakeMessagesAPI
from tests.unit.ptc_agent.agent.claude_code.harness import run_turn, spec

pytestmark = [pytest.mark.integration, pytest.mark.provider_docker, pytest.mark.enable_socket, pytest.mark.asyncio]

IMAGE = "langalpha-ssh-target"
ROOT = "/home/dev/langalpha"
BASE = f"{ROOT}/{SandboxLayout.CLAUDE_CODE_DIR}"
CLI = f"{BASE}/cli/test/claude"
CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"
OTHER = "5d0c1f2e-8a7b-4c3d-9e1f-0a2b3c4d5e6f"
TOKEN_DIR = "/home/dev/.config/langalpha/claude-code"
TOKEN_FILE = f"{TOKEN_DIR}/{CID}"
SPEC = spec("claude-haiku-5-5")


@dataclass
class Machine:
    name: str
    port: int
    key: str
    host_key: str

    def sh(self, script: str, *, user: str = "dev", stdin: str | None = None, check: bool = True) -> str:
        return subprocess.run(["docker", "exec", "-i", "-u", user, self.name, "sh", "-c", script],
                              input=stdin, capture_output=True, text=True, check=check).stdout

    def holding(self, secret: str, *paths: str, user: str = "root") -> list[str]:
        """Paths on the machine whose contents hold ``secret``; the secret goes in on stdin."""
        out = self.sh("grep -arlsFf /dev/stdin " + " ".join(paths) + " || true", user=user, stdin=secret)
        return [p for p in out.split() if not p.startswith(("/proc/self/", "/proc/thread-self/"))]

    def processes_holding(self, secret: str) -> list[str]:
        """Processes whose argv or environment hold ``secret``, read as root and as the login (each reads only some)."""
        paths = ("/proc/[0-9]*/cmdline", "/proc/[0-9]*/environ")
        return sorted({*self.holding(secret, *paths), *self.holding(secret, *paths, user="dev")})

    def files_holding(self, secret: str) -> list[str]:
        return [p for p in self.holding(secret, "/home", "/root", "/tmp", "/var/tmp", "/run", "/etc", "/opt") if p != TOKEN_FILE]

    def clis(self) -> list[str]:
        """The CLI processes running on the machine, by their command lines."""
        lines = self.sh("ps -u dev -ww -o pid=,args=").splitlines()
        return [ln.split()[0] for ln in lines if ln.split(maxsplit=1)[1:2] and ln.split(maxsplit=1)[1].startswith(CLI)]

    def config_dirs(self) -> list[str]:
        return self.sh(f"ls -A {BASE}/config 2>/dev/null || true").split()

    async def link(self) -> Link:
        target = SSHTarget(host="127.0.0.1", port=self.port, username="dev", host_key=self.host_key,
                           key_owner="claude-code-test", allow_private=True)

        async def load_key() -> str:
            return self.key

        return await pool().link(target, load_key)


async def write_machine_token(link: Link, token: str, computer_id: str = CID) -> None:
    """Place a token as a save does once its check passed: staged, then renamed in."""
    await promote_machine_token(link, computer_id, await stage_machine_token(link, computer_id, token))


def _docker(*args: str, check: bool = True) -> str:
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check).stdout.strip()


@pytest.fixture(scope="module")
def machine():
    if shutil.which("docker") is None:
        pytest.skip("needs Docker")
    if not _docker("images", "-q", IMAGE):
        _docker("build", "-q", "-t", IMAGE, str(Path(__file__).parents[1] / "sandbox" / "ssh_target"))
    key = asyncssh.generate_private_key("ssh-ed25519")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    name = "claude-code-ssh-" + uuid.uuid4().hex[:8]
    _docker("run", "-d", "--name", name, "--network", "host", "-e",
            f"AUTHORIZED_KEYS={key.export_public_key().decode().strip()}", "--entrypoint", "sh", IMAGE, "-c",
            f"printf 'Port {port}\nListenAddress 127.0.0.1\n' > /etc/ssh/sshd_config.d/20-port.conf"
            " && exec /usr/local/bin/entrypoint.sh")
    try:
        m = Machine(name, port, key.export_private_key().decode(), _docker("exec", name, "cat", "/etc/ssh/ssh_host_ed25519_key.pub"))
        import claude_agent_sdk

        bundled = Path(claude_agent_sdk.__file__).parent / "_bundled" / "claude"
        m.sh(f"mkdir -p {Path(CLI).parent} && chmod 700 {ROOT}/_internal", user="root")
        _docker("cp", str(bundled), f"{name}:{CLI}")
        m.sh(f"chown -R dev:dev {ROOT} && chmod 755 {CLI}", user="root")
        for _ in range(50):
            if m.sh(f"grep -q ':{port:04X} ' /proc/net/tcp && echo up", check=False).strip() == "up":
                break
            subprocess.run(["sleep", "0.2"])
        yield m
    finally:
        _docker("rm", "-f", name, check=False)


@pytest.fixture(autouse=True)
def _bundled_cli(monkeypatch):
    """The SDK's bundled CLI, placed by the fixture, stands in for the pinned release the installer fetches."""
    async def ensure_cli(link: Link, root: str, *, install: bool = True) -> str:
        assert root == ROOT
        return CLI

    monkeypatch.setattr(ssh_launcher.cli_install, "ensure_cli", ensure_cli)


def _launcher(m: Machine, api: FakeMessagesAPI, **kw) -> SSHLauncher:
    return SSHLauncher(_ssh_transport.Machine(m.link, lambda: ROOT), identity="test-user", token_file=kw.pop("token_file", CID),
                       env={**api.cli_env(), "CLAUDE_CODE_MAX_RETRIES": "0"}, **kw)


@pytest_asyncio.fixture
async def api(machine):
    """The first-party API, its CA where the machine's CLI reads it; the machine holds no token to begin with."""
    async with FakeMessagesAPI(first_party=True, refused=frozenset({"sk-ant-oat01-revoked"})) as api:
        ca = api.cli_env()["NODE_EXTRA_CA_CERTS"]
        machine.sh(f"mkdir -p {Path(ca).parent}", user="root")
        _docker("cp", ca, f"{machine.name}:{ca}")
        machine.sh(f"chmod 755 {Path(ca).parent} && chmod 644 {ca}", user="root")
        machine.sh(f"rm -rf {TOKEN_DIR}")
        yield api


SCAN: dict = {}


def _scanning_tool(m: Machine):
    @tool("Bash")
    async def bash(command: str) -> str:
        """Run a shell command."""
        if secret := SCAN.get("secret"):
            SCAN["procs"] = await asyncio.to_thread(m.processes_holding, secret)
            # The scan reaches the CLI: its environment holds the runtime's own variable.
            SCAN["sees_cli"] = bool(await asyncio.to_thread(m.holding, "CLAUDE_CODE_SDK_READS_SESSION_STATE",
                                                             "/proc/[0-9]*/environ", user="dev"))
        SCAN["clis_during"] = await asyncio.to_thread(m.clis)
        SCAN["config_dirs_during"] = await asyncio.to_thread(m.config_dirs)
        return f"ran: {command}"

    return bash


def _graph(m: Machine):
    return build_claude_code_graph(ToolNode([_scanning_tool(m)]), checkpointer=InMemorySaver())


def _after_turn(m: Machine) -> None:
    assert m.clis() == []
    assert m.config_dirs() == []


async def test_a_turn_runs_its_tools_in_the_backend_and_the_next_resumes_from_the_checkpoint(machine, api):
    link = await machine.link()
    await write_machine_token(link, TOKEN)
    assert await machine_token_state(link, CID) == "file"
    graph = _graph(machine)
    launcher = _launcher(machine, api)
    SCAN.clear()
    SCAN["secret"] = TOKEN
    first = await run_turn(SPEC, launcher, graph, "t-ssh", [HumanMessage("FACT: the code word is heron. RUN_TOOL please")])
    assert launcher.source == "file"
    ai_call, result, final = first["messages"][1:]
    assert isinstance(ai_call, AIMessage) and ai_call.tool_calls[0]["name"] == "Bash"
    assert isinstance(result, ToolMessage) and result.content == "ran: echo spike-$((6*7))"
    assert "ran: echo spike-$((6*7))" in final.text
    assert SCAN["sees_cli"] and SCAN["procs"] == []
    assert len(SCAN["clis_during"]) == 1 and len(SCAN["config_dirs_during"]) == 1
    _after_turn(machine)
    second = await run_turn(SPEC, _launcher(machine, api), graph, "t-ssh", [HumanMessage("RECALL it")])
    assert second["claude_session"]["session_id"] == first["claude_session"]["session_id"]
    assert "FACT: the code word is heron" in second["messages"][-1].text
    assert set(api.credentials) == {TOKEN}
    sent = api.model_requests
    devices = {json.loads(r["metadata"]["user_id"])["device_id"] for r in sent}
    assert len(devices) == 1
    cwd = next(ln for r in sent[:1] for m in r["messages"] for b in (m["content"] if isinstance(m["content"], list) else [])
               for ln in b.get("text", "").splitlines() if "Primary working directory" in ln)
    assert f"{BASE}/threads/t-ssh" in cwd
    _after_turn(machine)
    assert machine.files_holding(TOKEN) == []


class Relay:
    """The user's stored token as the backend relays it, and what it is told."""

    def __init__(self, token: str | None) -> None:
        self.token, self.reads, self.refusals = token, 0, []

    async def read(self) -> str | None:
        self.reads += 1
        return self.token

    async def refused(self, source: str) -> None:
        self.refusals.append(source)


async def _turn(m: Machine, api: FakeMessagesAPI, thread: str, text: str, relay: Relay | None = None) -> tuple[SSHLauncher, dict]:
    launcher = _launcher(m, api, relay=relay.read if relay else None, on_refused=relay.refused if relay else None)
    values = await run_turn(SPEC, launcher, _graph(m), thread, [HumanMessage(text)])
    return launcher, values


async def test_a_machine_with_its_own_token_is_never_relayed_the_stored_one(machine, api):
    await write_machine_token(await machine.link(), TOKEN)
    relay = Relay("sk-ant-oat01-stored-" + uuid.uuid4().hex)
    launcher, _ = await _turn(machine, api, "t-file", "ECHO hi", relay)
    assert (launcher.source, relay.reads, set(api.credentials)) == ("file", 0, {TOKEN})
    _after_turn(machine)


async def test_a_machine_without_a_token_runs_on_the_relayed_one_held_by_no_process_or_file(machine, api):
    relay = Relay(TOKEN)
    SCAN.clear()
    SCAN["secret"] = TOKEN
    launcher, values = await _turn(machine, api, "t-relay", "RUN_TOOL please", relay)
    assert (launcher.source, relay.reads, set(api.credentials)) == ("relay", 1, {TOKEN})
    assert not _holds(launcher, TOKEN)
    assert isinstance(values["messages"][-1], AIMessage) and "ran: echo" in values["messages"][-1].text
    assert SCAN["sees_cli"] and SCAN["procs"] == []
    _after_turn(machine)
    assert machine.files_holding(TOKEN) == []


@pytest.mark.parametrize("stored", [None, "no-relay"], ids=["relay-has-no-token", "relay-off"])
async def test_a_machine_with_no_token_to_use_runs_signed_out(machine, api, stored):
    relay = Relay(None) if stored is None else None
    with pytest.raises(ClaudeTurnFailed) as failed:
        await _turn(machine, api, "t-none", "ECHO hi", relay)
    assert failed.value.kind == "auth_failed"
    assert api.credentials == []
    assert (relay.reads if relay else 0) == (1 if relay else 0)
    _after_turn(machine)


def _break(m: Machine, how: str) -> None:
    """A token file the shim must not use: it holds a good token, so only the refusal keeps it out."""
    d = Path(TOKEN_FILE).parent
    m.sh(f"mkdir -p {d} && chmod 700 {d}")
    if how == "symlink":
        m.sh(f"umask 077 && printf '%s' \"$(cat)\" > /home/dev/elsewhere && ln -sfn /home/dev/elsewhere {TOKEN_FILE}", stdin=TOKEN)
    elif how == "mode":
        m.sh(f"printf '%s' \"$(cat)\" > {TOKEN_FILE} && chmod 644 {TOKEN_FILE}", stdin=TOKEN)
    else:
        m.sh(f"umask 077 && printf '%s' \"$(cat)\" > {TOKEN_FILE} && chown nosudo {TOKEN_FILE}", user="root", stdin=TOKEN)


@pytest.mark.parametrize("how", ["symlink", "mode", "owner"])
async def test_a_token_file_the_machine_does_not_keep_privately_is_refused(machine, api, how):
    _break(machine, how)
    try:
        assert await machine_token_state(await machine.link(), CID) == "invalid"
        stored = "sk-ant-oat01-stored-" + uuid.uuid4().hex
        relay = Relay(stored)
        launcher, _ = await _turn(machine, api, "t-broken", "ECHO hi", relay)
        assert (launcher.source, set(api.credentials)) == ("relay", {stored})
        with pytest.raises(ClaudeTurnFailed) as failed:
            await _turn(machine, api, "t-broken-2", "ECHO hi")
        assert failed.value.kind == "auth_failed"
        assert TOKEN not in api.credentials
        _after_turn(machine)
    finally:
        machine.sh(f"rm -f /home/dev/elsewhere {TOKEN_FILE}", user="root")


def _holds(launcher: SSHLauncher, secret: str) -> bool:
    """Whether any attribute of the launcher holds ``secret``."""
    return any(secret in repr(v) for v in vars(launcher).values())


async def test_a_refused_token_fails_the_turn_and_is_reported_by_where_it_came_from(machine, api):
    link = await machine.link()
    await write_machine_token(link, "sk-ant-oat01-revoked")
    relay = Relay(TOKEN)
    with pytest.raises(ClaudeTurnFailed) as from_file:
        await _turn(machine, api, "t-revoked-file", "ECHO hi", relay)
    assert await remove_machine_token(link, CID)
    revoked = Relay("sk-ant-oat01-revoked")
    with pytest.raises(ClaudeTurnFailed) as relayed:
        await _turn(machine, api, "t-revoked-relay", "ECHO hi", revoked)
    assert from_file.value.kind == relayed.value.kind == "auth_failed"
    assert (relay.reads, relay.refusals, revoked.refusals) == (0, ["file"], ["relay"])
    _after_turn(machine)


async def test_a_turn_signed_out_reports_no_refusal(machine, api):
    relay = Relay(None)
    with pytest.raises(ClaudeTurnFailed):
        await _turn(machine, api, "t-out", "ECHO hi", relay)
    assert relay.refusals == []
    _after_turn(machine)


async def test_the_token_check_runs_on_the_machine_with_the_token_a_turn_there_would_use(machine, api):
    link = await machine.link()
    good = _launcher(machine, api)
    assert not await token_works(good, "claude-haiku-5-5") and good.source == "none"
    await write_machine_token(link, TOKEN)
    assert await token_works(good, "claude-haiku-5-5") and good.source == "file"
    await remove_machine_token(link, CID)
    relayed = Relay("sk-ant-oat01-revoked")
    bad = _launcher(machine, api, relay=relayed.read)
    assert not await token_works(bad, "claude-haiku-5-5") and bad.source == "relay"
    assert api.credentials[0] == TOKEN and set(api.credentials[1:]) == {"sk-ant-oat01-revoked"}
    _after_turn(machine)


async def test_the_machines_own_claude_login_is_neither_used_nor_touched(machine, api):
    login = "sk-ant-oat01-machine-login-" + uuid.uuid4().hex
    creds = json.dumps({"claudeAiOauth": {"accessToken": login, "refreshToken": "sk-ant-ort01-machine",
                                          "expiresAt": 4102444800000, "scopes": ["user:inference"], "subscriptionType": "max"}})
    machine.sh("rm -rf ~/.claude && mkdir -p ~/.claude && cat > ~/.claude/.credentials.json", stdin=creds)
    before = machine.sh("sha256sum ~/.claude/.credentials.json")
    try:
        with pytest.raises(ClaudeTurnFailed) as failed:
            await _turn(machine, api, "t-login", "ECHO hi")
        assert failed.value.kind == "auth_failed"
        await write_machine_token(await machine.link(), TOKEN)
        await _turn(machine, api, "t-login", "ECHO hi")
        assert set(api.credentials) == {TOKEN}
        assert machine.sh("sha256sum ~/.claude/.credentials.json") == before
        assert machine.sh("ls -A ~/.claude").split() == [".credentials.json"]
        _after_turn(machine)
    finally:
        machine.sh("rm -rf ~/.claude")


async def _started(m: Machine, api: FakeMessagesAPI) -> tuple[SSHLauncher, ClaudeSDKClient, object]:
    launcher = _launcher(m, api)
    prepared = await launcher.scratch()
    options = ClaudeAgentOptions(tools=[], system_prompt="x", setting_sources=[], model="claude-haiku-5-5", cwd=prepared.cwd,
                                 env={"CLAUDE_CONFIG_DIR": prepared.config_dir})
    client = ClaudeSDKClient(options, transport=launcher.transport(options))
    await client.connect()
    return launcher, client, prepared


async def test_closing_ends_the_cli_even_when_the_close_is_cancelled(machine, api):
    launcher, client, prepared = await _started(machine, api)
    assert len(machine.clis()) == 1
    closing = asyncio.ensure_future(client._transport.close())
    await asyncio.sleep(0)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    for _ in range(50):
        if not machine.clis():
            break
        await asyncio.sleep(0.2)
    await client.disconnect()
    await launcher.release(prepared)
    _after_turn(machine)


async def test_a_cli_that_outlives_its_input_is_signalled_by_its_own_pid(machine, api):
    launcher, client, prepared = await _started(machine, api)
    transport = client._transport
    [pid] = machine.clis()
    assert transport._pid == int(pid)
    await transport._signal("KILL")
    assert await transport._ended(5) and machine.clis() == []
    await client.disconnect()
    await launcher.release(prepared)
    _after_turn(machine)


async def test_the_token_is_written_privately_and_a_symlink_in_its_place_is_refused(machine, api):
    link = await machine.link()
    await write_machine_token(link, TOKEN)
    modes = machine.sh(f"stat -c '%a %U' {Path(TOKEN_DIR).parent} {TOKEN_DIR} {TOKEN_FILE}").split()
    assert modes == ["700", "dev", "700", "dev", "600", "dev"]
    assert machine.sh(f"ls -A {TOKEN_DIR}").split() == [CID], "nothing staged is left behind"
    assert machine.sh(f"cat {TOKEN_FILE}").strip() == TOKEN
    assert machine.files_holding(TOKEN) == []
    machine.sh(f"rm -f {TOKEN_FILE} && echo keep > /home/dev/target && ln -s /home/dev/target {TOKEN_FILE}")
    try:
        with pytest.raises(RuntimeError) as refused:
            await write_machine_token(link, "sk-ant-oat01-other")
        assert "symbolic link" in str(refused.value) and "sk-ant" not in str(refused.value)
        with pytest.raises(RuntimeError):
            await remove_machine_token(link, CID)
        assert machine.sh("cat /home/dev/target").strip() == "keep"
        assert machine.sh(f"test -L {TOKEN_FILE} && echo link").strip() == "link"
    finally:
        machine.sh(f"rm -f {TOKEN_FILE} {TOKEN_DIR}/.staged.* /home/dev/target")
    assert await machine_token_state(link, CID) == "none"
    assert not await remove_machine_token(link, CID)


async def test_a_machine_without_perl_says_so(machine, api):
    machine.sh("mv /usr/bin/perl /usr/bin/perl.off", user="root")
    try:
        with pytest.raises(Exception, match="needs perl"):
            await _turn(machine, api, "t-perl", "ECHO hi")
        assert await machine_token_state(await machine.link(), CID) == "none", "no token a turn could use"
        _after_turn(machine)
    finally:
        machine.sh("mv /usr/bin/perl.off /usr/bin/perl", user="root")


async def test_two_computers_on_one_login_each_sign_in_with_their_own_token(machine, api):
    link = await machine.link()
    await write_machine_token(link, TOKEN)
    await write_machine_token(link, "sk-ant-oat01-revoked", OTHER)
    with pytest.raises(ClaudeTurnFailed):
        await run_turn(SPEC, _launcher(machine, api, token_file=OTHER), _graph(machine), "t-other", [HumanMessage("ECHO hi")])
    await _turn(machine, api, "t-mine", "ECHO hi")
    assert set(api.credentials) == {"sk-ant-oat01-revoked", TOKEN} and api.credentials[-1] == TOKEN
    assert await remove_machine_token(link, OTHER) and await machine_token_state(link, CID) == "file"
    _after_turn(machine)


async def test_a_replacement_claude_refuses_leaves_the_token_in_use_on_the_machine(machine, api, monkeypatch):
    """The save's own staging, check and clean-up, on a real machine."""
    from src.server.services.claude_code import flow, machine as machines
    from src.server.services.oauth_signin import OAuthRefusal
    from tests.unit.server.services.claude_code.fakes import Cache, serving

    link = await machine.link()
    await write_machine_token(link, TOKEN)
    checked = []

    def check_launcher(handle, user_id, token_file):
        checked.append(token_file)
        return _launcher(machine, api, token_file=token_file)

    async def held() -> bool:
        return True

    monkeypatch.setattr(machines, "check_launcher", check_launcher)
    rejected = OAuthRefusal(400, "token_rejected", "Claude did not accept this token.")
    with serving(Cache()):
        home = machines.MachineHome("test-user", {"computer_id": CID})
        home._machine = _ssh_transport.Machine(machine.link, lambda: ROOT)
        with pytest.raises(OAuthRefusal) as refused:
            await flow._save(home, "sk-ant-oat01-revoked", held=held, rejected=rejected, retryable=True,
                             expires_at=datetime.now(timezone.utc))
    assert refused.value is rejected and checked[0].startswith(f".staged.{CID}.")
    assert machine.sh(f"cat {TOKEN_FILE}").strip() == TOKEN
    assert machine.sh(f"ls -A {TOKEN_DIR}").split() == [CID], "the staged token is gone"
    await _turn(machine, api, "t-kept", "ECHO hi")
    assert set(api.credentials) == {"sk-ant-oat01-revoked", TOKEN} and api.credentials[-1] == TOKEN, "the next turn runs on the token in use"
    _after_turn(machine)
