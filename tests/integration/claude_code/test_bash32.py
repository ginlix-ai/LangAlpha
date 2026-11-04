"""The scripts a user's computer runs, under a Mac's bash (3.2) and BusyBox.

The official ``bash:3.2`` image is Alpine, so its userland is BusyBox's;
perl is added, as the shim and the token's rules need it, and a getconf that
says glibc, since a machine on musl reads as one Claude Code cannot run on
before any token script looks further. Every script goes
through a real Link whose connection is ``docker exec`` into the container,
and the turn's command runs as sshd would start it there.
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import uuid
from types import SimpleNamespace

import pytest
from claude_agent_sdk import ClaudeAgentOptions

from ptc_agent.agent.claude_code.launcher import TOKEN_FD_ENV
from ptc_agent.agent.claude_code.machine_token import (
    STREAM_BOOT,
    discard_staged_token,
    machine_state,
    promote_machine_token,
    remove_machine_token,
    stage_machine_token,
)
from ptc_agent.agent.claude_code.ssh_launcher import SSHLauncher
from ptc_agent.core.sandbox.providers._ssh_transport import Link, Machine
from src.llms.claude_code import ClaudeTurnFailed
from tests.integration.claude_code.test_ssh_launcher import launcher_finds
from tests.unit.ptc_agent.agent.claude_code.local_link import LocalConn, LocalProcess

pytestmark = [pytest.mark.integration, pytest.mark.provider_docker, pytest.mark.enable_socket]

IMAGE = "langalpha-bash32-perl"
HOME = "/home/dev"
TOKENS = f"{HOME}/.config/langalpha/claude-code"
CID = "0b9e6a7c-3f0e-4c47-9a53-2f4b1d1c8e21"
TOKEN = "sk-ant-oat01-" + "9" * 80
OLD = "sk-ant-oat01-" + "8" * 80
CLI = "/usr/local/bin/claude-stand-in"
GETCONF = "/usr/local/bin/getconf"
#: Reads the token from the descriptor the shim names, then says what it got.
STAND_IN = f"""#!/bin/sh
t=
fd=${{{TOKEN_FD_ENV}:-}}
if [ -n "$fd" ]; then eval "read -r t <&$fd"; fi
echo "token=$t"
env | sort
"""


def _docker(*args: str, stdin: str | None = None) -> str:
    return subprocess.run(["docker", *args], input=stdin, capture_output=True, text=True, check=True).stdout


class DockerConn(LocalConn):
    """The connection a Link rides, as ``docker exec`` into the container."""

    def __init__(self, container: str) -> None:
        super().__init__("/usr/bin")
        self.container = container

    async def create_process(self, command: str, encoding: None = None) -> LocalProcess:
        pipe = asyncio.subprocess.PIPE
        return LocalProcess(await asyncio.create_subprocess_exec(
            "docker", "exec", "-i", "-e", f"HOME={HOME}", self.container, "sh", "-c", command,
            stdin=pipe, stdout=pipe, stderr=pipe,
        ))


@pytest.fixture(scope="module")
def container():
    if shutil.which("docker") is None:
        pytest.skip("needs Docker")
    if not _docker("images", "-q", IMAGE).strip():
        _docker("build", "-q", "-t", IMAGE, "-", stdin="FROM bash:3.2\nRUN apk add --no-cache perl\n")
    name = "claude-code-bash32-" + uuid.uuid4().hex[:8]
    _docker("run", "-d", "--name", name, IMAGE, "sleep", "3600")
    try:
        assert _docker("exec", name, "bash", "-c", "echo $BASH_VERSION").startswith("3.2.")
        _docker("exec", "-i", name, "sh", "-c", f"mkdir -p {HOME} && cat > {CLI} && chmod 755 {CLI}", stdin=STAND_IN)
        _docker("exec", name, "sh", "-c", f"printf '#!/bin/sh\\necho glibc 2.99\\n' > {GETCONF} && chmod 755 {GETCONF}")
        yield name
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _link(container: str) -> Link:
    return Link(SimpleNamespace(label="bash32", host="bash32"), DockerConn(container))


def _ls(container: str) -> list[str]:
    return sorted(_docker("exec", container, "sh", "-c", f"ls -A {TOKENS} 2>/dev/null || true").split())


@pytest.mark.asyncio
async def test_every_token_script_runs_under_bash_3_2_and_busybox(container):
    link = _link(container)
    await promote_machine_token(link, CID, await stage_machine_token(link, CID, OLD))
    staged = await stage_machine_token(link, CID, TOKEN)
    assert (await machine_state(link, CID)).file == "file"
    await discard_staged_token(link, staged)
    assert _docker("exec", container, "cat", f"{TOKENS}/{CID}").strip() == OLD
    stale = await stage_machine_token(link, CID, TOKEN)
    _docker("exec", container, "touch", "-t", "202001010000", f"{TOKENS}/{stale}")
    assert (await machine_state(link, CID)).file == "file" and _ls(container) == [CID], "the stale one is swept"
    await stage_machine_token(link, CID, TOKEN)
    assert await remove_machine_token(link, CID) and _ls(container) == []


@pytest.mark.asyncio
async def test_a_token_file_of_another_owners_is_replaced_as_a_removal_would_take_it(container):
    """The status check and a turn refuse it, so a new token goes in its place."""
    link = _link(container)
    path = f"{TOKENS}/{CID}"
    _docker("exec", container, "sh", "-c", f"mkdir -p {TOKENS} && printf x > {path} && chmod 600 {path} && chown 1234 {path}")
    assert (await machine_state(link, CID)).file == "invalid"
    await promote_machine_token(link, CID, await stage_machine_token(link, CID, TOKEN))
    assert (await machine_state(link, CID)).file == "file"
    assert _docker("exec", container, "cat", path).strip() == TOKEN
    assert await remove_machine_token(link, CID) and _ls(container) == []


@pytest.mark.asyncio
async def test_a_machine_on_musl_reads_as_unsupported_and_is_refused_before_the_cli(container):
    """The image's own libc, with the glibc getconf set aside: what a computer
    registered before the probe refused musl finds."""
    link = _link(container)

    async def connect() -> Link:
        return link

    _docker("exec", container, "mv", GETCONF, f"{GETCONF}.glibc")
    try:
        assert (await machine_state(link, CID)).file == "unsupported"
        launcher = SSHLauncher(Machine(connect, lambda: "/root"), identity="u", token_file=CID, install=False)
        with pytest.raises(ClaudeTurnFailed) as refused:
            await launcher.ready()
        assert refused.value.kind == "computer_unsupported"
    finally:
        _docker("exec", container, "mv", f"{GETCONF}.glibc", GETCONF)


def _turn(container: str, *, relay: bool, after: str = "") -> list[str]:
    """The turn's command, as the CLI's channel runs it, with the stand-in CLI."""
    launcher = SSHLauncher(Machine(None, lambda: "/root"), identity="u", token_file=CID, relay=(lambda: None) if relay else None)
    options = ClaudeAgentOptions(cli_path=CLI, cwd="/tmp", env={"CLAUDE_CONFIG_DIR": "/tmp/cfg", "ODD": "it's\na \"value\" $HOME"})
    done = subprocess.run(
        ["docker", "exec", "-i", "-e", f"HOME={HOME}", "-e", "MACHINE_SECRET=machine-only", container, "sh", "-c", STREAM_BOOT],
        input=launcher.command(options) + "\n" + after, capture_output=True, text=True, timeout=60,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout.splitlines()


def test_a_turns_command_runs_under_bash_3_2_and_busybox(container):
    link = _link(container)
    asyncio.run(promote_machine_token(link, CID, asyncio.run(stage_machine_token(link, CID, TOKEN))))
    announced, token, *env = _turn(container, relay=False)
    assert (announced, token) == ("langalpha-token file", f"token={TOKEN}")
    assert f"HOME={HOME}" in env and "CLAUDE_CONFIG_DIR=/tmp/cfg" in env and not any("MACHINE_SECRET" in e for e in env)
    assert not any(TOKEN in e for e in env)
    assert asyncio.run(remove_machine_token(link, CID))
    announced, token, *_ = _turn(container, relay=True, after=OLD + "\n")
    assert (announced, token) == ("langalpha-token relay", f"token={OLD}")


@pytest.mark.asyncio
async def test_the_managed_settings_are_named_under_bash_3_2_and_busybox(container):
    """Linux's place, then a Mac's (``uname`` says Darwin), each file readable by root only."""
    link = _link(container)

    def sh(script: str) -> None:
        _docker("exec", container, "sh", "-c", script)

    mac, prefs = "/Library/Application Support/ClaudeCode", "/Library/Managed Preferences"
    sh("mkdir -p /etc/claude-code/managed-settings.d && cd /etc/claude-code"
       " && touch managed-settings.d/a.json managed-mcp.json && chmod 0 managed-settings.d/a.json managed-mcp.json")
    try:
        assert (await machine_state(link, CID)).file == "policy"
        assert await launcher_finds(link) == (("dropins", "mcp"), True)
        sh(f"printf '#!/bin/sh\\n[ \"$1\" = -s ] && exec echo Darwin\\nexec /bin/uname \"$@\"\\n' > /usr/local/bin/uname"
           f" && chmod 755 /usr/local/bin/uname && mkdir -p '{mac}' '{prefs}/root'"
           f" && touch '{mac}/managed-settings.json' '{prefs}/root/com.anthropic.claudecode.plist'")
        assert await launcher_finds(link) == (("settings", "profile"), True), "a Mac's places, the profile under the login's name"
    finally:
        sh("rm -rf /etc/claude-code /Library /usr/local/bin/uname")
