"""The Claude Code CLI installer against real sshd machines in Docker, glibc and BusyBox.

Downloads the real release (about 290 MB per platform) from Anthropic's
release host, so it is opt-in beyond its markers:

    CLAUDE_CODE_CLI_E2E=1 uv run --extra claude-code pytest -m integration \
        tests/integration/test_claude_code_cli_install.py -s

The real release is installed on the glibc machine only. The BusyBox (Alpine)
machine, which has no perl, so the installer falls to its flock and sha256sum,
gets a small made-up release from the local server below, since the real
binary cannot run there and the installer never runs it.

Each machine logs every curl it runs, so a reuse is told from a download by
the log, not by timing alone. Steps a version bump would take serve the
release under made-up versions from a local server the container reaches
through Docker's bridge.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import http.server
import os
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest
import pytest_asyncio

asyncssh = pytest.importorskip("asyncssh")
pytest.importorskip("claude_agent_sdk")

from ptc_agent.agent.claude_code import cli_install  # noqa: E402
from ptc_agent.agent.claude_code.cli_install import CliInstallError, CliPin, CliUnsupported, PlatformPin, ensure_cli  # noqa: E402
from ptc_agent.agent.claude_code.ssh_launcher import kill_if_cli  # noqa: E402
from ptc_agent.core.sandbox.providers._ssh_transport import Link  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
IMAGES = {
    "glibc": ("langalpha-ssh-target", REPO / "tests/integration/sandbox/ssh_target"),
    "busybox": ("langalpha-ssh-target-alpine", REPO / "tests/integration/sandbox/ssh_target_alpine"),
}
PLATFORM = "linux-x64"
ROOT = "/home/dev/langalpha"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.provider_docker,
    pytest.mark.slow,
    pytest.mark.enable_socket,
    pytest.mark.asyncio,
    pytest.mark.skipif(os.environ.get("CLAUDE_CODE_CLI_E2E") != "1", reason="set CLAUDE_CODE_CLI_E2E=1: downloads the release"),
    pytest.mark.skipif(shutil.which("docker") is None, reason="needs docker"),
]


def sh(*args: str, check: bool = True) -> str:
    return subprocess.run(args, check=check, capture_output=True, text=True).stdout.strip()


class Machine:
    def __init__(self, name: str, conn: asyncssh.SSHClientConnection) -> None:
        self.name, self.conn = name, conn
        self.link = Link(SimpleNamespace(label=name), conn)

    def root(self, *cmd: str) -> str:
        return sh("docker", "exec", self.name, "sh", "-c", " ".join(cmd))

    def curls(self) -> int:
        return int(self.root("cat /tmp/curl.log 2>/dev/null | wc -l") or 0)

    async def dev(self, cmd: str) -> str:
        return (await self.conn.run(cmd, check=True)).stdout.strip()


@pytest.fixture(scope="module")
def cache_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("cli-cache")


@pytest.fixture(scope="module")
def served(tmp_path_factory):
    """A local release host the containers reach at the bridge's gateway."""
    directory = tmp_path_factory.mktemp("served")
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(directory))
    handler.log_message = lambda *a: None
    server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    gateway = sh("docker", "network", "inspect", "bridge", "-f", "{{(index .IPAM.Config 0).Gateway}}")
    yield directory, f"http://{gateway}:{server.server_address[1]}"
    server.shutdown()


@contextlib.asynccontextmanager
async def started(kind: str):
    image, context = IMAGES[kind]
    if not sh("docker", "images", "-q", image):
        sh("docker", "build", "-q", "-t", image, str(context))
    key = asyncssh.generate_private_key("ssh-ed25519")
    name = f"cli-install-{kind}-{uuid.uuid4().hex[:6]}"
    sh("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::22", "-e",
       f"AUTHORIZED_KEYS={key.export_public_key().decode().strip()}", image)
    try:
        # Every curl the machine runs is logged before it runs.
        sh("docker", "exec", name, "sh", "-c",
           "mv /usr/bin/curl /usr/bin/curl.real && printf '#!/bin/sh\\necho \"$*\" >> /tmp/curl.log\\nexec /usr/bin/curl.real \"$@\"\\n'"
           " > /usr/bin/curl && chmod 755 /usr/bin/curl && touch /tmp/curl.log && chmod 666 /tmp/curl.log")
        port = int(sh("docker", "port", name, "22/tcp").rsplit(":", 1)[1])
        for _ in range(60):
            try:
                conn = await asyncssh.connect("127.0.0.1", port=port, username="dev", client_keys=[key], known_hosts=None)
                break
            except (OSError, asyncssh.Error):
                await asyncio.sleep(0.5)
        async with conn:
            yield Machine(name, conn)
    finally:
        sh("docker", "rm", "-f", name, check=False)


@pytest_asyncio.fixture(params=sorted(IMAGES))
async def machine(request):
    async with started(request.param) as m:
        yield m


@pytest_asyncio.fixture
async def glibc_machine():
    async with started("glibc") as m:
        yield m


async def timed(coro) -> tuple[str, float]:
    started = time.monotonic()
    result = await coro
    return result, round(time.monotonic() - started, 2)


async def test_the_installer_on_a_real_machine(glibc_machine, cache_dir, served):
    m, platform = glibc_machine, PLATFORM
    pin = cli_install.pinned()
    want = pin.platforms[platform]
    directory, local_url = served
    report: dict[str, object] = {"machine": m.name, "platform": platform}

    path, report["first_s"] = await timed(ensure_cli(m.link, ROOT, cache_dir=cache_dir))
    assert path == f"{ROOT}/_internal/claude-code/cli/{pin.version}/{platform}/claude"
    assert m.curls() == 1
    assert (await m.dev(f"sha256sum {path}")).split()[0] == want.sha256
    report["version"] = await m.dev(f"{path} --version")
    assert report["version"].startswith(pin.version)

    again, report["reuse_s"] = await timed(ensure_cli(m.link, ROOT, cache_dir=cache_dir))
    assert again == path and m.curls() == 1, "a ready copy is reused without a download"

    await m.dev(f"printf tampered >> {path}")
    _, report["wrong_size_s"] = await timed(ensure_cli(m.link, ROOT, cache_dir=cache_dir))
    assert m.curls() == 2
    assert (await m.dev(f"sha256sum {path}")).split()[0] == want.sha256

    await m.dev(f"rm {path}.verified && printf X | dd of={path} bs=1 seek=4096 conv=notrunc 2>/dev/null")
    _, report["same_size_unmarked_s"] = await timed(ensure_cli(m.link, ROOT, cache_dir=cache_dir))
    assert m.curls() == 3, "an unmarked copy is hashed, and one that fails is fetched again"
    assert (await m.dev(f"sha256sum {path}")).split()[0] == want.sha256

    # No curl: the backend downloads the release itself and sends it over SFTP.
    m.root("mv /usr/bin/curl /usr/bin/curl.off")
    fallback_root = "/home/dev/other-root"
    sent, report["fallback_s"] = await timed(ensure_cli(m.link, fallback_root, cache_dir=cache_dir))
    assert m.curls() == 3
    assert (await m.dev(f"sha256sum {sent}")).split()[0] == want.sha256
    assert (await m.dev(f"{sent} --version")).startswith(pin.version)
    assert (cache_dir / pin.version / platform / "claude").stat().st_size == want.size
    assert await m.dev(f"ls -A {fallback_root}/_internal/claude-code/cli/{pin.version}/{platform}") == "claude\nclaude.verified"
    m.root("mv /usr/bin/curl.off /usr/bin/curl")

    # A pin bump installs beside the copy in use; the next one prunes the oldest.
    cached = cache_dir / pin.version / platform / "claude"
    for version in ("9.9.1-test", "9.9.2-test"):
        target = directory / version / platform / "claude"
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            os.link(cached, target)
    cli = f"{ROOT}/_internal/claude-code/cli"
    bump = CliPin("9.9.1-test", pin.platforms)
    bumped, report["bump_s"] = await timed(ensure_cli(m.link, ROOT, pin=bump, base_url=local_url, cache_dir=cache_dir))
    assert bumped == f"{cli}/9.9.1-test/{platform}/claude" and m.curls() == 4
    assert (await m.dev(f"ls {cli}")).split() == [pin.version, "9.9.1-test"]
    await ensure_cli(m.link, ROOT, pin=CliPin("9.9.2-test", pin.platforms), base_url=local_url, cache_dir=cache_dir)
    assert (await m.dev(f"ls {cli}")).split() == ["9.9.1-test", "9.9.2-test"]

    # A release that does not match its pin is never installed, on the machine or from the backend.
    wrong = CliPin("9.9.2-test", {**pin.platforms, platform: PlatformPin(hashlib.sha256(b"x").hexdigest(), want.size)})
    await m.dev(f"rm -rf {cli}/9.9.2-test")
    with pytest.raises(CliInstallError, match="does not match its pinned SHA-256"):
        await ensure_cli(m.link, ROOT, pin=wrong, base_url=local_url, cache_dir=cache_dir)
    assert await m.dev(f"ls -A {cli}/9.9.2-test/{platform}") == ""

    # The machine's own Claude Code is never touched.
    assert await m.dev("ls -A ~/.claude ~/.claude.json ~/.local/bin/claude 2>/dev/null; true") == ""
    print(report)


async def test_a_busybox_machine_on_musl_is_refused_before_anything_is_fetched(served):
    directory, local_url = served
    content = b"not a real claude\n"
    pin = CliPin("9.7.0-fake", {PLATFORM: PlatformPin(hashlib.sha256(content).hexdigest(), len(content))})
    async with started("busybox") as m:
        with pytest.raises(CliUnsupported, match="runs musl"):
            await ensure_cli(m.link, ROOT, pin=pin, base_url=local_url)
        assert m.curls() == 0 and await m.dev(f"ls -A {ROOT} 2>/dev/null; true") == ""


async def test_the_installer_under_busybox_and_gnu_on_a_made_up_release(machine, served):
    """The lock, size, hash and marker steps on the userland each image has,
    against a small release. The BusyBox image is Alpine, which the installer
    refuses as musl, so a getconf there says glibc for the steps past that."""
    m = machine
    if m.name.startswith("cli-install-busybox"):
        m.root("printf '#!/bin/sh\\necho glibc 2.99\\n' > /usr/local/bin/getconf && chmod 755 /usr/local/bin/getconf")
    directory, local_url = served
    content = b"not a real claude\n" * 4096
    version = f"9.8.{uuid.uuid4().int % 1000}-fake"
    target = directory / version / PLATFORM / "claude"
    target.parent.mkdir(parents=True)
    target.write_bytes(content)
    pin = CliPin(version, {PLATFORM: PlatformPin(hashlib.sha256(content).hexdigest(), len(content))})
    path = f"{ROOT}/_internal/claude-code/cli/{version}/{PLATFORM}/claude"
    before = m.curls()

    assert await ensure_cli(m.link, ROOT, pin=pin, base_url=local_url) == path
    assert m.curls() == before + 1
    assert (await m.dev(f"sha256sum {path}")).split()[0] == pin.platforms[PLATFORM].sha256

    assert await ensure_cli(m.link, ROOT, pin=pin, base_url=local_url) == path
    assert m.curls() == before + 1, "a ready copy is reused without a download"

    await m.dev(f"printf tampered >> {path}")
    await ensure_cli(m.link, ROOT, pin=pin, base_url=local_url)
    assert m.curls() == before + 2

    await m.dev(f"rm {path}.verified && printf X | dd of={path} bs=1 seek=4096 conv=notrunc 2>/dev/null")
    await ensure_cli(m.link, ROOT, pin=pin, base_url=local_url)
    assert m.curls() == before + 3, "an unmarked copy is hashed, and one that fails is fetched again"
    assert (await m.dev(f"sha256sum {path}")).split()[0] == pin.platforms[PLATFORM].sha256


async def test_a_cli_is_signalled_by_its_pid_only_while_it_runs_there(machine):
    """The turn's stop signal on glibc and on BusyBox, whose ps takes no -p."""
    m = machine
    # Named sleep: BusyBox picks its applet by the name it runs as.
    cli = f"{ROOT}/stand-in/sleep"
    await m.dev(f"mkdir -p {ROOT}/stand-in && cp \"$(readlink -f \"$(command -v sleep)\")\" {cli}")
    pid = int(await m.dev(f"nohup {cli} 300 >/dev/null 2>&1 & echo $!"))
    await m.dev(kill_if_cli(pid, f"{ROOT}/other", "KILL"))
    assert await m.dev(f"kill -0 {pid} && echo alive") == "alive"
    await m.dev(kill_if_cli(pid, cli, "KILL"))
    for _ in range(20):
        if await m.dev(f"kill -0 {pid} 2>/dev/null && echo alive || true") == "":
            break
        await asyncio.sleep(0.1)
    assert await m.dev(f"kill -0 {pid} 2>/dev/null && echo alive || true") == ""
