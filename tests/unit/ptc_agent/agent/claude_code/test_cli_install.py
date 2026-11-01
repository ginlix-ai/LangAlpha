"""The CLI installer's contract, run against this machine through a real Link on a local stand-in connection.

The install script is the same bash a user's computer runs, sent through the
same Link; only the connection under it is local. Downloads come from a local HTTP server, and every curl
the script starts is logged, so a test can tell a reuse from a download.
"""

from __future__ import annotations

import asyncio
import fcntl
import functools
import hashlib
import http.server
import os
import shutil
import stat
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.agent.claude_code import cli_install
from tests.unit.ptc_agent.agent.claude_code.test_cli_pins import PLATFORMS, backend_platform
from ptc_agent.agent.claude_code.cli_install import CliInstallError, CliMissing, CliPin, CliUnsupported, PlatformPin, ensure_cli
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from ptc_agent.core.sandbox.providers import _ssh_transport
from ptc_agent.core.sandbox.providers._ssh_transport import Link
from tests.unit.ptc_agent.agent.claude_code.bsd_userland import bsd_path, on_linux
from tests.unit.ptc_agent.agent.claude_code.local_link import LocalConn, LocalSftp

CURL = shutil.which("curl")
pytestmark = [pytest.mark.macos_scripts, pytest.mark.skipif(CURL is None, reason="needs curl"), pytest.mark.enable_socket, pytest.mark.asyncio]


def release(content: bytes) -> PlatformPin:
    return PlatformPin(hashlib.sha256(content).hexdigest(), len(content))


class Env:
    def __init__(self, tmp: Path, url: str, served: Path) -> None:
        self.tmp, self.url, self.served = tmp, url, served
        self.root = str(tmp / "root")
        self.cache = tmp / "cache"
        self.log = tmp / "curl.log"
        tools = tmp / "tools"
        tools.mkdir()
        self.hashes = tmp / "sha256sum.log"
        self.held_lock = tmp / "curl-held-the-lock"
        hasher = "sha256sum" if shutil.which("sha256sum") else "shasum"
        for name, log in (("curl", self.log), (hasher, self.hashes)):
            wrapper = tools / name
            held = f"[ -e /dev/fd/9 ] && touch {self.held_lock}\n" if name == "curl" else ""
            wrapper.write_text(f'#!/bin/sh\necho "$*" >> {log}\n{held}exec {shutil.which(name)} "$@"\n')
            wrapper.chmod(0o755)
        self.path = f"{tools}:{os.environ['PATH']}"
        LocalSftp.sent = []

    def publish(self, version: str, content: bytes) -> CliPin:
        """Serve *content* as every platform's build of *version*, and the pin that names it."""
        for name in PLATFORMS:
            target = self.served / version / name / "claude"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        return CliPin(version, {name: release(content) for name in PLATFORMS})

    def link(self, path: str | None = None) -> Link:
        return Link(SimpleNamespace(label="local"), LocalConn(path or self.path))

    def without(self, *names: str) -> str:
        """A PATH holding every command this one does but *names*."""
        tools = self.tmp / ("without-" + "-".join(names))
        tools.mkdir(exist_ok=True)
        for directory in os.environ["PATH"].split(":"):
            if not os.path.isdir(directory):
                continue
            for entry in os.listdir(directory):
                link = tools / entry
                if entry not in names and not link.exists() and os.access(os.path.join(directory, entry), os.X_OK):
                    link.symlink_to(os.path.join(directory, entry))
        return str(tools)

    def downloads(self) -> int:
        return len(self.log.read_text().splitlines()) if self.log.exists() else 0

    def hashed(self) -> int:
        return len(self.hashes.read_text().splitlines()) if self.hashes.exists() else 0

    async def ensure(self, pin: CliPin, link: Link | None = None) -> str:
        return await ensure_cli(link or self.link(), self.root, pin=pin, base_url=self.url, cache_dir=self.cache)


@pytest.fixture
def env(tmp_path):
    served = tmp_path / "served"
    served.mkdir()
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(served))
    handler.log_message = lambda *a: None
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield Env(tmp_path, f"http://127.0.0.1:{server.server_address[1]}", served)
    finally:
        server.shutdown()


V1 = b"#!/bin/sh\necho claude one\n" + b"x" * 4096
V2 = b"#!/bin/sh\necho claude two\n" + b"y" * 4096
V3 = b"#!/bin/sh\necho claude three\n" + b"z" * 4096


async def test_installs_once_then_reuses_without_downloading(env):
    pin = env.publish("1.0.0", V1)
    path = await env.ensure(pin)
    sent = env.log.read_text().split()
    assert sent[sent.index("--speed-limit") + 1 : sent.index("--speed-time") + 2] == ["1024", "--speed-time", "60"]
    assert 60 <= int(sent[sent.index("--max-time") + 1]) < cli_install._RUN_TIMEOUT_S, "cut only near the run's own end"
    assert not env.held_lock.exists(), "curl never holds the install lock"
    assert path == f"{env.root}/_internal/claude-code/cli/1.0.0/{backend_platform()}/claude"
    assert Path(path).read_bytes() == V1
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o755
    assert (env.downloads(), env.hashed()) == (1, 1)
    assert await env.ensure(pin) == path
    assert (env.downloads(), env.hashed()) == (1, 1), "a marked copy of the right size is neither fetched nor hashed again"
    assert sorted(os.listdir(Path(path).parent)) == ["claude", "claude.verified"]


async def test_only_looking_never_installs_and_finds_a_ready_copy(env):
    """A status check looks for the CLI and never downloads it under its hold."""
    pin = env.publish("1.0.0", V1)
    with pytest.raises(CliMissing):
        await ensure_cli(env.link(), env.root, install=False, pin=pin, base_url=env.url, cache_dir=env.cache)
    assert env.downloads() == 0 and not os.path.exists(f"{env.root}/_internal/claude-code")
    path = await env.ensure(pin)
    assert await ensure_cli(env.link(), env.root, install=False, pin=pin, base_url=env.url, cache_dir=env.cache) == path
    assert env.downloads() == 1


async def test_a_copy_of_the_wrong_size_is_replaced(env):
    pin = env.publish("1.0.0", V1)
    path = await env.ensure(pin)
    with open(path, "ab") as f:
        f.write(b"tampered")
    assert await env.ensure(pin) == path
    assert Path(path).read_bytes() == V1
    assert env.downloads() == 2


async def test_without_its_marker_a_copy_is_hashed_again(env):
    pin = env.publish("1.0.0", V1)
    path = await env.ensure(pin)
    os.unlink(path + ".verified")
    assert await env.ensure(pin) == path
    assert env.downloads() == 1, "an intact copy is kept once its hash checks out"
    os.unlink(path + ".verified")
    Path(path).write_bytes(V1.replace(b"one", b"0ne"))
    assert await env.ensure(pin) == path
    assert Path(path).read_bytes() == V1
    assert env.downloads() == 2


async def test_a_new_pin_installs_beside_and_keeps_the_last_one_used(env):
    cli = Path(env.root) / "_internal/claude-code/cli"
    await env.ensure(env.publish("1.0.0", V1))
    two = await env.ensure(env.publish("2.0.0", V2))
    assert Path(two).read_bytes() == V2
    assert sorted(os.listdir(cli)) == [".lock", "1.0.0", "2.0.0"]
    three = await env.ensure(env.publish("3.0.0", V3))
    assert Path(three).read_bytes() == V3
    assert sorted(os.listdir(cli)) == [".lock", "2.0.0", "3.0.0"]


async def test_a_worker_on_the_previous_pin_keeps_its_copy(env):
    """Through a rolling deploy two pins run at once; neither prunes the other's."""
    cli = Path(env.root) / "_internal/claude-code/cli"
    one, two = env.publish("1.0.0", V1), env.publish("2.0.0", V2)
    await env.ensure(one)
    await env.ensure(two)
    await env.ensure(one)
    await env.ensure(two)
    assert sorted(os.listdir(cli)) == [".lock", "1.0.0", "2.0.0"]
    assert env.downloads() == 2


async def test_without_curl_the_backend_sends_its_copy(env):
    pin = env.publish("1.0.0", V1)
    path = await env.ensure(pin, env.link(env.without("curl")))
    assert Path(path).read_bytes() == V1
    assert env.downloads() == 0
    assert len(LocalSftp.sent) == 1 and not os.path.exists(LocalSftp.sent[0])
    cached = env.cache / "1.0.0" / backend_platform() / "claude"
    assert cached.read_bytes() == V1


def _noting(opener, calls: list[tuple[str, str, bool]]):
    """``opener``, noting its opens and every call on the files it opens as
    (path, call, whether it ran on the event loop's thread)."""

    def on_loop() -> bool:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False
        return True

    class Noted:
        def __init__(self, path: str, file) -> None:
            self.path, self.file = path, file

        def __getattr__(self, name: str):
            def call(*args, **kwargs):
                calls.append((self.path, name, on_loop()))
                return getattr(self.file, name)(*args, **kwargs)

            return call

        def __enter__(self):
            return self

        def __exit__(self, *exc) -> None:
            self.close()

    def opened(path, *args, **kwargs):
        calls.append((str(path), "open", on_loop()))
        return Noted(str(path), opener(path, *args, **kwargs))

    return opened


async def test_the_backend_copy_is_fetched_and_sent_with_its_file_calls_off_the_event_loop(env, monkeypatch):
    """The copy is about 290 MB: written or read on the event loop, it would
    hold up every other request on the worker while a slow disk caught up."""
    pin = env.publish("1.0.0", V1)
    link = env.link(env.without("curl"))
    calls: list[tuple[str, str, bool]] = []
    monkeypatch.setattr(Path, "open", _noting(Path.open, calls))
    monkeypatch.setattr(_ssh_transport, "open", _noting(open, calls), raising=False)
    await env.ensure(pin, link)
    copy = [(name, on_loop) for path, name, on_loop in calls if path.startswith(str(env.cache))]
    assert {"open", "write", "read", "close"} <= {name for name, _ in copy}
    assert [name for name, on_loop in copy if on_loop] == []


async def test_a_download_that_does_not_match_is_never_installed(env):
    env.publish("1.0.0", V1)
    wrong = CliPin("1.0.0", {name: PlatformPin(hashlib.sha256(b"other").hexdigest(), len(V1)) for name in PLATFORMS})
    with pytest.raises(CliInstallError, match="does not match its pinned SHA-256"):
        await env.ensure(wrong)
    folder = Path(env.root) / "_internal/claude-code/cli/1.0.0" / backend_platform()
    assert os.listdir(folder) == []
    assert not any((env.cache / "1.0.0").rglob("*claude*")) if (env.cache / "1.0.0").exists() else True


async def test_a_sent_copy_that_does_not_match_is_refused(env):
    pin = env.publish("1.0.0", V1)
    folder = Path(env.root) / "_internal/claude-code/cli/1.0.0" / backend_platform()
    folder.mkdir(parents=True)
    sent = folder / ".upload.test"
    sent.write_bytes(V1.replace(b"one", b"0ne"))
    with pytest.raises(CliInstallError, match="sent to this computer does not match"):
        await cli_install._run(env.link(), cli_install._script(pin, env.root, env.url, upload=str(sent)))
    assert os.listdir(folder) == []


async def test_a_platform_without_a_build_is_refused(env):
    pin = env.publish("1.0.0", V1)
    tools = env.tmp / "sun"
    tools.mkdir()
    (tools / "uname").write_text('#!/bin/sh\ncase "$1" in -s) echo SunOS ;; *) echo sun4v ;; esac\n')
    (tools / "uname").chmod(0o755)
    with pytest.raises(CliInstallError, match="Claude Code has no build for SunOS on sun4v"):
        await env.ensure(pin, env.link(f"{tools}:{env.path}"))
    assert not os.path.exists(f"{env.root}/_internal/claude-code")


@pytest.mark.parametrize("install", [True, False])
async def test_a_linux_on_musl_is_refused_before_anything_is_fetched(env, install):
    """A computer registered before the probe refused musl still gets no
    glibc build, which could not run there, and is told why in the probe's words."""
    pin = env.publish("1.0.0", V1)
    tools = env.tmp / "musl"
    tools.mkdir()
    (tools / "getconf").write_text("#!/bin/sh\nexit 1\n")
    (tools / "ldd").write_text("#!/bin/sh\necho 'musl libc (x86_64)' >&2\nexit 1\n")
    (tools / "uname").write_text('#!/bin/sh\ncase "$1" in -s) echo Linux ;; *) echo x86_64 ;; esac\n')
    for tool in tools.iterdir():
        tool.chmod(0o755)
    link = env.link(f"{tools}:{env.path}")
    with pytest.raises(CliUnsupported) as refused:
        await ensure_cli(link, env.root, install=install, pin=pin, base_url=env.url, cache_dir=env.cache)
    assert str(refused.value) == MUSL_UNSUPPORTED
    assert env.downloads() == 0 and not os.path.exists(f"{env.root}/_internal/claude-code")


async def test_two_turns_installing_at_once_download_once(env):
    pin = env.publish("1.0.0", V1)
    paths = await asyncio.gather(env.ensure(pin), env.ensure(pin))
    assert paths[0] == paths[1] and Path(paths[0]).read_bytes() == V1
    assert env.downloads() == 1
    assert sorted(os.listdir(Path(paths[0]).parent)) == ["claude", "claude.verified"]


async def test_a_ready_copy_does_not_wait_on_another_install(env):
    """A turn whose copy is ready is not held up by another pin's install holding the lock."""
    pin = env.publish("1.0.0", V1)
    path = await env.ensure(pin)
    lock = Path(env.root) / "_internal/claude-code/cli/.lock"
    with lock.open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        assert await asyncio.wait_for(env.ensure(pin), 10) == path


async def test_a_relative_root_is_refused(env):
    with pytest.raises(ValueError):
        await ensure_cli(env.link(), "langalpha", pin=env.publish("1.0.0", V1), base_url=env.url)


@on_linux
async def test_every_install_path_runs_on_a_macs_userland(env):
    """BSD tools read a ``--`` after an operand as a file, have no long options,
    stat -c or sha256sum, and a Mac has no flock: the download, a marked
    copy, an unmarked copy hashed again, and a sent copy all install there."""
    mac = env.link(bsd_path(env.tmp))
    pin = env.publish("1.0.0", V1)
    path = await env.ensure(pin, mac)
    assert Path(path).read_bytes() == V1 and stat.S_IMODE(os.stat(path).st_mode) == 0o755
    assert await env.ensure(pin, mac) == path
    os.unlink(path + ".verified")
    assert await env.ensure(pin, mac) == path and os.path.exists(path + ".verified")
    folder = Path(path).parent
    shutil.rmtree(folder)
    folder.mkdir()
    sent = folder / ".upload.test"
    sent.write_bytes(V1)
    outcome = await cli_install._run(mac, cli_install._script(pin, env.root, env.url, upload=str(sent)))
    assert (outcome.kind, outcome.path) == ("ready", path) and Path(path).read_bytes() == V1


@pytest.mark.parametrize("copy", ["download", "kept", "sent"])
async def test_a_copy_that_matches_but_cannot_be_put_in_place_says_so(env, copy):
    """Never as a mismatch, and never by asking for the backend's copy instead."""
    pin = env.publish("1.0.0", V1)
    folder = Path(env.root) / "_internal/claude-code/cli/1.0.0" / backend_platform()
    folder.mkdir(parents=True)
    script = cli_install._script(pin, env.root, env.url)
    if copy == "kept":
        (folder / "claude").write_bytes(V1)
    if copy == "sent":
        (folder / ".upload.test").write_bytes(V1)
        script = cli_install._script(pin, env.root, env.url, upload=str(folder / ".upload.test"))
    broken = env.tmp / "broken"
    broken.mkdir()
    (broken / "chmod").write_text("#!/bin/sh\nexit 1\n")
    (broken / "chmod").chmod(0o755)
    with pytest.raises(CliInstallError) as failed:
        await cli_install._run(env.link(f"{broken}:{env.path}"), script)
    assert str(failed.value).endswith(f"Claude Code could not be set up at {folder}.")
    assert "SHA-256" not in str(failed.value) and LocalSftp.sent == []
    assert env.downloads() == (1 if copy == "download" else 0), "a copy that matches is not fetched again"
    assert [name for name in os.listdir(folder) if name != "claude"] == []
