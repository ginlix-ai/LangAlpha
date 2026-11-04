"""The Claude Code CLI on a user's own computer, installed where only LangAlpha looks.

The CLI the SDK bundles is built for the backend's own platform, and a user's
computer may be a Mac or another architecture. So each computer gets Anthropic's
release build of exactly the version the pinned SDK speaks
(``cli_pins.json``, written from the release's signed manifest by
``scripts/utils/pin_claude_code_cli.py``), under the computer root's
``_internal``, which the agent's own tools cannot reach. The machine's own
Claude Code, its login and whatever version it runs, is the person's: it is
never looked up, run or touched.

The machine downloads the release itself, and only when the installed copy's
marker is missing or its size is wrong is the hash computed again, so a turn
pays one short exec. A machine that cannot download it is sent the backend's
copy over SFTP. Either way the machine checks the bytes against the pin before
the binary takes its name, so an unverified file is never run.

An installed copy is never replaced in place: a new pin installs beside it,
and once it is ready every older version goes but the most recently used one.
Deleting a running binary is safe on Linux and macOS (the process keeps the
inode), so the one exposure is a worker still on the previous pin starting it
after the new one landed, which keeping it covers through a rolling deploy;
only a second pin move inside one turn's lifetime could take it.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import os
import secrets
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import httpx

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers._ssh_bootstrap import LOCK_SH
from ptc_agent.core.sandbox.providers.ssh import IS_MUSL_SH, MUSL_UNSUPPORTED

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.providers._ssh_transport import Link

logger = logging.getLogger(__name__)

RELEASES_URL = "https://downloads.claude.ai/claude-code-releases"
_PINS_FILE = Path(__file__).with_name("cli_pins.json")
_SCRIPT_FILE = Path(__file__).with_name("install_cli.sh")
#: Long enough for one install to download the release on a slow link while
#: another waits on its lock.
_LOCK_WAIT_S = 900
_RUN_TIMEOUT_S = _LOCK_WAIT_S + 900
_UPLOAD_TIMEOUT_S = 1800
#: A crashed upload's partial copy is swept once it is older than any upload.
_UPLOAD_STALE_MIN = _UPLOAD_TIMEOUT_S // 60 + 30
_CHUNK = 1 << 20


class CliInstallError(RuntimeError):
    """The CLI could not be made ready on the machine; the message says why, in words for the user."""


class CliMissing(CliInstallError):
    """No ready CLI is on the machine, and the caller asked only to look (``install=False``)."""


class CliUnsupported(CliInstallError):
    """The machine runs musl, which no pinned build runs on, so trying again cannot help.
    A computer registered before the probe refused musl can still be one."""


#: How ``install_cli.sh`` exits on musl.
_MUSL_EXIT = 64


@dataclass(frozen=True)
class PlatformPin:
    sha256: str
    size: int


@dataclass(frozen=True)
class CliPin:
    version: str
    platforms: dict[str, PlatformPin]

    @classmethod
    def load(cls, path: Path = _PINS_FILE) -> CliPin:
        data = json.loads(path.read_text())
        return cls(
            version=data["version"],
            platforms={name: PlatformPin(p["sha256"], int(p["size"])) for name, p in data["platforms"].items()},
        )


@functools.cache
def pinned() -> CliPin:
    return CliPin.load()


def cli_dir(root: str) -> str:
    return f"{SandboxLayout.for_root(root).claude_code}/cli"


def default_cache_dir() -> Path:
    """Where the backend keeps the copies it sends to machines that cannot download one."""
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "langalpha" / "claude-code-cli"


@functools.cache
def _install_sh() -> str:
    return _SCRIPT_FILE.read_text()


def _script(pin: CliPin, root: str, base_url: str, upload: str = "", *, install: bool = True) -> str:
    """``install_cli.sh`` with its arguments: bash that makes the pinned CLI
    ready, printing ``ready <path>``, ``missing <dir>`` or ``upload <dir>``.

    *upload* is the path a copy sent from the backend was put at; *install*
    False only looks, so a status check never starts a download.
    """
    pins = [word for name, p in sorted(pin.platforms.items()) for word in (name, p.sha256, str(p.size))]
    args = [cli_dir(root), pin.version, base_url.rstrip("/"), upload, "1" if install else "0",
            str(_LOCK_WAIT_S), str(_UPLOAD_STALE_MIN), str(_RUN_TIMEOUT_S), *pins]
    return f"set -- {shlex.join(args)}\n{LOCK_SH}{IS_MUSL_SH}{_install_sh()}"


@dataclass(frozen=True)
class _Outcome:
    kind: Literal["ready", "missing", "upload"]
    path: str
    platform: str


async def _run(link: Link, script: str) -> _Outcome:
    """Run *script* in a command slot, since a download can take minutes."""
    try:
        result = await link.run(script, timeout=_RUN_TIMEOUT_S, slot=link.slots)
    except TimeoutError as e:
        raise CliInstallError("Installing Claude Code on this computer took too long.") from e
    if result.exit_code == _MUSL_EXIT:
        raise CliUnsupported(MUSL_UNSUPPORTED)
    out = result.stdout.decode(errors="replace").splitlines()
    err = result.stderr.decode(errors="replace").strip()
    found = {key: value for key, _, value in (line.partition(" ") for line in out)}
    kind = next((k for k in ("ready", "missing", "upload") if k in found), None)
    if result.exit_code != 0 or kind is None:
        detail = err.splitlines()[-1] if err else f"exit {result.exit_code}"
        raise CliInstallError(f"Claude Code could not be installed on this computer: {detail}")
    if err:
        logger.info("[ClaudeCode] CLI install on %s: %s", found.get("platform"), err)
    return _Outcome(kind, found[kind], found["platform"])


def _matches(path: Path, want: PlatformPin) -> bool:
    if not path.is_file() or path.stat().st_size != want.size:
        return False
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest() == want.sha256


async def backend_copy(pin: CliPin, platform: str, *, base_url: str = RELEASES_URL, cache_dir: Path | None = None) -> Path:
    """The release binary for *platform* on the backend's disk, downloaded once and hash-checked on every use.
    Every file call runs in a thread, since a large write on a slow disk would hold up the event loop."""
    want = pin.platforms[platform]
    path = (cache_dir or default_cache_dir()) / pin.version / platform / "claude"
    if await asyncio.to_thread(_matches, path, want):
        return path
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    tmp = path.with_name(f".download.{secrets.token_hex(8)}")
    url = f"{base_url.rstrip('/')}/{pin.version}/{platform}/claude"
    digest, size = hashlib.sha256(), 0
    try:
        f = await asyncio.to_thread(tmp.open, "wb")
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(60, connect=20)) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes(_CHUNK):
                        size += len(chunk)
                        if size > want.size:
                            break
                        digest.update(chunk)
                        await asyncio.to_thread(f.write, chunk)
        finally:
            await asyncio.to_thread(f.close)
        if size != want.size or digest.hexdigest() != want.sha256:
            raise CliInstallError(f"The download of Claude Code {pin.version} for {platform} does not match its pinned SHA-256.")
        await asyncio.to_thread(os.replace, tmp, path)
    except httpx.HTTPError as e:
        raise CliInstallError(f"Claude Code {pin.version} for {platform} could not be downloaded: {e}") from e
    finally:
        await asyncio.to_thread(tmp.unlink, missing_ok=True)
    return path


async def ensure_cli(
    link: Link,
    root: str,
    *,
    install: bool = True,
    pin: CliPin | None = None,
    base_url: str = RELEASES_URL,
    cache_dir: Path | None = None,
) -> str:
    """The path of a verified pinned CLI on the machine *link* reaches, installing it under *root* first if needed.

    Call it when a Claude Code turn or check is about to start the CLI, never
    ahead: nothing is copied to a computer that does not use Claude Code. With
    *install* False it only looks, one short exec however the machine stands,
    for a check that must not outlast its hold.

    Raises:
        CliMissing: *install* is False and no ready copy is there.
        CliUnsupported: The machine runs musl.
        CliInstallError: The machine's platform has no build, or no copy could
            be fetched and verified there.
    """
    if not root.startswith("/"):
        raise ValueError(f"root must be absolute: {root!r}")
    pin = pin or pinned()
    outcome = await _run(link, _script(pin, root, base_url, install=install))
    if outcome.kind == "ready":
        return outcome.path
    if outcome.kind == "missing":
        raise CliMissing("Claude Code is not installed on this computer yet.")
    local = await backend_copy(pin, outcome.platform, base_url=base_url, cache_dir=cache_dir)
    remote = f"{outcome.path}/.upload.{secrets.token_hex(8)}"
    try:
        await link.upload(str(local), remote, timeout=_UPLOAD_TIMEOUT_S)
    except TimeoutError as e:
        raise CliInstallError("Sending Claude Code to this computer took too long.") from e
    outcome = await _run(link, _script(pin, root, base_url, upload=remote))
    if outcome.kind != "ready":
        raise CliInstallError("Claude Code could not be installed on this computer.")
    return outcome.path
