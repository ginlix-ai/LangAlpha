"""SSH provider: computers on Linux machines and Macs their owners bring, reached over SSH.

The machine belongs to the person who added it, so the provider is the
opposite of a hosted one in the ways that matter: nothing the deployment holds
is sent there unless its operator vouches for every machine (``trusted_host``,
from ``sandbox.ssh.trusted_host``), a lost machine waits for its owner rather
than being rebuilt (``recreatable``), and stopping a computer ends only its
own processes, never the machine.

Each user has one client key, which the server keeps and hands over through
the loader it registers in every worker (:func:`set_client_key_loader`). A
computer's settings name the key's owner, never the key, so the key stays out
of anything hashed or returned to a client. The machine's host key is pinned
when the computer is added (:func:`probe_ssh_target` reports it), and a
connection presenting any other is refused.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import asyncssh

from ptc_agent.config.core import PlatformSecretDefinition, SSHConfig
from ptc_agent.core.paths import DEFAULT_SANDBOX_ROOT
from ptc_agent.core.sandbox.providers._ssh_connect import (
    GatewayAccess,
    SSHTarget,
    answered_by_helper,
    connect,
    host_key_line,
    is_public_address,
    set_gateway_loader,
)
from ptc_agent.core.sandbox.providers._ssh_shell import (
    SUDO_CHECK,
    claim_root_script,
    computer_dir,
    knows_computer_script,
    computers_dir,
    delete_script,
    folder_marked_message,
    home_problem,
    root_dir_error,
    root_dir_script,
    stop_script,
)
from ptc_agent.core.sandbox.providers._ssh_transport import TRANSPORT_ERRORS, Link, pool
from ptc_agent.core.sandbox.providers.ssh_errors import (
    MACHINE_ID_PATTERN,
    SSHConnectError,
    SSHFolderMarked,
    connect_failure,
)
from ptc_agent.core.sandbox.providers.ssh_runtime import (
    SUPPORTED_ARCHES,
    SUPPORTED_OS,
    SSHRuntime,
)
from ptc_agent.core.sandbox.runtime import (
    SandboxFailureKind,
    SandboxHostUnavailableError,
    SandboxProvider,
    SandboxTransientError,
)

__all__ = [
    "ClientKeyLoader",
    "GatewayAccess",
    "SSHClientKey",
    "SSHConnectError",
    "SSHProbe",
    "SSHProvider",
    "SSHRuntime",
    "generate_client_key",
    "is_public_address",
    "probe_ssh_target",
    "public_key_fingerprint",
    "run_script",
    "set_client_key_loader",
    "set_gateway_loader",
    "ssh_target",
]

ClientKeyLoader = Callable[[str], Awaitable[str]]

# Set once per worker by the server; execution context, not state.
_client_key_loader: ClientKeyLoader | None = None

# What the runtime and its bootstrap need on the machine beyond bash. macOS
# has no flock, setsid or timeout, and the runtime does without them there.
_REQUIRED_TOOLS = (
    "tar",
    "gzip",
    "sha256sum",
    "flock",
    "setsid",
    "timeout",
    "base64",
    "install",
    "mktemp",
)
_REQUIRED_TOOLS_DARWIN = (
    "tar",
    "gzip",
    "shasum",
    "base64",
    "perl",
    "install",
    "mktemp",
)
# The installed runtime takes about this much of the home's filesystem.
_DISK_NEEDED_BYTES = 3 * 1024**3
# What the Computers dialog shows of a machine: its system and its hardware.
# Read by the probe, and again as the machine starts, since a system updates.
MACHINE_FACTS_SCRIPT = r"""
echo "arch=$(uname -m)"
if [ "$(uname -s)" = Darwin ]; then echo "distro=macOS $(sw_vers -productVersion 2>/dev/null)"
echo "model=$(system_profiler -json SPHardwareDataType 2>/dev/null | sed -n 's/^[[:space:]]*"machine_name"[[:space:]]*:[[:space:]]*\("[^"\\]*\(\\.[^"\\]*\)*"\).*/\1/p' | head -n 1)"
echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
echo "cores=$(sysctl -n hw.ncpu 2>/dev/null)"
echo "memory=$(sysctl -n hw.memsize 2>/dev/null)"
else echo "distro=$( . /etc/os-release 2>/dev/null; echo "${PRETTY_NAME:-}")"
echo "cpu=$({ sed -n 's/^model name[[:space:]]*:[[:space:]]*//p' /proc/cpuinfo; lscpu | sed -n 's/^Model name:[[:space:]]*//p'; } 2>/dev/null | head -n 1)"
echo "cores=$(nproc 2>/dev/null)"
echo "memory=$(awk '/^MemTotal:/ {printf "%.0f", $2 * 1024}' /proc/meminfo 2>/dev/null)"; fi
"""
_PROBE_SCRIPT = (r"""
echo "os=$(uname -s)"
echo "hostname=$(hostname 2>/dev/null || uname -n)"
""" + MACHINE_FACTS_SCRIPT + r"""
h=$(cd -P -- "$HOME" 2>/dev/null && env pwd -P) || h=$HOME; echo "home=$h"
echo "shell=${SHELL:-}"
echo "uid=$(id -u)"
if [ "$(id -u)" = 0 ]; then echo sudo=1
elif e=$(LC_ALL=C sudo -n true </dev/null 2>&1 >/dev/null); then echo sudo=1
else case "$e" in *tty*) echo sudo=tty ;; *) echo sudo=0 ;; esac; fi
if command -v curl >/dev/null 2>&1 || command -v wget >/dev/null 2>&1; then echo fetch=1; fi
tools="TOOLS"; [ "$(uname -s)" = Darwin ] && tools="DARWIN_TOOLS"
m=""; for t in $tools; do command -v "$t" >/dev/null 2>&1 || m="$m $t"; done; echo "missing=${m# }"
[ -r /proc/sys/kernel/random/boot_id ] && echo bootid=1
[ -n "$(sysctl -n kern.bootsessionuuid 2>/dev/null)" ] && echo bootid=1
d="$HOME/.langalpha"; [ -d "$d" ] || d="$HOME"
echo "disk=$(df -Pk "$d" 2>/dev/null | awk 'NR==2 {print $4 * 1024}')"
""").replace("DARWIN_TOOLS", " ".join(_REQUIRED_TOOLS_DARWIN)).replace(
    "TOOLS", " ".join(_REQUIRED_TOOLS)
)
_KEY_LINE = re.compile(r"^\S+ \S+")
# Two short lines are all the probe reads from a login shell without bash.
_FALLBACK_OUTPUT_CAP = 4096


@dataclass(frozen=True)
class SSHClientKey:
    """A client key pair, as the server stores it."""

    private_key: str  # OpenSSH private key text
    public_key: str  # "ssh-ed25519 AAAA... <comment>"
    fingerprint: str  # "SHA256:..."


def generate_client_key(comment: str) -> SSHClientKey:
    """A new ed25519 key pair, its public half ready for authorized_keys."""
    key = asyncssh.generate_private_key("ssh-ed25519", comment=comment)
    return SSHClientKey(
        private_key=key.export_private_key("openssh").decode(),
        public_key=key.export_public_key("openssh").decode().strip(),
        fingerprint=key.get_fingerprint("sha256"),
    )


def public_key_fingerprint(public_key_line: str) -> str:
    """The SHA256 fingerprint of an OpenSSH public key line.

    Raises:
        ValueError: The line is not a public key.
    """
    try:
        return asyncssh.import_public_key(public_key_line.strip()).get_fingerprint(
            "sha256"
        )
    except (asyncssh.KeyImportError, ValueError) as e:
        raise ValueError(f"Not an SSH public key: {e}") from None


def set_client_key_loader(loader: ClientKeyLoader | None) -> None:
    """Register how this worker reads a user's private key, by user id."""
    global _client_key_loader
    _client_key_loader = loader


@dataclass(frozen=True)
class SSHProbe:
    """What one connection learned about a machine before it is added."""

    host_key: str
    fingerprint: str
    os: str
    arch: str
    distro: str
    home: str
    shell: str
    is_root: bool
    has_sudo: bool
    disk_free_bytes: int | None
    supported: bool
    problems: list[str] = field(default_factory=list)
    hostname: str = ""
    # LangAlpha's helper answers for the machine, not an sshd.
    helper: bool = False
    # A Mac's model name ("MacBook Pro", "Mac Studio"), empty elsewhere. Only
    # the Computers dialog reads it, to draw the machine.
    model: str = ""
    # The processor as the system names it, its cores, and the memory. Only
    # the Computers dialog reads them; empty or None when not read.
    cpu: str = ""
    cores: int | None = None
    memory_bytes: int | None = None
    # Whether the machine holds the computer the probe was asked about
    # (``known_as``); False when it was asked about none.
    knows_computer: bool = False

    def machine(self) -> dict[str, Any]:
        """The facts kept on the computer for its details, only those read."""
        return machine_facts(
            {"distro": self.distro, "arch": self.arch, "cpu": self.cpu}
            | ({"cores": str(self.cores)} if self.cores else {})
            | ({"memory": str(self.memory_bytes)} if self.memory_bytes else {})
        )


async def probe_ssh_target(
    *,
    host: str,
    port: int,
    username: str,
    private_key: str,
    host_key: str | None = None,
    connect_timeout: float = 10,
    allow_private_addresses: bool = False,
    root_dir: str | None = None,
    tunnel: str | None = None,
    reclaim: Collection[str] = (),
    taking_over: str | None = None,
    known_as: str | None = None,
) -> SSHProbe:
    """Log in once and report whether the machine can host a computer.

    ``host_key`` None is first contact: whatever key the machine presents is
    accepted and reported, for its owner to confirm and pin. Given, it must
    match. ``problems`` holds sentences for the owner; ``supported`` is False
    when any of them rules the machine out, and a low disk alone does not.

    ``allow_private_addresses`` is ``SSHConfig.allow_private_addresses``:
    off, a host that is or resolves to a private address is refused before
    any connection, with code ``private_address``.

    ``root_dir``, when given, is the folder a computer would work in, checked
    without being made on a machine that is otherwise supported. ``reclaim``
    names computers the caller removed: one whose removal never reached the
    machine still marks its folder, and is cleared from it as that removal
    would have done (see :func:`_check_root`). A mark naming ``taking_over``
    passes the check untouched: that computer is cleared only once its owner's
    create is decided, by :func:`take_over_root`.

    ``known_as``, a computer's id on its machine, sets ``knows_computer``
    when the machine holds that computer (see :func:`knows_computer_script`),
    read before any reclaim touches the folder.

    ``tunnel`` reaches the machine through the gateway, where ``host`` is only
    its name and ``port`` its sshd's port on the machine.

    Raises:
        SSHConnectError: The machine could not be reached or refused the key,
            its host key differs from ``host_key``, its address is private,
            or ``root_dir`` cannot be the login user's (code ``root_dir``).
    """
    target = SSHTarget(
        host=host,
        port=port,
        username=username,
        host_key=host_key or None,
        key_owner="",
        connect_timeout=connect_timeout,
        allow_private=allow_private_addresses,
        tunnel=tunnel or None,
    )
    async with _one_off(target, private_key) as link:
        try:
            line, fingerprint = host_key_line(link.conn)
            facts = await _probe_facts(link, connect_timeout)
            sftp_ok = await _sftp_works(link.conn, connect_timeout)
            result = _probe_result(
                line, fingerprint, facts, sftp_ok, helper=answered_by_helper(link.conn)
            )
            if known_as and result.home:
                result = replace(
                    result,
                    knows_computer=await _knows_computer(
                        link, result.home, known_as, root_dir, connect_timeout
                    ),
                )
            if root_dir and result.supported:
                # Checked, never made: the create makes it, by the same rule.
                exit_code, owner = await _check_root(
                    link,
                    root_dir,
                    home=result.home,
                    reclaim=reclaim,
                    timeout=connect_timeout + 20,
                )
                if exit_code == 10 and taking_over and owner == taking_over:
                    return result
                if exit_code == 10 and re.match(MACHINE_ID_PATTERN, owner):
                    raise SSHFolderMarked(
                        folder_marked_message(root_dir, host), owner
                    )
                error = root_dir_error(
                    exit_code, path=root_dir, user=username, host=host
                )
                if error is not None:
                    raise error
        except TimeoutError:
            raise SSHConnectError(
                "timeout", f"{host} stopped answering while it was being checked."
            ) from None
        except TRANSPORT_ERRORS as e:
            raise SSHConnectError(
                "unreachable",
                f"{host} closed the connection while it was being checked.",
            ) from e
    return result


async def _knows_computer(
    link: Link, home: str, computer_id: str, root_dir: str | None, timeout: float
) -> bool:
    try:
        script = knows_computer_script(home, computer_id, root_dir)
    except SSHConnectError:
        # A home no computer can live in holds none.
        return False
    found = await link.run(script, timeout=timeout)
    return found.stdout.decode(errors="replace").strip() == "1"


async def take_over_root(
    *,
    host: str,
    port: int,
    username: str,
    private_key: str,
    host_key: str,
    root_dir: str,
    owner: str,
    connect_timeout: float = 10,
    allow_private_addresses: bool = False,
    tunnel: str | None = None,
    claim_as: str | None = None,
    forget: Sequence[str] = (),
) -> None:
    """Stop *owner*, the computer whose mark is on *root_dir*, and clear it
    from the folder, for a create its owner chose to take the folder over.

    Only that step, after the create's own probe already checked the machine,
    so it is short enough to run under the lock the insert holds. A mark
    there of any other computer is refused as the probe refuses it.

    *claim_as*, a computer that already has its id on the machine, marks the
    folder as its own at once (see :func:`claim_root_script`, which *forget*
    is passed to); a create's computer marks it at its first start, and so
    does one with no id yet, whose *forget* still runs.

    Raises:
        SSHFolderMarked: Another computer's mark is on the folder now.
        SSHConnectError: As :func:`probe_ssh_target` raises it.
    """
    target = SSHTarget(
        host=host,
        port=port,
        username=username,
        host_key=host_key,
        key_owner="",
        connect_timeout=connect_timeout,
        allow_private=allow_private_addresses,
        tunnel=tunnel or None,
    )
    async with _one_off(target, private_key) as link:
        try:
            found = await link.run(
                'cd -P -- "$HOME" 2>/dev/null && env pwd -P || printf "%s\\n" "$HOME"',
                timeout=connect_timeout,
            )
            home = found.stdout.decode(errors="replace").strip()
            exit_code, marked = await _check_root(
                link, root_dir, home=home, reclaim=(owner,), timeout=connect_timeout + 20
            )
            if exit_code == 10 and re.match(MACHINE_ID_PATTERN, marked):
                raise SSHFolderMarked(folder_marked_message(root_dir, host), marked)
            error = root_dir_error(exit_code, path=root_dir, user=username, host=host)
            if error is not None:
                raise error
            if claim_as or forget:
                # Under the hold starts take: one that took the folder since
                # it was cleared keeps it, and the take-over is refused.
                claimed = await link.run(
                    claim_root_script(
                        root_dir,
                        claim_as,
                        computers=computers_dir(home),
                        cleared=owner,
                        forget=forget,
                    ),
                    timeout=connect_timeout + 20,
                )
                taker = claimed.stdout.decode(errors="replace").strip()
                if claimed.exit_code == 10 and re.match(MACHINE_ID_PATTERN, taker):
                    raise SSHFolderMarked(folder_marked_message(root_dir, host), taker)
                error = root_dir_error(
                    claimed.exit_code, path=root_dir, user=username, host=host
                )
                if error is not None:
                    raise error
        except TimeoutError:
            raise SSHConnectError(
                "timeout", f"{host} stopped answering while it was being checked."
            ) from None
        except TRANSPORT_ERRORS as e:
            raise SSHConnectError(
                "unreachable",
                f"{host} closed the connection while it was being checked.",
            ) from e


async def _check_root(
    link: Link,
    root_dir: str,
    *,
    home: str,
    reclaim: Collection[str],
    timeout: float,
) -> tuple[int | None, str]:
    """``root_dir_script``'s exit for *root_dir*, which it checks, and the id
    of the computer whose mark refused it ("" for any other exit).

    A folder marked by one of *reclaim*, a computer whose removal could not
    reach the machine, holds what that removal would have taken: the
    computer's own folder, its processes, and the credentials it kept in
    the root. Those go, as its delete would have taken them, and the folder
    is checked again. A mark naming any other computer is refused as before.
    """
    script = root_dir_script(root_dir, computers=computers_dir(home), computer_id=None)
    check = await link.run(script, timeout=timeout)
    owner = check.stdout.decode(errors="replace").strip()
    if check.exit_code != 10 or owner not in reclaim:
        return check.exit_code, owner if check.exit_code == 10 else ""
    cdir = computer_dir(home, owner)
    stop = await link.run(
        stop_script(cdir, root=False, computer_root=root_dir), timeout=timeout
    )
    if b"has-root" in stop.stdout:
        sudo = await link.run(SUDO_CHECK, timeout=timeout)
        if sudo.exit_code == 0:
            await link.run(stop_script(cdir, root=True), timeout=timeout, root=True)
    await link.run(delete_script(cdir, root=root_dir), timeout=timeout)
    again = await link.run(script, timeout=timeout)
    return again.exit_code, again.stdout.decode(errors="replace").strip()


async def run_script(
    config: SSHConfig, private_key: str, script: str, *, timeout: float
) -> int | None:
    """Log in once to *config*'s pinned target and run *script* in bash.

    For a one-off outside any computer's runtime, such as taking away the
    tunnel of a removed computer. Returns the script's exit status, None when
    it ended without one.

    Raises:
        SSHConnectError: As for :func:`probe_ssh_target`.
        ValueError: *config* lacks its target or pinned host key.
        TimeoutError: The script ran past *timeout*.
    """
    async with _one_off(ssh_target(config), private_key) as link:
        return (await link.run(script, timeout=timeout + 20)).exit_code


@contextlib.asynccontextmanager
async def removal_link(config: SSHConfig) -> AsyncIterator[Link | None]:
    """The connection a removed computer's delete ran on, closed once the
    steps after it are done; None when none is open.

    Those steps (taking the key's line away, then the tunnel) run there rather
    than on a new login: a machine the delete could not reach is not dialed
    again, and the key going first cannot lock out the uninstall after it.

    Raises:
        ValueError: As for :func:`ssh_target`.
    """
    target = ssh_target(config)
    try:
        yield pool().peek(target)
    finally:
        # Other computers on the login may share it; the idle sweep has those.
        pool().retire(target, only_if_idle=True)


@contextlib.asynccontextmanager
async def _one_off(target: SSHTarget, private_key: str) -> AsyncIterator[Link]:
    """A connection of its own, outside the pool, closed when the call is done.

    Neither a probe nor a one-off script is a computer's, so neither may
    keep or reuse a connection a computer would share.
    """
    conn = await connect(target, private_key)
    try:
        yield Link(target, conn)
    finally:
        conn.close()
        await conn.wait_closed()


async def _probe_facts(link: Link, timeout: float) -> dict[str, str]:
    """Key=value facts from the probe script, or from ``uname`` without bash."""
    result = await link.run(_PROBE_SCRIPT, timeout=timeout + 20)
    facts: dict[str, str] = {}
    if result.exit_code == 0:
        for raw in result.stdout.decode(errors="replace").splitlines():
            key, sep, value = raw.partition("=")
            if sep:
                facts[key] = value
        facts["bash"] = "1"
        return facts
    # No bash, or a login shell that refuses it: say what the machine is.
    async with asyncio.timeout(timeout + 10):
        out = await _login_shell_output(link.conn, "uname -s; uname -m")
    lines = out.splitlines()
    if len(lines) >= 2:
        facts["os"], facts["arch"] = lines[0].strip(), lines[1].strip()
    return facts


async def _login_shell_output(conn: Any, command: str) -> str:
    """The first ``_FALLBACK_OUTPUT_CAP`` bytes *command* prints in the login
    shell; the rest is never read, since the machine chooses how much comes."""
    process = await conn.create_process(
        command, encoding=None, stdin=asyncssh.DEVNULL, stderr=asyncssh.DEVNULL
    )
    out = b""
    try:
        while len(out) < _FALLBACK_OUTPUT_CAP:
            chunk = await process.stdout.read(_FALLBACK_OUTPUT_CAP - len(out))
            if not chunk:
                break
            out += chunk
    finally:
        process.close()
    return out.decode(errors="replace")


async def _sftp_works(conn: Any, timeout: float) -> bool:
    try:
        async with asyncio.timeout(timeout + 10):
            sftp = await conn.start_sftp_client()
            sftp.exit()
        return True
    except (asyncssh.Error, OSError, TimeoutError):
        return False


def _mac_model(raw: str) -> str:
    """The model name from the probe's ``model=`` line: ``machine_name``'s JSON
    string as ``system_profiler -json`` gives it. The JSON keys, unlike the
    text report's labels, are the same in every language a Mac is set to."""
    try:
        name = json.loads(raw) if raw.startswith('"') else ""
    except ValueError:
        return ""
    return " ".join(name.split())[:64] if isinstance(name, str) else ""


def machine_facts(facts: dict[str, str]) -> dict[str, Any]:
    """``MACHINE_FACTS_SCRIPT``'s lines as the computer keeps them, leaving out
    any the machine did not answer. The model is the drawing's, kept apart."""
    out: dict[str, Any] = {}
    for key in ("distro", "arch", "cpu"):
        value = " ".join(facts.get(key, "").split())[:80]
        # A virtual machine's lscpu names its processor "-".
        if re.search(r"[0-9A-Za-z]", value) and value != "macOS":
            out[key] = value
    for key, name in (("cores", "cores"), ("memory", "memory_bytes")):
        value = facts.get(key, "").strip()
        if value.isdigit() and int(value) > 0:
            out[name] = int(value)
    return out


def read_machine(stdout: str) -> tuple[str, dict[str, Any]]:
    """``MACHINE_FACTS_SCRIPT``'s output: the Mac's model name (empty
    elsewhere) and the facts ``machine_facts`` keeps."""
    facts: dict[str, str] = {}
    for line in stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            facts[key] = value
    return _mac_model(facts.get("model", "")), machine_facts(facts)


def _probe_result(
    line: str,
    fingerprint: str,
    facts: dict[str, str],
    sftp_ok: bool,
    *,
    helper: bool = False,
) -> SSHProbe:
    os_name = facts.get("os", "")
    arch = facts.get("arch", "")
    home = facts.get("home", "")
    disk = facts.get("disk", "")
    disk_free = int(disk) if disk.isdigit() else None
    blocking: list[str] = []
    if os_name not in SUPPORTED_OS:
        blocking.append(
            f"Only Linux and macOS machines can host a computer for now; this "
            f"one runs {os_name or 'an unknown system'}."
        )
    elif arch not in SUPPORTED_ARCHES:
        blocking.append(
            f"The {arch or 'unknown'} processor architecture is not supported; "
            "use an x86_64 or arm64 machine."
        )
    if os_name in SUPPORTED_OS:
        if facts.get("bash") != "1":
            blocking.append("bash is not installed, and every command runs in it.")
        else:
            if facts.get("fetch") != "1":
                blocking.append(
                    "Neither curl nor wget is installed, so the Python runtime "
                    "cannot be downloaded."
                )
            missing = facts.get("missing", "").split()
            if missing:
                blocking.append(
                    f"Missing commands the runtime needs: {', '.join(missing)}."
                )
            problem = home_problem(home)
            if problem is not None:
                blocking.append(problem)
            if facts.get("bootid") != "1":
                blocking.append(
                    "kern.bootsessionuuid is not readable."
                    if os_name == "Darwin"
                    else "/proc/sys/kernel/random/boot_id is not readable."
                )
        if not sftp_ok:
            blocking.append(
                "SFTP did not start. File transfers need sshd's SFTP subsystem, "
                "and the login shell's startup files must not print anything."
            )
    problems = list(blocking)
    if disk_free is not None and disk_free < _DISK_NEEDED_BYTES:
        problems.append(
            f"Only {disk_free / 1024**3:.1f} GB is free in {home}; the Python "
            "runtime needs about 3 GB."
        )
    if facts.get("sudo") == "tty":
        problems.append(
            "sudo for this account requires a terminal (requiretty in sudoers), "
            "so commands that need root cannot run; everything else works. "
            "Turning requiretty off for the account lifts it."
        )
    uid = facts.get("uid", "")
    read = machine_facts(facts)
    return SSHProbe(
        host_key=line,
        fingerprint=fingerprint,
        os=os_name,
        arch=arch,
        distro=facts.get("distro", ""),
        home=home,
        shell=facts.get("shell", ""),
        is_root=uid == "0",
        has_sudo=facts.get("sudo") == "1",
        disk_free_bytes=disk_free,
        supported=not blocking,
        problems=problems,
        hostname=facts.get("hostname", ""),
        helper=helper,
        model=_mac_model(facts.get("model", "")),
        cpu=read.get("cpu", ""),
        cores=read.get("cores"),
        memory_bytes=read.get("memory_bytes"),
    )


def ssh_target(config: SSHConfig) -> SSHTarget:
    """The pinned login a computer's settings name.

    Raises:
        ValueError: The settings lack the target or hold no valid host key.
    """
    missing = [
        name
        for name in ("host", "username", "host_key", "key_owner")
        if not getattr(config, name)
    ]
    if missing:
        raise ValueError(f"SSH computer settings lack {', '.join(missing)}")
    if not _KEY_LINE.match(config.host_key.strip()):
        raise ValueError("SSH computer settings hold an invalid host key")
    return SSHTarget(
        host=config.host,
        port=config.port,
        username=config.username,
        host_key=config.host_key.strip(),
        key_owner=config.key_owner,
        connect_timeout=config.connect_timeout,
        keepalive_interval=config.keepalive_interval,
        keepalive_count_max=config.keepalive_count_max,
        allow_private=config.allow_private_addresses,
        tunnel=config.tunnel or None,
    )


class SSHProvider(SandboxProvider):
    """Provider for computers on SSH machines.

    Construction does no I/O, and neither does ``get``: the server builds a
    provider per computer just to classify an error. Connections are pooled
    per process, so ``close`` leaves them to the runtimes that share them.
    """

    label = "SSH"
    runs_remote = True
    # The machine has no runtime cap; it runs as long as its owner keeps it on.
    holds_always_on = True
    recreatable = False
    metered = False
    converges_env = True
    # The file mount needs root and /dev/fuse, which the owner need not grant.
    mounts_livefs = False
    platform_secret_namespace_env = None
    # Every computer here was laid out at v4 or later, and the older steps
    # move and delete folders by name in a root that holds the owner's files.
    first_layout_version = 4
    default_computer_name = "SSH computer"

    def __init__(
        self,
        config: SSHConfig,
        working_dir: str | None = None,
        *,
        platform_secrets: Sequence[PlatformSecretDefinition] = (),
    ) -> None:
        # The machine holds no managed Secrets.
        del platform_secrets
        self._config = config
        self._working_dir = working_dir or DEFAULT_SANDBOX_ROOT

    @classmethod
    def trusted_host(cls, sandbox: Any) -> bool:
        return sandbox.ssh.trusted_host

    @property
    def channel(self) -> str | None:
        # Through the gateway is another network path than a direct dial, with
        # its own latency.
        return "ssh_tunnel" if self._config.tunnel else None

    def _runtime(self, sandbox_id: str) -> SSHRuntime:
        target = ssh_target(self._config)
        loader = _client_key_loader
        if loader is None:
            raise ValueError("No SSH client key loader is registered in this process")
        owner = target.key_owner

        async def load_key() -> str:
            return await loader(owner)

        return SSHRuntime(
            target=target,
            sandbox_id=sandbox_id,
            working_dir=self._working_dir,
            load_key=load_key,
            bootstrap_timeout=self._config.bootstrap_timeout,
            login_from=tuple(self._config.login_from),
        )

    # -- SandboxProvider interface --

    async def create(
        self,
        *,
        env_vars: dict[str, str] | None = None,
        platform_secret_bindings: dict[str, str] | None = None,
        tier: str | None = None,
        auto_stop_minutes: int | None = None,
        **kwargs: Any,
    ) -> SSHRuntime:
        """Lay out a new computer on the machine and install its runtime.

        The machine's size is its own, so ``tier`` is ignored; platform
        secrets and image packages never apply to it.
        """
        runtime = self._runtime(uuid.uuid4().hex)
        await runtime.create(env_vars or {})
        return runtime

    async def get(self, sandbox_id: str) -> SSHRuntime:
        return self._runtime(sandbox_id)

    async def close(self) -> None:
        # The pool belongs to the process, and other runtimes still use it.
        return None

    def is_transient_error(self, exc: Exception) -> bool:
        """Whether *exc* may clear on retry, judged by type.

        A machine that cannot be reached is transient, but a start that says
        so is not retried here: the host-unavailable path decides about it.
        """
        if isinstance(exc, SandboxHostUnavailableError):
            return False
        failure = connect_failure(exc)
        if failure is not None:
            return not failure.answered
        return isinstance(exc, (SandboxTransientError, TimeoutError, *TRANSPORT_ERRORS))

    def is_host_unavailable(self, exc: Exception) -> bool:
        """True when the machine could not be reached or did not answer."""
        seen: BaseException | None = exc
        while seen is not None:
            if isinstance(seen, SandboxHostUnavailableError):
                return True
            if isinstance(seen, SSHConnectError):
                return not seen.answered
            seen = seen.__cause__ or seen.__context__
        return False

    def classify_error(self, exc: Exception) -> SandboxFailureKind:
        """Never ``SANDBOX_GONE``: the machine is not the server's to replace,
        and a missing computer folder is laid out again by a start."""
        if isinstance(exc, FileNotFoundError):
            return SandboxFailureKind.PATH_ABSENT
        if isinstance(exc, SandboxHostUnavailableError) or self.is_transient_error(exc):
            return SandboxFailureKind.TRANSIENT
        return SandboxFailureKind.UNKNOWN
