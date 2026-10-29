"""Reading the machine's report, dialing what it names, and saying why none worked."""

from __future__ import annotations

import asyncio
import ipaddress
import re
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence, Tuple

from ptc_agent.config.core import SSHConfig
from ptc_agent.core.sandbox.providers import ssh as ssh_provider
from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError
from src.server.models.computer import (
    ComputerSSHPairingError,
    ComputerSSHPairingReport,
    is_login_name,
)
from src.server.services.ssh_computers import is_public_key, probe
from src.server.services.ssh_pairing.state import PRODUCT, ReportedMachine
from src.server.services.ssh_tunnels import gateway_public_address, tunnels_enabled

# A report is the machine's word about itself, so each list is capped: a dial
# per address and port it names, and every one at once.
_MAX_HOST_KEYS = 8
_MAX_PORTS = 3
_MAX_ADDRESSES = 6
# Once an address logs in, how much longer one ranked above it may take to.
_PREFER_WAIT_S = 2.0
# A tunnel the script has just started needs a moment to reach the gateway:
# its loop dials at once, and a slow link or a slow ssh start eats into this.
TUNNEL_WAIT_S = 45.0
_TUNNEL_RETRY_S = 1.0

_IP = ipaddress.IPv4Address | ipaddress.IPv6Address


def _ip(value: str) -> Optional[_IP]:
    try:
        ip = ipaddress.ip_address(value.strip().strip("[]"))
    except ValueError:
        return None
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return ip


def _dialable(ip: _IP, settings: SSHConfig) -> bool:
    if ip.is_unspecified or ip.is_multicast or ip.is_link_local:
        return False
    return settings.allow_private_addresses or ssh_provider.is_public_address(str(ip))


def source_addresses(client_host: Optional[str], headers: Any) -> List[str]:
    """Where the report came from, the addresses a VPS most often answers SSH on.

    The proxy headers come first, since behind a proxy the socket's peer is
    the proxy. Trusting them costs nothing: an address is only somewhere to
    try, and the machine's host keys decide whether what answers is it.
    """
    found: List[str] = []
    for header in ("cf-connecting-ip", "x-real-ip", "x-forwarded-for"):
        found.extend(part.strip() for part in (headers.get(header) or "").split(","))
    if client_host:
        found.append(client_host)
    return [a for a in found if a]


def _unique(items: Iterable[str]) -> List[str]:
    return list(dict.fromkeys(items))


def printable(value: str, limit: int) -> str:
    return "".join(c for c in value.strip() if c.isprintable())[:limit]


def _tunnel_id(value: str) -> Optional[str]:
    try:
        return str(uuid.UUID(value.strip()))
    except ValueError:
        return None


def machine_name(hostname: str) -> str:
    """What a tunneled target's host holds: the machine's name as one DNS label.

    Nothing dials it, but the create validates it as a host, and every message
    about the machine names it.
    """
    label = re.sub(r"[^a-z0-9-]+", "-", hostname.split(".")[0].lower())
    return label.strip("-")[:63].strip("-") or "machine"


def parse_report(
    form: ComputerSSHPairingReport, sources: Sequence[str]
) -> ReportedMachine:
    """The machine's report, cleaned; its addresses lead with where it came from.

    Raises:
        ValueError: The report names no account, or one that cannot be.
    """
    username = form.user.strip()
    if not is_login_name(username):
        raise ValueError(f"{username!r} is not an account name SSH can log in as.")

    host_keys: List[str] = []
    for line in form.host_keys.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        candidate = f"{parts[0]} {parts[1]}"
        if is_public_key(candidate):
            host_keys.append(candidate)

    ports = _unique(
        p for p in form.ports.split() if p.isdigit() and 0 < int(p) < 65536
    )[:_MAX_PORTS] or ["22"]

    addresses: List[str] = []
    for raw in [*sources, *form.addresses.split()]:
        ip = _ip(raw)
        if ip is not None and not ip.is_unspecified:
            addresses.append(str(ip))

    # A machine with no SSH server to dial can only connect out.
    tunnel_only = form.tunnel_only.strip() == "1"
    return ReportedMachine(
        username=username,
        hostname=printable(form.hostname, 255),
        os=form.os.strip()[:32],
        addresses=_unique(addresses),
        host_keys=_unique(host_keys)[:_MAX_HOST_KEYS],
        ports=[int(p) for p in ports],
        can_tunnel=tunnel_only or form.can_tunnel.strip() == "1",
        tunnel_only=tunnel_only,
        tunnel_id=_tunnel_id(form.tunnel_id),
        run=form.run.strip()[:64],
    )


@dataclass(frozen=True)
class Candidate:
    """Somewhere to try the machine: an address, or its tunnel by id."""

    host: str
    port: int
    tunnel_id: Optional[str] = None


@dataclass(frozen=True)
class Reached:
    host: str
    port: int
    tunnel_id: Optional[str]
    found: ssh_provider.SSHProbe


@dataclass(frozen=True)
class Missed:
    host: str
    port: int
    failure: SSHConnectError


def candidates(
    machine: ReportedMachine, *, settings: SSHConfig, tunnel_id: Optional[str]
) -> List[Candidate]:
    """Where to try the machine, best first.

    Through its tunnel alone when it has one, at its sshd's first reported
    port: the tunnel is there because a direct dial failed. Nowhere else for
    a machine that is reached only through one.
    """
    if tunnel_id is not None:
        return [Candidate(machine_name(machine.hostname), machine.ports[0], tunnel_id)]
    if machine.tunnel_only:
        return []
    dialable = [
        a
        for a in machine.addresses
        if (ip := _ip(a)) is not None and _dialable(ip, settings)
    ][:_MAX_ADDRESSES]
    return [Candidate(a, p) for a in dialable for p in machine.ports]


def no_address() -> Missed:
    """What dialing a machine with nowhere to try it comes to."""
    return Missed("", 0, SSHConnectError("private_address", "No public address."))


async def first_reachable(
    private_key: str,
    machine: ReportedMachine,
    tried: Sequence[Candidate],
    *,
    settings: SSHConfig,
    wait_s: float = 0.0,
) -> Tuple[Optional[Reached], List[Missed]]:
    """Every candidate at once; the best ranked to log in wins, the rest are dropped.

    At once because a dead address costs a whole connect timeout, and a VPS
    reports docker bridges and v6 routes beside the address that answers.
    Ranked because one of those may answer a moment sooner, and a bridge is
    no address to keep: once one logs in, those ranked above it (where the
    report came from, first) get ``_PREFER_WAIT_S`` more to log in too.
    ``wait_s`` is for a tunnel the script has only just started, tried again
    for as long as the gateway has no tunnel from it yet.
    """
    loop = asyncio.get_running_loop()
    give_up = loop.time() + wait_s
    host_key = machine.pinned_host_key

    async def attempt(candidate: Candidate) -> Reached | Missed:
        while True:
            try:
                found = await probe(
                    private_key,
                    settings=settings,
                    host=candidate.host,
                    port=candidate.port,
                    username=machine.username,
                    host_key=host_key,
                    tunnel_id=candidate.tunnel_id,
                )
                return Reached(
                    candidate.host, candidate.port, candidate.tunnel_id, found
                )
            except SSHConnectError as e:
                if e.code != "not_connected" or loop.time() + _TUNNEL_RETRY_S > give_up:
                    return Missed(candidate.host, candidate.port, e)
            await asyncio.sleep(_TUNNEL_RETRY_S)

    tasks = [asyncio.create_task(attempt(candidate)) for candidate in tried]
    missed: List[Missed] = []
    best: Optional[Tuple[int, Reached]] = None
    deadline: Optional[float] = None
    pending = set(tasks)
    try:
        while pending:
            wait = None if deadline is None else max(0.0, deadline - loop.time())
            done, pending = await asyncio.wait(
                pending, timeout=wait, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break
            for task in done:
                outcome = task.result()
                rank = tasks.index(task)
                if isinstance(outcome, Missed):
                    missed.append(outcome)
                elif best is None or rank < best[0]:
                    best = (rank, outcome)
            if best is not None:
                if all(task.done() for task in tasks[: best[0]]):
                    break
                if deadline is None:
                    deadline = loop.time() + _PREFER_WAIT_S
        return (best[1] if best else None), missed
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def may_tunnel(machine: ReportedMachine, missed: Sequence[Missed]) -> bool:
    """Whether a machine no address reached should connect out instead.

    Not when its sshd answered and refused our key: a tunnel ends at the same
    sshd. Something else answering at its address, a home router most often,
    or an address on a private network, is just what a tunnel gets around.
    """
    return (
        tunnels_enabled()
        and machine.can_tunnel
        and not any(m.failure.code == "auth_failed" for m in missed)
    )


def no_tunnel_note(machine: ReportedMachine) -> str:
    """Why the machine was not connected out instead, for the end of a diagnosis."""
    if not tunnels_enabled():
        return (
            " This server has no tunnel for machines it cannot reach, such as "
            "one on a home or office network."
        )
    if not machine.can_tunnel:
        return (
            " Or install the OpenSSH client (ssh and ssh-keygen) and run the "
            f"command again, and the machine will connect out to {PRODUCT} instead."
        )
    return ""


def diagnosis(
    machine: ReportedMachine, missed: Sequence[Missed], *, tunneled: bool
) -> ComputerSSHPairingError:
    """One code and one sentence for why no candidate worked.

    A refused key on a machine whose host key matched says more than any
    address that did not answer, so it wins, through a tunnel too, since the
    same sshd refused it. A tunnel that never reached the gateway is worded
    for the terminal that just set it up, and any other tunnel failure is the
    transport's sentence. Of addresses dialed, something else answering comes
    next, then silence.
    """
    user = machine.username
    codes = {m.failure.code for m in missed}
    if "auth_failed" in codes:
        if user == "root":
            message = (
                "This machine refused our key for root, most likely because sshd "
                "does not allow root logins. Run the command as the account the "
                "agent should use, or allow root logins by key "
                "(PermitRootLogin prohibit-password)."
            )
        else:
            message = (
                f"This machine refused our key for {user}. Check that sshd allows "
                "key logins for this account (PubkeyAuthentication, AllowUsers), "
                "then run the command again."
            )
        return ComputerSSHPairingError(code="auth_failed", message=message)
    if tunneled:
        failure = missed[0].failure
        if failure.code != "not_connected":
            return ComputerSSHPairingError(code=failure.code, message=str(failure))
        gateway = gateway_public_address()
        where = (
            f" {gateway.host} on port {gateway.port}" if gateway else " the internet"
        )
        return ComputerSSHPairingError(
            code="not_connected",
            message=(
                f"This machine's tunnel did not connect to {PRODUCT}. Check that "
                f"the machine can reach{where}, then run the command again."
            ),
        )
    tried = ", ".join(f"{m.host}:{m.port}" for m in missed[:3])
    note = no_tunnel_note(machine)
    # These two already say what this server does not do; the note adds only
    # what the owner could do about it.
    remedy = note if tunnels_enabled() else ""
    if codes == {"host_key_mismatch"}:
        return ComputerSSHPairingError(
            code="host_key_mismatch",
            message=(
                f"Something other than this machine answered at {tried}. If SSH "
                "reaches it at another address, enter that address instead." + remedy
            ),
        )
    if codes and codes <= {"private_address"}:
        return ComputerSSHPairingError(
            code="private_address",
            message=(
                "This machine is only on a private network, which this server "
                "does not connect to." + remedy
            ),
        )
    if machine.os == "Darwin":
        fix = (
            "On a Mac, turn on Remote Login (System Settings, General, Sharing), "
            "then run the command again."
        )
    else:
        fix = (
            "If a firewall or a cloud security group guards it, allow SSH from "
            "the internet, then run the command again."
        )
    return ComputerSSHPairingError(
        code="timeout" if codes == {"timeout"} else "unreachable",
        message=(
            f"This machine checked in, but SSH did not answer at "
            f"{tried or 'any of its addresses'}. {fix}" + note
        ),
    )
