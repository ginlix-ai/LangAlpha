"""Reaching an SSH machine: the addresses the server may dial, the tunnel
gateway for machines it cannot, and one login that trusts only the pinned key.

Every connect is the server's own decision. Nothing on the server's host
vouches for a machine or for the server (``_NO_LOCAL_TRUST``), so a key or
config file there never changes what a computer can reach.
"""

from __future__ import annotations

import asyncio
import errno
import ipaddress
import logging
import socket
import time
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import asyncssh
import structlog

from ptc_agent.core.sandbox.providers.ssh_errors import SSHConnectError

logger = structlog.get_logger(__name__)

# asyncssh logs every connection and channel it opens at INFO; one line per
# command is noise in a server log. An explicit level set by the deployment is
# left alone.
if logging.getLogger("asyncssh").level == logging.NOTSET:
    logging.getLogger("asyncssh").setLevel(logging.WARNING)

# Refused or reset before the handshake finished: a busy sshd drops new
# connections past MaxStartups, and a retry a moment later gets in.
_CONNECT_RETRY_DELAYS_S = (0.5, 1.5)

# No agent, ssh config, CA store or login but the key handed over: what this
# host holds must never decide what a connection trusts or offers.
_NO_LOCAL_TRUST: dict[str, Any] = {
    "agent_path": None,
    "agent_forwarding": False,
    "config": None,
    "x509_trusted_certs": None,
    "password_auth": False,
    "kbdint_auth": False,
    "gss_auth": False,
    "host_based_auth": False,
}


@dataclass(frozen=True)
class SSHTarget:
    """One login on one machine, and how hard to try reaching it.

    ``host_key`` is the pinned OpenSSH line; None only for a probe's first
    contact, which accepts whatever key the machine presents and reports it.
    A probe of a machine that reported its own keys passes them all, one per
    line, and the one it presents is the one pinned.

    ``tunnel`` names the tunnel of a machine that connects out to the
    deployment's gateway rather than being dialed. ``host`` is then only the
    machine's name, and ``port`` its sshd's port on the machine itself.
    """

    host: str
    port: int
    username: str
    host_key: str | None
    key_owner: str
    connect_timeout: float = 10
    # As OpenSSH's ServerAliveCountMax: a machine silent for interval x count
    # seconds is given up on.
    keepalive_interval: float = 15
    keepalive_count_max: int = 3
    # Off, only public addresses are dialed (see ``dial_addresses``).
    allow_private: bool = False
    tunnel: str | None = None

    @property
    def pool_key(self) -> tuple[str, int, str, str | None, str, bool, str | None]:
        # A different pinned key, key owner or address policy is a different
        # trust decision, so it never shares a connection made under the old one.
        return (
            self.host,
            self.port,
            self.username,
            self.host_key,
            self.key_owner,
            self.allow_private,
            self.tunnel,
        )

    @property
    def label(self) -> str:
        return f"{self.username}@{self.host}"


# -- Addresses ------------------------------------------------------------------

# These carry an IPv4 address in their last 32 bits, which a translator on the
# server's network would reach: NAT64's well-known prefix, SIIT's
# IPv4-translated form, and the deprecated IPv4-compatible one (::a.b.c.d).
_CARRY_IPV4 = tuple(
    ipaddress.ip_network(prefix)
    for prefix in ("64:ff9b::/96", "::ffff:0:0:0/96", "::/96")
)


def is_public_address(address: str) -> bool:
    """Whether *address* is one the server may dial without allowing private ones.

    An IPv6 form that carries an IPv4 address is judged by that address, and
    multicast never passes, though Python counts some of it as global.
    """
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address = ipaddress.ip_address(address)
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        elif any(ip in prefix for prefix in _CARRY_IPV4):
            ip = ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return ip.is_global and not ip.is_multicast


async def dial_addresses(
    host: str, port: int, *, allow_private: bool, timeout: float = 10
) -> list[str]:
    """The addresses to connect to for *host*, every one of them public.

    The server dials the machine from its own network, so a private, loopback
    or link-local address would reach the server's neighbours rather than a
    machine the user brought. A name is resolved here, on every connect, and
    only the addresses that passed are dialed, so a name pointed somewhere
    else later is caught at the next connect. ``allow_private`` skips the
    check and returns *host* for the connect to resolve itself.

    Raises:
        SSHConnectError: ``unreachable`` when the name does not resolve,
            ``timeout`` when resolving takes longer than *timeout*, and
            ``private_address`` when any address it resolves to is not public.
    """
    if allow_private:
        return [host]
    name = host.strip().strip("[]").rstrip(".")
    try:
        addresses = [str(ipaddress.ip_address(name))]
    except ValueError:
        try:
            async with asyncio.timeout(timeout):
                infos = await asyncio.get_running_loop().getaddrinfo(
                    name, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
                )
        except TimeoutError:
            raise SSHConnectError(
                "timeout", f"Looking up {host} took longer than {int(timeout)} seconds."
            ) from None
        except OSError:
            raise SSHConnectError(
                "unreachable",
                f"{host} could not be found. Check the name, or enter the "
                "machine's IP address.",
            ) from None
        addresses = list(dict.fromkeys(str(info[4][0]) for info in infos))
        if not addresses:
            raise SSHConnectError("unreachable", f"{host} could not be found.")
    # Every address must pass: a name that maps to one public and one private
    # address is an attack shape, not a configuration.
    if not all(is_public_address(a) for a in addresses):
        what = "is" if addresses == [name] else "points to"
        raise SSHConnectError(
            "private_address",
            f"{host} {what} a private or local address. This server connects "
            "only to machines at a public address.",
        )
    return addresses


# -- The tunnel gateway -------------------------------------------------------

# The login the server's workers use at the gateway, which may only open
# connections to machines; a machine logs in as its tunnel's id.
GATEWAY_DIALER = "dial"
# The reason the gateway gives for a machine with no tunnel open to it.
GATEWAY_NOT_CONNECTED = "not connected"
_GATEWAY_CONNECT_TIMEOUT_S = 10
_GATEWAY_DOWN = (
    "The tunnel that machines connect through is not answering right now. "
    "Try again in a moment."
)


@dataclass(frozen=True)
class GatewayAccess:
    """How this process reaches the deployment's tunnel gateway."""

    host: str
    port: int
    host_key: str  # the gateway's public host key line, the one pinned
    dialer_key: str  # OpenSSH private key text of the dialer login


GatewayLoader = Callable[[], Awaitable[GatewayAccess]]

# Set once per worker by the server, like the client key loader.
_gateway_loader: GatewayLoader | None = None


def set_gateway_loader(loader: GatewayLoader | None) -> None:
    """Register how this worker learns the gateway's address and keys."""
    global _gateway_loader
    _gateway_loader = loader


class _Gateway:
    """This loop's one connection to the gateway, carrying every tunneled dial.

    Each machine reached through it is a channel on the connection, so a
    hundred tunneled computers cost the gateway one login per worker.
    """

    def __init__(self) -> None:
        self._conn: asyncssh.SSHClientConnection | None = None
        self._dialing: asyncio.Future[asyncssh.SSHClientConnection] | None = None
        self._silent_until = 0.0

    async def connection(self) -> asyncssh.SSHClientConnection:
        """The open connection, or one dial shared by every caller waiting.

        Shared, failure included: when the gateway connection drops, every
        tunneled call in flight asks at once, and a gateway that does not
        answer should cost them one connect timeout, not one each in turn.
        A dial that timed out answers for one more connect timeout, so the
        retries above it fail at once rather than each wait as long again.
        """
        conn = self._conn
        if conn is not None and not conn.is_closed():
            return conn
        if self._dialing is None:
            if time.monotonic() < self._silent_until:
                raise SSHConnectError("gateway_unreachable", _GATEWAY_DOWN)
            self._dialing = asyncio.ensure_future(self._dial())
            self._dialing.add_done_callback(self._dialed)
        return await asyncio.shield(self._dialing)

    def _dialed(self, dialing: asyncio.Future[asyncssh.SSHClientConnection]) -> None:
        self._dialing = None
        if dialing.cancelled():
            return
        exc = dialing.exception()
        if exc is None:
            self._conn = dialing.result()
        elif isinstance(exc.__cause__, TimeoutError):
            # Not a refusal, which a gateway restarting answers for a second
            # or so before it is back.
            self._silent_until = time.monotonic() + _GATEWAY_CONNECT_TIMEOUT_S

    def drop(self, conn: asyncssh.SSHClientConnection) -> None:
        if self._conn is conn:
            self._conn = None
        conn.abort()

    @staticmethod
    async def _dial() -> asyncssh.SSHClientConnection:
        loader = _gateway_loader
        if loader is None:
            raise SSHConnectError(
                "gateway_unreachable",
                "This machine connects through a tunnel, and this server has "
                "no tunnel gateway configured.",
            )
        access = await loader()
        try:
            return await asyncssh.connect(
                access.host,
                access.port,
                username=GATEWAY_DIALER,
                known_hosts=([asyncssh.import_public_key(access.host_key)], [], []),
                client_keys=[asyncssh.import_private_key(access.dialer_key)],
                **_NO_LOCAL_TRUST,
                connect_timeout=_GATEWAY_CONNECT_TIMEOUT_S,
                keepalive_interval=15,
                keepalive_count_max=2,
            )
        except (OSError, asyncssh.Error, TimeoutError) as e:
            # The address and the reason are the operator's; the machine's
            # owner is told only that the tunnel is down.
            logger.warning(
                "Could not reach the tunnel gateway",
                address=f"{access.host}:{access.port}",
                error=(
                    f"no answer within {_GATEWAY_CONNECT_TIMEOUT_S}s"
                    if isinstance(e, TimeoutError)
                    else _reason(e)
                ),
            )
            raise SSHConnectError("gateway_unreachable", _GATEWAY_DOWN) from e


_gateways: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _Gateway] = (
    weakref.WeakKeyDictionary()
)


def _gateway() -> _Gateway:
    """The running loop's gateway connection. Connections never cross event loops."""
    loop = asyncio.get_running_loop()
    found = _gateways.get(loop)
    if found is None:
        found = _gateways[loop] = _Gateway()
    return found


def _tunnel_error(target: SSHTarget, exc: asyncssh.ChannelOpenError) -> SSHConnectError:
    name = target.host or "The machine"
    if exc.reason == GATEWAY_NOT_CONNECTED:
        return SSHConnectError(
            "not_connected",
            f"{name} is not connected. It connects by itself whenever it is on "
            "and online, so check that it is awake and has internet.",
        )
    return SSHConnectError(
        "unreachable",
        f"{name} is connected, but its SSH server did not take the connection "
        f"({exc.reason or 'refused'}). On a Mac, check that Remote Login is on "
        "in System Settings, General, Sharing.",
    )


async def _through_gateway(
    target: SSHTarget, options: dict[str, Any]
) -> asyncssh.SSHClientConnection:
    """Log in to the machine's own sshd over a channel through the gateway.

    The gateway only relays bytes: the handshake, the pinned host key and the
    owner's key are between this process and the machine. A gateway
    connection that died while idle is found out by the channel it fails to
    open, and dialed again once.
    """
    gateway = _gateway()
    for attempt in range(2):
        conn = await gateway.connection()
        try:
            return await asyncssh.connect(
                target.tunnel or "", target.port, tunnel=conn, **options
            )
        except (OSError, asyncssh.Error, TimeoutError) as e:
            # A connection that is closing refuses new channels a loop turn
            # before it reads as closed.
            await asyncio.sleep(0)
            if attempt == 0 and conn.is_closed():
                # Including a ChannelOpenError: asyncssh answers a channel
                # asked of a connection already gone with one.
                gateway.drop(conn)
                continue
            if isinstance(e, asyncssh.ChannelOpenError):
                raise _tunnel_error(target, e) from e
            raise _connect_error(target, e) from e
    raise AssertionError("unreachable")  # pragma: no cover


# -- Connecting -----------------------------------------------------------------

# On first contact, the key a machine is asked for is the one that gets pinned.
# OpenSSH's own order, rather than asyncssh's, which leads with RSA.
_FIRST_CONTACT_HOST_KEY_ALGS = (
    "ssh-ed25519",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "rsa-sha2-512",
    "rsa-sha2-256",
    "ssh-rsa",
)


def _pinned_host_key_algs(keys: list[asyncssh.SSHKey]) -> list[str]:
    """The pinned keys' algorithms in first contact's order of preference.

    asyncssh would take them in the order the keys are listed, so a machine
    pinned by all its keys would be held to whichever its pairing read first.
    """
    offered = [alg.decode() for key in keys for alg in key.sig_algorithms]
    preferred = [alg for alg in _FIRST_CONTACT_HOST_KEY_ALGS if alg in offered]
    return preferred + [alg for alg in dict.fromkeys(offered) if alg not in preferred]


def _reason(exc: BaseException) -> str:
    """Why a connect failed, in words, not an errno dump."""
    if isinstance(exc, socket.gaierror):
        return "its name did not resolve"
    if isinstance(exc, ConnectionRefusedError):
        return "the connection was refused"
    if isinstance(exc, (ConnectionResetError, asyncssh.ConnectionLost)):
        return "the connection closed before login finished"
    if isinstance(exc, OSError) and exc.errno in (
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
    ):
        return "there is no route to it"
    return str(exc).strip() or type(exc).__name__


def _connect_error(target: SSHTarget, exc: BaseException) -> SSHConnectError:
    where = f"{target.host} on port {target.port}"
    if isinstance(exc, asyncssh.PermissionDenied):
        return SSHConnectError(
            "auth_failed",
            f"{target.label} did not accept our key. Add the public key to "
            f"~/.ssh/authorized_keys for {target.username} on that machine.",
        )
    if isinstance(exc, asyncssh.HostKeyNotVerifiable):
        return SSHConnectError(
            "host_key_mismatch",
            f"{target.host} presented a different host key from the one pinned "
            "when this computer was added. If the machine was reinstalled, "
            "reconnect the computer to it.",
        )
    if isinstance(exc, TimeoutError):
        return SSHConnectError(
            "timeout",
            f"{where} did not answer within {int(target.connect_timeout)} seconds.",
        )
    return SSHConnectError("unreachable", f"Could not reach {where}: {_reason(exc)}.")


def _retryable_connect(exc: BaseException) -> bool:
    if isinstance(exc, (asyncssh.PermissionDenied, asyncssh.HostKeyNotVerifiable)):
        return False
    return isinstance(exc, (ConnectionResetError, asyncssh.ConnectionLost))


def _try_next_address(exc: BaseException) -> bool:
    """Whether another of the machine's addresses may connect where this did not.

    A timeout counts: as with OpenSSH's ConnectTimeout, each address gets its
    own, so an IPv6 route that drops packets does not hide a working IPv4 one.
    """
    return isinstance(exc, (ConnectionRefusedError, TimeoutError)) or (
        isinstance(exc, OSError)
        and exc.errno
        in (errno.EHOSTUNREACH, errno.ENETUNREACH, errno.EADDRNOTAVAIL, errno.ETIMEDOUT)
    )


async def connect(target: SSHTarget, private_key: str) -> asyncssh.SSHClientConnection:
    """Open and authenticate one connection, trusting only the pinned host key.

    Nothing on this host is consulted: no ssh config, no known_hosts, no
    agent, no default keys. The machine's next address is tried when one is
    refused, has no route or does not answer in time, a reset connect is
    retried briefly, and an auth or host key failure never is. A target with
    a tunnel is reached through the gateway instead of being dialed.

    Raises:
        SSHConnectError: The connection could not be made, or the machine's
            address is not one ``dial_addresses`` allows.
        ValueError: ``private_key`` or ``target.host_key`` is not a key.
    """
    try:
        client_key = asyncssh.import_private_key(private_key)
        known_hosts = (
            (
                [
                    asyncssh.import_public_key(line)
                    for line in target.host_key.splitlines()
                    if line.strip()
                ],
                [],
                [],
            )
            if target.host_key
            else None
        )
    except (asyncssh.KeyImportError, ValueError) as e:
        raise ValueError(f"Invalid SSH key: {e}") from None
    options: dict[str, Any] = {
        **_NO_LOCAL_TRUST,
        "username": target.username,
        "known_hosts": known_hosts,
        "client_keys": [client_key],
        "connect_timeout": target.connect_timeout,
        "server_host_key_algs": (
            _pinned_host_key_algs(known_hosts[0])
            if known_hosts
            else list(_FIRST_CONTACT_HOST_KEY_ALGS)
        ),
        "keepalive_interval": target.keepalive_interval,
        # asyncssh drops the connection on the tick after count_max unanswered
        # ones, one interval later than OpenSSH does for the same number.
        "keepalive_count_max": max(1, target.keepalive_count_max - 1),
    }
    if target.tunnel:
        return await _through_gateway(target, options)
    addresses = await dial_addresses(
        target.host,
        target.port,
        allow_private=target.allow_private,
        timeout=target.connect_timeout,
    )
    delays = iter(_CONNECT_RETRY_DELAYS_S)
    index = 0
    while True:
        try:
            return await asyncssh.connect(addresses[index], target.port, **options)
        except (OSError, asyncssh.Error, TimeoutError) as e:
            if index + 1 < len(addresses) and _try_next_address(e):
                index += 1
                continue
            delay = next(delays, None) if _retryable_connect(e) else None
            if delay is None:
                raise _connect_error(target, e) from e
            await asyncio.sleep(delay)


# How LangAlpha's helper identifies itself in the SSH handshake. A machine it
# answers for runs no sshd, and keeps the deployment's key in the helper's own
# folder rather than in ~/.ssh/authorized_keys.
HELPER_ID_PREFIX = "SSH-2.0-LangAlphaHelper_"


def answered_by_helper(conn: asyncssh.SSHClientConnection) -> bool:
    """Whether LangAlpha's helper, rather than an sshd, answered the handshake."""
    return str(conn.get_extra_info("server_version") or "").startswith(HELPER_ID_PREFIX)


def host_key_line(conn: asyncssh.SSHClientConnection) -> tuple[str, str]:
    """The key the machine presented, as an OpenSSH line, and its fingerprint."""
    key = conn.get_server_host_key()
    if key is None:  # pragma: no cover - set by every completed handshake
        raise SSHConnectError("unreachable", "The machine presented no host key.")
    line = key.export_public_key("openssh").decode().strip()
    return line, key.get_fingerprint("sha256")
