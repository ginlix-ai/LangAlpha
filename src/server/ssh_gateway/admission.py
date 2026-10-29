"""Which connections on the machines' address may go on to log in, and what
they may cost until they do.

Anyone may connect there, and until its login is checked a connection costs
the gateway a file, a key exchange's CPU on its one loop, and memory for
whatever it sends meanwhile: asyncssh keeps reading a connection while its
login is looked up, and buffers all of it. So connections waiting on their
login are capped in all and per source, each source may open them only so
fast, and each may send only so much before its login completes. A
connection refused here is closed before the gateway's banner, which costs
nothing, and a machine's tunnel connects again within seconds.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import resource
import time

import asyncssh

from src.server.ssh_gateway.tally import Tally

logger = logging.getLogger(__name__)

# Past this many connections waiting on their login at once a new one is
# closed, as sshd's MaxStartups closes them, so a flood cannot take the files
# and memory the tunnels already up need. A burst after a restart is far
# smaller, as a login is checked within a fraction of a second.
MAX_UNCHECKED_CONNECTIONS = 1024
# One source's share of those, so a single host cannot fill the cap for
# everyone, while dozens of machines behind one router, reconnecting together
# after a restart, get in over a few retries. A source is an IPv4 address or
# an IPv6 /64, the least a host is usually given.
MAX_UNCHECKED_PER_SOURCE = 16
# How fast one source may open connections: a burst, then a steady rate. Each
# costs a key exchange, so this bounds what one host can take of the loop.
SOURCE_BURST = 30
SOURCE_RATE_PER_S = 10.0
# What a connection may send before its login completes. A machine's login
# takes a few KB: the key exchange and the one key its tunnel offers.
MAX_BYTES_BEFORE_LOGIN = 64 * 1024
# Sources whose rate is tracked before those idle long enough to have their
# whole burst back are forgotten.
_TRACKED_SOURCES = 4096
# asyncssh checks every key offered as a certificate first, verifying its
# signature with a signing key the sender picked before asking whether that
# CA is trusted. No login here uses one.
_CERTIFICATE_CHECK = "_validate_client_certificate"


def unchecked_slots() -> int:
    """How many connections may wait on their login at once.

    A quarter of the open-file limit when that is lower than the cap: a
    native run on macOS starts at 256, which the cap alone would exhaust.
    """
    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY:
        return MAX_UNCHECKED_CONNECTIONS
    return max(1, min(MAX_UNCHECKED_CONNECTIONS, soft // 4))


def require_hooks() -> None:
    """Refuse to serve where the limits on a connection would silently not
    apply.

    Each replaces a method of asyncssh's on the connection itself: the byte
    budget ``data_received``, which asyncio's own transports look up on every
    read (uvloop's bind it once), and the certificate refusal a private
    method, which an asyncssh release could rename.
    """
    loop = asyncio.get_running_loop()
    if not isinstance(loop, asyncio.BaseEventLoop):
        raise RuntimeError(
            "The tunnel gateway needs asyncio's own event loop, not "
            f"{type(loop).__module__}.{type(loop).__name__}"
        )
    if not callable(getattr(asyncssh.SSHServerConnection, _CERTIFICATE_CHECK, None)):
        raise RuntimeError(
            f"asyncssh {asyncssh.__version__} has no {_CERTIFICATE_CHECK} to "
            "replace; the tunnel gateway would decode certificates"
        )


def _source(conn: asyncssh.SSHServerConnection) -> str:
    peer = conn.get_extra_info("peername")
    if not peer:
        return "unknown"
    # A link-local peer carries its interface, which is not part of the host.
    address = str(peer[0]).partition("%")[0]
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return address
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            return str(ip.ipv4_mapped)
        return str(ipaddress.IPv6Network((ip, 64), strict=False))
    return str(ip)


async def _no_certificate(username: str, key_data: bytes) -> None:
    """In place of asyncssh's certificate check: the key is then judged as a
    plain key, which the gateway compares without decoding a signature."""
    return None


class Admission:
    """The connections on the machines' address not logged in yet."""

    def __init__(self) -> None:
        self._slots = unchecked_slots()
        self._unchecked: dict[asyncssh.SSHServerConnection, str] = {}
        # Per source, its connections waiting, oldest first.
        self._waiting: dict[str, dict[asyncssh.SSHServerConnection, None]] = {}
        # The sources holding each number of places, in the order they came
        # to hold it, so the one to make room is found in a few steps.
        self._holding: list[dict[str, None]] = [
            {} for _ in range(MAX_UNCHECKED_PER_SOURCE + 1)
        ]
        # Per source: the connections it may still open at once, and when
        # that was last worked out.
        self._buckets: dict[str, tuple[float, float]] = {}
        self._forget_at = _TRACKED_SOURCES
        self._closed = Tally(
            logger, logging.WARNING, "Closed %d connections before their login"
        )

    def admit(self, conn: asyncssh.SSHServerConnection) -> bool:
        """Count *conn* as waiting on its login; False for one to close."""
        source = _source(conn)
        now = time.monotonic()
        held = len(self._waiting.get(source, ()))
        tokens = self._tokens(source, now)
        if tokens < 1:
            why = f"over a source's {SOURCE_RATE_PER_S:g} a second"
        elif held >= MAX_UNCHECKED_PER_SOURCE:
            why = f"over a source's {MAX_UNCHECKED_PER_SOURCE} at once"
        elif len(self._unchecked) >= self._slots and not self._make_room(held):
            why = f"over the {self._slots} at once"
        else:
            self._spend(source, tokens, now)
            self._hold(source, conn)
            self._unchecked[conn] = source
            self._meter(conn, source)
            setattr(conn, _CERTIFICATE_CHECK, _no_certificate)
            return True
        self._closed.add(why, source)
        return False

    def release(self, conn: asyncssh.SSHServerConnection) -> None:
        """*conn* logged in or closed, which frees its place."""
        if conn in self._unchecked:
            self._forget(conn)
            # Back to asyncssh's own method (see ``_meter``).
            vars(conn).pop("data_received", None)

    def _forget(self, conn: asyncssh.SSHServerConnection) -> None:
        source = self._unchecked.pop(conn)
        waiting = self._waiting[source]
        del self._holding[len(waiting)][source]
        del waiting[conn]
        if waiting:
            self._holding[len(waiting)][source] = None
        else:
            del self._waiting[source]

    def _hold(self, source: str, conn: asyncssh.SSHServerConnection) -> None:
        waiting = self._waiting.setdefault(source, {})
        self._holding[len(waiting)].pop(source, None)
        waiting[conn] = None
        self._holding[len(waiting)][source] = None

    def _make_room(self, held: int) -> bool:
        """Close the oldest connection of the source holding the most, when
        it holds at least two more than the newcomer's, so the swap never
        leaves the newcomer's source holding more.

        Hosts holding many places then cannot keep the rest out, and a
        machine's login, one place, is never closed for another: filling the
        cap with no room to make takes a source per place.
        """
        for count in range(MAX_UNCHECKED_PER_SOURCE, held + 1, -1):
            if self._holding[count]:
                source = next(iter(self._holding[count]))
                break
        else:
            return False
        oldest = next(iter(self._waiting[source]))
        # Its place is free now; its budget stays on until it has closed.
        self._forget(oldest)
        oldest.abort()
        self._closed.add("to make room for a source holding fewer", source)
        return True

    def _meter(self, conn: asyncssh.SSHServerConnection, source: str) -> None:
        """Close *conn* once it sends more than MAX_BYTES_BEFORE_LOGIN before
        its login completes.

        asyncssh has no option for this, so its ``data_received`` is wrapped
        on this connection alone until ``release`` takes the wrapper off.
        asyncio's transports look the method up on every read.
        """
        receive = conn.data_received
        left = MAX_BYTES_BEFORE_LOGIN

        def data_received(data: bytes, datatype: object = None) -> None:
            nonlocal left
            if left < 0:
                return  # closing already
            left -= len(data)
            if left < 0:
                self._closed.add(
                    f"sent over {MAX_BYTES_BEFORE_LOGIN // 1024} KiB first", source
                )
                conn.abort()
                return
            receive(data, datatype)

        conn.data_received = data_received

    def _tokens(self, source: str, now: float) -> float:
        tokens, at = self._buckets.get(source, (SOURCE_BURST, now))
        return min(SOURCE_BURST, tokens + (now - at) * SOURCE_RATE_PER_S)

    def _spend(self, source: str, tokens: float, now: float) -> None:
        """Take one of *source*'s turns; only an admitted connection spends
        one, so machines behind one router are not slowed by refusals."""
        if source not in self._buckets and len(self._buckets) >= self._forget_at:
            self._forget_idle(now)
        self._buckets[source] = (tokens - 1, now)

    def _forget_idle(self, now: float) -> None:
        refilled_in = SOURCE_BURST / SOURCE_RATE_PER_S
        self._buckets = {
            source: bucket
            for source, bucket in self._buckets.items()
            if now - bucket[1] < refilled_in
        }
        # Doubled while many sources stay busy, so a flood from more of them
        # than this does not scan them all on every new one.
        self._forget_at = max(_TRACKED_SOURCES, 2 * len(self._buckets))
