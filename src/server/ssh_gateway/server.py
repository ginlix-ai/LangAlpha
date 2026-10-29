"""The gateway's SSH server: who may log in, what each login may do, and the
tunnels' liveness in the database."""

from __future__ import annotations

import asyncio
import logging
import re
import resource
import signal
import sys
import weakref

import asyncssh
from asyncssh.kex import get_kex_algs
from psycopg import AsyncConnection

from ptc_agent.core.sandbox.providers._ssh_connect import (
    GATEWAY_DIALER,
    GATEWAY_NOT_CONNECTED,
)
from src.config.logging_config import configure_logging
from src.config.settings import get_conversation_pool_max
from src.server.database import ssh_tunnels as tunnels_db
from src.server.database.encryption import encryption_configured
from src.server.database.pool import get_db_connection, get_or_create_pool
from src.server.ssh_gateway import admission
from src.server.ssh_gateway.admission import Admission
from src.server.ssh_gateway.tally import Tally
from src.server.services.ssh_tunnels import (
    GatewayAddress,
    gateway_dial_listen,
    gateway_dialer_key,
    gateway_host_key,
    gateway_listen,
)

logger = logging.getLogger(__name__)

# asyncssh logs every channel and auth attempt at INFO; the gateway logs its
# own line per tunnel and per refused login instead. An explicit level set by
# the deployment is left alone.
if logging.getLogger("asyncssh").level == logging.NOTSET:
    logging.getLogger("asyncssh").setLevel(logging.WARNING)

# A login takes a few round trips, seconds even over a satellite link. Anyone
# may hold a connection open this long before its login is checked, buffering
# what it sends meanwhile, so it is kept short.
LOGIN_TIMEOUT_S = 10
# Keys one connection may offer, as sshd's MaxAuthTries caps them: asyncssh
# has no cap of its own, and each offer costs a decode. A machine's tunnel
# offers its one key twice, once to ask and once signed.
MAX_KEY_OFFERS = 6
# A machine logs in for its forward alone; a login with none up this long
# after it completed is closed rather than kept alive on keepalives.
FORWARD_TIMEOUT_S = 30
# A machine that went away without closing its connection (asleep, unplugged,
# off the network) is given up on after interval x count seconds of silence,
# so its tunnel reads as down and a dial to it fails at once.
KEEPALIVE_INTERVAL_S = 30
KEEPALIVE_COUNT_MAX = 3
# A machine that reconnected before its old connection timed out holds both
# for a while, and dials go to the newest. Past this many logins the oldest is
# closed, so a machine stuck reconnecting, or a tunnel's key used to open
# logins that never forward, cannot pile connections up.
MAX_CONNECTIONS_PER_TUNNEL = 4
# What a connection may negotiate. asyncssh's defaults also offer an RSA key
# exchange, which makes a new RSA key per connection, and Diffie-Hellman
# groups up to 8192 bits: tens of milliseconds of the gateway's one loop each,
# which anyone may ask for before logging in. These cost well under one, and
# OpenSSH has offered them since 6.5. No compression: zlib inflates what a
# stranger sends.
_KEX_ALGS = (
    "mlkem768x25519-sha256",
    "curve25519-sha256",
    "curve25519-sha256@libssh.org",
    "ecdh-sha2-nistp256",
)
_ENCRYPTION_ALGS = (
    "chacha20-poly1305@openssh.com",
    "aes256-gcm@openssh.com",
    "aes128-gcm@openssh.com",
    "aes256-ctr",
    "aes128-ctr",
)
_MAC_ALGS = ("hmac-sha2-256-etm@openssh.com", "hmac-sha2-512-etm@openssh.com")
# A machine's client answers a channel open as soon as its own sshd takes the
# connection. One that leaves it unanswered this long is wedged or hostile.
_CHANNEL_OPEN_TIMEOUT_S = 15
# How long a shutdown waits for connections to close and writes to land.
_SHUTDOWN_GRACE_S = 5
# A lock session that does not answer in this long is taken as gone: Postgres
# may already have ended it, and freed the lock for another gateway.
_LOCK_CHECK_TIMEOUT_S = 10
# Every machine connects again at once when the gateway restarts. asyncio's
# default backlog of 100 drops the rest of such a burst, and a dropped machine
# waits out TCP's retries, past its own connect timeout in the worst case. The
# kernel caps this at net.core.somaxconn.
_LISTEN_BACKLOG = 4096
# What a machine logs in as. Checked before the database is asked, so a
# stranger trying names costs no query.
_TUNNEL_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
# An unspecified address means every address, on both families: asyncio binds
# "0.0.0.0" for IPv4 alone, and a machine may only have a route over IPv6.
_ANY_ADDRESS = frozenset({"0.0.0.0", "::"})
# A refused login's name is logged as an example; a long one is cut, so a
# stranger cannot fill the log with it.
_LOGGED_NAME_MAX = 64


def _login_lookup_slots() -> int:
    """How many logins may wait on the database at once.

    Anyone may attempt a login, so their lookups get a third of the pool
    besides the lock's connection: a flood of them waits on itself, and the
    liveness writes that keep every tunnel up still find a connection.
    """
    return max(1, (get_conversation_pool_max() - 1) // 3)


def _kex_algs() -> list[str]:
    """The key exchanges above this asyncssh can do: the ML-KEM hybrid needs
    a crypto library that has it."""
    available = get_kex_algs()
    return [alg for alg in _KEX_ALGS if alg.encode() in available]


class _Forward(asyncssh.SSHListener):
    """A machine's ``-R`` forward, recorded rather than bound.

    Nothing listens on the gateway: a dial reaches the machine as a
    forwarded-tcpip channel the gateway opens itself. asyncssh closes a
    connection's listeners when the connection ends and on a
    cancel-tcpip-forward, which is how the gateway learns a forward is gone.
    """

    def __init__(
        self,
        gateway: Gateway,
        conn: asyncssh.SSHServerConnection,
        tunnel_id: str,
        listen_host: str,
        listen_port: int,
    ) -> None:
        super().__init__()
        self.gateway = gateway
        self.conn = conn
        self.tunnel_id = tunnel_id
        self.listen_host = listen_host
        self.listen_port = listen_port
        # Dials use a forward only once the database has its tunnel connected.
        self.ready = False

    def close(self) -> None:
        # A machine logs in for this forward alone, so a cancelled one ends
        # the login rather than leave it open with nothing to carry.
        self.gateway.close_login(self.tunnel_id, self.conn)

    async def wait_closed(self) -> None:
        return None


class Gateway:
    """The open tunnels, and their liveness as the workers read it."""

    def __init__(self, dialer_key: asyncssh.SSHKey) -> None:
        self.dialer_key = dialer_key
        # Per tunnel id, oldest first.
        self._forwards: dict[str, list[_Forward]] = {}
        self._logins: dict[str, list[asyncssh.SSHServerConnection]] = {}
        self._connections: set[asyncssh.SSHServerConnection] = set()
        # Connections on the machines' address not logged in yet.
        self._admission = Admission()
        self._refused_logins = Tally(logger, logging.INFO, "Refused %d logins")
        # The tunnels this run wrote connected and has not written down since:
        # a forward refused before its connect landed has nothing to undo.
        self._marked_up: set[str] = set()
        # Held by a login while it looks its tunnel up.
        self.login_lookups = asyncio.Semaphore(_login_lookup_slots())
        # One lock per tunnel with a liveness write in flight, so the
        # disconnect of a connection that just closed cannot land after the
        # connect of the one that replaced it.
        self._locks: weakref.WeakValueDictionary[str, asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._writes: set[asyncio.Task[None]] = set()
        # Set once the previous run's tunnels are marked down, which a
        # forward's connect waits for, so that sweep cannot land on it.
        self._swept = asyncio.Event()
        self._lock_lost = False
        self._stopping = False

    # -- Connections ----------------------------------------------------------

    def connection_made(
        self, conn: asyncssh.SSHServerConnection, *, checked: bool = True
    ) -> bool:
        """Take a new connection; False for one to close unanswered.

        ``checked`` is False on the machines' address, where anyone may
        connect: those are admitted (see ``admission``) and count against its
        caps until their login completes.
        """
        if not checked and not self._admission.admit(conn):
            return False
        self._connections.add(conn)
        return True

    def logged_in(self, conn: asyncssh.SSHServerConnection) -> None:
        self._admission.release(conn)

    def login_refused(self, why: str, example: str) -> None:
        self._refused_logins.add(why, example)

    def connection_lost(self, conn: asyncssh.SSHServerConnection) -> None:
        self._connections.discard(conn)
        self._admission.release(conn)

    # -- Machines' logins -------------------------------------------------------

    def login_completed(
        self, tunnel_id: str, conn: asyncssh.SSHServerConnection
    ) -> None:
        """Count a machine's login; past the cap, close its tunnel's oldest.

        Counted at login rather than at the forward: a login that never asks
        for one holds a file all the same.
        """
        logins = self._logins.setdefault(tunnel_id, [])
        logins.append(conn)
        if len(logins) > MAX_CONNECTIONS_PER_TUNNEL:
            logger.info(
                "Tunnel %s has more than %d connections; closing the oldest",
                tunnel_id,
                MAX_CONNECTIONS_PER_TUNNEL,
            )
            self.close_login(tunnel_id, logins[0])

    def login_lost(self, tunnel_id: str, conn: asyncssh.SSHServerConnection) -> None:
        logins = self._logins.get(tunnel_id)
        if not logins or conn not in logins:
            return
        logins.remove(conn)
        if not logins:
            del self._logins[tunnel_id]

    def close_login(self, tunnel_id: str, conn: asyncssh.SSHServerConnection) -> None:
        """Close one of a tunnel's logins, uncounted and its forward unlisted
        now: asyncssh reports the close a loop turn later."""
        self.login_lost(tunnel_id, conn)
        for fwd in [f for f in self._forwards.get(tunnel_id, ()) if f.conn is conn]:
            self.forward_closed(fwd)
        conn.close()

    # -- Forwards ---------------------------------------------------------------

    def _lock(self, tunnel_id: str) -> asyncio.Lock:
        lock = self._locks.get(tunnel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[tunnel_id] = lock
        return lock

    async def add_forward(self, fwd: _Forward) -> bool:
        """Record a machine's forward; False when it is refused.

        The forward is listed before the connect is written, so a disconnect
        written meanwhile for the tunnel's previous connection sees it and
        stands down.
        """
        tunnel_id = fwd.tunnel_id
        self._forwards.setdefault(tunnel_id, []).append(fwd)
        exists: bool | None
        await self._swept.wait()
        async with self._lock(tunnel_id):
            if fwd not in self._forwards.get(tunnel_id, ()):
                # Closed while it waited, and its disconnect may have gone
                # first: a connect written now would outlive the forward.
                return False
            try:
                exists = await tunnels_db.mark_connected(tunnel_id)
            except Exception as e:
                logger.warning(
                    "Refused the forward of tunnel %s: recording it as "
                    "connected failed: %s",
                    tunnel_id,
                    e,
                )
                exists = None
            if exists:
                self._marked_up.add(tunnel_id)
        if fwd not in self._forwards.get(tunnel_id, ()):
            return False  # its connection closed while the write was out
        if not exists:
            if exists is False:
                logger.info(
                    "Refused the forward of tunnel %s: it was removed", tunnel_id
                )
            self.forward_closed(fwd)
            return False
        fwd.ready = True
        logger.info(
            "Tunnel %s up (%d connected)", tunnel_id, len(self._forwards[tunnel_id])
        )
        return True

    def forward_closed(self, fwd: _Forward) -> None:
        """Forget a forward; when it was its tunnel's last, mark the tunnel down.

        Called by asyncssh on a connection's close and again by the login's own
        teardown, so a second call does nothing.
        """
        tunnel_id = fwd.tunnel_id
        forwards = self._forwards.get(tunnel_id)
        if not forwards or fwd not in forwards:
            return
        forwards.remove(fwd)
        if not forwards:
            del self._forwards[tunnel_id]
        if self._stopping:
            return
        if fwd.ready:
            logger.info("Tunnel %s down (%d connected)", tunnel_id, len(forwards))
        if not forwards:
            self._spawn(self._write_disconnected(tunnel_id))

    async def _write_disconnected(self, tunnel_id: str) -> None:
        async with self._lock(tunnel_id):
            if tunnel_id in self._forwards:
                return  # the machine came back meanwhile
            if tunnel_id not in self._marked_up:
                return  # no forward of it was written connected
            self._marked_up.discard(tunnel_id)
            try:
                await tunnels_db.mark_disconnected(tunnel_id)
            except Exception as e:
                # The row stops being refreshed, so it lapses on its own.
                logger.warning("Could not record tunnel %s as down: %s", tunnel_id, e)

    def _spawn(self, coro) -> None:
        task = asyncio.ensure_future(coro)
        self._writes.add(task)
        task.add_done_callback(self._writes.discard)

    def _drop(self, tunnel_id: str, why: str) -> None:
        logins = list(self._logins.get(tunnel_id, ()))
        if logins:
            logger.info("Closing tunnel %s: %s", tunnel_id, why)
        for conn in logins:
            self.close_login(tunnel_id, conn)

    # -- Dials ------------------------------------------------------------------

    def dial(
        self,
        dialer_conn: asyncssh.SSHServerConnection,
        tunnel_id: str,
        orig_host: str,
        orig_port: int,
    ):
        """A worker's channel to a machine, as asyncssh takes it from
        ``connection_requested``.

        The answer is awaitable: asyncssh confirms the worker's channel only
        once it yields, so the forwarded-tcpip channel opens on the machine's
        connection first, and a machine that refuses it fails the worker's open
        with the machine's own reason.

        Raises:
            asyncssh.ChannelOpenError: The tunnel has no connection open.
        """
        forwards = self._forwards.get(tunnel_id, ())
        fwd = next((f for f in reversed(forwards) if f.ready), None)
        if fwd is None:
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, GATEWAY_NOT_CONNECTED
            )
        return self._relay(fwd, dialer_conn, orig_host, orig_port)

    async def _relay(
        self,
        fwd: _Forward,
        dialer_conn: asyncssh.SSHServerConnection,
        orig_host: str,
        orig_port: int,
    ) -> asyncssh.SSHForwarder:
        """Open the machine's side, then hand asyncssh the worker's.

        As asyncssh's own ``forward_tunneled_connection``: the two forwarders
        are peers, and asyncssh moves the bytes between the channels with flow
        control on both. Nothing may be awaited between the open and the
        return, or the machine's first bytes (its sshd's banner) would reach a
        forwarder with no peer yet and be lost.

        Every failure leaves as a ``ChannelOpenError``: asyncssh answers the
        worker's open only for that one, and treats anything else as a fault
        that closes the worker's whole connection, every other dial on it with
        it.
        """
        try:
            async with asyncio.timeout(_CHANNEL_OPEN_TIMEOUT_S):
                _, peer = await fwd.conn.create_connection(
                    asyncssh.SSHForwarder,
                    fwd.listen_host,
                    fwd.listen_port,
                    orig_host,
                    orig_port,
                )
        except TimeoutError:
            # SSH cannot withdraw an open the peer has not answered: the
            # channel would stay pending as long as the machine stays
            # connected, and closing its connection is what lets go of it.
            logger.warning(
                "Tunnel %s left a channel open unanswered for %ds; closing it",
                fwd.tunnel_id,
                _CHANNEL_OPEN_TIMEOUT_S,
            )
            self.close_login(fwd.tunnel_id, fwd.conn)
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, GATEWAY_NOT_CONNECTED
            ) from None
        except asyncssh.ChannelOpenError:
            # A connection that is closing refuses new channels a loop turn
            # before it reads as closed, and that is a machine gone, not a
            # machine refusing.
            await asyncio.sleep(0)
            if fwd.conn.is_closed():
                raise asyncssh.ChannelOpenError(
                    asyncssh.OPEN_CONNECT_FAILED, GATEWAY_NOT_CONNECTED
                ) from None
            raise
        except (OSError, asyncssh.Error):
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, GATEWAY_NOT_CONNECTED
            ) from None
        except Exception as e:
            logger.warning(
                "Opening a channel to tunnel %s failed", fwd.tunnel_id, exc_info=e
            )
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, GATEWAY_NOT_CONNECTED
            ) from None
        if dialer_conn.is_closed():
            # asyncssh drops the worker's side unopened; the machine's side
            # would otherwise stay open until its sshd gave up on the login.
            peer.close()
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_CONNECT_FAILED, "SSH connection closed"
            )
        return asyncssh.SSHForwarder(peer)

    # -- Heartbeat --------------------------------------------------------------

    async def _heartbeat(self, lock_conn: AsyncConnection, stop: asyncio.Event) -> None:
        """Keep the open tunnels' rows fresh, and close any whose row is gone.

        A deleted row is how a removed computer's tunnel ends: the workers
        delete it, and the gateway is the one holding the connection.

        The gateway's lock lasts only as long as its session. Once a Postgres
        restart or a reset connection ends it, a second gateway may start and
        write liveness too, so this one stops, to exit non-zero and be
        restarted to take the lock again.
        """
        while True:
            await asyncio.sleep(tunnels_db.HEARTBEAT.total_seconds())
            try:
                async with asyncio.timeout(_LOCK_CHECK_TIMEOUT_S):
                    await tunnels_db.check_gateway_lock(lock_conn)
            except Exception as e:
                logger.error("Lost the tunnel gateway's database lock; stopping: %r", e)
                self._lock_lost = True
                stop.set()
                return
            tunnel_ids = list(self._forwards)
            if not tunnel_ids:
                continue
            try:
                existing = await tunnels_db.mark_seen(tunnel_ids)
            except Exception as e:
                logger.warning(
                    "Could not refresh %d open tunnels: %s", len(tunnel_ids), e
                )
                continue
            for tunnel_id in tunnel_ids:
                if tunnel_id not in existing:
                    self._drop(tunnel_id, "it was removed")

    # -- Serving ----------------------------------------------------------------

    async def listen(self, host_key: asyncssh.SSHKey) -> asyncssh.SSHAcceptor:
        """Bind the gateway's addresses; logins are taken from here on.

        With ``SSH_GATEWAY_DIAL_LISTEN`` set, the workers' login has an
        address of its own, one the deployment keeps off the internet, and
        the machines' address refuses it: a copy of the dialer key is then of
        no use from outside. Without it, one address takes both.

        Raises:
            OSError: An address could not be bound.
        """
        dial_address = gateway_dial_listen()
        acceptors = [
            await self._listen(host_key, gateway_listen(), dialer=dial_address is None)
        ]
        if dial_address is not None:
            try:
                acceptors.append(
                    await self._listen(host_key, dial_address, machines=False)
                )
            except BaseException:
                acceptors[0].close()
                raise
        return acceptors

    async def _listen(
        self,
        host_key: asyncssh.SSHKey,
        address: GatewayAddress,
        *,
        dialer: bool = True,
        machines: bool = True,
    ) -> asyncssh.SSHAcceptor:
        if machines:
            admission.require_hooks()
        host = "" if address.host in _ANY_ADDRESS else address.host
        acceptor = await asyncssh.listen(
            host,
            address.port,
            backlog=_LISTEN_BACKLOG,
            server_factory=lambda: _Login(self, dialer=dialer, machines=machines),
            server_host_keys=[host_key],
            config=None,
            login_timeout=LOGIN_TIMEOUT_S,
            kex_algs=_kex_algs(),
            encryption_algs=list(_ENCRYPTION_ALGS),
            mac_algs=list(_MAC_ALGS),
            compression_algs=["none"],
            keepalive_interval=KEEPALIVE_INTERVAL_S,
            keepalive_count_max=KEEPALIVE_COUNT_MAX,
            public_key_auth=True,
            password_auth=False,
            kbdint_auth=False,
            host_based_auth=False,
            gss_host=None,
            gss_kex=False,
            gss_auth=False,
            authorized_client_keys=None,
            x509_trusted_certs=None,
            allow_pty=False,
            agent_forwarding=False,
            x11_forwarding=False,
            sftp_factory=None,
            allow_scp=False,
        )
        bound = ", ".join(_bound(a) for a in acceptor.get_addresses())
        takes = {(True, True): "machines and the workers", (False, True): "machines"}
        takes = takes.get((dialer, machines), "the workers")
        logger.info("Tunnel gateway listening on %s for %s", bound, takes)
        return acceptor

    async def serve(
        self,
        acceptors: list[asyncssh.SSHAcceptor],
        stop: asyncio.Event,
        lock_conn: AsyncConnection,
    ) -> bool:
        """Serve what *acceptors* take until *stop* is set, then close it all.

        Marks the previous run's tunnels down first, only now that this run
        holds the address: a gateway that cannot bind writes nothing. False
        when the gateway stopped for losing the lock held on *lock_conn*.
        """
        heartbeat: asyncio.Task[None] | None = None
        try:
            closed = await tunnels_db.mark_all_disconnected()
            if closed:
                logger.info("Marked %d tunnels down from the previous run", closed)
            self._swept.set()
            heartbeat = asyncio.create_task(self._heartbeat(lock_conn, stop))
            await stop.wait()
        finally:
            self._stopping = True
            if heartbeat is not None:
                heartbeat.cancel()
            for acceptor in acceptors:
                acceptor.close()
            await self._shutdown()
        return not self._lock_lost

    async def _shutdown(self) -> None:
        conns = list(self._connections)
        logger.info("Tunnel gateway stopping; closing %d connections", len(conns))
        for conn in conns:
            conn.close()
        pending = [conn.wait_closed() for conn in conns]
        pending += list(self._writes)
        if pending:
            await asyncio.wait(
                [asyncio.ensure_future(p) for p in pending], timeout=_SHUTDOWN_GRACE_S
            )
        if self._lock_lost:
            # The tunnels up now may be another gateway's, which took the
            # lock; whichever gateway holds it next marks this run's down.
            return
        try:
            await tunnels_db.mark_all_disconnected()
        except Exception as e:
            logger.warning("Could not mark the tunnels down at shutdown: %s", e)


def _bound(addr: tuple) -> str:
    host, port = addr[0], addr[1]
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


class _Login(asyncssh.SSHServer):
    """One connection to the gateway: a worker's or a machine's.

    The login decides what the connection may do. The dialer may only open
    channels to machines, and a machine may only ask for its forward. Sessions,
    SFTP, agent and X11 forwarding and Unix socket forwarding stay refused, as
    asyncssh's defaults have them.
    """

    def __init__(self, gateway: Gateway, *, dialer: bool, machines: bool) -> None:
        self._gateway = gateway
        # Which logins this connection's address takes (see ``Gateway.listen``).
        self._takes_dialer = dialer
        self._takes_machines = machines
        self._conn: asyncssh.SSHServerConnection | None = None
        self._username: str | None = None
        self._key: asyncssh.SSHKey | None = None
        self._tunnel_id: str | None = None
        self._refusal = "its key was not accepted"
        self._lookup_error: str | None = None
        self._key_offers = 0
        self._authed = False
        self._closed = False
        self._forward: _Forward | None = None
        self._forward_due: asyncio.TimerHandle | None = None

    def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
        self._conn = conn
        if not self._gateway.connection_made(conn, checked=not self._takes_machines):
            # Before asyncssh sends its banner, which it then skips.
            conn.abort()

    def connection_lost(self, exc: Exception | None) -> None:
        self._closed = True
        if self._forward_due is not None:
            self._forward_due.cancel()
            self._forward_due = None
        if self._conn is not None:
            self._gateway.connection_lost(self._conn)
            if self._tunnel_id is not None:
                self._gateway.login_lost(self._tunnel_id, self._conn)
        if self._forward is not None:
            # Also covers a forward still waiting on its connect write, which
            # asyncssh has not taken as a listener yet.
            self._gateway.forward_closed(self._forward)
            self._forward = None
        if self._username is not None and not self._authed:
            example = repr(self._username[:_LOGGED_NAME_MAX])
            if self._lookup_error is not None:
                example += f" ({self._lookup_error})"
            self._gateway.login_refused(self._refusal, example)

    # -- Authentication ---------------------------------------------------------

    async def begin_auth(self, username: str) -> bool:
        if self._username is not None:
            # asyncssh begins again on every change of name, and has no cap
            # on attempts: each tunnel id tried would cost a lookup. As with
            # OpenSSH's sshd, a connection keeps the name it gave first.
            self._refusal = "it changed its login name"
            raise asyncssh.ProtocolError("Change of username not allowed")
        self._username = username
        # Until the key to accept is known, a connection that closes (the
        # login timeout, over a lookup that hangs) was never checked at all.
        self._refusal = "it closed before its login was checked"
        if username == GATEWAY_DIALER:
            if not self._takes_dialer:
                self._refusal = "the workers log in on the dial address"
                return True
            self._key = self._gateway.dialer_key
            self._refusal = "its key was not accepted"
            return True
        if not self._takes_machines or not _TUNNEL_ID.fullmatch(username):
            self._refusal = "no such login"
            return True
        try:
            # No longer than the login itself: asyncssh does not cancel this
            # when the connection closes, so a lookup held up by the database
            # would otherwise outlive it, and each connection after it add one.
            async with asyncio.timeout(LOGIN_TIMEOUT_S):
                async with self._gateway.login_lookups:
                    if self._closed:
                        return True  # its login timed out while it waited
                    row = await tunnels_db.get_tunnel(username)
        except Exception as e:
            # Not a refusal: a machine reads "Permission denied" as its tunnel
            # removed and waits ten minutes before asking again. A disconnect
            # is retried within seconds, which is what a database that is
            # down or busy for a moment calls for.
            self._refusal = "looking up the tunnel failed (told to retry)"
            self._lookup_error = str(e) or type(e).__name__
            raise asyncssh.ServiceNotAvailable(
                "The gateway cannot check logins right now; try again shortly"
            ) from None
        if row is None:
            self._refusal = "no such tunnel"
            return True
        try:
            self._key = asyncssh.import_public_key(row["public_key"])
        except (asyncssh.KeyImportError, ValueError):
            self._refusal = "the tunnel's key does not parse"
            return True
        self._tunnel_id = row["tunnel_id"]
        self._refusal = "its key was not accepted"
        return True

    def public_key_auth_supported(self) -> bool:
        return True

    def validate_public_key(self, username: str, key: asyncssh.SSHKey) -> bool:
        self._key_offers += 1
        if self._key_offers > MAX_KEY_OFFERS:
            self._refusal = "it offered too many keys"
            assert self._conn is not None
            self._conn.abort()
            return False
        return self._key is not None and key.public_data == self._key.public_data

    def password_auth_supported(self) -> bool:
        return False

    def kbdint_auth_supported(self) -> bool:
        return False

    def host_based_auth_supported(self) -> bool:
        return False

    def auth_completed(self) -> None:
        self._authed = True
        assert self._conn is not None
        self._gateway.logged_in(self._conn)
        if self._tunnel_id is None:
            return  # the workers' login
        self._gateway.login_completed(self._tunnel_id, self._conn)
        self._forward_due = asyncio.get_running_loop().call_later(
            FORWARD_TIMEOUT_S, self._forward_overdue
        )

    def _forward_overdue(self) -> None:
        self._forward_due = None
        if self._forward is not None and self._forward.ready:
            return
        assert self._conn is not None and self._tunnel_id is not None
        logger.info(
            "Closing a login of tunnel %s: no forward up within %ds",
            self._tunnel_id,
            FORWARD_TIMEOUT_S,
        )
        self._gateway.close_login(self._tunnel_id, self._conn)

    # -- What each login may do ---------------------------------------------------

    def connection_requested(
        self, dest_host: str, dest_port: int, orig_host: str, orig_port: int
    ):
        """A worker's dial; ``dest_port`` is not used, as the machine chose
        where its forward leads when it opened it."""
        if not self._authed or self._username != GATEWAY_DIALER:
            raise asyncssh.ChannelOpenError(
                asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED, "not permitted"
            )
        assert self._conn is not None
        return self._gateway.dial(self._conn, dest_host, orig_host, orig_port)

    def server_requested(self, listen_host: str, listen_port: int):
        # One forward per connection keeps where a dial leads unambiguous.
        if not self._authed or self._tunnel_id is None or self._forward is not None:
            return False
        return self._open_forward(listen_host, listen_port)

    async def _open_forward(self, listen_host: str, listen_port: int):
        assert self._conn is not None and self._tunnel_id is not None
        fwd = _Forward(
            self._gateway, self._conn, self._tunnel_id, listen_host, listen_port
        )
        # Kept even when refused: each ask costs a write, so a login gets one,
        # and the deadline set at login then ends it.
        self._forward = fwd
        if not await self._gateway.add_forward(fwd):
            return False
        if self._forward_due is not None:
            self._forward_due.cancel()
            self._forward_due = None
        return fwd


async def _run() -> int:
    pool = get_or_create_pool()
    await pool.open(wait=True)
    try:
        # Held for the life of the process, so one pooled connection is the
        # lock's; the lock ends with its session when the pool closes.
        async with get_db_connection() as lock_conn:
            if not await tunnels_db.try_gateway_lock(lock_conn):
                logger.error("Another tunnel gateway holds this database")
                return 1
            _, host_private = await gateway_host_key()
            dialer_public, _ = await gateway_dialer_key()
            gateway = Gateway(asyncssh.import_public_key(dialer_public))
            try:
                acceptors = await gateway.listen(
                    asyncssh.import_private_key(host_private)
                )
            except OSError as e:
                logger.error("Tunnel gateway could not listen: %s", e)
                return 1
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stop.set)
            if not await gateway.serve(acceptors, stop, lock_conn):
                return 1
    finally:
        await pool.close()
    logger.info("Tunnel gateway stopped")
    return 0


def _raise_open_file_limit() -> None:
    """Lift the soft limit on open files to the hard one.

    Every connection holds a file. The soft limit is often 1024 for select()'s
    sake while the hard one is far higher, and the gateway's loop does not use
    select(). At the limit asyncio's accept loop spins, logging a traceback per
    refused accept, and the gateway serves almost nothing else.
    """
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # An unlimited hard limit means macOS, which caps files per process with a
    # sysctl instead; a native run there keeps the soft limit it was started
    # with (raise it with ulimit -n). The container's hard limit is a number.
    if hard == resource.RLIM_INFINITY or soft >= hard:
        return
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except (ValueError, OSError):
        return  # a hard limit above what the kernel allows one process
    logger.info("Raised the open file limit from %d to %d", soft, hard)


def main() -> None:
    configure_logging()
    if not encryption_configured():
        # The gateway's keys are kept encrypted, as the workers read them.
        logger.error("BYOK_ENCRYPTION_KEY is not set; the tunnel gateway needs it")
        sys.exit(1)
    _raise_open_file_limit()
    sys.exit(asyncio.run(_run()))
