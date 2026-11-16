"""A worker's dial relayed to a machine: the channel opened on the machine's
connection, the two forwarders that carry the bytes between it and the
worker's channel, and the opens a worker gave up on.

SSH cannot withdraw a channel open, so an open the worker stopped waiting for
is still waited out here and its channel closed once it opens, and a machine
that leaves too many unanswered, or one too long, has its connection closed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Protocol

import asyncssh

from ptc_agent.core.sandbox.providers._ssh_connect import (
    GATEWAY_CHANNEL_OPEN_TIMEOUT_S,
    GATEWAY_NO_ANSWER,
    GATEWAY_NOT_CONNECTED,
)

logger = logging.getLogger(__name__)

# SSH cannot withdraw an open, so one a worker gave up on is still waited for
# and closed once it opens. A machine that leaves one unanswered this long, or
# this many at once, is wedged rather than busy, and its connection is closed,
# which lets go of them all.
_ABANDONED_OPEN_LIMIT_S = 300
_MAX_ABANDONED_OPENS = 64
# What the machine's end holds for a worker's end not yet open: an sshd's
# banner is a line, so more is not a login, and one gateway serves every user.
_MAX_HELD_BYTES = 64 * 1024
# The worker's end opens a few loop turns after the machine's; one that has
# not joined by now never will, and the machine's channel is let go.
_JOIN_TIMEOUT_S = 10


class Forward(Protocol):
    """A machine's ``-R`` forward, as a relay reaches the machine through it."""

    conn: asyncssh.SSHServerConnection
    tunnel_id: str
    listen_host: str
    listen_port: int


class _MachineSide(asyncssh.SSHForwarder):
    """The machine's end of a relay. Its sshd speaks first, and the banner can
    land before the worker's end has its channel open: the open is awaited
    (shielded, past a timeout) before the worker's end exists. asyncssh's own
    forwarder holds such bytes but never passes them on, and a worker that
    never sees the banner waits out its login. So what arrives early is held
    here and handed over once that end is open.

    Held in fields of its own, through ``SSHForwarder``'s public methods
    alone, so an asyncssh that changes its forwarder's internals cannot drop
    the banner or send it twice. Its channel's ``pause_reading`` would hold
    the bytes too, but an EOF with nothing before it still reaches a paused
    session, and a close behind held bytes waits for a reader that may never
    come.
    """

    def __init__(self) -> None:
        super().__init__()
        self._worker: _WorkerSide | None = None  # once its channel is open
        self._held = b""
        self._held_eof = False
        self._released = False
        self._join_due: asyncio.TimerHandle | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        self._join_due = asyncio.get_running_loop().call_later(
            _JOIN_TIMEOUT_S, self._join_overdue
        )

    def _join_overdue(self) -> None:
        if self._worker is None:
            self.close()

    def data_received(self, data: bytes, datatype: int | None = None) -> None:
        if self._worker is not None:
            super().data_received(data, datatype)
        elif not self._released:
            if len(self._held) + len(data) > _MAX_HELD_BYTES:
                self.close()
            else:
                self._held += data

    def eof_received(self) -> bool:
        if self._worker is None:
            self._held_eof = True
            return True
        return super().eof_received()

    def was_eof_received(self) -> bool:
        return self._held_eof or super().was_eof_received()

    def close(self) -> None:
        self._released = True
        if self._join_due is not None:
            self._join_due.cancel()
            self._join_due = None
        super().close()

    def join(self, worker: _WorkerSide) -> None:
        """Pass on to the worker's end, its channel now open, what arrived
        before it, and close that end when this one closed meanwhile: its
        peer was not yet set to close with it, and a worker left open would
        wait out its login for a machine that is gone."""
        if self._join_due is not None:
            self._join_due.cancel()
            self._join_due = None
        self._worker = worker
        held, self._held = self._held, b""
        if held:
            worker.write(held)
        if self._held_eof:
            worker.write_eof()
        if self._released:
            worker.close()


class _WorkerSide(asyncssh.SSHForwarder):
    """The worker's end, which takes what the machine's end held for it as
    soon as its channel is open."""

    def __init__(self, machine: _MachineSide) -> None:
        super().__init__(machine)
        self._machine = machine

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        self._machine.join(self)


class AbandonedOpens:
    """Per machine connection, the channel opens a worker gave up on that are
    still unanswered."""

    def __init__(
        self, close_login: Callable[[str, asyncssh.SSHServerConnection], None]
    ) -> None:
        self._close_login = close_login
        self._pending: dict[asyncssh.SSHServerConnection, set[asyncio.Future]] = {}

    def pending_for(self, conn: asyncssh.SSHServerConnection) -> int:
        """How many opens on ``conn`` are still unanswered."""
        return len(self._pending.get(conn, ()))

    def add(self, fwd: Forward, opening: asyncio.Future, asked: float) -> None:
        """Wait out an open no worker waits for: close its channel if it
        opens, and close the machine's connection if it stays unanswered."""
        conn = fwd.conn
        pending = self._pending.setdefault(conn, set())
        pending.add(opening)
        loop = asyncio.get_running_loop()
        overdue = loop.call_later(_ABANDONED_OPEN_LIMIT_S, self._overdue, fwd, opening)

        def landed(task: asyncio.Future) -> None:
            overdue.cancel()
            pending.discard(task)
            if not pending and self._pending.get(conn) is pending:
                del self._pending[conn]
            if task.cancelled() or task.exception() is not None:
                return  # refused, or let go with its connection
            _, peer = task.result()
            logger.info(
                "Tunnel %s answered a channel open after %ds; closing the channel",
                fwd.tunnel_id,
                loop.time() - asked,
            )
            peer.close()

        opening.add_done_callback(landed)
        if len(pending) > _MAX_ABANDONED_OPENS:
            logger.warning(
                "Tunnel %s has %d channel opens unanswered; closing it",
                fwd.tunnel_id,
                len(pending),
            )
            self._close_login(fwd.tunnel_id, conn)

    def _overdue(self, fwd: Forward, opening: asyncio.Future) -> None:
        if opening.done():
            return
        logger.warning(
            "Tunnel %s left a channel open unanswered for %ds; closing it",
            fwd.tunnel_id,
            _ABANDONED_OPEN_LIMIT_S,
        )
        self._close_login(fwd.tunnel_id, fwd.conn)


async def open_relay(
    fwd: Forward,
    dialer_conn: asyncssh.SSHServerConnection,
    orig_host: str,
    orig_port: int,
    abandoned: AbandonedOpens,
) -> asyncssh.SSHForwarder:
    """Open the machine's side, then hand asyncssh the worker's.

    As asyncssh's own ``forward_tunneled_connection``: the two forwarders
    are peers, and asyncssh moves the bytes between the channels with flow
    control on both. What the machine sends before the worker's side is
    open (its sshd's banner) is held by the machine's side and handed
    over (see ``_MachineSide``).

    Every failure leaves as a ``ChannelOpenError``: asyncssh answers the
    worker's open only for that one, and treats anything else as a fault
    that closes the worker's whole connection, every other dial on it with
    it. A machine that does not answer in time fails only this dial: its
    tunnel stays up, since a busy uplink is slow rather than gone.

    The open runs shielded: a cancelled one would still open on the
    machine, as a channel nothing reads or closes, so one given up on is
    handed to ``abandoned`` instead.
    """
    asked = asyncio.get_running_loop().time()
    opening = asyncio.ensure_future(
        fwd.conn.create_connection(
            _MachineSide,
            fwd.listen_host,
            fwd.listen_port,
            orig_host,
            orig_port,
        )
    )
    try:
        # A machine's client answers a channel open as soon as its own sshd
        # takes the connection, but the answer travels behind whatever the
        # machine is already sending: an uplink full of a backup or a large
        # upload holds it back far longer than a worker should wait. Past this
        # the worker's dial fails and the machine's connection stays up, its
        # liveness the keepalives' to judge.
        async with asyncio.timeout(GATEWAY_CHANNEL_OPEN_TIMEOUT_S):
            _, peer = await asyncio.shield(opening)
    except TimeoutError:
        logger.warning(
            "Tunnel %s left a channel open unanswered for %ds; failing the "
            "dial and keeping the tunnel",
            fwd.tunnel_id,
            GATEWAY_CHANNEL_OPEN_TIMEOUT_S,
        )
        abandoned.add(fwd, opening, asked)
        raise asyncssh.ChannelOpenError(
            asyncssh.OPEN_CONNECT_FAILED, GATEWAY_NO_ANSWER
        ) from None
    except asyncio.CancelledError:
        abandoned.add(fwd, opening, asked)
        raise
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
    return _WorkerSide(peer)
