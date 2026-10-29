"""The tunnel gateway's liveness writes and its one-instance rule.

The gateway is the only writer of whether a machine's tunnel is up, so a
second gateway on the same database exits before it writes anything, and one
that cannot bind its address writes nothing either. One tunnel's writes land
in the order its connections came and went: a connection that closes after
its replacement came up writes no disconnect.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta

import asyncssh
import pytest

from ptc_agent.core.sandbox.providers.ssh import generate_client_key
from src.server.ssh_gateway import admission, server
from src.server.services.ssh_tunnels import GatewayAddress
from src.server.ssh_gateway.server import Gateway, _Forward

TUNNEL = "0f8fad5b-d9cb-469f-a165-70867728950e"


class _Tunnels:
    """The database module as the gateway uses it, recording each write."""

    HEARTBEAT = timedelta(minutes=1)

    def __init__(self, *, locked_elsewhere: bool = False) -> None:
        self.locked_elsewhere = locked_elsewhere
        self.writes: list[tuple[str, ...]] = []
        self.sweep_gate: asyncio.Event | None = None
        self.disconnect_gate: asyncio.Event | None = None
        # "alive", "gone" (the session ended) or "hung" (it never answers).
        self.lock_session = "alive"
        self.tunnel_rows: dict[str, dict] = {}

    async def try_gateway_lock(self, conn: object) -> bool:
        return not self.locked_elsewhere

    async def check_gateway_lock(self, conn: object) -> None:
        if self.lock_session == "gone":
            raise ConnectionError("server closed the connection unexpectedly")
        if self.lock_session == "hung":
            await asyncio.Event().wait()

    async def mark_all_disconnected(self) -> int:
        if self.sweep_gate is not None:
            await self.sweep_gate.wait()
        self.writes.append(("all down",))
        return 0

    async def mark_connected(self, tunnel_id: str) -> bool:
        self.writes.append(("up", tunnel_id))
        return True

    async def mark_disconnected(self, tunnel_id: str) -> None:
        if self.disconnect_gate is not None:
            await self.disconnect_gate.wait()
        self.writes.append(("down", tunnel_id))

    async def mark_seen(self, tunnel_ids) -> set[str]:
        return set(tunnel_ids)

    async def get_tunnel(self, tunnel_id: str) -> dict | None:
        return self.tunnel_rows.get(tunnel_id)


class _Conn:
    def __init__(self, peer: str = "203.0.113.7") -> None:
        self.closed = False
        self.aborted = False
        self.peer = peer

    def get_extra_info(self, name: str, default: object = None) -> object:
        return (self.peer, 40000) if name == "peername" else default

    def close(self) -> None:
        self.closed = True

    def abort(self) -> None:
        self.closed = self.aborted = True

    def data_received(self, data: bytes, datatype: object = None) -> None:
        pass

    def is_closed(self) -> bool:
        return self.closed

    async def wait_closed(self) -> None:
        pass


class _Acceptor:
    def close(self) -> None:
        pass


@pytest.fixture
def tunnels(monkeypatch: pytest.MonkeyPatch) -> _Tunnels:
    fake = _Tunnels()
    monkeypatch.setattr(server, "tunnels_db", fake)
    return fake


def _forward(gateway: Gateway) -> _Forward:
    return _Forward(gateway, _Conn(), TUNNEL, "langalpha", 22)  # type: ignore[arg-type]


async def _settled(gateway: Gateway) -> None:
    """Let the liveness writes the gateway started land."""
    while gateway._writes:
        await asyncio.gather(*gateway._writes)


def _serve(gateway: Gateway, stop: asyncio.Event) -> asyncio.Task[bool]:
    return asyncio.create_task(gateway.serve([_Acceptor()], stop, object()))  # type: ignore[arg-type]


@asynccontextmanager
async def _serving(gateway: Gateway):
    stop = asyncio.Event()
    serving = _serve(gateway, stop)
    await gateway._swept.wait()
    try:
        yield
    finally:
        stop.set()
        assert await serving  # a stop asked for, not a lost lock


@asynccontextmanager
async def _machines_address():
    gateway = Gateway(object())  # type: ignore[arg-type]
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    acceptor = await gateway._listen(
        host_key, GatewayAddress("127.0.0.1", 0), dialer=False
    )
    try:
        yield acceptor.get_port()
    finally:
        acceptor.close()


async def _stranger(port: int, **options: object) -> None:
    conn = await asyncssh.connect(
        "127.0.0.1",
        port,
        **{
            "username": TUNNEL,
            "known_hosts": None,
            "client_keys": None,
            "agent_path": None,
            "config": None,
            "connect_timeout": 5,
            **options,
        },
    )
    conn.close()


def _liveness(tunnels: _Tunnels) -> list[tuple[str, ...]]:
    return [w for w in tunnels.writes if w[0] != "all down"]


class TestOrdering:
    @pytest.mark.asyncio
    async def test_a_connection_closing_after_its_replacement_came_up_writes_no_disconnect(
        self, tunnels
    ):
        gateway = Gateway(object())  # type: ignore[arg-type]
        old, new = _forward(gateway), _forward(gateway)
        async with _serving(gateway):
            assert await gateway.add_forward(old)
            assert await gateway.add_forward(new)

            gateway.forward_closed(old)
            await _settled(gateway)
            assert _liveness(tunnels) == [("up", TUNNEL), ("up", TUNNEL)]

            gateway.forward_closed(new)
            await _settled(gateway)
            assert _liveness(tunnels)[-1] == ("down", TUNNEL)

    @pytest.mark.asyncio
    async def test_a_disconnect_not_yet_written_stands_down_for_a_replacement(
        self, tunnels
    ):
        gateway = Gateway(object())  # type: ignore[arg-type]
        old, new = _forward(gateway), _forward(gateway)
        async with _serving(gateway):
            assert await gateway.add_forward(old)

            # The machine reconnects before the old connection's disconnect
            # has been written.
            gateway.forward_closed(old)
            assert await gateway.add_forward(new)
            await _settled(gateway)

            assert _liveness(tunnels) == [("up", TUNNEL), ("up", TUNNEL)]

    @pytest.mark.asyncio
    async def test_a_disconnect_already_being_written_lands_before_the_connect(
        self, tunnels
    ):
        gateway = Gateway(object())  # type: ignore[arg-type]
        old, new = _forward(gateway), _forward(gateway)
        async with _serving(gateway):
            assert await gateway.add_forward(old)
            tunnels.disconnect_gate = asyncio.Event()
            gateway.forward_closed(old)
            await asyncio.sleep(0)  # the write has the tunnel's lock

            adding = asyncio.create_task(gateway.add_forward(new))
            await asyncio.sleep(0)
            tunnels.disconnect_gate.set()

            assert await adding
            await _settled(gateway)
            # The tunnel reads as up: its connect is the newer write.
            assert _liveness(tunnels) == [
                ("up", TUNNEL),
                ("down", TUNNEL),
                ("up", TUNNEL),
            ]

    @pytest.mark.asyncio
    async def test_a_connect_waits_for_the_previous_run_to_be_marked_down(
        self, tunnels
    ):
        gateway = Gateway(object())  # type: ignore[arg-type]
        tunnels.sweep_gate = asyncio.Event()
        stop = asyncio.Event()
        serving = _serve(gateway, stop)
        adding = asyncio.create_task(gateway.add_forward(_forward(gateway)))
        for _ in range(5):
            await asyncio.sleep(0)
        assert tunnels.writes == []

        tunnels.sweep_gate.set()
        assert await adding
        stop.set()
        await serving

        assert tunnels.writes[:2] == [("all down",), ("up", TUNNEL)]

    @pytest.mark.asyncio
    async def test_a_forward_closed_before_its_connect_is_written_writes_neither(
        self, tunnels
    ):
        gateway = Gateway(object())  # type: ignore[arg-type]
        tunnels.sweep_gate = asyncio.Event()
        stop = asyncio.Event()
        serving = _serve(gateway, stop)
        fwd = _forward(gateway)
        adding = asyncio.create_task(gateway.add_forward(fwd))
        await asyncio.sleep(0)
        gateway.forward_closed(fwd)  # dropped during the startup sweep
        await _settled(gateway)

        tunnels.sweep_gate.set()
        assert not await adding
        await _settled(gateway)
        stop.set()
        await serving

        assert _liveness(tunnels) == []

    @pytest.mark.asyncio
    async def test_a_refused_forward_writes_no_disconnect(self, tunnels, monkeypatch):
        async def removed(tunnel_id: str) -> bool:
            return False

        monkeypatch.setattr(tunnels, "mark_connected", removed)
        gateway = Gateway(object())  # type: ignore[arg-type]
        async with _serving(gateway):
            assert not await gateway.add_forward(_forward(gateway))
            await _settled(gateway)

        assert _liveness(tunnels) == []


class _Pool:
    def __init__(self) -> None:
        self.closed = False

    async def open(self, wait: bool = False) -> None:
        pass

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def process(monkeypatch: pytest.MonkeyPatch) -> _Pool:
    """``_run``'s surroundings: a pool, a connection for the lock, the keys."""
    pool = _Pool()

    @asynccontextmanager
    async def connection(conn=None):
        yield object()

    host, dialer = generate_client_key("host"), generate_client_key("dialer")

    async def host_key():
        return host.public_key, host.private_key

    async def dialer_key():
        return dialer.public_key, dialer.private_key

    monkeypatch.setattr(server, "get_or_create_pool", lambda: pool)
    monkeypatch.setattr(server, "get_db_connection", connection)
    monkeypatch.setattr(server, "gateway_host_key", host_key)
    monkeypatch.setattr(server, "gateway_dialer_key", dialer_key)
    return pool


class TestOneInstance:
    @pytest.mark.asyncio
    async def test_a_second_gateway_exits_before_it_binds_or_writes(
        self, tunnels, process, monkeypatch, caplog
    ):
        tunnels.locked_elsewhere = True

        async def listen(self, host_key):
            raise AssertionError("a refused gateway bound its address")

        monkeypatch.setattr(Gateway, "listen", listen)

        with caplog.at_level(logging.ERROR, logger=server.__name__):
            assert await server._run() == 1

        assert tunnels.writes == []
        assert "Another tunnel gateway holds this database" in caplog.text
        assert process.closed

    @pytest.mark.asyncio
    async def test_a_gateway_that_cannot_bind_writes_nothing(
        self, tunnels, process, monkeypatch
    ):
        async def listen(self, host_key):
            raise OSError("address already in use")

        monkeypatch.setattr(Gateway, "listen", listen)

        assert await server._run() == 1

        assert tunnels.writes == []
        assert process.closed


class TestLostLock:
    """A gateway whose lock session ends stops, so a restart can take it again."""

    @pytest.fixture(autouse=True)
    def fast_heartbeat(self, tunnels: _Tunnels) -> None:
        tunnels.HEARTBEAT = timedelta(0)

    @pytest.mark.asyncio
    async def test_it_stops_without_marking_another_gateways_tunnels_down(
        self, tunnels
    ):
        gateway = Gateway(object())  # type: ignore[arg-type]
        tunnels.lock_session = "gone"
        stop = asyncio.Event()
        serving = _serve(gateway, stop)
        await gateway._swept.wait()
        assert await gateway.add_forward(_forward(gateway))

        assert await asyncio.wait_for(serving, 5) is False
        # Once another gateway may hold the lock, the tunnels up are its own:
        # neither this run's tunnel nor the rest are written down.
        assert tunnels.writes == [("all down",), ("up", TUNNEL)]

    @pytest.mark.asyncio
    async def test_a_session_that_never_answers_counts_as_gone(
        self, tunnels, monkeypatch
    ):
        monkeypatch.setattr(server, "_LOCK_CHECK_TIMEOUT_S", 0.01)
        tunnels.lock_session = "hung"

        stop = asyncio.Event()
        assert await asyncio.wait_for(_serve(Gateway(object()), stop), 5) is False  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_the_process_exits_non_zero(
        self, tunnels, process, monkeypatch, caplog
    ):
        async def listen(self, host_key):
            return [_Acceptor()]

        monkeypatch.setattr(Gateway, "listen", listen)
        tunnels.lock_session = "gone"

        with caplog.at_level(logging.ERROR, logger=server.__name__):
            assert await asyncio.wait_for(server._run(), 5) == 1

        assert "Lost the tunnel gateway's database lock" in caplog.text
        assert process.closed


class TestWhichAddressTakesWhichLogin:
    """With a dial address of its own, the workers' login is refused on the
    machines' address, so a copy of the dialer key is no use from outside;
    without one, one address takes both."""

    @pytest.fixture
    def keys(self, tunnels: _Tunnels):
        dialer, machine = (generate_client_key(c) for c in ("dialer", "machine"))
        tunnels.tunnel_rows = {
            TUNNEL: {"tunnel_id": TUNNEL, "public_key": machine.public_key}
        }
        return dialer, machine

    @staticmethod
    async def _accepted(login: server._Login, username: str, public_key: str) -> bool:
        await login.begin_auth(username)
        return login.validate_public_key(
            username, asyncssh.import_public_key(public_key)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "dialer,machines,dial_ok,machine_ok",
        [
            (True, True, True, True),
            (False, True, False, True),
            (True, False, True, False),
        ],
        ids=["one-address", "machines-address", "dial-address"],
    )
    async def test_each_address_takes_only_its_logins(
        self, keys, dialer, machines, dial_ok, machine_ok
    ):
        dialer_key, machine_key = keys
        gateway = Gateway(asyncssh.import_public_key(dialer_key.public_key))

        def login() -> server._Login:
            return server._Login(gateway, dialer=dialer, machines=machines)

        assert await self._accepted(login(), "dial", dialer_key.public_key) is dial_ok
        assert (
            await self._accepted(login(), TUNNEL, machine_key.public_key) is machine_ok
        )


class TestWhatALoginMayHold:
    """Anyone may attempt a login and a tunnel's key may open many, so each
    is bounded: a cap on connections not logged in yet, one lookup per
    connection, a share of the pool for lookups, a few logins per tunnel,
    and none left open without a forward."""

    @pytest.fixture
    def machine(self, tunnels: _Tunnels):
        key = generate_client_key("machine")
        tunnels.tunnel_rows = {
            TUNNEL: {"tunnel_id": TUNNEL, "public_key": key.public_key}
        }
        return key

    @staticmethod
    async def _logged_in(gateway: Gateway) -> tuple[server._Login, _Conn]:
        login, conn = server._Login(gateway, dialer=True, machines=True), _Conn()
        login.connection_made(conn)  # type: ignore[arg-type]
        await login.begin_auth(TUNNEL)
        login.auth_completed()
        return login, conn

    @staticmethod
    def _connected(
        gateway: Gateway, *, machines: bool = True
    ) -> tuple[server._Login, _Conn]:
        login, conn = server._Login(gateway, dialer=True, machines=machines), _Conn()
        login.connection_made(conn)  # type: ignore[arg-type]
        return login, conn

    @pytest.mark.asyncio
    async def test_past_the_cap_a_new_connection_is_closed_unanswered(
        self, machine, monkeypatch
    ):
        monkeypatch.setattr(admission, "unchecked_slots", lambda: 2)
        gateway = Gateway(object())  # type: ignore[arg-type]
        (first, _), (second, _), (_, refused) = (
            self._connected(gateway) for _ in range(3)
        )
        assert refused.aborted

        # A login that completes, and a connection that closes, each free a place.
        await first.begin_auth(TUNNEL)
        first.auth_completed()
        assert not self._connected(gateway)[1].aborted
        second.connection_lost(None)
        assert not self._connected(gateway)[1].aborted
        assert self._connected(gateway)[1].aborted

    @pytest.mark.asyncio
    async def test_the_workers_own_address_is_not_capped(self, monkeypatch):
        monkeypatch.setattr(admission, "unchecked_slots", lambda: 1)
        gateway = Gateway(object())  # type: ignore[arg-type]
        self._connected(gateway)

        assert not self._connected(gateway, machines=False)[1].aborted

    @pytest.mark.enable_socket  # a loopback listener, nothing beyond it
    @pytest.mark.asyncio
    async def test_a_stranger_cannot_pick_a_costly_key_exchange(self, tunnels):
        """RSA and Diffie-Hellman key exchanges cost the gateway's one loop
        tens of milliseconds each, and anyone may ask for them before logging
        in; what is left still serves every OpenSSH since 6.5."""
        async with _machines_address() as port:
            for kex in (
                "rsa2048-sha256",
                "diffie-hellman-group14-sha256",
                "diffie-hellman-group-exchange-sha256",
            ):
                with pytest.raises(asyncssh.KeyExchangeFailed):
                    await _stranger(port, kex_algs=[kex])
            # Past the key exchange, to a login refused: it has no key to offer.
            with pytest.raises(asyncssh.PermissionDenied):
                await _stranger(port, kex_algs=["curve25519-sha256@libssh.org"])

    @pytest.mark.enable_socket
    @pytest.mark.asyncio
    async def test_what_a_stranger_sends_before_logging_in_is_capped(
        self, tunnels, monkeypatch
    ):
        """asyncssh holds whatever arrives while a login is looked up; the
        budget is enforced on its real transport, here at a key exchange."""
        monkeypatch.setattr(admission, "MAX_BYTES_BEFORE_LOGIN", 256)
        async with _machines_address() as port:
            # Aborted with its key exchange unread, so reset or lost.
            with pytest.raises((asyncssh.ConnectionLost, ConnectionError)):
                await _stranger(port)

    @pytest.mark.enable_socket
    @pytest.mark.asyncio
    async def test_a_certificate_is_refused_without_checking_its_signature(
        self, tunnels, monkeypatch
    ):
        """asyncssh would verify it with a signing key the sender picked,
        before asking whether its CA is trusted."""
        ca = asyncssh.generate_private_key("ssh-ed25519")
        key = asyncssh.generate_private_key("ssh-ed25519")
        cert = ca.generate_user_certificate(key, TUNNEL, principals=[TUNNEL])
        decoded: list[bytes] = []
        decode = asyncssh.connection.decode_ssh_certificate

        def recording(data: bytes, *args: object) -> object:
            # The client checks the gateway's host key here too, a plain key.
            if b"-cert-v01@openssh.com" in data[:64]:
                decoded.append(data)
            return decode(data, *args)

        monkeypatch.setattr(asyncssh.connection, "decode_ssh_certificate", recording)
        async with _machines_address() as port:
            with pytest.raises(asyncssh.PermissionDenied):
                await _stranger(port, client_keys=[(key, cert)])

        assert decoded == []

    @pytest.mark.asyncio
    async def test_a_connection_offering_too_many_keys_is_closed(self, tunnels):
        login, conn = self._connected(Gateway(object()))  # type: ignore[arg-type]
        await login.begin_auth(TUNNEL)
        key = asyncssh.generate_private_key("ssh-ed25519")
        for _ in range(server.MAX_KEY_OFFERS):
            assert not login.validate_public_key(TUNNEL, key)
        assert not conn.aborted

        assert not login.validate_public_key(TUNNEL, key)
        assert conn.aborted

    @pytest.mark.asyncio
    async def test_a_lookup_held_up_ends_with_its_login(self, tunnels, monkeypatch):
        """Nothing cancels it when its connection closes, so each held one
        behind a stuck query would otherwise stay."""
        monkeypatch.setattr(server, "LOGIN_TIMEOUT_S", 0.05)
        monkeypatch.setattr(server, "_login_lookup_slots", lambda: 1)

        async def get_tunnel(tunnel_id: str) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(tunnels, "get_tunnel", get_tunnel)
        gateway = Gateway(object())  # type: ignore[arg-type]
        # One stuck on the query, one waiting its turn behind it.
        logins = [server._Login(gateway, dialer=True, machines=True) for _ in range(2)]
        checks = [login.begin_auth(TUNNEL) for login in logins]
        results = await asyncio.wait_for(
            asyncio.gather(*checks, return_exceptions=True), 1
        )

        assert all(isinstance(r, asyncssh.ServiceNotAvailable) for r in results)

    def test_the_cap_leaves_most_of_a_low_file_limit_free(self, monkeypatch):
        monkeypatch.setattr(admission.resource, "getrlimit", lambda _: (256, 256))
        assert admission.unchecked_slots() == 64

        monkeypatch.setattr(admission.resource, "getrlimit", lambda _: (65536, 65536))
        assert admission.unchecked_slots() == admission.MAX_UNCHECKED_CONNECTIONS

    @pytest.mark.asyncio
    async def test_a_connection_that_changes_its_name_is_disconnected(
        self, tunnels, monkeypatch
    ):
        looked_up: list[str] = []

        async def get_tunnel(tunnel_id: str) -> None:
            looked_up.append(tunnel_id)

        monkeypatch.setattr(tunnels, "get_tunnel", get_tunnel)
        login = server._Login(Gateway(object()), dialer=True, machines=True)  # type: ignore[arg-type]
        await login.begin_auth(TUNNEL)
        with pytest.raises(asyncssh.ProtocolError):
            await login.begin_auth("1f8fad5b-d9cb-469f-a165-70867728950e")

        assert looked_up == [TUNNEL]

    @pytest.mark.asyncio
    async def test_lookups_take_only_their_share_of_the_pool(
        self, tunnels, monkeypatch
    ):
        monkeypatch.setattr(server, "_login_lookup_slots", lambda: 2)
        gate, in_flight, peak = asyncio.Event(), [0], [0]

        async def get_tunnel(tunnel_id: str) -> None:
            in_flight[0] += 1
            peak[0] = max(peak[0], in_flight[0])
            await gate.wait()
            in_flight[0] -= 1

        monkeypatch.setattr(tunnels, "get_tunnel", get_tunnel)
        gateway = Gateway(object())  # type: ignore[arg-type]
        logins = [server._Login(gateway, dialer=True, machines=True) for _ in range(5)]
        checking = [asyncio.create_task(login.begin_auth(TUNNEL)) for login in logins]
        for _ in range(5):
            await asyncio.sleep(0)
        assert peak[0] == 2

        gate.set()
        await asyncio.gather(*checking)

    @pytest.mark.asyncio
    async def test_past_the_cap_a_tunnels_oldest_login_is_closed(self, machine):
        gateway = Gateway(object())  # type: ignore[arg-type]
        conns = [
            (await self._logged_in(gateway))[1]
            for _ in range(server.MAX_CONNECTIONS_PER_TUNNEL + 1)
        ]

        assert [c.closed for c in conns] == [True] + [False] * (len(conns) - 1)

    @pytest.mark.asyncio
    async def test_a_login_with_no_forward_up_is_closed(self, machine, monkeypatch):
        monkeypatch.setattr(server, "FORWARD_TIMEOUT_S", 0.01)
        _, conn = await self._logged_in(Gateway(object()))  # type: ignore[arg-type]

        await asyncio.sleep(0.05)
        assert conn.closed

    @pytest.mark.asyncio
    async def test_a_cancelled_forward_ends_its_login(self, tunnels, machine):
        gateway = Gateway(object())  # type: ignore[arg-type]
        async with _serving(gateway):
            login, conn = await self._logged_in(gateway)
            fwd = await login.server_requested("langalpha", 22)
            assert fwd

            fwd.close()  # asyncssh's answer to a cancel-tcpip-forward
            assert conn.closed
            await _settled(gateway)
            assert _liveness(tunnels) == [("up", TUNNEL), ("down", TUNNEL)]


class _SilentConn(_Conn):
    """A machine that never answers a channel open."""

    async def create_connection(self, *args: object) -> None:
        await asyncio.Event().wait()


class TestDials:
    @pytest.mark.asyncio
    async def test_a_machine_that_leaves_a_dial_unanswered_is_closed(
        self, tunnels, monkeypatch
    ):
        monkeypatch.setattr(server, "_CHANNEL_OPEN_TIMEOUT_S", 0.01)
        gateway = Gateway(object())  # type: ignore[arg-type]
        fwd = _Forward(gateway, _SilentConn(), TUNNEL, "langalpha", 22)  # type: ignore[arg-type]
        async with _serving(gateway):
            assert await gateway.add_forward(fwd)

            with pytest.raises(asyncssh.ChannelOpenError) as raised:
                await gateway.dial(_Conn(), TUNNEL, "127.0.0.1", 0)  # type: ignore[arg-type]

            assert raised.value.reason == server.GATEWAY_NOT_CONNECTED
            assert fwd.conn.closed
            assert TUNNEL not in gateway._forwards
