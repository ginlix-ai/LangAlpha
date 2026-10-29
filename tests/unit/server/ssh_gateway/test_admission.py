"""Which connections on the machines' address may go on to log in.

Anyone may connect there, so a single source can neither hold the places
everyone shares, nor open connections faster than a machine ever needs to,
nor make the gateway hold what it sends before logging in.
"""

from __future__ import annotations

import pytest

from src.server.ssh_gateway import admission
from src.server.ssh_gateway.admission import Admission


class _Conn:
    def __init__(self, peer: str) -> None:
        self.peer = peer
        self.received = 0
        self.aborted = False

    def get_extra_info(self, name: str, default: object = None) -> object:
        return (self.peer, 40000) if name == "peername" else default

    def data_received(self, data: bytes, datatype: object = None) -> None:
        self.received += len(data)

    def abort(self) -> None:
        self.aborted = True


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(admission.time, "monotonic", fake)
    return fake


def _admit(gate: Admission, peer: str, n: int = 1) -> list[bool]:
    return [gate.admit(_Conn(peer)) for _ in range(n)]


@pytest.mark.asyncio
async def test_one_source_holds_only_its_share(clock):
    gate = Admission()
    per_source = admission.MAX_UNCHECKED_PER_SOURCE
    held = [_Conn("198.51.100.7") for _ in range(per_source)]

    assert all(gate.admit(conn) for conn in held)
    assert _admit(gate, "198.51.100.7") == [False]
    assert _admit(gate, "203.0.113.9") == [True]

    # A login that completes, or a connection that closes, frees its place.
    gate.release(held[0])
    assert _admit(gate, "198.51.100.7") == [True]


@pytest.mark.asyncio
async def test_an_ipv6_source_is_its_64_and_a_mapped_one_its_ipv4(clock):
    gate = Admission()
    per_source = admission.MAX_UNCHECKED_PER_SOURCE
    for i in range(per_source):
        assert gate.admit(_Conn(f"2001:db8:1:2::{i + 1:x}"))

    assert _admit(gate, "2001:db8:1:2:ffff::1") == [False]
    assert _admit(gate, "2001:db8:1:3::1") == [True]

    assert all(_admit(gate, "::ffff:198.51.100.7", per_source))
    assert _admit(gate, "198.51.100.7") == [False]

    # A link-local peer's interface is not part of the host.
    assert all(_admit(gate, "fe80::%eth0", per_source - 1))
    assert _admit(gate, "fe80::1%eth0", 2) == [True, False]


@pytest.mark.asyncio
async def test_peers_with_no_address_share_one_source(clock):
    gate = Admission()
    nameless = [_Conn("") for _ in range(admission.MAX_UNCHECKED_PER_SOURCE + 1)]
    for conn in nameless:
        conn.get_extra_info = lambda name, default=None: None

    assert [gate.admit(conn) for conn in nameless][-2:] == [True, False]


@pytest.mark.asyncio
async def test_a_source_opens_a_burst_then_only_at_its_rate(clock):
    gate = Admission()
    burst = admission.SOURCE_BURST
    opened = 0
    for _ in range(burst + 5):
        conn = _Conn("198.51.100.7")
        if gate.admit(conn):
            opened += 1
            gate.release(conn)  # closes at once, so only the rate holds it back

    assert opened == burst
    clock.now += 1
    assert sum(_admit(gate, "198.51.100.7", burst)) == int(admission.SOURCE_RATE_PER_S)


@pytest.mark.asyncio
async def test_a_refused_connection_spends_none_of_its_sources_turns(clock):
    """Machines behind one router, retrying while their share is full, still
    have their turns once places free up."""
    gate = Admission()
    per_source = admission.MAX_UNCHECKED_PER_SOURCE
    held = [_Conn("198.51.100.7") for _ in range(per_source)]
    assert all(gate.admit(conn) for conn in held)
    assert not any(_admit(gate, "198.51.100.7", 50))

    for conn in held:
        gate.release(conn)
    left = admission.SOURCE_BURST - per_source
    assert all(_admit(gate, "198.51.100.7", left))


@pytest.mark.asyncio
async def test_a_full_cap_makes_room_from_the_source_holding_most(clock, monkeypatch):
    monkeypatch.setattr(admission, "unchecked_slots", lambda: 4)
    gate = Admission()
    machine = _Conn("192.0.2.1")
    crowd = [_Conn("198.51.100.7") for _ in range(3)]
    assert gate.admit(machine) and all(gate.admit(conn) for conn in crowd)

    # The crowd's oldest goes while the crowd would still hold more than
    # the newcomer's source.
    assert _admit(gate, "203.0.113.9", 2) == [True, False]
    assert _admit(gate, "203.0.113.10") == [True]
    assert [conn.aborted for conn in crowd] == [True, True, False]

    # Every source holds one place now: a machine's login is never closed
    # for another's.
    assert _admit(gate, "203.0.113.11") == [False]
    assert not machine.aborted and not crowd[2].aborted

    # The closed ones' own release, when they are gone, changes nothing.
    gate.release(crowd[0])
    assert _admit(gate, "203.0.113.11") == [False]


@pytest.mark.asyncio
async def test_a_connection_is_closed_once_it_sends_too_much_before_its_login(clock):
    gate = Admission()
    conn = _Conn("198.51.100.7")
    assert gate.admit(conn)

    conn.data_received(b"x" * admission.MAX_BYTES_BEFORE_LOGIN)
    assert conn.received == admission.MAX_BYTES_BEFORE_LOGIN and not conn.aborted
    conn.data_received(b"x")
    conn.data_received(b"x" * 1000)
    assert conn.aborted and conn.received == admission.MAX_BYTES_BEFORE_LOGIN

    # Once logged in, what it sends is the tunnel's, with no budget.
    after = _Conn("198.51.100.8")
    assert gate.admit(after)
    gate.release(after)
    after.data_received(b"x" * (2 * admission.MAX_BYTES_BEFORE_LOGIN))
    assert not after.aborted


@pytest.mark.asyncio
async def test_idle_sources_are_forgotten_once_many_are_tracked(clock, monkeypatch):
    monkeypatch.setattr(admission, "_TRACKED_SOURCES", 4)
    gate = Admission()
    for i in range(4):
        conn = _Conn(f"198.51.100.{i}")
        assert gate.admit(conn)
        gate.release(conn)

    clock.now += admission.SOURCE_BURST / admission.SOURCE_RATE_PER_S
    assert gate.admit(_Conn("203.0.113.9"))
    assert set(gate._buckets) == {"203.0.113.9"}


def test_a_loop_the_budget_does_not_hold_on_is_refused(monkeypatch):
    """uvloop binds ``data_received`` once, so the budget would not apply."""
    monkeypatch.setattr(admission.asyncio, "get_running_loop", lambda: object())
    with pytest.raises(RuntimeError, match="asyncio's own event loop"):
        admission.require_hooks()


@pytest.mark.asyncio
async def test_an_asyncssh_with_no_certificate_check_to_replace_is_refused(
    monkeypatch,
):
    monkeypatch.delattr(
        admission.asyncssh.SSHServerConnection, admission._CERTIFICATE_CHECK
    )
    with pytest.raises(RuntimeError, match="would decode certificates"):
        admission.require_hooks()
