"""Where a worker dials a capability's port.

Pins the contract a process-local forward rests on: a stored origin is dialed
as stored, and a capability with no stored origin is opened by the serving
worker itself, after the computer row confirms the sandbox, through the
session runtime it holds or a detached one.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import pytest

from ptc_agent.core.sandbox.runtime import PreviewInfo
from src.server.preview_proxy import app as proxy_app
from src.server.preview_proxy.app import PreviewProxy
from src.server.services import preview_upstreams
from src.server.services.computer_manager._providers import ProviderMixin
from src.server.services.computer_manager._sessions import SessionCacheMixin
from src.server.services.preview_base import PreviewBase
from src.server.services.preview_capabilities import (
    Capability,
    PreviewProxyUnavailable,
    Upstream,
)
from src.server.services.preview_upstreams import upstream_for
from src.server.services.workspace_manager import WorkspaceManager

SANDBOX = "ssh-machine-1"
HOST = "8080-" + "a" * 32 + ".preview.example.com"


def _capability(
    upstream: Upstream | None = None, *, sandbox_id: str = SANDBOX, workspace_id: str = "ws-1"
) -> Capability:
    return Capability(
        sandbox_id=sandbox_id,
        port=8080,
        upstream=upstream,
        workspace_id=workspace_id,
        generation=1,
        expires_at=int(time.time()) + 600,
    )


class _Runtime:
    """A forward per process, on a port this process chose."""

    def __init__(
        self, local_port: int = 41000, *, fail: bool = False, hang: bool = False
    ) -> None:
        self.local_port = local_port
        self.fail = fail
        self.hang = hang
        self.asked: list[int] = []

    async def get_preview_url(self, port: int, expires_in: int = 3600) -> PreviewInfo:
        self.asked.append(port)
        if self.hang:
            await asyncio.Event().wait()
        if self.fail:
            raise ConnectionError("connection to the machine dropped")
        return PreviewInfo(
            url=f"http://127.0.0.1:{self.local_port}",
            token=None,
            browser_ready=False,
            process_local=True,
        )


class _Provider:
    def __init__(self, runtime: _Runtime) -> None:
        self.runtime = runtime
        self.opened: list[str] = []
        self.closed = False

    async def get(self, sandbox_id: str) -> _Runtime:
        self.opened.append(sandbox_id)
        return self.runtime

    async def close(self) -> None:
        self.closed = True


class _Session:
    def __init__(self, sandbox_id: str, runtime: _Runtime) -> None:
        self.sandbox = type("S", (), {"sandbox_id": sandbox_id, "runtime": runtime})()


class _Manager:
    """The manager's own runtime choice, over a fake session cache and provider."""

    _binding_from_computer = staticmethod(SessionCacheMixin._binding_from_computer)
    _session_sandbox_id = staticmethod(SessionCacheMixin._session_sandbox_id)
    computer_runtime = ProviderMixin.computer_runtime
    _detached_runtime = ProviderMixin._detached_runtime

    def __init__(self) -> None:
        self.session: _Session | None = None
        self.runtimes: list[_Runtime] = []
        self.providers: list[_Provider] = []

    def _cached_session(self, computer_id: str) -> _Session | None:
        return self.session

    def _provider_for(self, binding: Any) -> _Provider:
        runtime = self.runtimes.pop(0) if self.runtimes else _Runtime()
        provider = _Provider(runtime)
        self.providers.append(provider)
        return provider


class _Rows:
    def __init__(self) -> None:
        self.row: dict[str, Any] | None = {
            "computer_id": "c-1",
            "kind": "ssh",
            "status": "running",
            "provider_ref": SANDBOX,
            "provider_config": None,
        }
        self.down = False
        self.reads = 0

    async def __call__(self, workspace_id: str) -> dict[str, Any] | None:
        self.reads += 1
        if self.down:
            raise ConnectionError("postgres down")
        return None if self.row is None else dict(self.row)


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> _Manager:
    fake = _Manager()
    monkeypatch.setattr(
        WorkspaceManager, "get_instance", classmethod(lambda cls, *a, **k: fake)
    )
    return fake


@pytest.fixture
def rows(monkeypatch: pytest.MonkeyPatch) -> _Rows:
    fake = _Rows()
    monkeypatch.setattr(preview_upstreams, "get_computer_for_workspace", fake)
    return fake


class TestStoredOrigins:
    @pytest.mark.asyncio
    async def test_a_stored_origin_is_dialed_as_stored(self, manager, rows):
        stored = Upstream("https://8080-sbx.e2b.app", {"e2b-traffic-access-token": "t"})

        assert await upstream_for(_capability(stored)) == stored
        assert rows.reads == 0
        assert manager.providers == []


class TestInProcess:
    @pytest.mark.asyncio
    async def test_this_workers_session_runtime_opens_the_port(self, manager, rows):
        session_runtime = _Runtime(local_port=42000)
        manager.session = _Session(SANDBOX, session_runtime)

        upstream = await upstream_for(_capability())

        assert upstream == Upstream("http://127.0.0.1:42000", {})
        assert session_runtime.asked == [8080]
        assert manager.providers == []

    @pytest.mark.asyncio
    async def test_a_session_on_another_sandbox_is_not_used(self, manager, rows):
        manager.session = _Session("an-older-sandbox", _Runtime(local_port=42000))

        upstream = await upstream_for(_capability())

        assert upstream == Upstream("http://127.0.0.1:41000", {})
        assert [p.opened for p in manager.providers] == [[SANDBOX]]

    @pytest.mark.asyncio
    async def test_without_a_session_each_dial_asks_a_detached_runtime(
        self, manager, rows
    ):
        first, second = await asyncio.gather(
            upstream_for(_capability()), upstream_for(_capability())
        )

        assert first == second == Upstream("http://127.0.0.1:41000", {})
        # The provider's pool, not this module, keeps the connection and its
        # forward, so each client is closed as soon as it has answered.
        assert [(p.opened, p.closed) for p in manager.providers] == [
            ([SANDBOX], True),
            ([SANDBOX], True),
        ]
        # Every dial re-reads the row.
        assert rows.reads == 2

    @pytest.mark.asyncio
    async def test_many_sandboxes_at_once_each_open(self, manager, rows, monkeypatch):
        async def row_for(workspace_id: str):
            return {**rows.row, "provider_ref": f"sbx-{workspace_id}"}

        monkeypatch.setattr(preview_upstreams, "get_computer_for_workspace", row_for)
        capabilities = [
            _capability(sandbox_id=f"sbx-{n}", workspace_id=str(n)) for n in range(40)
        ]

        upstreams = await asyncio.gather(*(upstream_for(c) for c in capabilities))

        assert all(u == Upstream("http://127.0.0.1:41000", {}) for u in upstreams)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "change", [{"status": "stopped"}, {"provider_ref": "a-newer-sandbox"}]
    )
    async def test_a_row_that_no_longer_runs_the_sandbox_opens_nothing(
        self, manager, rows, change
    ):
        rows.row = {**rows.row, **change}

        assert await upstream_for(_capability()) is None
        assert manager.providers == []

    @pytest.mark.asyncio
    async def test_a_computer_gone_from_postgres_opens_nothing(self, manager, rows):
        rows.row = None

        assert await upstream_for(_capability()) is None
        assert manager.providers == []

    @pytest.mark.asyncio
    async def test_postgres_that_cannot_answer_is_unavailable_not_gone(
        self, manager, rows
    ):
        rows.down = True

        with pytest.raises(PreviewProxyUnavailable):
            await upstream_for(_capability())

    @pytest.mark.asyncio
    async def test_a_machine_that_cannot_be_reached_opens_nothing(self, manager, rows):
        manager.runtimes = [_Runtime(fail=True), _Runtime(local_port=43000)]

        assert await upstream_for(_capability()) is None
        assert await upstream_for(_capability()) == Upstream("http://127.0.0.1:43000", {})
        assert [p.closed for p in manager.providers] == [True, True]

    @pytest.mark.asyncio
    async def test_a_machine_that_does_not_answer_is_given_up_on(
        self, manager, rows, monkeypatch
    ):
        monkeypatch.setattr(preview_upstreams, "_OPEN_TIMEOUT_S", 0.01)
        manager.runtimes = [_Runtime(hang=True)]

        assert await upstream_for(_capability()) is None
        assert manager.providers[0].closed

    @pytest.mark.asyncio
    async def test_a_probe_checks_the_forward_this_worker_opens(
        self, manager, rows, monkeypatch
    ):
        capability = _capability()

        async def resolve_host(host, *, base):
            return capability

        monkeypatch.setattr(preview_upstreams, "resolve_host", resolve_host)
        monkeypatch.setattr(
            preview_upstreams,
            "preview_base",
            lambda: PreviewBase("https", "preview.example.com"),
        )

        target = await preview_upstreams.probe_target(f"https://{HOST}/app?x=1")

        assert target == ("http://127.0.0.1:41000/app?x=1", {})


class TestProxy:
    @pytest.fixture
    def proxy(self, monkeypatch):
        state: dict[str, Capability] = {}

        async def resolve_host(host, *, base):
            return state["capability"]

        monkeypatch.setattr(proxy_app, "resolve_host", resolve_host)
        proxy = PreviewProxy(PreviewBase("https", "preview.example.com"))
        monkeypatch.setattr(proxy.activity, "note", lambda capability: None)
        proxy.state = state  # type: ignore[attr-defined]
        return proxy

    async def _get(self, proxy: PreviewProxy) -> tuple[int, list[httpx.Request]]:
        seen: list[httpx.Request] = []

        def record(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            # A stream rather than content, which httpx would read up front.
            return httpx.Response(
                302,
                headers={"location": "http://127.0.0.1:41000/login"},
                stream=httpx.ByteStream(b""),
            )

        proxy._client = httpx.AsyncClient(transport=httpx.MockTransport(record))
        sent: list[dict] = []
        requested = False

        async def receive():
            # As uvicorn's does: the body, then a wait for the browser to leave.
            nonlocal requested
            if requested:
                await asyncio.Event().wait()
            requested = True
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": [(b"host", HOST.encode())],
        }
        try:
            await proxy(scope, receive, send)
        finally:
            await proxy.aclose()
        self.sent = sent
        return sent[0]["status"], seen

    @pytest.mark.asyncio
    async def test_an_in_process_capability_is_dialed_at_this_workers_forward(
        self, proxy, manager, rows
    ):
        proxy.state["capability"] = _capability()

        status, seen = await self._get(proxy)

        assert [str(r.url) for r in seen] == ["http://127.0.0.1:41000/"]
        # The forward's origin is rewritten like any upstream's.
        assert status == 302
        location = dict(self.sent[0]["headers"])[b"location"]
        assert location == b"https://" + HOST.encode() + b"/login"

    @pytest.mark.asyncio
    async def test_a_stopped_computer_is_a_preview_that_is_not_running(
        self, proxy, manager, rows
    ):
        proxy.state["capability"] = _capability()
        rows.row = {**rows.row, "status": "stopped"}

        status, seen = await self._get(proxy)

        assert status == 502
        assert seen == []

    @pytest.mark.asyncio
    async def test_postgres_that_cannot_answer_is_a_preview_unavailable(
        self, proxy, manager, rows
    ):
        proxy.state["capability"] = _capability()
        rows.down = True

        status, seen = await self._get(proxy)

        assert status == 503
        assert seen == []
