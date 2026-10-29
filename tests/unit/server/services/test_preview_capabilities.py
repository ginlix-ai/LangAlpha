"""Capabilities: the hostname a browser is handed for a port it cannot reach.

Pins what any worker's proxy relies on: a capability resolves only while it
is live and unrevoked, a lost revocation counter fails closed, and an unknown
answer from Redis is never read as a missing capability.
"""

from __future__ import annotations

import json

import pytest

from ptc_agent.core.sandbox.runtime import PreviewInfo
from src.config import env
from src.server.services import preview_capabilities as caps
from src.server.services import preview_upstreams
from src.server.services.preview_base import preview_base
from src.server.services.preview_capabilities import (
    PreviewProxyNotConfigured,
    PreviewProxyUnavailable,
    Upstream,
)

_TOKEN = "abcdefghijklmnopqrstuvwxyz234567"
_HEADERS = {"e2b-traffic-access-token": "tok-1"}


class _Pipeline:
    def __init__(self, redis: "_Redis") -> None:
        self._redis = redis
        self._ops: list = []

    async def __aenter__(self) -> "_Pipeline":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def __getattr__(self, name: str):
        def queue(*args, **kwargs) -> "_Pipeline":
            self._ops.append((name, args, kwargs))
            return self

        return queue

    async def execute(self) -> list:
        results = [await getattr(self._redis, n)(*a, **k) for n, a, k in self._ops]
        self._ops.clear()
        return results


class _Redis:
    """The raw client: bytes out, as the real one runs undecoded."""

    def __init__(self) -> None:
        self.kv: dict[str, str] = {}
        self.down = False

    def _check(self) -> None:
        if self.down:
            raise ConnectionError("redis down")

    async def get(self, key: str):
        self._check()
        value = self.kv.get(key)
        return value.encode() if value is not None else None

    async def set(self, key: str, value, nx: bool = False):
        self._check()
        if nx and key in self.kv:
            return None
        self.kv[key] = str(value)
        return True

    async def incr(self, key: str) -> int:
        self._check()
        self.kv[key] = str(int(self.kv.get(key, "0")) + 1)
        return int(self.kv[key])

    async def expire(self, key: str, seconds: int) -> bool:
        self._check()
        return True

    def pipeline(self, transaction: bool = True) -> _Pipeline:
        return _Pipeline(self)


class _Cache:
    def __init__(self) -> None:
        self.enabled = True
        self.client = _Redis()
        self.set_ok = True

    async def set(self, key: str, value, ttl: int | None = None) -> bool:
        if not self.set_ok:
            return False
        self.client.kv[key] = json.dumps(value)
        return True

    async def get_strict(self, key: str):
        if not self.enabled:
            raise RuntimeError("Redis cache disabled")
        raw = await self.client.get(key)
        return None if raw is None else json.loads(raw)


@pytest.fixture
def cache(monkeypatch: pytest.MonkeyPatch) -> _Cache:
    fake = _Cache()
    monkeypatch.setattr(caps, "get_cache_client", lambda: fake)
    monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "https://preview.example.com")
    return fake


def _private(url: str = "https://8080-sbx-1.e2b.app") -> PreviewInfo:
    return PreviewInfo(url=url, token=None, auth_headers=dict(_HEADERS), browser_ready=False)


async def _mint(sandbox_id: str = "sbx-1", expires_in: int = 3600) -> str:
    return await caps.publish(
        _private(), sandbox_id=sandbox_id, port=8080, workspace_id="ws-1", expires_in=expires_in
    )


def _token(url: str) -> str:
    base = preview_base()
    assert base is not None
    named = base.parse(url.split("://", 1)[1].split("/", 1)[0])
    assert named is not None
    return named[1]


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_a_minted_capability_resolves_to_what_it_opens(self, cache):
        url = await _mint()

        capability = await caps.resolve(_token(url))

        assert url.startswith("https://8080-") and url.endswith(".preview.example.com/")
        assert capability is not None
        assert capability.sandbox_id == "sbx-1"
        assert capability.port == 8080
        assert capability.workspace_id == "ws-1"
        assert capability.upstream == Upstream("https://8080-sbx-1.e2b.app", _HEADERS)
        assert "tok-1" not in repr(capability)

    @pytest.mark.asyncio
    async def test_the_record_is_keyed_by_a_digest_not_the_token(self, cache):
        url = await _mint()

        assert not any(_token(url) in key for key in cache.client.kv)

    @pytest.mark.asyncio
    async def test_a_lapsed_capability_resolves_to_nothing(self, cache, monkeypatch):
        url = await _mint(expires_in=60)
        later = caps.time.time() + 61
        monkeypatch.setattr(caps.time, "time", lambda: later)

        assert await caps.resolve(_token(url)) is None

    @pytest.mark.asyncio
    async def test_revoking_a_sandbox_ends_each_of_its_capabilities(self, cache):
        first, second = await _mint(), await _mint()
        other = await _mint(sandbox_id="sbx-2")

        await caps.revoke("sbx-1")

        assert await caps.resolve(_token(first)) is None
        assert await caps.resolve(_token(second)) is None
        assert await caps.resolve(_token(other)) is not None
        assert await caps.resolve(_token(await _mint())) is not None

    @pytest.mark.asyncio
    async def test_a_revocation_reaches_a_capability_already_resolved(self, cache):
        capability = await caps.resolve(_token(await _mint()))
        assert capability is not None and await caps.is_current(capability)

        await caps.revoke("sbx-1")

        assert not await caps.is_current(capability)

    @pytest.mark.asyncio
    async def test_a_lost_revocation_counter_fails_closed(self, cache):
        url = await _mint()
        del cache.client.kv["preview:cap:gen:sbx-1"]

        assert await caps.resolve(_token(url)) is None
        # A counter recreated by the next mint never matches the old records.
        await _mint()
        assert await caps.resolve(_token(url)) is None

    @pytest.mark.asyncio
    async def test_an_unknown_or_malformed_record_resolves_to_nothing(self, cache):
        url = await _mint()
        key = next(k for k in cache.client.kv if not k.startswith("preview:cap:gen:"))
        cache.client.kv[key] = "not json"

        assert await caps.resolve(_token(url)) is None
        assert await caps.resolve(_TOKEN) is None

    @pytest.mark.asyncio
    async def test_redis_that_cannot_answer_is_unavailable_not_a_miss(self, cache):
        url = await _mint()
        cache.client.down = True

        with pytest.raises(PreviewProxyUnavailable):
            await caps.resolve(_token(url))
        with pytest.raises(PreviewProxyUnavailable):
            await _mint()
        cache.enabled = False
        with pytest.raises(PreviewProxyUnavailable):
            await caps.resolve(_token(url))

    @pytest.mark.asyncio
    async def test_a_record_that_was_not_stored_is_never_handed_out(self, cache):
        cache.set_ok = False

        with pytest.raises(PreviewProxyUnavailable):
            await _mint()

    @pytest.mark.asyncio
    async def test_revoking_never_raises(self, cache):
        cache.client.down = True

        await caps.revoke("sbx-1")
        await caps.revoke(None)


class TestPublish:
    @pytest.mark.asyncio
    async def test_a_browser_ready_url_passes_through_untouched(self, cache):
        info = PreviewInfo(url="https://3000-sbx.example.org/?tkn=x", token="x")

        url = await caps.publish(
            info, sandbox_id="sbx-1", port=3000, workspace_id="ws-1", expires_in=3600
        )

        assert url == info.url
        assert cache.client.kv == {}

    @pytest.mark.asyncio
    async def test_a_port_that_needs_a_header_needs_the_proxy(self, cache, monkeypatch):
        monkeypatch.setattr(env, "PREVIEW_PROXY_BASE_URL", "")

        with pytest.raises(PreviewProxyNotConfigured):
            await _mint()

    @pytest.mark.asyncio
    async def test_a_capability_is_probed_at_its_sandbox_with_its_header(self, cache):
        url = await _mint()

        target = await preview_upstreams.probe_target(url.rstrip("/") + "/app/page?x=1")

        assert target == ("https://8080-sbx-1.e2b.app/app/page?x=1", _HEADERS)

    @pytest.mark.asyncio
    async def test_a_dead_capability_probes_as_nothing(self, cache):
        url = await _mint()
        await caps.revoke("sbx-1")

        assert await preview_upstreams.probe_target(url) is None

    @pytest.mark.asyncio
    async def test_any_other_url_is_probed_as_it_is(self, cache):
        url = "https://3000-sbx.example.org/?tkn=x"

        assert await preview_upstreams.probe_target(url) == (url, {})


class TestProcessLocal:
    """A process-local URL is a forward only the minting process holds."""

    @pytest.mark.asyncio
    async def test_a_process_local_url_is_stored_as_no_upstream(self, cache):
        info = PreviewInfo(
            url="http://127.0.0.1:41234",
            token=None,
            auth_headers={"x-forward": "f-1"},
            browser_ready=False,
            process_local=True,
        )

        url = await caps.publish(
            info, sandbox_id="ssh-1", port=8080, workspace_id="ws-1", expires_in=600
        )

        record = next(
            json.loads(v) for k, v in cache.client.kv.items()
            if not k.startswith("preview:cap:gen:")
        )
        assert record["upstream"] is None
        assert record["headers"] == {}
        # Not a 2, which an older reader would take and dial as "None".
        assert record["v"] == 3
        assert "41234" not in json.dumps(record)
        capability = await caps.resolve(_token(url))
        assert capability is not None
        assert capability.upstream is None
        assert (capability.sandbox_id, capability.port, capability.workspace_id) == (
            "ssh-1",
            8080,
            "ws-1",
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "url",
        [
            "http://127.0.0.1:41000",
            "http://localhost:41000",
            "http://[::1]:41000",
            "http://0.0.0.0:41000",
            "http://127.1:41000",
            "http://[::ffff:127.0.0.1]:41000",
        ],
    )
    async def test_a_loopback_url_that_is_not_process_local_is_never_stored(
        self, cache, url
    ):
        # Whichever worker serves a stored origin dials it, which on loopback
        # is that worker's own host.
        with pytest.raises(ValueError):
            await caps.publish(
                _private(url), sandbox_id="sbx-1", port=8080, workspace_id="ws-1",
                expires_in=600,
            )
        assert cache.client.kv == {}

    @pytest.mark.asyncio
    async def test_a_record_that_names_its_upstream_is_one_an_older_reader_takes(
        self, cache
    ):
        # During a cutover the draining color reads what this one mints, and a
        # reader that knows only version 2 takes exactly this shape.
        url = await _mint()
        key = next(k for k in cache.client.kv if not k.startswith("preview:cap:gen:"))
        record = json.loads(cache.client.kv[key])

        assert record["v"] == 2
        assert set(record) == {
            "v", "sandbox_id", "port", "upstream", "workspace_id", "generation",
            "expires_at", "headers",
        }
        assert record["upstream"] == "https://8080-sbx-1.e2b.app"
        assert record["headers"] == _HEADERS
        capability = await caps.resolve(_token(url))
        assert capability is not None
        assert capability.upstream == Upstream("https://8080-sbx-1.e2b.app", _HEADERS)
