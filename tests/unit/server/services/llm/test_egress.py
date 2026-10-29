"""Egress policy for the LLM endpoints a user chooses, in platform mode.

Locks the policy (which URLs are refused, and that manifest endpoints and OSS
mode are left alone), the held transport (every connection opened on a
validated address with the name kept for Host and SNI, a rebound name refused
on its next connection, happy eyeballs over the addresses, one bounded pool),
that the refusal is not retried by the SDKs or the model retry layer, and that
the factory and the BYOK resolver attach it only to endpoints a user chose.
"""

import socket
import threading
import time
from collections import Counter
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import anthropic
import anyio
import httpcore
import httpcore2
import httpx
import httpx2
import openai
import pytest
from fastapi import HTTPException
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.messages import HumanMessage

from src.llms.error_classification import extract_status_code, is_retryable_error
from src.llms.llm import LLM, create_llm_from_custom
from src.server.services.llm import clients, egress, thread_model
from src.server.utils.egress_guard import EgressResolutionError, resolve_public_ips_sync

PUBLIC = "93.184.215.14"
PUBLIC_V6 = "2606:2800:21f:cb07:6820:80da:af6b:8b2c"


@pytest.fixture
def dns(monkeypatch):
    """Answers lookups from ``table``; asyncio resolves through the same call."""
    table: dict[str, list[str]] = {}
    lookups: Counter = Counter()

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        # As the real call does, a name IDNA cannot encode fails before a lookup.
        host.encode("idna")
        lookups[host] += 1
        if host not in table:
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        return [
            (socket.AF_INET6 if ":" in ip else socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))
            for ip in table[host]
        ]

    monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo)
    return SimpleNamespace(table=table, lookups=lookups)


@pytest.fixture
def platform(monkeypatch):
    monkeypatch.setattr("src.config.settings.HOST_MODE", "platform")


def _manifest_url(predicate) -> str:
    return next(
        info["base_url"]
        for info in LLM.get_model_config().flat_providers.values()
        if info.get("base_url") and predicate(info)
    )


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:11434/v1",
        "http://[::1]:8000/v1",
        "http://169.254.169.254/latest",
        "http://10.0.0.5/v1",
        "http://100.64.0.1/v1",
        "http://localhost:1234/v1",
        "http://ollama.localhost/v1",
        "https://loopback.test/v1",
        "https://mixed.test/v1",
        # NAT64 and IPv4-compatible forms of internal addresses.
        "http://[64:ff9b::a9fe:a9fe]/latest",
        "http://[64:ff9b:1::a00:5]/v1",
        "http://[::127.0.0.1]/v1",
        "https://nat64.test/v1",
        # Not globally reachable per the IANA registries, though Python's
        # is_global passes them, bare and inside an IPv4-carrying form.
        "http://[fec0::1]:8000/v1",
        "http://[ff02::1]/v1",
        "http://[5f00::1]/v1",
        "http://[100:0:0:1::1]/v1",
        "http://224.0.0.1/v1",
        "http://192.88.99.2/v1",
        "http://[::ffff:224.0.0.1]/v1",
        "http://[64:ff9b::e000:1]/v1",
        "https://sitelocal.test/v1",
    ],
)
async def test_a_private_or_reserved_endpoint_is_refused(dns, url):
    dns.table["loopback.test"] = ["127.0.0.1"]
    dns.table["mixed.test"] = [PUBLIC, "10.1.2.3"]
    dns.table["nat64.test"] = ["64:ff9b::a00:5"]
    dns.table["sitelocal.test"] = ["fec0::1"]
    assert await egress.endpoint_refusal(url) == egress.PRIVATE_ADDRESS
    assert await egress.endpoint_refusal(url, require_resolution=False) == egress.PRIVATE_ADDRESS


@pytest.mark.asyncio
async def test_a_name_that_does_not_resolve_is_refused_only_where_resolution_is_required(dns):
    assert await egress.endpoint_refusal("https://nowhere.test/v1") == egress.UNRESOLVED
    assert await egress.endpoint_refusal("https://nowhere.test/v1", require_resolution=False) is None


@pytest.mark.asyncio
async def test_a_name_idna_cannot_encode_is_unresolved_not_a_crash(dns):
    long_label = f"{'a' * 64}.test"
    assert await egress.endpoint_refusal(f"https://{long_label}/v1") == egress.UNRESOLVED
    with pytest.raises(EgressResolutionError):
        resolve_public_ips_sync(long_label)


@pytest.mark.asyncio
async def test_a_public_endpoint_passes(dns):
    dns.table["gateway.test"] = [PUBLIC]
    assert await egress.endpoint_refusal("https://gateway.test/v1") is None
    # A NAT64 or IPv4-mapped address of a public one is as public as it is.
    assert await egress.endpoint_refusal("http://[64:ff9b::808:808]/v1") is None
    assert await egress.endpoint_refusal("http://[::ffff:8.8.8.8]/v1") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ["ftp://gateway.test/v1", "gateway.test/v1", "http://:80/v1"])
async def test_a_url_that_is_not_http_is_refused(dns, url):
    assert await egress.endpoint_refusal(url) == egress.NOT_HTTP


@pytest.mark.asyncio
async def test_a_manifest_endpoint_is_trusted_without_a_lookup(dns):
    url = _manifest_url(lambda i: not i.get("platform") and i.get("access_type") != "local")
    assert egress.is_operator_endpoint(url)
    assert await egress.endpoint_refusal(url) is None
    assert not dns.lookups


@pytest.mark.asyncio
async def test_a_local_providers_default_is_checked_at_the_servers_own_address(dns, monkeypatch):
    monkeypatch.setattr("src.config.env.HOST_IP", "10.0.0.7")
    url = _manifest_url(lambda i: i.get("access_type") == "local")
    assert not egress.is_operator_endpoint(url)
    assert await egress.endpoint_refusal(url) == egress.PRIVATE_ADDRESS


def test_endpoints_and_connections_in_parameters_are_found():
    params = {
        "base_url": "https://a.test",
        "client_options": {"api_endpoint": "https://b.test"},
        "openai_proxy": "http://127.0.0.1:3128",
        "temperature": 0.2,
    }
    assert egress.parameter_urls(params) == ["https://a.test", "https://b.test"]
    assert egress.connection_params(params) == ["openai_proxy"]
    assert egress.vertex_params({"vertexai": True, "location": "x", "temperature": 0}) == [
        "location",
        "vertexai",
    ]
    assert egress.parameter_urls(None) == [] and egress.connection_params(None) == []

    # Every client the factory builds promotes these out of ``model_kwargs``.
    nested = {"model_kwargs": {**params, "vertexai": True}}
    assert egress.parameter_urls(nested) == ["https://a.test", "https://b.test"]
    assert egress.connection_params(nested) == ["model_kwargs.openai_proxy"]
    assert egress.vertex_params(nested) == ["model_kwargs.vertexai"]
    assert egress.routing_params(nested) != egress.routing_params({"model_kwargs": {}})


# ---------------------------------------------------------------------------
# Held transport
# ---------------------------------------------------------------------------

_RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}"
_CLOSING = _RESPONSE.replace(b"\r\n\r\n", b"\r\nConnection: close\r\n\r\n", 1)


@pytest.fixture
def network(monkeypatch):
    """Stands in for the network under the async side of both httpcore lines,
    at the backend the held one dials each address through. An address in
    ``dead`` drops packets (the attempt hangs until cancelled), one in
    ``refusing`` resets at once, any other answers HTTP."""
    net = SimpleNamespace(
        dialed=[], sni=[], cancelled=[], streams=[], dead=set(), refusing=set(), response=_RESPONSE
    )

    for hc in (httpcore, httpcore2):

        class Stream(hc.AsyncMockStream):
            async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
                net.sni.append(server_hostname)
                return self

        async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None, *, _hc=hc, _stream=Stream):
            net.dialed.append(host)
            if host in net.dead:
                try:
                    await anyio.sleep_forever()
                finally:
                    net.cancelled.append(host)
            if host in net.refusing:
                raise _hc.ConnectError("connection refused")
            stream = _stream([net.response] * 4)
            net.streams.append(stream)
            return stream

        monkeypatch.setattr(hc.AnyIOBackend, "connect_tcp", connect_tcp)
    net.open = lambda: sum(not stream._closed for stream in net.streams)
    return net


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "hx, family", [(httpx, "anthropic"), (httpx, "gemini"), (httpx2, "openai")]
)
async def test_a_connection_is_opened_on_the_validated_address_under_its_name(dns, network, hx, family):
    dns.table["gateway.test"] = [PUBLIC]
    async with hx.AsyncClient(transport=egress.sdk_transport.__wrapped__(family)) as client:
        response = await client.get("https://gateway.test:8443/v1/models")

    assert response.json() == {"ok": True}
    assert network.dialed == [PUBLIC]
    assert network.sni == ["gateway.test"]
    assert response.request.url.host == "gateway.test"
    assert response.request.headers["host"] == "gateway.test:8443"


@pytest.mark.asyncio
@pytest.mark.parametrize("side", ["async", "sync"])
@pytest.mark.parametrize("answer", ["127.0.0.1", "fec0::1", "ff02::1", "5f00::1"])
async def test_a_non_global_answer_is_refused_before_anything_is_dialed(dns, network, monkeypatch, side, answer):
    dns.table["loopback.test"] = [answer]
    dialed = []
    monkeypatch.setattr(egress, "_dial", lambda ip, *a: dialed.append(ip))
    with pytest.raises(egress.EndpointRefused) as refused:
        if side == "async":
            async with httpx.AsyncClient(transport=egress.http_transport()) as client:
                await client.get("https://loopback.test/v1")
        else:
            with httpx.Client(transport=egress.http_transport()) as client:
                client.get("https://loopback.test/v1")
    assert refused.value.status_code == 422
    assert network.dialed == [] and dialed == []


@pytest.mark.asyncio
async def test_a_name_that_rebinds_is_refused_on_its_next_connection(dns, network):
    dns.table["rebind.test"] = [PUBLIC]
    network.response = _CLOSING
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        await client.get("https://rebind.test/v1")
        dns.table["rebind.test"] = ["127.0.0.1"]
        with pytest.raises(egress.EndpointRefused):
            await client.get("https://rebind.test/v1")
    assert network.dialed == [PUBLIC]


@pytest.mark.asyncio
async def test_a_name_that_does_not_resolve_is_a_connection_error(dns, network):
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("https://nowhere.test/v1")


@pytest.mark.asyncio
async def test_an_address_that_drops_packets_costs_a_stagger_not_the_connect_timeout(dns, network):
    dns.table["gateway.test"] = [PUBLIC_V6, PUBLIC]
    network.dead.add(PUBLIC_V6)
    started = time.monotonic()
    async with httpx.AsyncClient(
        transport=egress.http_transport(), timeout=httpx.Timeout(30, connect=30)
    ) as client:
        assert (await client.get("https://gateway.test/v1")).status_code == 200
    assert egress._STAGGER <= time.monotonic() - started < 5
    assert network.dialed == [PUBLIC_V6, PUBLIC]
    assert network.cancelled == [PUBLIC_V6]


@pytest.mark.asyncio
async def test_an_address_that_refuses_moves_on_at_once(dns, network):
    dns.table["gateway.test"] = [PUBLIC_V6, PUBLIC]
    network.refusing.add(PUBLIC_V6)
    started = time.monotonic()
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        assert (await client.get("https://gateway.test/v1")).status_code == 200
    assert time.monotonic() - started < egress._STAGGER
    assert network.dialed == [PUBLIC_V6, PUBLIC]


@pytest.mark.asyncio
async def test_a_held_connection_looks_its_name_up_off_the_default_pool(dns, network, monkeypatch):
    # A name that stalls holds a thread of the transport's own, never one of the
    # loop's default pool, which every other blocking call in the worker shares.
    dns.table["gateway.test"] = [PUBLIC]
    threads: list[str] = []
    answer = socket.getaddrinfo

    def recorded(host, *args, **kwargs):
        threads.append(threading.current_thread().name)
        return answer(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", recorded)
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        assert (await client.get("https://gateway.test/v1")).status_code == 200
    assert threads and all(name.startswith("egress-connect") for name in threads)


@pytest.mark.asyncio
async def test_every_address_failing_is_a_connection_error(dns, network):
    dns.table["gateway.test"] = [PUBLIC_V6, PUBLIC]
    network.refusing.update({PUBLIC_V6, PUBLIC})
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        with pytest.raises(httpx.ConnectError):
            await client.get("https://gateway.test/v1")


@pytest.mark.asyncio
async def test_a_connect_timeout_covers_the_whole_race(dns, network):
    dns.table["gateway.test"] = [PUBLIC_V6, PUBLIC]
    network.dead.update({PUBLIC_V6, PUBLIC})
    async with httpx.AsyncClient(
        transport=egress.http_transport(), timeout=httpx.Timeout(5, connect=0.4)
    ) as client:
        with pytest.raises(httpx.ConnectTimeout):
            await client.get("https://gateway.test/v1")
    assert sorted(network.cancelled) == sorted([PUBLIC_V6, PUBLIC])


def test_addresses_alternate_families_and_only_the_first_few_are_dialed():
    v6 = [f"2606:2800::{n}" for n in range(1, 7)]
    v4 = [f"93.184.215.{n}" for n in range(1, 7)]
    assert egress._interleaved(v6 + v4) == [ip for pair in zip(v6, v4) for ip in pair][:8]
    assert egress._interleaved(v4[:2] + v6[:1]) == [v4[0], v6[0], v4[1]]
    assert egress._interleaved([PUBLIC]) == [PUBLIC]


@pytest.mark.asyncio
async def test_two_names_on_one_address_do_not_share_a_connection(dns, network):
    dns.table["a.test"] = [PUBLIC]
    dns.table["b.test"] = [PUBLIC]
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        for name in ("a", "b", "a"):
            await client.get(f"https://{name}.test/")
    assert network.dialed == [PUBLIC, PUBLIC]
    assert network.sni == ["a.test", "b.test"]


@pytest.mark.asyncio
async def test_one_pool_bounds_what_any_number_of_names_leave_open(dns, network, monkeypatch):
    monkeypatch.setitem(egress._LIMITS, "max_keepalive_connections", 2)
    async with httpx.AsyncClient(transport=egress.http_transport()) as client:
        for n in range(6):
            dns.table[f"r{n}.test"] = [PUBLIC]
            await client.get(f"https://r{n}.test/")
            # The idle ones over the cap, plus the one just used.
            assert network.open() <= 3
    assert len(network.dialed) == 6


@pytest.mark.asyncio
async def test_clients_share_one_transport_per_family_and_closing_one_keeps_it(dns, network):
    assert egress.sdk_transport("anthropic") is egress.sdk_transport("anthropic")
    assert egress.sdk_transport("openai") is not egress.sdk_transport("anthropic")
    dns.table["gateway.test"] = [PUBLIC]
    transport = egress.sdk_transport.__wrapped__("anthropic")
    for _ in range(2):
        async with httpx.AsyncClient(transport=transport) as client:
            await client.get("https://gateway.test/v1")
    assert network.dialed == [PUBLIC]


def test_a_transport_that_no_longer_sends_through_its_pool_is_refused(monkeypatch):
    built = httpx.AsyncHTTPTransport.__init__

    def renamed(self, *args, **kwargs):
        built(self, *args, **kwargs)
        self._connection_pool = self.__dict__.pop("_pool")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "__init__", renamed)
    with pytest.raises(TypeError):
        egress.http_transport()


def _hanging_socket() -> tuple[socket.socket, socket.socket]:
    # A connect that never completes: a socket that never turns writable. The
    # caller keeps the peer open, since a plain socket takes no new attribute.
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    try:
        while True:
            sock.send(b"x" * 65536)
    except BlockingIOError:
        pass
    return sock, peer


def _answering_socket(hosts: list[str]) -> socket.socket:
    # A connected socket with an HTTP server on its other end.
    sock, peer = socket.socketpair()

    def serve():
        with peer:
            request = b""
            while b"\r\n\r\n" not in request:
                request += peer.recv(4096)
            hosts.extend(
                line.split(b":", 1)[1].strip().decode()
                for line in request.split(b"\r\n")
                if line.lower().startswith(b"host:")
            )
            peer.sendall(_CLOSING)

    threading.Thread(target=serve, daemon=True).start()
    sock.setblocking(False)
    return sock


def test_the_sync_side_opens_its_connection_on_the_validated_address(dns, monkeypatch):
    dns.table["gateway.test"] = [PUBLIC_V6, PUBLIC]
    dialed, hanging, peers, hosts = [], [], [], []

    def dial(ip, port, local_address, socket_options):
        dialed.append(ip)
        if ip == PUBLIC_V6:
            sock, peer = _hanging_socket()
            hanging.append(sock)
            peers.append(peer)
            return sock
        return _answering_socket(hosts)

    monkeypatch.setattr(egress, "_dial", dial)
    started = time.monotonic()
    with httpx.Client(transport=egress.http_transport(), timeout=httpx.Timeout(30, connect=30)) as client:
        assert client.get("http://gateway.test/v1").json() == {"ok": True}
    assert egress._STAGGER <= time.monotonic() - started < 5
    assert dialed == [PUBLIC_V6, PUBLIC]
    assert hosts == ["gateway.test"]
    # The attempt that lost was closed, not left connecting.
    assert hanging[0].fileno() == -1
    peers[0].close()


def test_the_sync_side_counts_the_lookup_against_the_connect_deadline(monkeypatch):
    def slow(host, **kwargs):
        time.sleep(0.5)
        return [PUBLIC]

    peers = []

    def dial(ip, port, local_address, socket_options):
        sock, peer = _hanging_socket()
        peers.append(peer)
        return sock

    monkeypatch.setattr(egress, "resolve_public_ips_sync", slow)
    monkeypatch.setattr(egress, "_dial", dial)
    started = time.monotonic()
    with httpx.Client(transport=egress.http_transport(), timeout=httpx.Timeout(30, connect=0.6)) as client:
        with pytest.raises(httpx.ConnectTimeout):
            client.get("http://gateway.test/v1")
    # The race gets what the lookup left of the deadline, as on the async side.
    assert time.monotonic() - started < 0.9
    for peer in peers:
        peer.close()


# ---------------------------------------------------------------------------
# Not retried
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_openai_sdk_gives_up_on_the_refusal_at_once(dns):
    dns.table["loopback.test"] = ["127.0.0.1"]
    client = openai.AsyncOpenAI(
        api_key="sk-test",
        base_url="https://loopback.test/v1",
        max_retries=3,
        http_client=openai.DefaultAsyncHttpxClient(transport=egress.sdk_transport("openai")),
    )
    with pytest.raises(openai.OpenAIError) as refused:
        await client.models.list()
    assert isinstance(refused.value, egress.EndpointRefused)
    assert dns.lookups["loopback.test"] == 1


@pytest.mark.asyncio
async def test_the_anthropic_sdk_gives_up_on_the_refusal_at_once(dns):
    dns.table["loopback.test"] = ["127.0.0.1"]
    client = anthropic.AsyncAnthropic(
        api_key="sk-test",
        base_url="https://loopback.test",
        max_retries=3,
        http_client=anthropic.DefaultAsyncHttpxClient(transport=egress.sdk_transport("anthropic")),
    )
    with pytest.raises(anthropic.AnthropicError) as refused:
        await client.models.list()
    assert isinstance(refused.value, egress.EndpointRefused)
    assert dns.lookups["loopback.test"] == 1


@pytest.mark.asyncio
async def test_the_google_genai_sdk_gives_up_on_the_refusal_at_once(dns):
    from google import genai
    from google.genai.types import HttpOptions, HttpRetryOptions

    dns.table["loopback.test"] = ["127.0.0.1"]
    transport = egress.sdk_transport("gemini")
    client = genai.Client(
        api_key="test-key",
        http_options=HttpOptions(
            base_url="https://loopback.test",
            client_args={"transport": transport},
            async_client_args={"transport": transport},
            retry_options=HttpRetryOptions(attempts=3),
        ),
    )
    with pytest.raises(egress.EndpointRefused):
        await client.aio.models.generate_content(model="gemini-x", contents="hi")
    assert dns.lookups["loopback.test"] == 1


@pytest.mark.asyncio
async def test_the_model_retry_layer_moves_on_after_one_attempt():
    from langchain.agents.middleware import ModelRequest

    from ptc_agent.agent.middleware.model_resilience import ModelResilienceMiddleware

    refusal = egress.EndpointRefused("The base URL cannot be used.")
    assert extract_status_code(refusal) == 422 and not is_retryable_error(refusal)

    calls = Counter()

    async def handler(_request):
        calls["n"] += 1
        raise refusal

    middleware = ModelResilienceMiddleware(primary_name="m", max_retries=3, initial_delay=30)
    request = ModelRequest(model=FakeListChatModel(responses=["x"]), messages=[HumanMessage("hi")])
    with pytest.raises(egress.EndpointRefused):
        await middleware.awrap_model_call(request, handler)
    assert calls["n"] == 1


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def _custom(provider, **params):
    return {"name": "probe", "model_id": "probe", "provider": provider, "parameters": params}


@pytest.mark.parametrize(
    "params, named",
    [
        ({"openai_proxy": "http://127.0.0.1:3128"}, "openai_proxy"),
        # Promoted to the field by the constructor, so dropped as one.
        ({"model_kwargs": {"openai_proxy": "http://127.0.0.1:3128", "http_client": None, "user": "u"}},
         "model_kwargs.openai_proxy"),
    ],
)
def test_an_openai_family_client_is_held_and_its_own_routing_dropped(monkeypatch, caplog, params, named):
    monkeypatch.setenv("OPENAI_PROXY", "http://127.0.0.1:3128")
    custom = _custom("openai", **params)
    saved = repr(custom)
    with caplog.at_level("WARNING", logger="src.llms.llm"):
        llm = create_llm_from_custom(
            custom, api_key="sk-test", base_url="https://gateway.test/v1",
            egress_transport=egress.sdk_transport,
        )
    for http_client in (llm.http_async_client, llm.http_client):
        assert isinstance(http_client._transport, egress._HeldHttpx2)
        assert http_client.follow_redirects is False
    assert llm.openai_proxy is None
    assert named in caplog.text
    if "model_kwargs" in params:
        assert llm.model_kwargs == {"user": "u"}
    # The model's own parameters are left as saved.
    assert repr(custom) == saved


def test_without_a_hold_a_client_is_built_as_before():
    llm = create_llm_from_custom(
        _custom("openai"), api_key="sk-test", base_url="https://gateway.test/v1"
    )
    assert llm.http_client is None and llm.http_async_client is None


@pytest.mark.parametrize("provider, oauth", [("deepseek", False), ("claude-oauth", True)])
def test_an_anthropic_client_is_seeded_with_held_sdk_clients(provider, oauth):
    llm = create_llm_from_custom(
        _custom(provider), api_key="sk-ant-test-key", base_url="https://gateway.test",
        egress_transport=egress.sdk_transport,
    )
    for sdk_client in (llm._client, llm._async_client):
        assert isinstance(sdk_client._client._transport, egress._HeldHttpx)
        assert sdk_client._client.follow_redirects is False
        assert str(sdk_client.base_url).startswith("https://gateway.test")
        if oauth:
            assert sdk_client.api_key is None and sdk_client.auth_token == "sk-ant-test-key"


def test_an_anthropic_client_that_no_longer_caches_its_sdk_client_is_refused():
    from langchain_anthropic import ChatAnthropic

    class Uncached(ChatAnthropic):
        @property
        def _client(self):
            return super()._client

    factory = LLM.__new__(LLM)
    factory.egress_transport = egress.sdk_transport
    with pytest.raises(TypeError):
        factory._hold_anthropic_to_egress(Uncached(model="m", api_key="sk-test"))


@pytest.mark.parametrize(
    "params",
    [
        {"client_args": {"proxy": "http://127.0.0.1:3128"}},
        {"model_kwargs": {"client_args": {"proxy": "http://127.0.0.1:3128"}}},
    ],
)
def test_a_gemini_client_is_held_on_both_its_clients(params):
    llm = create_llm_from_custom(
        _custom("gemini", **params),
        api_key="test-key", base_url="https://gateway.test",
        egress_transport=egress.sdk_transport,
    )
    assert set(llm.client_args) == {"transport", "follow_redirects"}
    assert isinstance(llm.client_args["transport"], egress._HeldHttpx)
    assert llm.client_args["follow_redirects"] is False


# ---------------------------------------------------------------------------
# BYOK resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oss_mode_holds_nothing(dns):
    assert await clients._egress_kwargs("m", "http://127.0.0.1:11434/v1") == {}


@pytest.mark.asyncio
async def test_a_manifest_endpoint_is_not_held(platform, dns):
    url = _manifest_url(lambda i: not i.get("platform") and i.get("access_type") != "local")
    assert await clients._egress_kwargs("m", url) == {}
    assert await clients._egress_kwargs("m", None) == {}


@pytest.mark.asyncio
async def test_a_users_public_endpoint_is_held(platform, dns):
    dns.table["gateway.test"] = [PUBLIC]
    assert await clients._egress_kwargs("m", "https://gateway.test/v1") == {
        "egress_transport": egress.sdk_transport
    }
    # A resolver that fails now is left to the call, which may retry it.
    assert "egress_transport" in await clients._egress_kwargs("m", "https://nowhere.test/v1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [{"openai_proxy": "http://127.0.0.1:3128"}, {"model_kwargs": {"openai_proxy": "http://127.0.0.1:3128"}}],
)
async def test_routing_in_parameters_is_held_even_on_a_manifest_endpoint(platform, dns, params):
    url = _manifest_url(lambda i: not i.get("platform") and i.get("access_type") != "local")
    kwargs = await clients._egress_kwargs("m", url, params)
    assert kwargs == {"egress_transport": egress.sdk_transport}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params, named",
    [
        ({"vertexai": True, "location": "evil.test/x?"}, "parameters.location"),
        ({"model_kwargs": {"location": "evil.test/x?"}}, "parameters.model_kwargs.location"),
    ],
)
async def test_vertex_parameters_are_refused_at_build_even_on_a_manifest_endpoint(platform, dns, params, named):
    url = _manifest_url(lambda i: not i.get("platform") and i.get("access_type") != "local")
    with pytest.raises(HTTPException) as refused:
        await clients._egress_kwargs("m", url, params)
    assert refused.value.status_code == 400
    assert refused.value.detail["type"] == "base_url_blocked"
    assert named in refused.value.detail["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["gemini", "anthropic", "openai"])
async def test_an_endpoint_under_model_kwargs_is_refused_before_the_client_is_built(platform, dns, provider):
    # On a manifest endpoint, so the nested URL is the only one the user chose.
    custom = _custom(provider, model_kwargs={"base_url": "http://127.0.0.1:8080"})
    with (
        patch.object(clients.user_models, "classify_model", AsyncMock(return_value=(clients.ModelSource.CUSTOM, custom))),
        patch.object(clients.user_models, "get_custom_provider_config", AsyncMock(return_value=None)),
        patch.object(clients, "walk_byok_candidates", AsyncMock(return_value=({"api_key": "k"}, provider))),
        patch("src.llms.llm.create_llm_from_custom") as create,
        pytest.raises(HTTPException) as refused,
    ):
        await clients.resolve_byok_llm_client("user-1", "probe", True)

    create.assert_not_called()
    assert refused.value.detail["type"] == "base_url_blocked"


@pytest.mark.asyncio
async def test_a_users_private_endpoint_is_refused_before_the_client_is_built(platform, dns):
    with (
        patch.object(
            clients.user_models, "classify_model",
            AsyncMock(return_value=(clients.ModelSource.SYSTEM, {"provider": "openai"})),
        ),
        patch.object(
            clients, "walk_byok_candidates",
            AsyncMock(return_value=({"api_key": "sk-test", "base_url": "http://127.0.0.1:8080/v1"}, "openai")),
        ),
        patch("src.llms.llm.create_llm") as create,
        pytest.raises(HTTPException) as refused,
    ):
        await clients.resolve_byok_llm_client("user-1", "some-model", True)

    create.assert_not_called()
    assert refused.value.status_code == 400
    assert refused.value.detail["type"] == "base_url_blocked"
    assert refused.value.detail["link"]["url"] == "/settings?tab=model"


def test_a_turn_that_inherited_a_blocked_model_runs_the_default():
    exc = HTTPException(status_code=400, detail={"type": "base_url_blocked", "message": "x"})
    assert thread_model._runs_default_instead(exc, named=None, model="m")
    assert not thread_model._runs_default_instead(exc, named="m", model="m")


# ---------------------------------------------------------------------------
# Custom model parameters (preferences write)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {"base_url": "http://127.0.0.1:8080/v1"},
        {"client_options": {"api_endpoint": "http://169.254.169.254"}},
        {"openai_proxy": "http://gateway.test:3128"},
        {"client_args": {"transport": None}},
        {"vertexai": True, "project": "p", "location": "evil.test/x?"},
        {"credentials": {"type": "service_account"}},
        # Each promoted out of ``model_kwargs`` by the constructor.
        {"model_kwargs": {"base_url": "http://127.0.0.1:8080"}},
        {"model_kwargs": {"client_options": {"api_endpoint": "http://169.254.169.254"}}},
        {"model_kwargs": {"anthropic_proxy": "http://gateway.test:3128"}},
        {"model_kwargs": {"vertexai": True, "location": "evil.test/x?"}},
    ],
)
async def test_a_custom_model_that_routes_around_the_policy_is_refused(platform, dns, params):
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    dns.table["gateway.test"] = [PUBLIC]
    with pytest.raises(HTTPException) as refused:
        await refuse_custom_model_endpoints([{"name": "m", "provider": "openai", "parameters": params}])
    assert refused.value.status_code == 400
    assert refused.value.detail.startswith("custom_models[0]:")


@pytest.mark.asyncio
async def test_a_custom_model_on_a_public_endpoint_is_saved(platform, dns):
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    dns.table["gateway.test"] = [PUBLIC]
    await refuse_custom_model_endpoints(
        [{"name": "m", "provider": "openai", "parameters": {"base_url": "https://gateway.test/v1"}}]
    )


@pytest.mark.asyncio
async def test_a_saved_model_moved_onto_a_subscription_is_checked_again(dns):
    # Its parameters are unchanged, but a subscription's model may set none.
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    params = {"base_url": "https://gateway.test/v1"}
    stored = [{"name": "m", "provider": "openai", "parameters": params}]
    with pytest.raises(HTTPException) as refused:
        await refuse_custom_model_endpoints([{"name": "m", "provider": "codex-oauth", "parameters": params}], stored)
    assert "subscription" in refused.value.detail


@pytest.mark.asyncio
async def test_oss_mode_saves_a_local_custom_model(dns):
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    await refuse_custom_model_endpoints(
        [{"name": "m", "provider": "openai", "parameters": {"base_url": "http://localhost:1234/v1"}}]
    )


@pytest.mark.asyncio
async def test_a_save_looks_up_each_new_base_url_once_up_to_a_bound(platform, dns):
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    def model(i, host):
        return {"name": f"m{i}", "provider": "openai", "parameters": {"base_url": f"https://{host}/v1"}}

    dns.table["gateway.test"] = [PUBLIC]
    await refuse_custom_model_endpoints([model(i, "gateway.test") for i in range(50)])
    assert dns.lookups["gateway.test"] == 1

    hosts = [f"h{i}.gateway.test" for i in range(egress.MAX_SAVE_CHECKS + 1)]
    with pytest.raises(HTTPException) as refused:
        await refuse_custom_model_endpoints([model(i, h) for i, h in enumerate(hosts)])
    assert refused.value.detail == egress.TOO_MANY_ENDPOINTS
    assert not any(dns.lookups[h] for h in hosts)


@pytest.mark.asyncio
async def test_a_key_save_is_bounded_however_many_providers_it_names(platform, dns):
    # Custom providers widen the names a save may carry, so the bound is on URLs.
    from src.server.app.api_keys import _refuse_base_urls

    hosts = [f"h{i}.gateway.test" for i in range(egress.MAX_SAVE_CHECKS + 1)]
    with pytest.raises(HTTPException) as refused:
        await _refuse_base_urls({f"custom-{i}": f"https://{h}/v1" for i, h in enumerate(hosts)})
    assert refused.value.detail == egress.TOO_MANY_ENDPOINTS
    assert not any(dns.lookups[h] for h in hosts)


@pytest.mark.asyncio
async def test_a_stalled_resolver_holds_a_save_only_until_the_deadline(dns, monkeypatch):
    released = threading.Event()
    threads: list[str] = []
    answer = socket.getaddrinfo

    def stalled(host, *args, **kwargs):
        threads.append(threading.current_thread().name)
        if host.startswith("slow"):
            released.wait(10)
        return answer(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", stalled)
    monkeypatch.setattr(egress, "CHECK_TIMEOUT", 0.2)
    dns.table["gateway.test"] = [PUBLIC]
    try:
        started = time.monotonic()
        reasons = await egress.save_refusals(
            ["https://slow1.test/v1", "https://slow2.test/v1", "https://gateway.test/v1"]
        )
        assert time.monotonic() - started < 1
        assert reasons == {
            "https://slow1.test/v1": egress.UNRESOLVED,
            "https://slow2.test/v1": egress.UNRESOLVED,
            "https://gateway.test/v1": None,
        }
        # Where a client is built, the name passes and the dial checks it again.
        assert await egress.endpoint_refusal("https://slow1.test/v1", require_resolution=False) is None
    finally:
        released.set()
    # Never on the loop's default pool, which every other blocking call shares.
    assert threads and all(name.startswith("egress-check") for name in threads)


@pytest.mark.asyncio
async def test_a_client_build_does_not_wait_behind_lookups_stuck_past_their_deadline(
    dns, monkeypatch
):
    # A lookup cannot be cancelled, so names whose nameserver never answers keep
    # every check thread after their checks have given up.
    released = threading.Event()
    answer = socket.getaddrinfo

    def stalled(host, *args, **kwargs):
        if host.startswith("slow"):
            released.wait(10)
        return answer(host, *args, **kwargs)

    pool = type(egress._CHECK_RESOLVER)(max_workers=2, thread_name_prefix="egress-check")
    monkeypatch.setattr(egress, "_CHECK_RESOLVER", pool)
    monkeypatch.setattr(socket, "getaddrinfo", stalled)
    monkeypatch.setattr(egress, "CHECK_TIMEOUT", 0.3)
    dns.table["gateway.test"] = [PUBLIC]
    try:
        await egress.save_refusals(["https://slow1.test/v1", "https://slow2.test/v1"])

        started = time.monotonic()
        # The dial checks the name as it connects.
        assert await egress.endpoint_refusal("https://gateway.test/v1", require_resolution=False) is None
        assert time.monotonic() - started < 0.1
        # An address needs no lookup, so a build still refuses one.
        refusal = await egress.endpoint_refusal("http://10.0.0.5/v1", require_resolution=False)
        assert refusal == egress.PRIVATE_ADDRESS
        # A save still waits for its lookup, and fails closed.
        reasons = await egress.save_refusals(["https://gateway.test/v1"])
        assert reasons == {"https://gateway.test/v1": egress.UNRESOLVED}
    finally:
        released.set()
        pool.shutdown(wait=True)


@pytest.mark.asyncio
async def test_a_client_build_looks_its_endpoints_up_side_by_side(dns, monkeypatch):
    released = threading.Event()
    answer = socket.getaddrinfo

    def stalled(host, *args, **kwargs):
        if host.startswith("slow"):
            released.wait(10)
        return answer(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", stalled)
    monkeypatch.setattr(egress, "CHECK_TIMEOUT", 0.3)
    urls = ["https://slow1.test/v1", "https://slow2.test/v1", "https://slow3.test/v1"]
    try:
        started = time.monotonic()
        # Names not answered in time pass here, and the dial checks them again.
        await egress.refuse_model_endpoints("custom", urls)
        assert time.monotonic() - started < 0.6
    finally:
        released.set()


@pytest.mark.asyncio
async def test_a_save_checks_only_the_entries_it_adds_or_reroutes(platform, dns):
    from src.server.services.llm.custom_catalog import refuse_custom_model_endpoints

    dns.table["gateway.test"] = [PUBLIC]
    dns.table["inside.test"] = ["10.0.0.5"]
    # Saved before the policy, or since gone from DNS: carried in every save.
    stored = [
        {"name": "gone", "provider": "openai", "parameters": {"base_url": "https://gone.test/v1"}},
        {"name": "legacy", "provider": "openai", "parameters": {"openai_proxy": "http://p.test:1"}},
    ]
    added = {"name": "new", "provider": "openai", "parameters": {"base_url": "https://gateway.test/v1"}}
    await refuse_custom_model_endpoints([*stored, added], stored)

    inside = {"name": "new", "provider": "openai", "parameters": {"base_url": "https://inside.test/v1"}}
    with pytest.raises(HTTPException) as refused:
        await refuse_custom_model_endpoints([*stored, inside], stored)
    assert refused.value.detail.startswith("custom_models[2]:")

    moved = {**stored[0], "parameters": {"base_url": "https://inside.test/v1"}}
    with pytest.raises(HTTPException) as refused:
        await refuse_custom_model_endpoints([moved, stored[1]], stored)
    assert refused.value.detail.startswith("custom_models[0]:")

    # Rerouted under ``model_kwargs``, the top level as stored.
    nested = {**stored[0], "parameters": {**stored[0]["parameters"], "model_kwargs": {"base_url": "https://inside.test/v1"}}}
    with pytest.raises(HTTPException) as refused:
        await refuse_custom_model_endpoints([nested, stored[1]], stored)
    assert refused.value.detail.startswith("custom_models[0]:")
