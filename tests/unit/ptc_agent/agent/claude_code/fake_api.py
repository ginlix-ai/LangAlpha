"""A loopback stand-in for the Anthropic Messages API, for keyless runs of the real CLI.

The runtime's launcher points the bundled claude binary at it with
``ANTHROPIC_BASE_URL`` and a dummy key, so a test drives the real CLI, the real
SDK control protocol and the real session records with no credential. Each
request is answered from a script chosen by what the conversation holds, and
every request body is kept so a test can check what the CLI sent.

With ``first_party=True`` the CLI keeps its own API host and signs in with a
dummy setup token (``TOKEN``, the launcher's to hand over), so it sends the
request a Claude plan login sends (the
1h cache, the billing header, request diagnostics): its ``HTTPS_PROXY`` is a
tunnel to a TLS listener that answers as api.anthropic.com with a throwaway
CA, trusted through ``NODE_EXTRA_CA_CERTS`` in the CLI's environment only.

- ``RUN_TOOL`` in the newest user text: call the ``Bash`` tool once; once the
  turn holds its result, answer quoting it.
- ``RECALL``: answer with every earlier ``FACT:`` line the CLI sent.
- ``FAIL_AUTH`` / ``FAIL_LIMIT``: refuse with 401 / 429.
- ``MAX_TOKENS``: answer ``part one`` and stop at the output limit, so the CLI
  continues the reply in a request of its own.
- Anything else: ``ECHO <text>``.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import json
import ssl
import tempfile
import uuid
from pathlib import Path
from typing import Any

from aiohttp import web

API_HOST = "api.anthropic.com"
#: The dummy setup token a first-party test launches with.
TOKEN = "sk-ant-oat01-" + "0" * 80


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _user_blocks(body: dict[str, Any]) -> list[list[dict[str, Any]]]:
    out = []
    for msg in body.get("messages", []):
        if msg.get("role") == "user":
            content = msg.get("content")
            out.append([{"type": "text", "text": content}] if isinstance(content, str) else content)
    return out


def _texts(blocks: list[dict[str, Any]]) -> list[str]:
    return [b.get("text", "") for b in blocks if b.get("type") == "text" and not b.get("text", "").startswith("<system-reminder>")]


def plan(body: dict[str, Any]) -> list[dict[str, Any]]:
    turns = _user_blocks(body)
    prompt = next((t[-1] for t in map(_texts, reversed(turns)) if t), "")
    result = None
    for blocks in turns:
        found = [b for b in blocks if b.get("type") == "tool_result"]
        if found:
            result = found[-1]
        elif _texts(blocks):
            result = None
    think = {"type": "thinking", "thinking": "Reasoning about it.", "signature": "sig-" + uuid.uuid4().hex}
    if result is not None:
        content = result.get("content")
        if isinstance(content, list):
            content = " ".join(b.get("text", "") for b in content if b.get("type") == "text")
        return [think, {"type": "text", "text": f"The tool said: {str(content).strip()[:300]}"}]
    if "RUN_TOOL" in prompt and any(t.get("name") == "Bash" for t in body.get("tools", [])):
        return [think, {"type": "tool_use", "id": "toolu_" + uuid.uuid4().hex[:20], "name": "Bash",
                        "input": {"command": "echo spike-$((6*7))"}}]
    if "RECALL" in prompt:
        facts = [ln for blocks in turns for t in _texts(blocks) for ln in t.splitlines() if ln.startswith("FACT:")]
        return [think, {"type": "text", "text": "I remember: " + " | ".join(facts)}]
    return [think, {"type": "text", "text": f"ECHO {prompt.strip()[:120]}"}]


async def _anything(request: web.Request) -> web.Response:
    return web.json_response({})


def _api_host_cert(folder: Path) -> tuple[str, str, str]:
    """A CA and an api.anthropic.com certificate it signed, for one run: (ca, cert chain, key)."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.UTC)
    span = {"not_valid_before": now - datetime.timedelta(minutes=5), "not_valid_after": now + datetime.timedelta(hours=1)}

    def cert(subject, key, issuer, signer, extensions):
        b = (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(key.public_key())
             .serial_number(x509.random_serial_number()).not_valid_before(span["not_valid_before"])
             .not_valid_after(span["not_valid_after"]))
        for ext, critical in extensions:
            b = b.add_extension(ext, critical=critical)
        return b.sign(signer, hashes.SHA256())

    ca_key, leaf_key = ec.generate_private_key(ec.SECP256R1()), ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "langalpha test CA")])
    ca = cert(ca_name, ca_key, ca_name, ca_key, [(x509.BasicConstraints(ca=True, path_length=0), True)])
    leaf = cert(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, API_HOST)]), leaf_key, ca_name, ca_key, [
        (x509.SubjectAlternativeName([x509.DNSName(API_HOST)]), False),
        (x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False),
    ])
    pem = serialization.Encoding.PEM
    paths = (folder / "ca.pem", folder / "leaf.pem", folder / "leaf.key")
    paths[0].write_bytes(ca.public_bytes(pem))
    paths[1].write_bytes(leaf.public_bytes(pem) + ca.public_bytes(pem))
    paths[2].write_bytes(leaf_key.private_bytes(pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    return str(paths[0]), str(paths[1]), str(paths[2])


async def _pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
    with contextlib.suppress(ConnectionError):
        while data := await src.read(65536):
            dst.write(data)
            await dst.drain()
    dst.close()


class FakeMessagesAPI:
    """The server and the request bodies it was sent."""

    def __init__(self, *, first_party: bool = False, refused: frozenset[str] = frozenset()) -> None:
        self.first_party = first_party
        #: Credentials answered with a 401, as Anthropic answers a revoked token.
        self.refused = refused
        self.requests: list[dict[str, Any]] = []
        #: The credential each request carried: its bearer token, else its API key.
        self.credentials: list[str | None] = []
        self._runner: web.AppRunner | None = None
        self._tunnel: asyncio.Server | None = None
        self._dir = tempfile.TemporaryDirectory(prefix="fake-api-")
        self._ca = ""
        self.url = ""

    async def __aenter__(self) -> FakeMessagesAPI:
        app = web.Application(client_max_size=64 * 1024 * 1024)
        app.router.add_post("/v1/messages", self._messages)
        app.router.add_route("*", "/{tail:.*}", _anything)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        tls = None
        if self.first_party:
            self._ca, chain, key = _api_host_cert(Path(self._dir.name))
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(chain, key)
        site = web.TCPSite(self._runner, "127.0.0.1", 0, ssl_context=tls)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
        self.url = f"http://127.0.0.1:{port}"
        if self.first_party:
            self._tunnel = await asyncio.start_server(lambda r, w: self._connect(r, w, port), "127.0.0.1", 0)
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self._runner is not None
        if self._tunnel is not None:
            self._tunnel.close()
        await self._runner.cleanup()
        self._dir.cleanup()

    def cli_env(self) -> dict[str, str]:
        """What the CLI's environment needs to reach this API: a dummy key, or first-party the route to it alone."""
        if not self.first_party:
            return {"ANTHROPIC_BASE_URL": self.url, "ANTHROPIC_API_KEY": "sk-ant-test-fake"}
        proxy = f"http://127.0.0.1:{self._tunnel.sockets[0].getsockname()[1]}"  # type: ignore[union-attr]
        return {"HTTPS_PROXY": proxy, "NODE_EXTRA_CA_CERTS": self._ca}

    async def _connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int) -> None:
        """A CONNECT to api.anthropic.com reaches the TLS listener; any other host is refused."""
        head = await reader.readuntil(b"\r\n\r\n")
        if not head.startswith(f"CONNECT {API_HOST}:443 ".encode()):
            writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            writer.close()
            return
        up_r, up_w = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await asyncio.gather(_pipe(reader, up_w), _pipe(up_r, writer))

    @property
    def model_requests(self) -> list[dict[str, Any]]:
        """The main loop's requests: the ones that carry the tool list."""
        return [b for b in self.requests if b.get("tools")]

    async def _messages(self, request: web.Request) -> web.StreamResponse:
        body = await request.json()
        self.requests.append(body)
        auth = request.headers.get("authorization", "")
        credential = auth.removeprefix("Bearer ") if auth else request.headers.get("x-api-key")
        self.credentials.append(credential)
        if credential in self.refused:
            return web.json_response({"type": "error", "error": {"type": "authentication_error", "message": "invalid token"}}, status=401)
        prompt = " ".join(t for blocks in _user_blocks(body)[-1:] for t in _texts(blocks))
        for word, (status, kind) in {"FAIL_AUTH": (401, "authentication_error"), "FAIL_LIMIT": (429, "rate_limit_error")}.items():
            if word in prompt:
                return web.json_response({"type": "error", "error": {"type": kind, "message": word}}, status=status)
        blocks = plan(body)
        msg_id = "msg_" + uuid.uuid4().hex[:24]
        stop = "tool_use" if any(b["type"] == "tool_use" for b in blocks) else "end_turn"
        if "MAX_TOKENS" in prompt:
            blocks, stop = [{"type": "text", "text": "part one"}], "max_tokens"
        usage = {"input_tokens": 120, "output_tokens": 0, "cache_read_input_tokens": 3000, "cache_creation_input_tokens": 40}
        resp = web.StreamResponse(headers={"content-type": "text/event-stream"})
        await resp.prepare(request)
        await resp.write(_sse("message_start", {"type": "message_start", "message": {
            "id": msg_id, "type": "message", "role": "assistant", "model": body.get("model"), "content": [],
            "stop_reason": None, "stop_sequence": None, "usage": usage}}))
        for i, b in enumerate(blocks):
            if b["type"] == "thinking":
                start, deltas = {"type": "thinking", "thinking": "", "signature": ""}, [
                    {"type": "thinking_delta", "thinking": b["thinking"]}, {"type": "signature_delta", "signature": b["signature"]}]
            elif b["type"] == "text":
                start, deltas = {"type": "text", "text": ""}, [{"type": "text_delta", "text": b["text"]}]
            else:
                start = {"type": "tool_use", "id": b["id"], "name": b["name"], "input": {}}
                deltas = [{"type": "input_json_delta", "partial_json": json.dumps(b["input"])}]
            await resp.write(_sse("content_block_start", {"type": "content_block_start", "index": i, "content_block": start}))
            for d in deltas:
                await resp.write(_sse("content_block_delta", {"type": "content_block_delta", "index": i, "delta": d}))
            await resp.write(_sse("content_block_stop", {"type": "content_block_stop", "index": i}))
        await resp.write(_sse("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop, "stop_sequence": None},
                                                "usage": {**usage, "output_tokens": 30}}))
        await resp.write(_sse("message_stop", {"type": "message_stop"}))
        await resp.write_eof()
        return resp
