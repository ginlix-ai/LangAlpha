"""The calls the runtime makes to Chrome itself, over its DevTools socket.

The engine drives pages; a call it has no command for, or one whose command
loses what is passed (its cookie import keeps only names and values), goes here.
Chrome's DevTools listens on loopback only, so this is a plain WebSocket client
with nothing to negotiate.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import struct
from urllib.parse import urlsplit

_ACCEPT_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
_TEXT, _CLOSE, _PING, _PONG = 0x1, 0x8, 0x9, 0xA


class DevToolsError(Exception):
    """Chrome refused a call, or the socket ended before it answered."""


class DevTools:
    """One connection to a browser's DevTools, for calls made in turn."""

    def __init__(self, url: str, timeout: float) -> None:
        address = urlsplit(url)
        if address.scheme != "ws" or address.hostname not in ("127.0.0.1", "localhost", "::1") or not address.port:
            raise DevToolsError("not a local DevTools address")
        self._sock = socket.create_connection((address.hostname, address.port), timeout=timeout)
        self._next_id = 0
        self._buffer = b""
        try:
            self._handshake(address.hostname, address.port, address.path or "/")
        except BaseException:
            self._sock.close()
            raise

    def __enter__(self) -> DevTools:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._send(_CLOSE, b"")
        except OSError:
            pass
        self._sock.close()

    def call(self, method: str, params: dict[str, object] | None = None) -> dict[str, object]:
        """Chrome's result for *method*, or ``DevToolsError`` with its message."""
        self._next_id += 1
        call_id = self._next_id
        self._send(_TEXT, json.dumps({"id": call_id, "method": method, "params": params or {}}).encode())
        while True:
            message = json.loads(self._message())
            if message.get("id") != call_id:
                # An event, which nothing here subscribed to.
                continue
            if "error" in message:
                raise DevToolsError(str(message["error"].get("message", "refused")))
            result = message.get("result")
            return result if isinstance(result, dict) else {}

    def _handshake(self, host: str, port: int, path: str) -> None:
        key = base64.b64encode(os.urandom(16))
        # No Origin: Chrome refuses a DevTools connection that names one it was
        # not told to allow, and a local client is no page.
        self._sock.sendall(
            f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key.decode()}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n".encode()
        )
        head = self._until(b"\r\n\r\n")
        status, _, rest = head.partition(b"\r\n")
        if b" 101 " not in status + b" ":
            raise DevToolsError("DevTools refused the connection")
        accept = base64.b64encode(hashlib.sha1(key + _ACCEPT_GUID).digest())
        headers = {
            name.strip().lower(): value.strip()
            for name, _, value in (line.partition(b":") for line in rest.split(b"\r\n"))
        }
        if headers.get(b"sec-websocket-accept") != accept:
            raise DevToolsError("DevTools answered as no WebSocket")

    def _send(self, opcode: int, payload: bytes) -> None:
        # A client's frames are masked, whatever the server.
        mask = os.urandom(4)
        size = len(payload)
        if size < 126:
            head = struct.pack("!BB", 0x80 | opcode, 0x80 | size)
        elif size < 1 << 16:
            head = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, size)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, size)
        self._sock.sendall(head + mask + _masked(payload, mask))

    def _message(self) -> bytes:
        """The next whole text message, answering pings on the way."""
        parts: list[bytes] = []
        while True:
            first, second = self._read(2)
            opcode = first & 0x0F
            size = second & 0x7F
            if size == 126:
                (size,) = struct.unpack("!H", self._read(2))
            elif size == 127:
                (size,) = struct.unpack("!Q", self._read(8))
            mask = self._read(4) if second & 0x80 else b""
            payload = self._read(size)
            if mask:
                payload = _masked(payload, mask)
            if opcode == _CLOSE:
                raise DevToolsError("DevTools closed the connection")
            if opcode == _PING:
                self._send(_PONG, payload)
                continue
            if opcode == _PONG:
                continue
            parts.append(payload)
            if first & 0x80:
                return b"".join(parts)

    def _read(self, size: int) -> bytes:
        while len(self._buffer) < size:
            chunk = self._sock.recv(max(size - len(self._buffer), 1 << 16))
            if not chunk:
                raise DevToolsError("DevTools closed the connection")
            self._buffer += chunk
        data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data

    def _until(self, end: bytes) -> bytes:
        while end not in self._buffer:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise DevToolsError("DevTools closed the connection")
            self._buffer += chunk
        head, _, self._buffer = self._buffer.partition(end)
        return head


def _masked(payload: bytes, mask: bytes) -> bytes:
    if not payload:
        return payload
    repeated = (mask * (len(payload) // 4 + 1))[: len(payload)]
    return (int.from_bytes(payload, "big") ^ int.from_bytes(repeated, "big")).to_bytes(len(payload), "big")
