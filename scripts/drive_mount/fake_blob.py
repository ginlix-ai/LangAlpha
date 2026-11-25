#!/usr/bin/env python3
"""A stand-in for object storage behind signed URLs: ``GET /o/<sha>?f=<file>``
with HTTP Range, on kept-alive HTTP/1.1 connections, plain and over TLS, with
injected network conditions. Runs in a container of its own (``harness.py``
starts it for the blocks suite), so what it spends never counts against the computer under test.

Conditions (``POST /admin/net``, JSON): ``ttfb`` seconds before each
answer, ``rate`` bytes a second per connection, ``connect`` seconds before a
new connection's first answer (its TCP and TLS setup). ``GET /admin/stats``
answers what was sent; ``GET /admin/cert`` the certificate to trust.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

CHUNK = 256 * 1024


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        #: ``cut_after``/``cuts``: the next ``cuts`` answers stop after that
        #: many bytes and drop the connection (a fetch cut off part way).
        self.net = {"ttfb": 0.0, "rate": 0.0, "connect": 0.0, "cut_after": 0, "cuts": 0}
        self.stats = {"bytes": 0, "requests": 0, "connections": 0, "cut": 0}
        self.cert = b""


STATE = State()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args) -> None:
        pass

    def setup(self) -> None:
        super().setup()
        self.fresh = True
        with STATE.lock:
            STATE.stats["connections"] += 1

    def _json(self, status: int, obj) -> None:
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path == "/admin/net":
            with STATE.lock:
                STATE.net.update(json.loads(body or b"{}"))
            return self._json(200, STATE.net)
        if self.path == "/admin/reset":
            with STATE.lock:
                STATE.stats = {"bytes": 0, "requests": 0, "connections": 0, "cut": 0}
            return self._json(200, {})
        return self._json(404, {})

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        if url.path == "/admin/stats":
            with STATE.lock:
                return self._json(200, dict(STATE.stats))
        if url.path == "/admin/cert":
            self.send_response(200)
            self.send_header("Content-Length", str(len(STATE.cert)))
            self.end_headers()
            self.wfile.write(STATE.cert)
            return None
        if not url.path.startswith("/o/"):
            return self._json(404, {})
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if int(q.get("exp", "0")) < time.time():
            return self._json(403, {"error": "expired"})
        path = q["f"]
        size = os.path.getsize(path)
        start, end, status = 0, size, 200
        rng = self.headers.get("Range", "")
        if rng.startswith("bytes="):
            a, _, b = rng[6:].partition("-")
            start, end, status = int(a), min(int(b) + 1 if b else size, size), 206
        with STATE.lock:
            net = dict(STATE.net)
            STATE.stats["requests"] += 1
            cut = 0
            if STATE.net.get("cuts") and STATE.net.get("cut_after") and end - start > STATE.net["cut_after"]:
                STATE.net["cuts"] -= 1
                STATE.stats["cut"] += 1
                cut = int(STATE.net["cut_after"])
        wait = net["ttfb"] + (net["connect"] if self.fresh else 0.0)
        self.fresh = False
        if wait:
            time.sleep(wait)
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(end - start))
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end - 1}/{size}")
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.flush()
        rate = float(net["rate"] or 0)
        began = time.monotonic()
        sent = 0
        tls = isinstance(self.connection, ssl.SSLSocket)
        with open(path, "rb") as f:
            while sent < end - start:
                if cut and sent >= cut:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    break
                n = min(CHUNK, end - start - sent)
                if tls:
                    self.connection.sendall(os.pread(f.fileno(), n, start + sent))
                else:
                    self.connection.sendfile(f, start + sent, n)
                sent += n
                if rate:
                    ahead = began + sent / rate - time.monotonic()
                    if ahead > 0:
                        time.sleep(ahead)
        with STATE.lock:
            STATE.stats["bytes"] += sent


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def server_bind(self) -> None:
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        super().server_bind()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8791)
    ap.add_argument("--tls-port", type=int, default=8792)
    ap.add_argument("--host", default="blob")
    a = ap.parse_args()
    tls_dir = "/tmp/blob-tls"
    os.makedirs(tls_dir, exist_ok=True)
    cert, key = f"{tls_dir}/cert.pem", f"{tls_dir}/key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
                    "-nodes", "-days", "2", "-subj", f"/CN={a.host}", "-addext", f"subjectAltName=DNS:{a.host}",
                    "-keyout", key, "-out", cert], check=True, capture_output=True)
    with open(cert, "rb") as f:
        STATE.cert = f.read()
    plain = Server(("0.0.0.0", a.port), Handler)
    secure = Server(("0.0.0.0", a.tls_port), Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    secure.socket = ctx.wrap_socket(secure.socket, server_side=True)
    threading.Thread(target=secure.serve_forever, daemon=True).start()
    print(f"blob: :{a.port} and TLS :{a.tls_port}", flush=True)
    plain.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
