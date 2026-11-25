"""A ranged read takes only the bytes it asked for, and reads no more of a
refusal than its cap, whatever the far side sends."""

from __future__ import annotations

import http.server
import threading

import pytest

from ptc_agent.core.sandbox.livefs_runtime import drive_ranges

pytestmark = pytest.mark.enable_socket

DATA = bytes(range(256)) * 64


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    mode = "right"

    def do_GET(self):  # noqa: N802 - the stdlib's name
        if self.mode == "big_chunked_refusal":
            self.send_response(503)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            chunk = b"x" * 65536
            for _ in range(64):  # 4 MiB, more than the cap
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
            self.wfile.write(b"0\r\n\r\n")
            return
        if self.mode == "chunked_whole":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            for at in range(0, len(DATA), 4096):
                chunk = DATA[at : at + 4096]
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
            self.wfile.write(b"0\r\n\r\n")
            return
        first, last = (int(x) for x in self.headers["Range"].removeprefix("bytes=").split("-"))
        if self.mode == "wrong_slice":
            first, last = 0, last - first  # as many bytes, from the wrong place
        body = DATA[first : last + 1]
        self.send_response(206)
        self.send_header("Content-Range", f"bytes {first}-{last}/{len(DATA)}")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):  # noqa: N802 - the stdlib's name
        self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(200)
        self.send_header("Content-Length", "100")
        self.end_headers()
        self.wfile.write(b"cut short")  # then the connection drops
        self.close_connection = True

    def log_message(self, *args):
        pass


@pytest.fixture
def far_side():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()


def _read(server, mode: str, start: int, end: int, got: bytearray | None = None):
    _Handler.mode = mode
    got = bytearray() if got is None else got
    buf = memoryview(bytearray(4096))
    answer = drive_ranges.get_range(drive_ranges.Connections(timeout=5), f"http://127.0.0.1:{server.server_port}/f",
                                    {}, start, end, lambda b: got.extend(b), buf)
    return answer, bytes(got)


def test_the_range_asked_for_is_read(far_side):
    answer, got = _read(far_side, "right", 4096, 8192)
    assert answer is None and got == DATA[4096:8192]


def test_a_206_of_another_slice_is_never_taken(far_side):
    got = bytearray()
    with pytest.raises(OSError):
        _read(far_side, "wrong_slice", 4096, 8192, got)
    assert bytes(got) == b"", "another slice's bytes reached the sink"


def test_a_refusal_with_no_length_is_read_only_to_the_cap(far_side):
    answer, _ = _read(far_side, "big_chunked_refusal", 0, 4096)
    assert answer is not None and answer.status == 503
    assert len(answer.body) <= drive_ranges.REFUSAL_MAX


def test_a_whole_file_answer_read_in_part_is_not_kept_for_reuse(far_side):
    """A chunked 200 for a range from 0 is read only as far as the range: its
    connection, with the rest unread, must not go back to the pool."""
    _Handler.mode = "chunked_whole"
    conns = drive_ranges.Connections(timeout=5)
    got = bytearray()
    url = f"http://127.0.0.1:{far_side.server_port}/f"
    assert drive_ranges.get_range(conns, url, {}, 0, 4096, lambda b: got.extend(b), memoryview(bytearray(4096))) is None
    assert bytes(got) == DATA[:4096]
    assert not any(conns._idle.values()), "a connection with an unread answer went back to the pool"


def test_a_range_read_whole_keeps_its_connection(far_side):
    _Handler.mode = "right"
    conns = drive_ranges.Connections(timeout=5)
    url = f"http://127.0.0.1:{far_side.server_port}/f"
    assert drive_ranges.get_range(conns, url, {}, 0, 4096, lambda b: None, memoryview(bytearray(4096))) is None
    assert sum(len(p) for p in conns._idle.values()) == 1


def test_a_put_answered_then_cut_short_reports_the_answer(far_side):
    """The store said 200 and the connection then dropped mid-body: the part
    is stored, not something to send again."""
    url = f"http://127.0.0.1:{far_side.server_port}/part"
    assert drive_ranges.put(drive_ranges.Connections(timeout=5), url, {}, b"body") == 200
