"""server.py names its own listener for the calls the process makes to itself."""

import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location("server_script", Path(__file__).resolve().parents[2] / "server.py")
server = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(server)


@pytest.mark.parametrize(
    ("host", "port", "url"),
    [
        ("0.0.0.0", 18000, "http://127.0.0.1:18000"),
        ("127.0.0.1", 8000, "http://127.0.0.1:8000"),
        ("localhost", 8101, "http://localhost:8101"),
        ("127.0.0.2", 8000, "http://127.0.0.2:8000"),
        ("::", 8000, "http://[::1]:8000"),
        ("::1", 8000, "http://[::1]:8000"),
        ("10.0.0.5", 8000, "http://10.0.0.5:8000"),
        ("fd00::5", 8000, "http://[fd00::5]:8000"),
    ],
)
def test_the_bind_names_the_loopback_url(host, port, url):
    assert server.listen_url(host, port) == url


def test_port_zero_names_none():
    assert server.listen_url("0.0.0.0", 0) is None
