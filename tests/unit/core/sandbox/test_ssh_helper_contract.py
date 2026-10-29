"""What the server and LangAlpha's helper must agree on, read from both sides.

The helper is built and tested on its own, so nothing else would notice one
side changing without the other.
"""

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from ptc_agent.core.sandbox.providers._ssh_connect import (
    HELPER_ID_PREFIX,
    answered_by_helper,
)
from ptc_agent.core.sandbox.providers._ssh_shell import BOOT

_HELPER_SRC = Path(__file__).resolve().parents[4] / "helper" / "src"


def _rust_sources() -> str:
    return "\n".join(p.read_text() for p in sorted(_HELPER_SRC.rglob("*.rs")))


def _conn(server_version):
    return SimpleNamespace(
        get_extra_info=lambda name: server_version if name == "server_version" else None
    )


@pytest.mark.parametrize(
    ("server_version", "helper"),
    [
        (f"{HELPER_ID_PREFIX}0.1.0", True),
        ("SSH-2.0-OpenSSH_9.6", False),
        (None, False),
    ],
)
def test_the_helper_is_told_apart_by_its_handshake(server_version, helper):
    assert answered_by_helper(_conn(server_version)) is helper


def test_the_helper_runs_commands_the_way_the_server_sends_them():
    """The helper's tests run the server's boot line, so it must be this one."""
    found = re.search(r'const RUNNER: &str = r#"(.*?)"#;', _rust_sources())

    assert found is not None
    assert found.group(1) == BOOT


def test_the_helper_names_itself_as_the_server_looks_for():
    assert HELPER_ID_PREFIX in _rust_sources()
