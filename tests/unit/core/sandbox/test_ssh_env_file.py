"""An SSH computer's env file points the data client at the root's token file.

The path comes from the computer's root, not from the env it was created with,
so a start that lost ``env.json`` (and starts on an empty env) still finds the
token.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox.providers._ssh_shell import env_file

ROOT = "/home/u/langalpha"


def _token_file(script: str) -> str:
    out = subprocess.run(
        ["bash", "-c", f'{script}printf %s "$GINLIX_TOKEN_FILE"'],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX shell")
@pytest.mark.parametrize(
    "env",
    [{}, {"GINLIX_TOKEN_FILE": "/elsewhere/.mcp_tokens.json", "A": "1"}],
    ids=["empty env", "stale value"],
)
def test_the_token_file_follows_the_root(env):
    script = env_file(env, "/home/u", ROOT)

    assert _token_file(script) == SandboxLayout(ROOT).mcp_tokens
    assert script.count("GINLIX_TOKEN_FILE=") == 1
