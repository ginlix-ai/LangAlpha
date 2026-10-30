"""The pinned CLI release stays the one the pinned SDK bundles."""

from __future__ import annotations

import hashlib
import json
import platform
from pathlib import Path

from ptc_agent.agent.claude_code import cli_install
from ptc_agent.agent.claude_code.cli_install import PlatformPin

PLATFORMS = ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64")


def backend_platform() -> str:
    arch = {"x86_64": "x64", "amd64": "x64", "aarch64": "arm64", "arm64": "arm64"}[platform.machine().lower()]
    if platform.system() == "Darwin":
        return f"darwin-{arch}"
    return f"linux-{arch}"


def test_the_pins_are_the_pinned_sdks_cli():
    """Moving the SDK pin without re-running scripts/utils/pin_claude_code_cli.py fails here."""
    from claude_agent_sdk import __file__ as sdk_init
    from claude_agent_sdk._cli_version import __cli_version__

    pin = cli_install.pinned()
    assert pin.version == __cli_version__
    assert sorted(pin.platforms) == sorted(PLATFORMS)
    bundled = Path(sdk_init).parent / "_bundled" / "claude"
    data = bundled.read_bytes()
    assert pin.platforms[backend_platform()] == PlatformPin(hashlib.sha256(data).hexdigest(), len(data))


def test_every_pin_was_written_from_a_verified_signature():
    """The pin script writes signed_by only once gpg verified the manifest with the release key."""
    from scripts.utils.pin_claude_code_cli import KEY_FINGERPRINT, PINS_FILE

    pins = json.loads(PINS_FILE.read_text())
    assert pins["signed_by"] == KEY_FINGERPRINT
    assert pins["platforms"] and all(pin["sha256"] and pin["size"] for pin in pins["platforms"].values())
