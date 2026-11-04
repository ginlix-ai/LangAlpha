#!/usr/bin/env python
"""Pin the Claude Code CLI release a user's own computer installs.

Writes ``src/ptc_agent/agent/claude_code/cli_pins.json``: the release
version and, for each platform the runtime supports, its binary's SHA-256 and
size, read from the release's ``manifest.json``. Run it whenever the
``claude-agent-sdk`` pin moves; a unit test fails until the pins match the
installed SDK's ``__cli_version__``.

    uv run --extra claude-code python scripts/utils/pin_claude_code_cli.py
    uv run python scripts/utils/pin_claude_code_cli.py --version 2.1.310

The manifest's detached signature is checked with ``gpg``, and the pins are
written only once it verifies. The release key is public: it is fetched from ``KEY_URL`` into a throwaway keyring
(the user's own keyring is never touched) and accepted only when its
fingerprint equals ``KEY_FINGERPRINT``, the one Anthropic publishes at
https://code.claude.com/docs/en/setup ("Binary integrity and code signing").
Without gpg nothing is written, unless ``--unsigned`` asks for pins with no
signature checked, which leave ``signed_by`` null and fail the unit test.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]
PINS_FILE = REPO_ROOT / "src/ptc_agent/agent/claude_code/cli_pins.json"
RELEASES_URL = "https://downloads.claude.ai/claude-code-releases"
KEY_URL = "https://downloads.claude.ai/keys/claude-code.asc"
KEY_FINGERPRINT = "31DDDE24DDFAB679F42D7BD2BAA929FF1A7ECACE"
PLATFORMS = ("darwin-arm64", "darwin-x64", "linux-arm64", "linux-x64")


def sdk_cli_version() -> str:
    try:
        from claude_agent_sdk._cli_version import __cli_version__
    except ImportError:
        sys.exit("claude-agent-sdk is not installed: pass --version, or run with --extra claude-code")
    return __cli_version__


def verify_signature(manifest: bytes, signature: bytes, key: bytes) -> str:
    """The signing key's fingerprint, once gpg finds the signature good."""
    with tempfile.TemporaryDirectory() as home:
        files = Path(home)
        (files / "manifest.json").write_bytes(manifest)
        (files / "manifest.json.sig").write_bytes(signature)
        gpg = ["gpg", "--batch", "--homedir", home]
        subprocess.run([*gpg, "--quiet", "--import"], input=key, check=True, capture_output=True)
        listed = subprocess.run([*gpg, "--with-colons", "--fingerprint"], check=True, capture_output=True, text=True)
        fingerprints = [line.split(":")[9] for line in listed.stdout.splitlines() if line.startswith("fpr:")]
        if KEY_FINGERPRINT not in fingerprints:
            sys.exit(f"the key at {KEY_URL} is not {KEY_FINGERPRINT}: {fingerprints}")
        status = subprocess.run(
            [*gpg, "--status-fd", "1", "--verify", str(files / "manifest.json.sig"), str(files / "manifest.json")],
            capture_output=True,
            text=True,
        ).stdout
        if f"[GNUPG:] VALIDSIG {KEY_FINGERPRINT} " not in status:
            sys.exit(f"the manifest's signature does not verify with {KEY_FINGERPRINT}")
    return KEY_FINGERPRINT


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--version", help="release to pin (default: the installed SDK's __cli_version__)")
    parser.add_argument("--unsigned", action="store_true", help="write the pins without gpg, signature unchecked")
    args = parser.parse_args()
    version = args.version or sdk_cli_version()

    with httpx.Client(follow_redirects=True, timeout=60) as client:

        def get(url: str) -> bytes:
            response = client.get(url)
            response.raise_for_status()
            return response.content

        manifest_bytes = get(f"{RELEASES_URL}/{version}/manifest.json")
        signed_by = None
        if shutil.which("gpg"):
            signed_by = verify_signature(manifest_bytes, get(f"{RELEASES_URL}/{version}/manifest.json.sig"), get(KEY_URL))
        elif not args.unsigned:
            sys.exit("gpg is not installed, so the manifest's signature cannot be checked; install it, or pass --unsigned")
        else:
            print("warning: gpg is not installed; the pins are written without checking the signature", file=sys.stderr)

    manifest = json.loads(manifest_bytes)
    if manifest.get("version") != version:
        sys.exit(f"the manifest names version {manifest.get('version')!r}, not {version!r}")
    platforms = {}
    for name in PLATFORMS:
        entry = manifest["platforms"][name]
        if entry.get("binary") != "claude":
            sys.exit(f"{name}'s binary is {entry.get('binary')!r}, not 'claude'")
        platforms[name] = {"sha256": entry["checksum"], "size": int(entry["size"])}
    pins = {
        "version": version,
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "signed_by": signed_by,
        "platforms": platforms,
    }
    PINS_FILE.write_text(json.dumps(pins, indent=2) + "\n")
    state = "signature verified" if signed_by else "signature not checked"
    print(f"pinned Claude Code {version} ({state}) in {PINS_FILE.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
