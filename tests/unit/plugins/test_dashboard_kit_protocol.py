"""The dashboard kit is copied into built pages and never updated, so a method,
scope or protocol version it names that the web bridge lacks fails only in a
user's page. The web suite cannot read ``plugins/``, so the pin lives here."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BRIDGE = ROOT / "web/src/lib/appBridge"
KIT = ROOT / "plugins/langalpha_deliverables/skills/dashboard-page/references/kit.js"


def _block(text: str, start: str) -> str:
    """The text from ``start``, which ends in ``{`` or ``[``, to the first matching closer (no nesting of the same kind)."""
    i = text.index(start) + len(start)
    return text[i : text.index("}" if start.endswith("{") else "]", i)]


def _host():
    protocol = (BRIDGE / "protocol.ts").read_text()
    methods = (BRIDGE / "methods.ts").read_text()
    version = int(re.search(r"PROTOCOL_VERSION = (\d+);", protocol).group(1))
    scopes = set(re.findall(r"'(\w+)'", _block(protocol, "export const SCOPES = [")))
    registry = methods[methods.index("export const METHODS = {") :]
    names = set(re.findall(r"^  '([\w.]+)':", registry, re.M))
    return version, scopes, names


def _kit():
    kit = KIT.read_text()
    names = set(re.findall(r"'([\w.]+)':", _block(kit, "var CADENCE = {")))
    names |= set(re.findall(r"(?:method:|\.watch\('\w+',|la\.get\(|la\.watch\()\s*'(\w+\.\w+)'", kit))
    scopes = set(re.findall(r"'(\w+)'", re.sub(r"'la-[\w-]+':", "", _block(kit, "var TAG_SCOPES = {"))))
    scopes |= set(re.findall(r"^\s+(\w+):", _block(kit, "var SCOPE_NAME = {"), re.M))
    versions = {int(v) for v in re.findall(r"\bla: (\d+)", kit)}
    return versions, scopes, names


def test_kit_names_only_what_the_host_has():
    version, host_scopes, host_methods = _host()
    kit_versions, kit_scopes, kit_methods = _kit()
    assert host_methods and host_scopes and kit_methods and kit_scopes and kit_versions
    assert kit_methods <= host_methods, kit_methods - host_methods
    assert kit_scopes <= host_scopes, kit_scopes - host_scopes
    assert kit_versions == {version}
