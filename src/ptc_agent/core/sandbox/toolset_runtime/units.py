"""The table of units every computer's toolset converges to.

Stdlib only and Python 3.9: the server reads this module from source to know
the target version, and the machine runs it under whatever Python it has.
"""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

SCHEMA = 1
#: What a swap names the new link and the old one beside an exposed name.
STAGED_SUFFIX, KEPT_SUFFIX = ".toolset.tmp", ".toolset.was"
#: Where a computer's toolset lives, ``<root>/<name>/<pin>/<file>``.
DEFAULT_ROOT = "/opt/langalpha/toolset"
TABLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "units.json")

Unit = dict[str, Any]

# A name and a version are folders under the root, so neither may climb out of
# it or hide among the root's own dotted files.
_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")


def plain_name(name: object) -> bool:
    """Whether *name* is a unit name, a folder under the root that cannot climb out."""
    return isinstance(name, str) and bool(_NAME.fullmatch(name))
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")
_FILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MODE = re.compile(r"0[0-7]{3}")
# A release is checked against its digest wherever it comes from; the scheme
# keeps the table to https releases and the file:// ones a test or a local
# mirror serves.
_SCHEMES = ("https", "file")

_COMMON = {"name", "version", "kind", "file", "expose"}
_FIELDS = {
    "binary": _COMMON | {"platforms", "mode"},
    "link": _COMMON | {"target_glob"},
    "file": _COMMON | {"content", "mode"},
}
_PLATFORM_FIELDS = {"url", "sha256"}
_PLATFORM_OPTIONAL = {"size"}


class UnitsError(ValueError):
    """The table cannot be used: nothing is installed from it."""


@dataclass(frozen=True)
class Table:
    """The units in the order they install, with the table's revision.

    The revision only rises, so a machine that has taken one table refuses an
    older one: while two servers run side by side in a deploy, both converge
    the same machines, and an older table would pull them back.
    """

    revision: int
    units: tuple[Unit, ...]

    @property
    def version(self) -> str:
        """The set's version: any change to the revision, to the units' order
        or to any unit moves it."""
        body = canonical({"revision": self.revision, "units": list(self.units)})
        return hashlib.sha256(body).hexdigest()[:16]


def load(path: str | None = None) -> Table:
    """The table at *path*, the shipped one by default."""
    try:
        with open(path or TABLE, encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, ValueError) as exc:
        raise UnitsError(f"cannot read the table: {exc}") from exc
    return validate(raw)


def validate(raw: object) -> Table:
    if not isinstance(raw, dict) or set(raw) != {"schema", "revision", "units"}:
        raise UnitsError("the table is an object of schema, revision and units")
    if not (type(raw["schema"]) is int and raw["schema"] == SCHEMA):
        raise UnitsError(f"schema {raw['schema']!r} is not {SCHEMA}")
    revision = raw["revision"]
    if not (type(revision) is int and revision > 0):
        raise UnitsError(f"revision {revision!r} is not a positive integer")
    units = raw["units"]
    if not (isinstance(units, list) and units):
        # A run's failures are its units', so a table of none could not fail.
        raise UnitsError("units is a non-empty list")
    names, exposed = set(), set()
    for unit in units:
        _check(unit)
        if unit["name"] in names:
            raise UnitsError(f"{unit['name']}: named twice")
        names.add(unit["name"])
        for path in unit["expose"]:
            if path in exposed:
                raise UnitsError(f"{unit['name']}: {path} is exposed twice")
            if path.endswith((STAGED_SUFFIX, KEPT_SUFFIX)):
                # A swap's own names beside each exposed one.
                raise UnitsError(f"{unit['name']}: {path} ends as a swap's own names do")
            exposed.add(path)
    return Table(revision, tuple(units))


def version(table: Table | None = None) -> str:
    """*table*'s version, the shipped table's by default."""
    return (table if table is not None else load()).version


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def content_bytes(unit: Unit) -> bytes:
    """What a ``file`` unit writes: text as it is, anything else as JSON with
    sorted keys and a closing newline.

    That is byte for byte what images built before the toolset wrote (``echo``
    of ``json.dumps(..., sort_keys=True)``), so a converge adopts the policy
    file they carry instead of rewriting it.
    """
    content = unit["content"]
    if isinstance(content, str):
        return content.encode("utf-8")
    return (json.dumps(content, sort_keys=True) + "\n").encode("utf-8")


def content_version(unit: Unit) -> str:
    return hashlib.sha256(content_bytes(unit)).hexdigest()[:12]


def pin(unit: Unit) -> str:
    """What a machine records it holds of *unit*: the version and a digest of
    the whole entry, so a table that changes what a version is (a binary's
    release, a link's target) without moving the version is not taken for what
    the machine already has."""
    return f"{unit['version']}@{hashlib.sha256(canonical(unit)).hexdigest()[:12]}"


def mode(unit: Unit) -> int:
    return int(unit["mode"], 8)


def _check(unit: object) -> None:
    if not isinstance(unit, dict):
        raise UnitsError("a unit is an object")
    name = unit.get("name")
    if not plain_name(name):
        raise UnitsError(f"unit name {name!r} is not a plain lowercase name")
    kind = unit.get("kind")
    if not (isinstance(kind, str) and kind in _FIELDS):
        raise UnitsError(f"{name}: unknown kind {kind!r}")
    required = _FIELDS[kind]
    missing = required - set(unit)
    if missing:
        raise UnitsError(f"{name}: missing {', '.join(sorted(missing))}")
    extra = set(unit) - required
    if extra:
        raise UnitsError(f"{name}: unknown {', '.join(sorted(extra))}")
    if not (isinstance(unit["version"], str) and _VERSION.fullmatch(unit["version"])):
        raise UnitsError(f"{name}: version {unit['version']!r} is not a plain version")
    if unit["version"] == "current":
        raise UnitsError(f"{name}: version 'current' is the link's own name")
    if not (isinstance(unit["file"], str) and _FILE.fullmatch(unit["file"])):
        raise UnitsError(f"{name}: file {unit['file']!r} is not a plain file name")
    expose = unit["expose"]
    if not (isinstance(expose, list) and all(isinstance(p, str) and _absolute(p) for p in expose)):
        raise UnitsError(f"{name}: expose is a list of absolute paths")
    if "mode" in required and not (isinstance(unit["mode"], str) and _MODE.fullmatch(unit["mode"])):
        raise UnitsError(f"{name}: mode {unit['mode']!r} is not an octal mode like 0755")
    if kind == "binary":
        _check_platforms(name, unit["platforms"])
    elif kind == "link":
        if not (isinstance(unit["target_glob"], str) and _absolute(unit["target_glob"])):
            raise UnitsError(f"{name}: target_glob is an absolute pattern")
    elif _content_version(name, unit) != unit["version"]:
        # A file's version is its content's, so an edit cannot ship under the
        # old version and be taken for what the machine already has.
        raise UnitsError(f"{name}: version is not {content_version(unit)}, its content's")


def _content_version(name: str, unit: Unit) -> str:
    try:
        return content_version(unit)
    except UnicodeEncodeError as exc:
        raise UnitsError(f"{name}: content is not text UTF-8 can write") from exc


def _check_platforms(name: str, platforms: object) -> None:
    if not (isinstance(platforms, dict) and platforms):
        raise UnitsError(f"{name}: platforms is a non-empty object")
    for platform, release in platforms.items():
        if not isinstance(release, dict):
            raise UnitsError(f"{name} {platform}: a release is an object")
        missing = _PLATFORM_FIELDS - set(release)
        if missing:
            raise UnitsError(f"{name} {platform}: missing {', '.join(sorted(missing))}")
        extra = set(release) - _PLATFORM_FIELDS - _PLATFORM_OPTIONAL
        if extra:
            raise UnitsError(f"{name} {platform}: unknown {', '.join(sorted(extra))}")
        if not (isinstance(release["sha256"], str) and _SHA256.fullmatch(release["sha256"])):
            raise UnitsError(f"{name} {platform}: sha256 is 64 lowercase hex")
        url = release["url"]
        try:
            scheme = urlsplit(url).scheme if isinstance(url, str) else None
        except ValueError:
            scheme = None
        if scheme not in _SCHEMES:
            raise UnitsError(f"{name} {platform}: url is https")
        size = release.get("size")
        if size is not None and not (type(size) is int and size > 0):
            raise UnitsError(f"{name} {platform}: size is a positive integer")


def _absolute(path: str) -> bool:
    """An absolute path in its one spelling, so two units cannot expose one
    name under two and each take it from the other."""
    return (
        path.startswith("/")
        and not path.startswith("//")
        and "\0" not in path
        and posixpath.normpath(path) == path
    )
