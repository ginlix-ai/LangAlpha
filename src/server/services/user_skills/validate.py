"""Upload validation for user-tier skill archives.

The pipeline accepts a zip whose root is either ``SKILL.md`` itself or a single
top-level directory containing one, walks it with zip-slip/zip-bomb guards,
parses the frontmatter with the same ``parse_skill_metadata`` the sandbox scan
uses (host and sandbox can never disagree about a skill's identity), and
re-zips deterministically so the resulting ``content_hash`` is content-
addressed: identical content re-uploaded dedups to the same storage key and
the same host cache view.

Unlike the sandbox scan — which downgrades invalid frontmatter to an
unconfirmed entry — the API *rejects*, with the specific reason.
"""

from __future__ import annotations

import functools
import io
import re
import zipfile
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ptc_agent.agent.middleware.skills.discovery import (
    parse_skill_metadata,
    validate_skill_name,
)
from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY
from ptc_agent.config.agent import host_skill_dirs
from src.server.services.user_skills.limits import (
    MAX_SKILL_DESCRIPTION_CHARS,
    MAX_SKILL_FILES,
    MAX_SKILL_MD_BYTES,
    MAX_SKILL_SINGLE_FILE_BYTES,
    MAX_SKILL_UNCOMPRESSED_BYTES,
)

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)

# Unix file-type bits from a zip entry's external_attr high word.
_S_IFMT = 0o170000
_S_IFREG = 0o100000

_READ_CHUNK = 64 * 1024


class SkillValidationError(Exception):
    """The archive or its SKILL.md is invalid — maps to a 400."""


class SkillNamesUnavailable(RuntimeError):
    """A directory skills ship from could not be listed — maps to a 503.

    Raised rather than answered, for the reason ``BundleOwnershipUnavailable``
    is: the caller adds what comes back to a reservation set, so an incomplete
    answer reserves too little and the name a user takes is the one the sync
    then overwrites, silently replacing a shipped or operator skill.
    """


@dataclass(frozen=True)
class ValidatedSkill:
    """A fully validated upload, ready to store."""

    name: str
    description: str
    license: str | None
    frontmatter: dict[str, Any]
    allowed_tools: list[str]
    skill_md: str
    canonical_zip: bytes
    content_hash: str
    file_count: int
    # Declared ``command:`` alias, charset-checked; only ever a SEED — the DB
    # column is authoritative after creation. None when absent or invalid.
    command: str | None = None


# Triggers the composer owns outright: they run a client action or inject a
# directive instead of starting a skill turn, so a skill sharing one is
# unreachable by the default keystroke (the menu ranks skills last on an equal
# match). Mirrored from BUILTIN_SLASH_COMMANDS in
# web/src/components/ui/chat-input.helpers.tsx, which stays the source of truth
# because it also carries the pill type and copy; a test pins the two together.
COMPOSER_COMMANDS = frozenset(
    {"subagent", "compact", "compaction", "summarize", "offload", "truncate"}
)


def configured_skill_dirs() -> list[Path]:
    """Host-side skill sources as this process is actually configured.

    ``host_skill_dirs``'s default is the field default, not the operator's
    value, so every server-side reader that answers a question about the file
    the agent will load has to come through here. Two do: reservation, which
    is meaningless against a root delivery does not read, and the management
    preview, which would otherwise show the shipped copy of a skill the
    operator has overridden.
    """
    from src.server.app import setup

    config = setup.agent_config
    if config is None:
        return host_skill_dirs()
    return host_skill_dirs(config.skills.user_skills_dir)


def reserved_skill_names() -> frozenset[str]:
    """Names a user skill may not take, as this process is configured.

    Split from the cached body so the drop-in root is read live. Keying the
    cache on the value also means a call made before startup finished cannot
    pin the default over the configured answer.
    """
    from src.server.app import setup

    config = setup.agent_config
    return _reserved_skill_names(
        config.skills.user_skills_dir if config is not None else None
    )


@functools.lru_cache(maxsize=1)
def _registered_skill_names() -> frozenset[str]:
    """The half of the reservation that cannot change while the process runs.

    Registry keys preserve the no-shadowing invariant; command names prevent a
    user skill named e.g. ``dashboard`` from colliding with
    ``interactive-dashboard``'s slash command; composer commands are the same
    collision one layer up, in the client. All three are module-level Python,
    so reading them once is safe.
    """
    names: set[str] = set(SKILL_REGISTRY)
    names.update(s.command for s in SKILL_REGISTRY.values() if s.command)
    return frozenset(names | COMPOSER_COMMANDS)


def _reserved_skill_names(user_skills_dir: str | None) -> frozenset[str]:
    """Names a user skill may not take, all four sources.

    The fourth source is a directory listing rather than a list because that
    is what makes it catch shippers that never registered (e.g. ``x-api``), so
    it has to name every place a skill ships from: each bundle's own
    ``skills/``, and the operator's drop-in root. Reading one and not the
    other silently reserves nothing for the rest, and the sync overwrites
    last-source-wins, so the name a user took is the one the agent then loads.

    That listing is why only the registry half is cached. The operator's
    drop-in root is writable while the server runs, which is the whole point
    of it, so a set memoized per directory path would let a warm worker keep
    answering from a listing taken before the operator's skill landed and
    accept the very name it exists to reserve.
    """
    names = set(_registered_skill_names())
    dirs = (
        host_skill_dirs()
        if user_skills_dir is None
        else host_skill_dirs(user_skills_dir)
    )
    for skills_dir in dirs:
        if not skills_dir.is_dir():
            continue
        try:
            names.update(p.name for p in skills_dir.iterdir() if p.is_dir())
        except OSError as e:
            # ``is_dir`` swallows OSError and answers False; ``iterdir`` is
            # the one that raises, so this is the only hole in the guard above.
            # Reserving nothing for a directory we cannot read is how a user
            # skill comes to overwrite a shipped one, so refuse instead: the
            # caller turns this into a handled 503 rather than a bare 500 that
            # has already written a plugin row.
            raise SkillNamesUnavailable(
                f"cannot read {skills_dir}: {e}"
            ) from e
    return frozenset(names)


def _entry_mode(info: zipfile.ZipInfo) -> int:
    return (info.external_attr >> 16) & _S_IFMT


_UNSAFE_PATH_CHARS = frozenset("'\"`$;|&<>*?\n\r\t\\")


def _unsafe_component(part: str) -> bool:
    return (
        part.startswith("-")
        or any(c in _UNSAFE_PATH_CHARS or ord(c) < 0x20 or ord(c) == 0x7F for c in part)
    )


def _clean_member_path(info: zipfile.ZipInfo) -> str | None:
    """Return the member's sanitized posix path, or None for a directory entry.

    Raises SkillValidationError on anything that could escape the extraction
    root: absolute paths, drive letters, backslashes, ``..`` components, and
    non-regular entries (symlinks, devices, fifos). Shell metacharacters and
    leading dashes are rejected too — these paths are later interpolated into
    sandbox command lines, and quoting there should not be the only guard.
    """
    name = info.filename
    if info.is_dir():
        return None
    if "\\" in name or re.match(r"^[A-Za-z]:", name) or name.startswith("/"):
        raise SkillValidationError(f"unsafe path in archive: {name!r}")
    parts = [p for p in name.split("/") if p not in ("", ".")]
    if not parts or ".." in parts:
        raise SkillValidationError(f"unsafe path in archive: {name!r}")
    if any(_unsafe_component(p) for p in parts):
        raise SkillValidationError(f"unsafe path in archive: {name!r}")
    mode = _entry_mode(info)
    if mode and mode != _S_IFREG:
        raise SkillValidationError(
            f"archive entry {name!r} is not a regular file (symlinks and "
            "special files are not allowed)"
        )
    return "/".join(parts)


def _ignored(path: str) -> bool:
    """Entries the sandbox skill upload also skips."""
    parts = path.split("/")
    return "__pycache__" in parts or parts[-1] == "LICENSE.txt"


def _read_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    """Read one member with the size cap re-checked on actual bytes."""
    chunks: list[bytes] = []
    total = 0
    with zf.open(info) as fh:
        while True:
            chunk = fh.read(_READ_CHUNK)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_SKILL_SINGLE_FILE_BYTES:
                raise SkillValidationError(
                    f"file {info.filename!r} exceeds "
                    f"{MAX_SKILL_SINGLE_FILE_BYTES} bytes"
                )
            chunks.append(chunk)
    return b"".join(chunks)


def _forbid_alias_expansion(content: str) -> None:
    """Reject YAML anchors and aliases in the frontmatter block.

    ``yaml.safe_load`` blocks object construction but still expands aliases,
    so a frontmatter well under MAX_SKILL_MD_BYTES can inflate into gigabytes
    in memory (billion laughs) before any field check runs. Frontmatter has
    no legitimate use for anchors, so they are rejected outright — ahead of
    every ``safe_load`` in this module and in ``parse_skill_metadata``.
    ``yaml.parse`` emits events without composing, so the scan itself cannot
    expand anything.
    """
    match = _FRONTMATTER_RE.match(content)
    if not match:
        return
    try:
        for event in yaml.parse(match.group(1)):
            if getattr(event, "anchor", None) is not None:
                raise SkillValidationError(
                    "YAML anchors and aliases are not allowed in frontmatter"
                )
    except yaml.YAMLError:
        return  # the downstream parse names the syntax error


def _frontmatter(content: str) -> dict[str, Any]:
    """The raw frontmatter mapping, or empty when there isn't a usable one.

    Reads the declared values rather than ``parse_skill_metadata``'s output,
    which normalizes and truncates. Validation has to judge what the author
    wrote, so a too-long field is rejected instead of silently trimmed.
    """
    match = _FRONTMATTER_RE.match(content)
    if not match:
        return {}
    try:
        data = yaml.safe_load(match.group(1))
    except yaml.YAMLError:
        return {}
    return data if isinstance(data, dict) else {}


def _declared(content: str, key: str) -> str:
    return str(_frontmatter(content).get(key, "") or "").strip()


def _rejection_reason(content: str, dir_name: str) -> str:
    """Mirror parse_skill_metadata's downgrade branches to name the reason."""
    match = _FRONTMATTER_RE.match(content)
    if not match:
        return "SKILL.md must begin with YAML frontmatter (--- ... ---)"
    try:
        data = yaml.safe_load(match.group(1))
    except yaml.YAMLError as e:
        return f"invalid YAML frontmatter: {e}"
    if not isinstance(data, dict):
        return "frontmatter must be a YAML mapping"
    name = str(data.get("name", "")).strip()
    if not name:
        return "frontmatter must declare a name"
    ok, err = validate_skill_name(name, dir_name)
    if not ok:
        return err
    if not str(data.get("description", "")).strip():
        return "frontmatter must declare a description"
    return "invalid SKILL.md frontmatter"


_COMMAND_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")


def valid_command(command: str) -> bool:
    """Slash-command alias charset: same shape as a skill name."""
    return len(command) <= 64 and bool(_COMMAND_RE.match(command))


def _declared_command(content: str) -> str | None:
    """Frontmatter ``command:`` seed; parse_skill_metadata ignores the key.

    Invalid values degrade to None rather than failing validation — the alias
    is an optional nicety on an otherwise valid skill.
    """
    command = _declared(content, "command")
    return command if command and valid_command(command) else None


def _canonical_zip(name: str, files: dict[str, bytes]) -> bytes:
    """Deterministic re-zip: sorted names, fixed timestamps, 0644 modes.

    Determinism is what makes ``content_hash`` content-addressed — the same
    files always produce the same bytes regardless of upload order, source
    zip tool, or timestamps.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in sorted(files):
            info = zipfile.ZipInfo(f"{name}/{rel}", date_time=(1980, 1, 1, 0, 0, 0))
            info.external_attr = (_S_IFREG | 0o644) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, files[rel])
    return buf.getvalue()


def validate_skill_archive(raw: bytes) -> ValidatedSkill:
    """Validate an uploaded zip end to end; raise SkillValidationError on any
    defect. Returns the canonicalized archive plus parsed metadata."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as e:
        raise SkillValidationError(f"not a valid zip archive: {e}") from e

    with zf:
        members: list[tuple[str, zipfile.ZipInfo]] = []
        header_total = 0
        for info in zf.infolist():
            path = _clean_member_path(info)
            if path is None or _ignored(path):
                continue
            # Header sizes first for a fast reject; actual bytes re-checked
            # during the read below because headers lie.
            if info.file_size > MAX_SKILL_SINGLE_FILE_BYTES:
                raise SkillValidationError(
                    f"file {path!r} exceeds {MAX_SKILL_SINGLE_FILE_BYTES} bytes"
                )
            header_total += info.file_size
            if header_total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise SkillValidationError(
                    f"archive exceeds {MAX_SKILL_UNCOMPRESSED_BYTES} bytes uncompressed"
                )
            members.append((path, info))

        if not members:
            raise SkillValidationError("archive contains no files")
        if len(members) > MAX_SKILL_FILES:
            raise SkillValidationError(
                f"archive has {len(members)} files; max is {MAX_SKILL_FILES}"
            )

        paths = {path for path, _ in members}

        # A zip can hold both a file `x` and a file `x/y`; a filesystem cannot.
        # Extraction would write `x`, then fail creating it as a parent dir —
        # and that extraction runs inside resolve_llm_config, so one crafted
        # upload would 500 every one of this user's turns. Reject at the door,
        # before any member bytes are read.
        ancestors = {
            "/".join(path.split("/")[:i])
            for path in paths
            for i in range(1, path.count("/") + 1)
        }
        both = paths & ancestors
        if both:
            raise SkillValidationError(
                f"archive member {min(both)!r} is both a file and a directory"
            )

        top_dir: str | None = None
        if "SKILL.md" not in paths:
            top_levels = {path.split("/", 1)[0] for path in paths}
            if len(top_levels) == 1 and f"{next(iter(top_levels))}/SKILL.md" in paths:
                top_dir = next(iter(top_levels))
            else:
                raise SkillValidationError(
                    "archive must contain SKILL.md at its root or inside a "
                    "single top-level directory"
                )

        files: dict[str, bytes] = {}
        total = 0
        for path, info in members:
            rel = path[len(top_dir) + 1 :] if top_dir else path
            if not rel:
                continue
            data = _read_member(zf, info)
            total += len(data)
            if total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise SkillValidationError(
                    f"archive exceeds {MAX_SKILL_UNCOMPRESSED_BYTES} bytes uncompressed"
                )
            files[rel] = data

    skill_md_bytes = files["SKILL.md"]
    if len(skill_md_bytes) > MAX_SKILL_MD_BYTES:
        raise SkillValidationError(
            f"SKILL.md exceeds {MAX_SKILL_MD_BYTES} bytes (it is injected "
            "into the model prompt)"
        )
    try:
        skill_md = skill_md_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise SkillValidationError("SKILL.md is not valid UTF-8") from e

    _forbid_alias_expansion(skill_md)
    dir_name = top_dir or _declared(skill_md, "name")
    if not dir_name:
        raise SkillValidationError("frontmatter must declare a name")

    # The cap is measured on the declared value: parse_skill_metadata truncates
    # at the same boundary, so checking its output would silently accept an
    # over-long description instead of telling the author to shorten it.
    if len(_declared(skill_md, "description")) > MAX_SKILL_DESCRIPTION_CHARS:
        raise SkillValidationError(
            f"description exceeds {MAX_SKILL_DESCRIPTION_CHARS} characters "
            "(it is listed in the model's skill manifest every turn)"
        )

    meta = parse_skill_metadata(skill_md, f"{dir_name}/SKILL.md", dir_name)
    if not meta["confirmed"]:
        raise SkillValidationError(_rejection_reason(skill_md, dir_name))

    name = meta["name"]
    canonical = _canonical_zip(name, files)
    frontmatter = {k: v for k, v in meta.items() if k != "path"}
    return ValidatedSkill(
        name=name,
        description=meta["description"],
        license=meta["license"],
        frontmatter=frontmatter,
        allowed_tools=list(meta["allowed_tools"]),
        skill_md=skill_md,
        canonical_zip=canonical,
        content_hash=f"sha256:{sha256(canonical).hexdigest()}",
        file_count=len(files),
        command=_declared_command(skill_md),
    )


def archive_file_pairs(zip_bytes: bytes) -> list[tuple[str, bytes]]:
    """``(relpath, content)`` pairs from a stored canonical archive, with the
    same containment guards as extraction; the single top-level skill dir is
    stripped so the pairs are dir-relative."""
    pairs: list[tuple[str, bytes]] = []
    total = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for info in zf.infolist():
            path = _clean_member_path(info)
            if path is None or _ignored(path):
                continue
            rel = path.split("/", 1)[1] if "/" in path else path
            if not rel:
                continue
            data = _read_member(zf, info)
            total += len(data)
            if total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise SkillValidationError(
                    f"archive exceeds {MAX_SKILL_UNCOMPRESSED_BYTES} bytes uncompressed"
                )
            pairs.append((rel, data))
    return pairs


def safe_extract_archive(zip_bytes: bytes, dest: Path) -> None:
    """Extract a stored archive under ``dest`` with the same containment guards
    as validation.

    Defense in depth for the host cache: an archive stored before a validator
    fix must still not escape the extraction root or balloon on disk.
    """
    dest = dest.resolve()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        total = 0
        for info in zf.infolist():
            path = _clean_member_path(info)
            if path is None or _ignored(path):
                continue
            target = (dest / path).resolve()
            if not target.is_relative_to(dest):
                raise SkillValidationError(f"unsafe path in archive: {path!r}")
            data = _read_member(zf, info)
            total += len(data)
            if total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise SkillValidationError(
                    f"archive exceeds {MAX_SKILL_UNCOMPRESSED_BYTES} bytes uncompressed"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
