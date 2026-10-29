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
import lzma
import re
import zipfile
import zlib
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ptc_agent.agent.middleware.skills.discovery import (
    MAX_SKILL_NAME_LENGTH,
    parse_skill_metadata,
    validate_skill_name,
)
from ptc_agent.agent.middleware.skills.registry import SKILL_REGISTRY
from ptc_agent.config.agent import host_skill_dirs
from src.server.services.os_litter import is_os_litter
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

_UNREADABLE_MEMBER = (
    zipfile.BadZipFile,
    RuntimeError,
    EOFError,
    OSError,
    zlib.error,
    lzma.LZMAError,
)

# PyYAML composes nodes recursively, so a few hundred nested brackets exhaust
# the stack in a frontmatter far under MAX_SKILL_MD_BYTES.
_UNPARSABLE_YAML = (yaml.YAMLError, RecursionError)


class SkillValidationError(Exception):
    """The archive or its SKILL.md is invalid — maps to a 400.

    ``code`` names the defect and ``params`` carry the values its message
    quotes, so a client can say what to fix in its own words; the message is
    for everyone else.
    """

    def __init__(self, message: str, *, code: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


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


def _clean_member_path(info: zipfile.ZipInfo, *, upload: bool = False) -> str | None:
    """Return the member's sanitized posix path, or None for an entry that is
    skipped: a directory, or a file ``_ignored`` (and in a person's upload
    ``_not_uploaded``) sets aside.

    Raises SkillValidationError on anything that could escape the extraction
    root: absolute paths, drive letters, backslashes, ``..`` components, and
    non-regular entries (symlinks, devices, fifos). Shell metacharacters and
    leading dashes are rejected too — these paths are later interpolated into
    sandbox command lines, and quoting there should not be the only guard.

    An escape is refused wherever it is, but a file set aside is skipped
    before its characters and its type are judged: it is never read or
    written, and a zipped working folder carries symlinks under
    ``node_modules/.bin`` and ``.venv`` that are no reason to refuse the skill
    beside them.
    """
    name = info.filename
    if info.is_dir():
        return None
    parts = [p for p in name.split("/") if p not in ("", ".")]
    if (
        "\\" in name
        or re.match(r"^[A-Za-z]:", name)
        or name.startswith("/")
        or not parts
        or ".." in parts
    ):
        raise _unsafe_path(name)
    path = "/".join(parts)
    if _ignored(path) or (upload and _not_uploaded(path)):
        return None
    if any(_unsafe_component(p) for p in parts):
        raise _unsafe_path(name)
    mode = _entry_mode(info)
    if mode and mode != _S_IFREG:
        raise SkillValidationError(
            f"archive entry {name!r} is not a regular file (symlinks and "
            "special files are not allowed)",
            code="not_regular",
            path=name,
        )
    return path


def _unsafe_path(path: str) -> SkillValidationError:
    return SkillValidationError(
        f"unsafe path in archive: {path!r}", code="unsafe_path", path=path
    )


def _unpacked_too_large() -> SkillValidationError:
    return SkillValidationError(
        f"archive exceeds {MAX_SKILL_UNCOMPRESSED_BYTES} bytes uncompressed",
        code="unpacked_too_large",
        max=MAX_SKILL_UNCOMPRESSED_BYTES,
    )


def _file_too_large(path: str) -> SkillValidationError:
    return SkillValidationError(
        f"file {path!r} exceeds {MAX_SKILL_SINGLE_FILE_BYTES} bytes",
        code="file_too_large",
        path=path,
        max=MAX_SKILL_SINGLE_FILE_BYTES,
    )


def _ignored(path: str) -> bool:
    """Entries the sandbox skill upload also skips, plus the litter macOS adds.
    ``skill_sync``'s ``projected_files`` mirrors this exactly.

    Finder's Compress writes a ``__MACOSX/`` tree beside the folder it zipped,
    which read as a second top-level directory and refused the most common
    way a Mac user packages a skill.
    """
    parts = path.split("/")
    return is_os_litter(parts) or "__pycache__" in parts or parts[-1] == "LICENSE.txt"


def _not_uploaded(path: str) -> bool:
    """What a person's upload leaves out: hidden files (an editor's or VCS's
    state, a stray ``.env``) and installed dependencies. A zip goes up as its
    maker packed it, so the door sets these aside rather than each client.

    Only the door: a skill the agent made keeps its dotfiles (``.gitkeep``,
    ``.env.example``) through sync, export and extraction, which use
    ``_ignored`` alone.
    """
    return any(p.startswith(".") or p == "node_modules" for p in path.split("/"))


def _read_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    """Read one member with the size cap re-checked on actual bytes.

    A member that will not decompress is the archive's defect, refused like a
    zip that will not open: encrypted or in a method zipfile lacks
    (RuntimeError), a bad CRC or local header (BadZipFile), or a corrupt or
    truncated stream (zlib.error, lzma.LZMAError, bz2's OSError, EOFError).
    """
    chunks: list[bytes] = []
    total = 0
    try:
        with zf.open(info) as fh:
            while True:
                chunk = fh.read(_READ_CHUNK)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_SKILL_SINGLE_FILE_BYTES:
                    raise _file_too_large(info.filename)
                chunks.append(chunk)
    except _UNREADABLE_MEMBER as e:
        raise SkillValidationError(
            f"archive entry {info.filename!r} cannot be read: {e}",
            code="not_zip",
            path=info.filename,
        ) from e
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
                    "YAML anchors and aliases are not allowed in frontmatter",
                    code="yaml_anchors",
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
    except _UNPARSABLE_YAML:
        return {}
    return data if isinstance(data, dict) else {}


def _declared(content: str, key: str) -> str:
    return str(_frontmatter(content).get(key, "") or "").strip()


def peek_frontmatter(skill_md: bytes) -> dict[str, Any]:
    """A SKILL.md's frontmatter as written, or empty when unusable, read
    before the skill is validated.

    For a caller that has to place a skill by its own name first: a folder of
    skills with no plugin.json is laid out by what each SKILL.md calls itself.
    The bytes come from an archive nothing has checked yet, so they get the
    same two guards validation gives them, the prompt-size cap and no alias
    expansion, and reading a name costs no more than validating it would.
    """
    if len(skill_md) > MAX_SKILL_MD_BYTES:
        return {}
    try:
        content = skill_md.decode("utf-8")
        _forbid_alias_expansion(content)
    except (UnicodeDecodeError, SkillValidationError):
        return {}
    return _frontmatter(content)


def _rejection(content: str) -> SkillValidationError:
    """Mirror parse_skill_metadata's downgrade branches to name the reason.

    The skill's folder takes the declared name, so the name check can only
    fail on length or charset, never on a folder that disagrees.
    """
    match = _FRONTMATTER_RE.match(content)
    if not match:
        return SkillValidationError(
            "SKILL.md must begin with YAML frontmatter (--- ... ---)",
            code="no_frontmatter",
        )
    try:
        data = yaml.safe_load(match.group(1))
    except _UNPARSABLE_YAML as e:
        return SkillValidationError(
            f"invalid YAML frontmatter: {e}", code="bad_frontmatter"
        )
    if not isinstance(data, dict):
        return SkillValidationError(
            "frontmatter must be a YAML mapping", code="bad_frontmatter"
        )
    name = _declared(content, "name")
    if not name:
        return SkillValidationError(
            "frontmatter must declare a name", code="no_name"
        )
    ok, err = validate_skill_name(name, name)
    if not ok:
        if len(name) > MAX_SKILL_NAME_LENGTH:
            return SkillValidationError(
                err, code="name_too_long", max=MAX_SKILL_NAME_LENGTH
            )
        return SkillValidationError(err, code="bad_name")
    if not str(data.get("description", "")).strip():
        return SkillValidationError(
            "frontmatter must declare a description", code="no_description"
        )
    return SkillValidationError(
        "invalid SKILL.md frontmatter", code="bad_frontmatter"
    )


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


def validate_skill_archive(raw: bytes, *, upload: bool = False) -> ValidatedSkill:
    """Validate an uploaded zip end to end; raise SkillValidationError on any
    defect. Returns the canonicalized archive plus parsed metadata.

    A single top-level folder is unwrapped whatever it is called: the skill is
    named by its SKILL.md, and the folder a person zips is often named
    something else ("earnings-recap v2"), which is no reason to refuse it.
    ``upload`` is for a person's upload alone (see ``_not_uploaded``); a
    sandbox pull-up or a plugin's skill keeps every file it holds.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as e:
        raise SkillValidationError(
            f"not a valid zip archive: {e}", code="not_zip"
        ) from e

    with zf:
        members: list[tuple[str, zipfile.ZipInfo]] = []
        header_total = 0
        for info in zf.infolist():
            path = _clean_member_path(info, upload=upload)
            if path is None:
                continue
            # Header sizes first for a fast reject; actual bytes re-checked
            # during the read below because headers lie.
            if info.file_size > MAX_SKILL_SINGLE_FILE_BYTES:
                raise _file_too_large(path)
            header_total += info.file_size
            if header_total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise _unpacked_too_large()
            members.append((path, info))

        if not members:
            raise SkillValidationError("archive contains no files", code="empty")
        if len(members) > MAX_SKILL_FILES:
            raise SkillValidationError(
                f"archive has {len(members)} files; max is {MAX_SKILL_FILES}",
                code="too_many_files",
                count=len(members),
                max=MAX_SKILL_FILES,
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
                f"archive member {min(both)!r} is both a file and a directory",
                code="file_and_directory",
                path=min(both),
            )

        prefix = ""
        if "SKILL.md" not in paths:
            top_levels = {path.split("/", 1)[0] for path in paths}
            top = next(iter(top_levels))
            if len(top_levels) != 1 or f"{top}/SKILL.md" not in paths:
                raise SkillValidationError(
                    "archive must contain SKILL.md at its root or inside a "
                    "single top-level directory",
                    code="layout",
                )
            prefix = f"{top}/"

        files: dict[str, bytes] = {}
        total = 0
        for path, info in members:
            data = _read_member(zf, info)
            total += len(data)
            if total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise _unpacked_too_large()
            files[path[len(prefix) :]] = data

    skill_md_bytes = files["SKILL.md"]
    if len(skill_md_bytes) > MAX_SKILL_MD_BYTES:
        raise SkillValidationError(
            f"SKILL.md exceeds {MAX_SKILL_MD_BYTES} bytes (it is injected "
            "into the model prompt)",
            code="skill_md_too_large",
            max=MAX_SKILL_MD_BYTES,
        )
    try:
        skill_md = skill_md_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise SkillValidationError(
            "SKILL.md is not valid UTF-8", code="not_utf8"
        ) from e

    _forbid_alias_expansion(skill_md)
    declared = _declared(skill_md, "name")
    if not declared:
        raise _rejection(skill_md)

    # The cap is measured on the declared value: parse_skill_metadata truncates
    # at the same boundary, so checking its output would silently accept an
    # over-long description instead of telling the author to shorten it.
    if len(_declared(skill_md, "description")) > MAX_SKILL_DESCRIPTION_CHARS:
        raise SkillValidationError(
            f"description exceeds {MAX_SKILL_DESCRIPTION_CHARS} characters "
            "(it is listed in the model's skill manifest every turn)",
            code="description_too_long",
            max=MAX_SKILL_DESCRIPTION_CHARS,
        )

    meta = parse_skill_metadata(skill_md, f"{declared}/SKILL.md", declared)
    if not meta["confirmed"]:
        raise _rejection(skill_md)

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
            if path is None:
                continue
            rel = path.split("/", 1)[1] if "/" in path else path
            if not rel:
                continue
            data = _read_member(zf, info)
            total += len(data)
            if total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise _unpacked_too_large()
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
            if path is None:
                continue
            target = (dest / path).resolve()
            if not target.is_relative_to(dest):
                raise _unsafe_path(path)
            data = _read_member(zf, info)
            total += len(data)
            if total > MAX_SKILL_UNCOMPRESSED_BYTES:
                raise _unpacked_too_large()
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
