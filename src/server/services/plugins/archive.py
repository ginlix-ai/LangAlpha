"""Hardened plugin-package extraction (zip and tar, fully in memory).

Nothing else in src/ extracts archives, so the bomb/traversal posture lives
here in one place. Every cap is applied twice: once from the header, so an
honest bomb is refused without inflating a byte, and again on the bytes that
actually came out, because a header is written by whoever built the archive —
a member can declare twenty bytes and carry a stream that expands to
gigabytes. The second half is what makes the caps real, so every inflation on
either path goes through a reader bounded by the budget it is spending.
Devices, absolute paths, and ``..`` members are rejected outright. Extraction
never touches the filesystem — the result is a path→bytes map the validators
consume.

A link member contributes the *content* of whatever it points at inside the
archive, and is skipped with a diagnostic when it points anywhere else. It is
never recreated as a link, which is what makes resolving it safe: the escape
a link normally threatens needs a link on disk for a later member to be
written through, and nothing here is ever written to disk. Rejecting them
instead cost real packages — publishing one plugin for several vendors is
done with links (``.claude-plugin`` beside ``.agy``, ``AGENTS.md`` beside
``CLAUDE.md``), so a single link in a repo refused the whole install.

Forge tarballs (and zips exported the same way) wrap the repo in a single
``repo-ref/`` directory; that root is stripped when it is the only top-level
entry and ``plugin.json`` is not already at the root.
"""

import bz2
import gzip
import io
import lzma
import stat
import tarfile
import zipfile
from collections.abc import Iterator

from src.server.models.plugin import Diagnostic
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.paths import split_member

# Sized for marketplace repos, which arrive whole before one plugin is
# selected (openai/plugins: ~5300 files, ~45 MiB uncompressed). The ratio
# caps below are the bomb guard; these bound transient memory.
MAX_MEMBERS = 10_000
MAX_UNCOMPRESSED_BYTES = 200 * 1024 * 1024
MAX_COMPRESSION_RATIO = 200
# A link may point at another link; the cap is what stops a cycle.
MAX_LINK_HOPS = 8
# A zip stores a link's target as the member's content, so finding out where
# one points means inflating bytes chosen by whoever built the archive. The
# target is a path, so it answers to a path-sized cap of its own.
MAX_LINK_TARGET_BYTES = 4096

_SPEC_ARCHIVE = "https://agent-plugins.org/specification"


def _fatal(message: str, *, code: str, target: str = "") -> PluginFatal:
    return PluginFatal(
        message,
        diagnostics=[
            Diagnostic(
                level="error",
                scope="plugin" if not target else "file",
                target=target,
                code=code,
                message=message,
                spec_ref=_SPEC_ARCHIVE,
            )
        ],
    )


def _check_member_path(name: str) -> tuple[str, ...]:
    segments = split_member(name)
    if segments is None:
        raise _fatal(
            f"archive member {name!r} is not contained in the package root",
            code="member_escape",
            target=name,
        )
    return segments


def _link_target(link_path: str, linkname: str, *, hard: bool) -> str | None:
    """The archive path a link points at, or None when it leaves the tree.

    ``..`` is resolved here rather than by ``split_member``, which refuses it:
    a link target is relative to the link's own directory, so climbing is
    ordinary (``.agy/plugin.json -> ../plugin.json``) right up until it
    escapes the package root. Tar hardlink names are archive-root relative
    instead, and carry no ``..`` to resolve.
    """
    if not linkname or "\x00" in linkname or "\\" in linkname:
        return None
    if linkname.startswith("/") or (len(linkname) > 1 and linkname[1] == ":"):
        return None
    if hard:
        segments = split_member(linkname)
        return "/".join(segments) if segments else None
    parts = link_path.split("/")[:-1]
    for segment in linkname.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if not parts:
                return None
            parts.pop()
            continue
        parts.append(segment)
    return "/".join(parts) or None


def _resolve_links(
    files: dict[str, bytes], links: dict[str, str]
) -> list[tuple[str, str]]:
    """Fold every in-archive link into ``files``; return the ones that dangle.

    A link to a directory mirrors that subtree under the link's own path,
    which is how a package publishes the same skills under two vendor
    directories. Mirrored bytes count against the uncompressed cap like any
    other member, so a link cannot be used to multiply a tree past it.
    """
    unresolved: list[tuple[str, str]] = []
    budget = MAX_UNCOMPRESSED_BYTES - sum(len(b) for b in files.values())
    for path in sorted(links):
        target = links[path]
        seen = {path}
        for _ in range(MAX_LINK_HOPS):
            if target not in links or target in seen:
                break
            seen.add(target)
            target = links[target]
        else:
            unresolved.append((path, links[path]))
            continue
        if target in files:
            payload = {path: files[target]}
        else:
            prefix = target + "/"
            payload = {
                f"{path}/{p[len(prefix):]}": b
                for p, b in files.items()
                if p.startswith(prefix)
            }
        if not payload:
            unresolved.append((path, links[path]))
            continue
        budget -= sum(len(b) for b in payload.values())
        if budget < 0:
            raise _fatal(
                "archive exceeds the uncompressed size limit "
                f"({MAX_UNCOMPRESSED_BYTES // (1024 * 1024)} MiB) once its "
                "links are resolved",
                code="too_large",
            )
        files.update(payload)
    return unresolved


def _inflate(zf: zipfile.ZipFile, info: zipfile.ZipInfo, limit: int) -> bytes:
    """Inflate one member, stopping just past ``limit`` however big it claims.

    Every guard the caller applies first reads the central directory, which is
    written by whoever built the archive. ``ZipFile.read`` inflates the whole
    DEFLATE stream before noticing the CRC disagrees with the size that was
    declared, so a member claiming twenty bytes still costs whatever its
    stream really expands to. Reading through an open handle bounds the
    inflation itself, which is the only cap a dishonest header cannot walk
    past. Returns up to ``limit + 1`` bytes so the caller can tell the
    difference between a member that fits and one that does not.
    """
    try:
        with zf.open(info) as handle:
            return handle.read(limit + 1)
    except Exception as e:
        # A corrupt deflate payload raises zlib.error, and a size that does
        # not match its own CRC raises BadZipFile; only the ZipFile
        # constructor is wrapped. An interrupted upload is the ordinary way
        # to get here, so it owes the same 422 as any unreadable archive
        # rather than a 500.
        raise _fatal(
            f"member {info.filename!r} could not be decompressed: {e}",
            code="unreadable",
            target=info.filename,
        ) from e


def _extract_zip(raw: bytes) -> tuple[dict[str, bytes], dict[str, str]]:
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as e:
        raise _fatal(f"not a valid zip archive: {e}", code="unreadable") from e

    files: dict[str, bytes] = {}
    links: dict[str, str] = {}
    with zf:
        infos = zf.infolist()
        if len(infos) > MAX_MEMBERS:
            raise _fatal(
                f"archive has {len(infos)} members (max {MAX_MEMBERS})",
                code="too_many_members",
            )
        total = 0
        for info in infos:
            segments = _check_member_path(info.filename)
            if info.is_dir() or not segments:
                continue
            if info.flag_bits & 0x1:
                raise _fatal(
                    f"encrypted member {info.filename!r} is not supported",
                    code="encrypted_member",
                    target=info.filename,
                )
            # Only the type nibble matters; many writers store bare permission
            # bits (or nothing), which is a regular file.
            mode = info.external_attr >> 16
            path = "/".join(segments)
            if stat.S_ISLNK(mode):
                target = _inflate(zf, info, MAX_LINK_TARGET_BYTES)
                if len(target) > MAX_LINK_TARGET_BYTES:
                    raise _fatal(
                        f"link {info.filename!r} has a target longer than "
                        f"{MAX_LINK_TARGET_BYTES} bytes",
                        code="link_target_too_long",
                        target=info.filename,
                    )
                links[path] = (
                    _link_target(
                        path, target.decode("utf-8", "replace"), hard=False
                    )
                    or ""
                )
                continue
            if stat.S_IFMT(mode) and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise _fatal(
                    f"member {info.filename!r} is not a regular file",
                    code="special_member",
                    target=info.filename,
                )
            # Both caps are read off the header first so an honest bomb is
            # refused without inflating a byte, and then enforced again on
            # what actually came out, which is the half a lying header cannot
            # walk past. Whichever cap is nearer bounds the read, so no member
            # ever costs more than the budget it is about to be refused for.
            ratio_cap = MAX_COMPRESSION_RATIO * max(info.compress_size, 1)
            if info.file_size > ratio_cap:
                raise _fatal(
                    f"member {info.filename!r} exceeds the compression-ratio "
                    f"limit ({MAX_COMPRESSION_RATIO}:1)",
                    code="compression_ratio",
                    target=info.filename,
                )
            data = _inflate(
                zf, info, min(MAX_UNCOMPRESSED_BYTES - total, ratio_cap)
            )
            if len(data) > ratio_cap:
                raise _fatal(
                    f"member {info.filename!r} exceeds the compression-ratio "
                    f"limit ({MAX_COMPRESSION_RATIO}:1)",
                    code="compression_ratio",
                    target=info.filename,
                )
            total += len(data)
            if total > MAX_UNCOMPRESSED_BYTES:
                raise _fatal(
                    "archive exceeds the uncompressed size limit "
                    f"({MAX_UNCOMPRESSED_BYTES // (1024 * 1024)} MiB)",
                    code="too_large",
                )
            files[path] = data
    return files, links


def _members(tf: tarfile.TarFile) -> Iterator[tarfile.TarInfo]:
    """Advance the stream, turning a truncated one into the same 422.

    The header advance is the third place truncation surfaces, and the only
    one outside a guard: ``tarfile`` raises EOFError from ``__next__`` when
    the compressed stream ends mid-header. Kept narrow on purpose, since the
    loop body raises PluginFatal and must not be swallowed here.
    """
    it = iter(tf)
    while True:
        try:
            yield next(it)
        except StopIteration:
            return
        except (tarfile.TarError, EOFError) as e:
            raise _fatal(
                f"archive is truncated or corrupt: {e}", code="unreadable"
            ) from e


class _CappedReader:
    """A read-only view of a decompressed tar stream, bounded by the budget.

    tarfile inflates a GNU long-name or pax header's payload while *parsing
    the header*, before that member ever reaches the loop below, so a member
    declaring a 400 MiB name costs 400 MiB whatever the loop checks
    afterwards. Unwrapping the outer stream here rather than letting
    ``tarfile`` do it puts every read tarfile makes through this cap, which is
    the only place a size the archive wrote can be disbelieved.
    """

    def __init__(self, inner, limit: int) -> None:
        self._inner = inner
        self._limit = limit

    def read(self, size: int = -1) -> bytes:
        remaining = self._limit - self._inner.tell()
        if remaining <= 0:
            raise _fatal(
                "archive exceeds the uncompressed size limit "
                f"({self._limit // (1024 * 1024)} MiB)",
                code="too_large",
            )
        return self._inner.read(
            remaining if size is None or size < 0 else min(size, remaining)
        )

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._inner.seek(offset, whence)

    def tell(self) -> int:
        return self._inner.tell()


_DECOMPRESSORS = (
    (b"\x1f\x8b", lambda f: gzip.GzipFile(fileobj=f)),
    (b"BZh", bz2.BZ2File),
    (b"\xfd7zXZ\x00", lzma.LZMAFile),
)


def _tar_stream(raw: bytes) -> _CappedReader:
    inner = io.BytesIO(raw)
    for magic, opener in _DECOMPRESSORS:
        if raw.startswith(magic):
            inner = opener(inner)
            break
    return _CappedReader(inner, MAX_UNCOMPRESSED_BYTES)


def _extract_tar(raw: bytes) -> tuple[dict[str, bytes], dict[str, str]]:
    try:
        tf = tarfile.open(fileobj=_tar_stream(raw), mode="r:")
    except (tarfile.TarError, EOFError) as e:
        # EOFError, not just TarError: an upload cut short in its first bytes
        # leaves too little for a header, and open raises it rather than a
        # TarError. Left uncaught it reaches the router's bare handler as a
        # 500, for what is a client-side fault.
        raise _fatal(f"not a valid tar archive: {e}", code="unreadable") from e

    files: dict[str, bytes] = {}
    links: dict[str, str] = {}
    with tf:
        count = 0
        total = 0
        for member in _members(tf):
            count += 1
            if count > MAX_MEMBERS:
                raise _fatal(
                    f"archive has more than {MAX_MEMBERS} members",
                    code="too_many_members",
                )
            segments = _check_member_path(member.name)
            if member.isdir():
                continue
            if member.issym() or member.islnk():
                links["/".join(segments)] = _link_target(
                    "/".join(segments), member.linkname, hard=member.islnk()
                ) or ""
                continue
            if not member.isfile():
                raise _fatal(
                    f"member {member.name!r} is not a regular file "
                    "(device and special files are rejected)",
                    code="special_member",
                    target=member.name,
                )
            # PEP 706 belt-and-braces on top of the explicit checks above; the
            # destination is never written to, it only anchors link resolution.
            try:
                tarfile.data_filter(member, "/nonexistent-plugin-root")
            except tarfile.FilterError as e:
                raise _fatal(
                    f"member {member.name!r} rejected: {e}",
                    code="member_escape",
                    target=member.name,
                ) from e
            total += member.size
            if total > MAX_UNCOMPRESSED_BYTES:
                raise _fatal(
                    "archive exceeds the uncompressed size limit "
                    f"({MAX_UNCOMPRESSED_BYTES // (1024 * 1024)} MiB)",
                    code="too_large",
                )
            try:
                fobj = tf.extractfile(member)
                if fobj is None:
                    continue
                with fobj:
                    files["/".join(segments)] = fobj.read()
            except PluginFatal:
                # The capped reader's own verdict, which already says exactly
                # what it refused and why.
                raise
            except Exception as e:
                # A truncated .tar.gz raises EOFError here rather than at open:
                # the stream only discovers the missing end-of-stream marker
                # while a member is being read. Same 422 as the zip side.
                raise _fatal(
                    f"member {member.name!r} could not be read: {e}",
                    code="unreadable",
                    target=member.name,
                ) from e
    # The whole-archive ratio is the only one a tar stream offers (per-member
    # compressed sizes don't exist for .tar.gz).
    if sum(len(b) for b in files.values()) > MAX_COMPRESSION_RATIO * max(
        len(raw), 1
    ):
        raise _fatal(
            f"archive exceeds the compression-ratio limit "
            f"({MAX_COMPRESSION_RATIO}:1)",
            code="compression_ratio",
        )
    return files, links


def _single_root_prefix(files: dict[str, bytes]) -> str:
    """The wrapping ``repo-ref/`` directory to strip, or "" when there is none."""
    if "plugin.json" in files or not files:
        return ""
    roots = {path.split("/", 1)[0] for path in files}
    if len(roots) != 1:
        return ""
    prefix = next(iter(roots)) + "/"
    if not any(len(path) > len(prefix) for path in files):
        return ""
    return prefix


def extract_plugin_archive(
    raw: bytes,
) -> tuple[dict[str, bytes], list[Diagnostic]]:
    """Extract a plugin package to a path→bytes map, or raise PluginFatal.

    Accepts zip and (optionally compressed) tar; strips a single wrapping
    root directory. Whether the tree actually holds a plugin is discovery's
    question, not extraction's — marketplace repos keep theirs in
    subdirectories. The diagnostics report links that pointed outside the
    archive, so a file that silently went missing says so.
    """
    if raw[:4] in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"):
        files, links = _extract_zip(raw)
    else:
        files, links = _extract_tar(raw)
    unresolved = _resolve_links(files, links)
    prefix = _single_root_prefix(files)
    if prefix:
        files = {
            path[len(prefix):]: content
            for path, content in files.items()
            if path.startswith(prefix) and len(path) > len(prefix)
        }
    diagnostics = []
    for path, target in unresolved:
        shown = path[len(prefix):] if path.startswith(prefix) else path
        diagnostics.append(
            Diagnostic(
                level="warning",
                scope="file",
                target=shown,
                code="link_unresolved",
                message=(
                    f"{shown!r} is a link to something the package does not "
                    f"contain ({target or 'outside the package root'}); it "
                    "was skipped"
                ),
                spec_ref=_SPEC_ARCHIVE,
            )
        )
    return files, diagnostics
