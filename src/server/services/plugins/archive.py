"""Hardened plugin-package extraction (zip and tar, fully in memory).

Nothing else in src/ extracts archives, so the bomb/traversal posture lives
here in one place: member count, uncompressed size, and compression ratio are
capped before any bytes are inflated; symlinks, hardlinks, devices, absolute
paths, and ``..`` members are rejected outright. Extraction never touches the
filesystem — the result is a path→bytes map the validators consume.

Forge tarballs (and zips exported the same way) wrap the repo in a single
``repo-ref/`` directory; that root is stripped when it is the only top-level
entry and ``plugin.json`` is not already at the root.
"""

import io
import stat
import tarfile
import zipfile

from src.server.models.plugin import Diagnostic
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.paths import split_member

# Sized for marketplace repos, which arrive whole before one plugin is
# selected (openai/plugins: ~5300 files, ~45 MiB uncompressed). The ratio
# caps below are the bomb guard; these bound transient memory.
MAX_MEMBERS = 10_000
MAX_UNCOMPRESSED_BYTES = 200 * 1024 * 1024
MAX_COMPRESSION_RATIO = 200

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


def _extract_zip(raw: bytes) -> dict[str, bytes]:
    try:
        zf = zipfile.ZipFile(io.BytesIO(raw))
    except zipfile.BadZipFile as e:
        raise _fatal(f"not a valid zip archive: {e}", code="unreadable") from e

    files: dict[str, bytes] = {}
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
            if stat.S_IFMT(mode) and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise _fatal(
                    f"member {info.filename!r} is not a regular file",
                    code="special_member",
                    target=info.filename,
                )
            total += info.file_size
            if total > MAX_UNCOMPRESSED_BYTES:
                raise _fatal(
                    "archive exceeds the uncompressed size limit "
                    f"({MAX_UNCOMPRESSED_BYTES // (1024 * 1024)} MiB)",
                    code="too_large",
                )
            if info.file_size > MAX_COMPRESSION_RATIO * max(
                info.compress_size, 1
            ):
                raise _fatal(
                    f"member {info.filename!r} exceeds the compression-ratio "
                    f"limit ({MAX_COMPRESSION_RATIO}:1)",
                    code="compression_ratio",
                    target=info.filename,
                )
            try:
                files["/".join(segments)] = zf.read(info)
            except Exception as e:
                # A corrupt deflate payload raises zlib.error, not a zipfile
                # error, and only the constructor above is wrapped. An
                # interrupted upload is the ordinary way to get here, so it
                # owes the same 422 as any other unreadable archive rather
                # than a 500.
                raise _fatal(
                    f"member {info.filename!r} could not be decompressed: {e}",
                    code="unreadable",
                    target=info.filename,
                ) from e
    return files


def _extract_tar(raw: bytes) -> dict[str, bytes]:
    try:
        tf = tarfile.open(fileobj=io.BytesIO(raw), mode="r:*")
    except tarfile.TarError as e:
        raise _fatal(f"not a valid tar archive: {e}", code="unreadable") from e

    files: dict[str, bytes] = {}
    with tf:
        count = 0
        total = 0
        for member in tf:
            count += 1
            if count > MAX_MEMBERS:
                raise _fatal(
                    f"archive has more than {MAX_MEMBERS} members",
                    code="too_many_members",
                )
            segments = _check_member_path(member.name)
            if member.isdir():
                continue
            if not member.isfile():
                raise _fatal(
                    f"member {member.name!r} is not a regular file "
                    "(links and special files are rejected)",
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
    return files


def _strip_single_root(files: dict[str, bytes]) -> dict[str, bytes]:
    if "plugin.json" in files or not files:
        return files
    roots = {path.split("/", 1)[0] for path in files}
    if len(roots) != 1:
        return files
    prefix = next(iter(roots)) + "/"
    stripped = {
        path[len(prefix):]: content
        for path, content in files.items()
        if path.startswith(prefix) and len(path) > len(prefix)
    }
    return stripped or files


def extract_plugin_archive(raw: bytes) -> dict[str, bytes]:
    """Extract a plugin package to a path→bytes map, or raise PluginFatal.

    Accepts zip and (optionally compressed) tar; strips a single wrapping
    root directory. Whether the tree actually holds a plugin is discovery's
    question, not extraction's — marketplace repos keep theirs in
    subdirectories.
    """
    if raw[:4] in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"):
        files = _extract_zip(raw)
    else:
        files = _extract_tar(raw)
    return _strip_single_root(files)
