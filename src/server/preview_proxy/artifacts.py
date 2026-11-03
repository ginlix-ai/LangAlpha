"""Published artifact versions, each artifact served from an origin of its own
under the preview base.

An artifact's host is ``artifact-<artifact id>.<base>``, so its scripts run on
an origin that is neither the app's nor another artifact's, and since the id
is stable its storage survives a new grant. The grant rides in the path
(``services/artifacts/grants``). A deployment without a base serves the same
files on the API origin under a CSP sandbox (``app/artifact_frames.py``),
which costs the version its storage but still keeps it out of the app.

A version never changes, so its files come from Postgres and object storage
alone, never a sandbox: a version opens while its computer is stopped.

The owner's grant opens any version, read only. A visitor who came through
the artifact's link holds a grant for one version instead, which every fetch
checks against the link's row, so an unshare or an Update holds on the next
request on every worker. Either way a document carries the page bridge read
only (``page_bridge``): comments sit on the live file, which
``workspace_files/serve.py`` frames with them on.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any

import psycopg

from src.server.database.blob_keys import BlobError
from src.server.database.artifacts import (
    StoredVersionFile,
    get_shared_version_file,
    get_version_file,
)
from src.server.database.workspace_file_blobs import fetch_blob
from src.server.preview_proxy.page_bridge import bridge_for, with_bridge
from src.server.services.artifacts.grants import (
    ArtifactGrantError,
    is_share_grant,
    verify_artifact_grant,
    verify_artifact_share_grant,
)
from src.server.utils.html_policy import html_document_csp

__all__ = [
    "ArtifactFileUnavailable",
    "error_page",
    "load_artifact_file",
    "serve_artifact",
]

logger = logging.getLogger(__name__)

_ROUTE = re.compile(r"/g/([^/]+)/v/([^/]+)/(.*)", re.DOTALL)
_SEQ = re.compile(r"[1-9][0-9]{0,8}")
# What the version file rows admit (migration 101).
_MAX_PATH_CHARS = 1024

_PERMISSIONS_POLICY = ", ".join(
    f"{feature}=()"
    for feature in (
        "camera",
        "microphone",
        "geolocation",
        "display-capture",
        "usb",
        "hid",
        "serial",
        "bluetooth",
        "midi",
        "payment",
        "idle-detection",
    )
)
# A version never changes and its URL carries a bearer grant, so the browser
# may keep it as long as it likes and nothing shared may keep it at all.
_CACHE_CONTROL = "private, max-age=31536000, immutable"
# A document carries the bridge as well as the version, and the bridge changes
# with the server: held as immutable, a version viewed once would keep speaking
# the old bridge to a new viewer until the grant rotated. So the browser asks
# each time and the ETag, which names both, answers most asks with a 304.
_DOCUMENT_CACHE_CONTROL = "private, no-cache"
# A visitor's every file is asked for again, so each one meets the share's
# row: a file the browser kept would still open after the link was stopped.
# The ETag keeps the asking cheap.
_SHARED_CACHE_CONTROL = "private, no-cache"
# A service worker would outlive the grant and answer for the origin;
# registering one with any scope a document can reach fails against this.
_SERVICE_WORKER_ALLOWED = "/__no_sw__/"

_TEXT_TYPES = frozenset(
    {
        "application/javascript",
        "application/json",
        "application/xml",
        "application/yaml",
        "image/svg+xml",
    }
)

_ERROR_HEADERS = [
    ("content-type", "text/html; charset=utf-8"),
    ("cache-control", "no-store"),
    # So the app's read of a file sees the 404 that ends a grant, and asks
    # for a fresh one, rather than a CORS failure it cannot tell apart.
    ("access-control-allow-origin", "*"),
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "no-referrer"),
    ("x-robots-tag", "noindex"),
]
_ERROR_CSP = "default-src 'none'; style-src 'unsafe-inline'"
_ERRORS = {
    404: (
        "File not available",
        "This file wasn't shared with this link, or the link has expired. Go "
        "back, or open the link again.",
    ),
    405: ("Method not allowed", "A file does not accept this kind of request."),
    503: (
        "File unavailable",
        "This file can't be loaded right now. Try again shortly.",
    ),
}


class ArtifactFileUnavailable(Exception):
    """The file exists, or may, but cannot be read right now."""


@dataclass(frozen=True)
class ServedArtifactFile:
    body: bytes
    headers: list[tuple[str, str]]
    status: int = 200


def _valid_path(path: str) -> bool:
    """A file path the URL may name: relative, with no step outside or in place.

    The rows hold only such paths, so this is not containment; it keeps a
    malformed name from reaching the query. A directory is not a file.
    """
    if not path or len(path) > _MAX_PATH_CHARS or "\\" in path or "\x00" in path:
        return False
    return all(segment not in ("", ".", "..") for segment in path.split("/"))


def _content_type(stored: str) -> str:
    """The stored type, naming UTF-8 for a text type that names no charset.

    UTF-8 is what publishing reads text as, and a document without the
    parameter would leave the browser guessing.
    """
    media = stored.split(";", 1)[0].strip().lower()
    if "charset=" in stored.lower():
        return stored
    if (
        media.startswith("text/")
        or media in _TEXT_TYPES
        or media.endswith(("+json", "+xml"))
    ):
        return f"{stored}; charset=utf-8"
    return stored


def _is_html(content_type: str) -> bool:
    return content_type.split(";", 1)[0].strip().lower() == "text/html"


def _charset_is_utf8(content_type: str) -> bool:
    for parameter in content_type.split(";")[1:]:
        name, _, value = parameter.partition("=")
        if name.strip().lower() == "charset":
            return value.strip().strip('"').lower() in ("utf-8", "utf8")
    return True


def _headers(
    content_type: str,
    etag: str,
    *,
    cache_control: str,
    sandboxed: bool,
    frame_ancestors: str | None,
) -> list[tuple[str, str]]:
    # A page on its own host may frame its own files, an <object> or an <iframe>
    # of a chart, and each of those meets this same list.
    if frame_ancestors and not sandboxed:
        frame_ancestors = f"'self' {frame_ancestors}"
    csp = html_document_csp(sandboxed=sandboxed, frame_ancestors=frame_ancestors)
    headers = [
        ("content-type", content_type),
        ("cache-control", cache_control),
        ("etag", f'"{etag}"'),
        ("content-security-policy", csp),
        ("permissions-policy", _PERMISSIONS_POLICY),
        ("x-content-type-options", "nosniff"),
        # Another site hears only this origin, never the path and the grant in
        # it. Some embeds need that much: YouTube's refuses to play without it.
        ("referrer-policy", "strict-origin-when-cross-origin"),
        ("x-robots-tag", "noindex"),
        ("service-worker-allowed", _SERVICE_WORKER_ALLOWED),
        # A sandboxed document's origin is opaque, so its fetch() and module
        # scripts reach its own files cross-origin, as Origin: null; and the
        # app reads a version it cannot frame (a PDF, a CSV) from the
        # artifact's host, another origin. The grant in the URL is the whole
        # capability and no credential rides along, so opening the read to
        # any origin grants nothing new.
        ("access-control-allow-origin", "*"),
    ]
    return headers


async def _owner_file(
    grant: str, seq: int, path: str, host_artifact_id: str | None
) -> StoredVersionFile | None:
    """The file under an owner's grant, which opens any version."""
    try:
        artifact_id = await verify_artifact_grant(grant)
    except ArtifactGrantError as e:
        logger.info("Refusing artifact request: %s", e)
        return None
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(f"artifact grant key unreadable: {e}") from e
    if host_artifact_id is not None and artifact_id != host_artifact_id:
        logger.info("Refusing artifact request: grant is for another artifact")
        return None
    try:
        return await get_version_file(artifact_id, seq, path)
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(f"artifact file lookup failed: {e}") from e


async def _visitor_file(
    grant: str, seq: int, path: str, host_artifact_id: str | None
) -> StoredVersionFile | None:
    """The file under a visitor's grant: only the version it names, and only
    while the link's row still opens that version to anyone."""
    try:
        shared = await verify_artifact_share_grant(grant)
    except ArtifactGrantError as e:
        logger.info("Refusing shared artifact request: %s", e)
        return None
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(
            f"artifact share grant key unreadable: {e}"
        ) from e
    if host_artifact_id is not None and shared.artifact_id != host_artifact_id:
        logger.info("Refusing shared artifact request: grant is for another artifact")
        return None
    if seq != shared.seq:
        logger.info("Refusing shared artifact request: grant is for another version")
        return None
    try:
        return await get_shared_version_file(
            code=shared.code,
            artifact_id=shared.artifact_id,
            revision=shared.revision,
            seq=shared.seq,
            path=path,
        )
    except psycopg.Error as e:
        raise ArtifactFileUnavailable(
            f"shared artifact file lookup failed: {e}"
        ) from e


async def load_artifact_file(
    grant: str,
    seq: str,
    path: str,
    *,
    host_artifact_id: str | None = None,
    sandboxed: bool = False,
    frame_ancestors: str | None = None,
    if_none_match: str | None = None,
    head: bool = False,
) -> ServedArtifactFile | None:
    """The file ``v/<seq>/<path>`` names under a grant, or None for the 404.

    On an artifact's own host the grant must be for that artifact, or one
    artifact's grant would run another's files on this origin, with its
    storage. On the API origin the grant alone names the artifact. A visitor's
    grant is checked against its link as well (``_visitor_file``). A bad
    grant, another artifact's, an unshared link, a version no longer the
    latest, a missing version and a missing file all answer None, so the
    response never says which. Raises ``ArtifactFileUnavailable`` when a read
    fails, which a caller answers with a 503. A file the browser already
    holds answers 304 before its bytes are read, and only once the grant has
    passed every check. A ``head`` answer has GET's headers and no body, and
    reads no bytes either.
    """
    if _SEQ.fullmatch(seq) is None or not _valid_path(path):
        return None
    visitor = is_share_grant(grant)
    load = _visitor_file if visitor else _owner_file
    stored = await load(grant, int(seq), path, host_artifact_id)
    if stored is None:
        return None
    content_type = _content_type(stored.content_type)
    bridged = _is_html(content_type) and _charset_is_utf8(content_type)
    bridge = bridge_for("view")
    etag = f"{stored.sha256}-{bridge.digest}" if bridged else stored.sha256
    if visitor:
        cache_control = _SHARED_CACHE_CONTROL
    else:
        cache_control = _DOCUMENT_CACHE_CONTROL if bridged else _CACHE_CONTROL
    headers = _headers(
        content_type,
        etag,
        cache_control=cache_control,
        sandboxed=sandboxed,
        frame_ancestors=frame_ancestors,
    )
    if if_none_match is not None and _etag_matches(if_none_match, etag):
        return ServedArtifactFile(body=b"", headers=headers, status=304)
    if head:
        # The length sealed with the version, which serving never changes. A
        # bridged document's depends on where the bridge lands in it, so it
        # goes unsaid, as RFC 9110 lets a HEAD do (9.3.2).
        if not bridged:
            headers.append(("content-length", str(stored.byte_len)))
        return ServedArtifactFile(body=b"", headers=headers)

    body = await _version_file_bytes(stored)
    if bridged:
        body = await asyncio.to_thread(with_bridge, body, bridge)
    headers.append(("content-length", str(len(body))))
    return ServedArtifactFile(body=body, headers=headers)


async def _version_file_bytes(stored: StoredVersionFile) -> bytes:
    if stored.content is not None:
        return stored.content
    try:
        return await fetch_blob(stored.user_id, stored.sha256)
    except BlobError as e:
        raise ArtifactFileUnavailable(str(e)) from e


def _etag_matches(if_none_match: str, etag: str) -> bool:
    """Whether an If-None-Match names ``etag``, by the weak comparison it calls for."""
    for candidate in if_none_match.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if candidate.removeprefix("W/") == f'"{etag}"':
            return True
    return False


def error_page(status: int, frame_ancestors: str | None) -> ServedArtifactFile:
    """The page an error answers with: a frame a link took here shows it, and a
    fetch reads only the status."""
    title, text = _ERRORS[status]
    body = (
        '<!doctype html><html><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{title}</title></head>"
        '<body style="font-family:system-ui,sans-serif;max-width:32rem;'
        'margin:4rem auto;padding:0 1rem;color:#333">'
        f'<h1 style="font-size:1.25rem">{title}</h1><p>{text}</p></body></html>'
    ).encode()
    csp = _ERROR_CSP
    if frame_ancestors:
        # A version's page frames its own files, and one missing shows this.
        csp = f"{csp}; frame-ancestors 'self' {frame_ancestors}"
    headers = [
        *_ERROR_HEADERS,
        ("content-security-policy", csp),
        ("content-length", str(len(body))),
    ]
    if status == 405:
        headers.append(("allow", "GET, HEAD"))
    return ServedArtifactFile(body=body, headers=headers, status=status)


async def _answer(
    scope: dict[str, Any], artifact_id: str, frame_ancestors: str | None
) -> ServedArtifactFile:
    if scope["method"] not in ("GET", "HEAD"):
        return error_page(405, frame_ancestors)
    route = _ROUTE.fullmatch(scope.get("path", ""))
    if route is None:
        return error_page(404, frame_ancestors)
    grant, seq, path = route.groups()
    if_none_match = next(
        (
            v.decode("latin-1")
            for k, v in scope.get("headers", [])
            if k == b"if-none-match"
        ),
        None,
    )
    try:
        served = await load_artifact_file(
            grant,
            seq,
            path,
            host_artifact_id=artifact_id,
            frame_ancestors=frame_ancestors,
            if_none_match=if_none_match,
            head=scope["method"] == "HEAD",
        )
    except ArtifactFileUnavailable as e:
        logger.warning("Artifact %s could not be served: %s", artifact_id, e)
        return error_page(503, frame_ancestors)
    return served or error_page(404, frame_ancestors)


async def serve_artifact(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
    *,
    artifact_id: str,
    frame_ancestors: str | None,
) -> None:
    """Answer an http request to ``artifact-<artifact_id>.<base>``."""
    served = await _answer(scope, artifact_id, frame_ancestors)
    headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in served.headers]
    await send(
        {"type": "http.response.start", "status": served.status, "headers": headers}
    )
    body = b"" if scope["method"] == "HEAD" else served.body
    await send({"type": "http.response.body", "body": body})
