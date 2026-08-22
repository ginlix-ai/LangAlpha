"""Remote plugin sources: forge-URL normalization + pinned archive download.

``git`` is not in the backend image, and a subprocess clone would re-resolve
DNS past any pre-connect check — so git sources are fetched as HTTPS archive
snapshots from the forge's archive endpoint instead. Every hop (initial and
each of at most 3 redirects) is re-pinned through ``pin_public_url`` and
byte-bounded by the pinned stream client; all four supported forges accept
``HEAD`` as the ref, so a bare repo URL means "default branch".
"""

import asyncio
import re
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit

# The pinned stream client below is an httpx2 client, and httpx2's exception
# tree is unrelated to httpx's — catching the wrong one lets every connect
# failure and read timeout escape as a 500 instead of a PluginFatal.
import httpx2

from src.server.services.mcp_oauth.http import (
    OAuthHopBlocked,
    pinned_stream_client,
)
from src.server.services.plugins.errors import PluginFatal
from src.server.services.plugins.paths import split_member
from src.server.utils.egress_guard import EgressBlockedError, pin_public_url

# Wire cap on a plugin package (upload and remote fetch alike); the archive
# layer separately caps the uncompressed tree. Sized for marketplace repos,
# not just single plugins — openai/plugins alone tars to ~21 MiB.
MAX_PACKAGE_BYTES = 64 * 1024 * 1024
FETCH_DEADLINE_SECONDS = 30.0
_MAX_REDIRECTS = 3

_ARCHIVE_SUFFIXES = (".zip", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar")

_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _repo_parts(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def _after_marker(
    segments: list[str], marker: list[str]
) -> tuple[str | None, str | None]:
    """(ref, subpath) following a marker subsequence (e.g. ['tree']).

    A forge "tree" URL addresses a directory: everything past the ref is the
    path inside the repo — a deep link straight to one plugin of a
    marketplace repo.
    """
    for i in range(len(segments) - len(marker)):
        if segments[i:i + len(marker)] == marker:
            rest = segments[i + len(marker):]
            if not rest:
                return None, None
            return rest[0], "/".join(rest[1:]) or None
    return None, None


def _check_subdir(subdir: str | None) -> str | None:
    if subdir is None:
        return None
    segments = split_member(subdir)
    if not segments:
        raise ValueError(f"invalid plugin subdirectory {subdir!r}")
    return "/".join(segments)


def _fragment_subdir(fragment: str) -> str | None:
    """``#subdir=<path>`` — the deep-link form that works on every source
    shape, including direct archive URLs; compose_subdir_url emits it."""
    if fragment.startswith("subdir="):
        return unquote(fragment[len("subdir="):]) or None
    return None


def compose_tree_url(
    url: str, path: str | None = None, ref: str | None = None
) -> str | None:
    """A forge "tree" URL addressing (repo, ref, subdirectory).

    The source form a marketplace's external plugin entries resolve to —
    normalize_forge_url parses it straight back. Returns None when the host
    isn't a supported forge or a part fails validation; the caller skips the
    entry rather than failing anything.
    """
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if parts.scheme != "https":
        return None
    host = (parts.hostname or "").lower()
    segments = [s.removesuffix(".git") for s in _repo_parts(parts.path)]
    if not segments or not all(_SEGMENT_RE.match(s) for s in segments):
        return None
    if path is not None:
        checked = split_member(path)
        if not checked:
            return None
        path = "/".join(checked)
    ref = ref or "HEAD"
    if not _SEGMENT_RE.match(ref):
        return None
    tail = f"/{path}" if path else ""
    if host == "github.com" and len(segments) == 2:
        return (
            f"https://github.com/{segments[0]}/{segments[1]}/tree/{ref}{tail}"
        )
    if host == "gitlab.com" and len(segments) >= 2:
        return f"https://gitlab.com/{'/'.join(segments)}/-/tree/{ref}{tail}"
    if host == "codeberg.org" and len(segments) == 2:
        return (
            f"https://codeberg.org/{segments[0]}/{segments[1]}"
            f"/src/branch/{ref}{tail}"
        )
    if host == "bitbucket.org" and len(segments) == 2:
        return (
            f"https://bitbucket.org/{segments[0]}/{segments[1]}"
            f"/src/{ref}{tail}"
        )
    return None


def compose_subdir_url(url: str, subdir: str) -> str:
    """The source_ref for a picker install: the pasted URL, deep-linked.

    Update re-fetches this and lands on the same plugin without asking
    again.
    """
    subdir = _check_subdir(subdir)
    parts = urlsplit(url.strip())
    return urlunsplit(parts._replace(fragment=f"subdir={quote(subdir)}"))


def normalize_forge_url(url: str) -> tuple[str, str | None]:
    """Turn a repo/homepage URL into (archive URL, subdirectory or None).

    Supported: github.com, gitlab.com (nested groups included), codeberg.org,
    bitbucket.org, and any https URL already pointing at an archive file.
    The subdirectory comes from a forge "tree"-style path or a ``#subdir=``
    fragment (path form wins). Raises ValueError for anything else — unknown
    hosts are refused rather than fetched blind.
    """
    parts = urlsplit(url.strip())
    if parts.scheme != "https":
        raise ValueError("plugin source must be an https URL")
    host = (parts.hostname or "").lower()
    path = parts.path
    fragment_sub = _fragment_subdir(parts.fragment)

    if path.lower().endswith(_ARCHIVE_SUFFIXES):
        bare = urlunsplit(parts._replace(fragment=""))
        return bare, _check_subdir(fragment_sub)

    segments = _repo_parts(path)
    if host == "github.com" and len(segments) >= 2:
        owner, repo = segments[0], segments[1].removesuffix(".git")
        ref, subdir = _after_marker(segments, ["tree"])
        if _SEGMENT_RE.match(owner) and _SEGMENT_RE.match(repo):
            return (
                f"https://codeload.github.com/{owner}/{repo}/tar.gz/"
                f"{quote(ref or 'HEAD', safe='')}"
            ), _check_subdir(subdir or fragment_sub)
    if host == "gitlab.com" and len(segments) >= 2:
        # Nested groups: the project path is everything before a '/-/' marker.
        project = segments[: segments.index("-")] if "-" in segments else segments
        project = [s.removesuffix(".git") for s in project]
        ref, subdir = _after_marker(segments, ["-", "tree"])
        if all(_SEGMENT_RE.match(s) for s in project):
            base = (
                "https://gitlab.com/api/v4/projects/"
                f"{quote('/'.join(project), safe='')}/repository/archive.tar.gz"
            )
            archive = f"{base}?sha={quote(ref, safe='')}" if ref else base
            return archive, _check_subdir(subdir or fragment_sub)
    if host in ("codeberg.org", "bitbucket.org") and len(segments) >= 2:
        owner, repo = segments[0], segments[1].removesuffix(".git")
        if _SEGMENT_RE.match(owner) and _SEGMENT_RE.match(repo):
            if host == "codeberg.org":
                ref, subdir = _after_marker(segments, ["src", "branch"])
                return (
                    f"https://codeberg.org/{owner}/{repo}/archive/"
                    f"{quote(ref or 'HEAD', safe='')}.tar.gz"
                ), _check_subdir(subdir or fragment_sub)
            ref, subdir = _after_marker(segments, ["src"])
            return (
                f"https://bitbucket.org/{owner}/{repo}/get/"
                f"{quote(ref or 'HEAD', safe='')}.tar.gz"
            ), _check_subdir(subdir or fragment_sub)
    raise ValueError(
        "unsupported plugin source; use a github.com / gitlab.com / "
        "codeberg.org / bitbucket.org repository URL or a direct https "
        "archive URL"
    )


async def fetch_plugin_source(url: str) -> tuple[bytes, str | None]:
    """Download a plugin package from a normalized source URL.

    Returns (bytes, subdirectory the URL deep-links to, or None). Raises
    PluginFatal with a user-facing message on every failure class — bad URL,
    SSRF refusal, redirect chain, oversize, timeout, HTTP error.
    """
    try:
        normalized, subdir = normalize_forge_url(url)
    except ValueError as e:
        raise PluginFatal(str(e)) from e

    current = normalized
    try:
        async with asyncio.timeout(FETCH_DEADLINE_SECONDS):
            for _hop in range(_MAX_REDIRECTS + 1):
                target = await pin_public_url(current)
                async with pinned_stream_client(
                    target, max_bytes=MAX_PACKAGE_BYTES
                ) as client:
                    response = await client.get(current)
                    if response.status_code in (301, 302, 303, 307, 308):
                        location = response.headers.get("location")
                        if not location:
                            raise PluginFatal(
                                "source answered a redirect without a location"
                            )
                        current = urljoin(current, location)
                        continue
                    if response.status_code != 200:
                        raise PluginFatal(
                            f"source answered HTTP {response.status_code}"
                        )
                    return response.content, subdir
            raise PluginFatal("source redirected more than "
                              f"{_MAX_REDIRECTS} times")
    except TimeoutError:
        raise PluginFatal(
            f"fetching the plugin source took longer than "
            f"{int(FETCH_DEADLINE_SECONDS)}s"
        ) from None
    except (EgressBlockedError, OAuthHopBlocked) as e:
        raise PluginFatal(f"plugin source refused: {e}") from e
    except (httpx2.HTTPError, OSError) as e:
        raise PluginFatal(f"could not reach the plugin source: {e}") from e
