"""Remote plugin sources: forge-URL normalization + pinned archive download.

``git`` is not in the backend image, and a subprocess clone would re-resolve
DNS past any pre-connect check — so git sources are fetched as HTTPS archive
snapshots from the forge's archive endpoint instead. Every hop (initial and
each of at most 3 redirects) is re-pinned through ``pin_public_url`` and
byte-bounded by the pinned stream client; all four supported forges accept
``HEAD`` as the ref, so a bare repo URL means "default branch".

The one credential this module ever sends is the marketplace token, and only
to GitHub's API for the marketplace's own repository at its configured ref,
for a source the catalog lists (see ``_first_hop``).
"""

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit

# The pinned stream client below is an httpx2 client, and httpx2's exception
# tree is unrelated to httpx's — catching the wrong one lets every connect
# failure and read timeout escape as a 500 instead of a PluginFatal.
import httpx2

from src.config.env import PLUGIN_MARKETPLACE_TOKEN, PLUGIN_MARKETPLACE_URL
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

# What a direct download ends in, as opposed to a repository page.
ARCHIVE_SUFFIXES = (".zip", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar")

_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _repo_parts(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def _after_marker(
    segments: list[str], marker: tuple[str, ...]
) -> tuple[str | None, str | None]:
    """(ref, subpath) following a marker subsequence (e.g. ('tree',)).

    A forge "tree" URL addresses a directory: everything past the ref is the
    path inside the repo — a deep link straight to one plugin of a
    marketplace repo.
    """
    for i in range(len(segments) - len(marker)):
        if tuple(segments[i:i + len(marker)]) == marker:
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


def _ref(ref: str | None) -> str:
    return quote(ref or "HEAD", safe="")


@dataclass(frozen=True)
class _Forge:
    """How one forge spells a repository's tree and its archive."""

    # The path segments between the project and the ref of a tree URL.
    marker: tuple[str, ...]
    archive: Callable[[str, str | None], str]
    # GitLab nests groups, so its project runs up to the '/-/' marker rather
    # than being owner/repo.
    nested: bool = False


def _gitlab_archive(project: str, ref: str | None) -> str:
    base = (
        "https://gitlab.com/api/v4/projects/"
        f"{quote(project, safe='')}/repository/archive.tar.gz"
    )
    return f"{base}?sha={quote(ref, safe='')}" if ref else base


_FORGES = {
    "github.com": _Forge(
        ("tree",),
        lambda project, ref: (
            f"https://codeload.github.com/{project}/tar.gz/{_ref(ref)}"
        ),
    ),
    "gitlab.com": _Forge(("-", "tree"), _gitlab_archive, nested=True),
    "codeberg.org": _Forge(
        ("src", "branch"),
        lambda project, ref: (
            f"https://codeberg.org/{project}/archive/{_ref(ref)}.tar.gz"
        ),
    ),
    "bitbucket.org": _Forge(
        ("src",),
        lambda project, ref: (
            f"https://bitbucket.org/{project}/get/{_ref(ref)}.tar.gz"
        ),
    ),
}


@dataclass(frozen=True)
class ForgeRef:
    """A repository on a supported forge, and the tree a URL points into."""

    host: str
    project: tuple[str, ...]
    ref: str | None
    # Unchecked. A ``#subdir=`` fragment wins over the path form: it is the
    # chooser's pick, a path from the repository root, written onto whatever
    # folder the pasted link named.
    subdir: str | None
    # The URL names the repository and nothing past it.
    bare: bool

    @property
    def repository(self) -> str:
        return f"https://{self.host}/{'/'.join(self.project)}"

    @property
    def name(self) -> str:
        return self.project[-1]

    def archive_url(self) -> str:
        return _FORGES[self.host].archive("/".join(self.project), self.ref)

    def tree_url(self, path: str | None, ref: str) -> str:
        marker = "/".join(_FORGES[self.host].marker)
        tail = f"/{path}" if path else ""
        return f"{self.repository}/{marker}/{ref}{tail}"


@dataclass(frozen=True)
class ArchiveRef:
    """A direct https download of an archive file."""

    url: str  # without its fragment
    # What the download is called: the file without its suffix, or the
    # project when a forge serves it.
    name: str | None
    subdir: str | None  # from a ``#subdir=`` fragment; unchecked


def _project(forge: _Forge, segments: list[str]) -> tuple[str, ...] | None:
    if forge.nested:
        raw = segments[: segments.index("-")] if "-" in segments else segments
    else:
        raw = segments[:2]
    project = tuple(s.removesuffix(".git") for s in raw)
    if len(project) < 2 or not all(_SEGMENT_RE.match(s) for s in project):
        return None
    return project


def parse_forge_url(url: str) -> ForgeRef | ArchiveRef | None:
    """What an https source URL addresses, or None for anything unsupported.

    The one reading of a source URL: the archive to download, the repository
    and ref a marketplace composes its entries onto, and the name a package
    that does not name itself is given all come from here.
    """
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return None
    if parts.scheme != "https":
        return None
    host = (parts.hostname or "").lower()
    forge = _FORGES.get(host)
    segments = _repo_parts(parts.path)
    project = _project(forge, segments) if forge else None
    fragment_sub = _fragment_subdir(parts.fragment)
    if parts.path.lower().endswith(ARCHIVE_SUFFIXES):
        stem = segments[-1] if segments else ""
        for suffix in ARCHIVE_SUFFIXES:
            if stem.lower().endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        return ArchiveRef(
            url=urlunsplit(parts._replace(fragment="")),
            name=project[-1] if project else stem or None,
            subdir=fragment_sub,
        )
    if forge is None or project is None:
        return None
    rest = segments[len(project):]
    ref, subdir = _after_marker(rest, forge.marker)
    return ForgeRef(
        host=host,
        project=project,
        ref=ref,
        subdir=fragment_sub or subdir,
        bare=not rest,
    )


def compose_tree_url(
    url: str, path: str | None = None, ref: str | None = None
) -> str | None:
    """A forge "tree" URL addressing (repo, ref, subdirectory).

    The source form a marketplace's external plugin entries resolve to —
    normalize_forge_url parses it straight back. ``url`` names a repository
    and nothing past it. Returns None when the host isn't a supported forge
    or a part fails validation; the caller skips the entry rather than
    failing anything.
    """
    repo = parse_forge_url(url)
    if not isinstance(repo, ForgeRef) or not repo.bare:
        return None
    if path is not None:
        checked = split_member(path)
        if not checked:
            return None
        path = "/".join(checked)
    ref = ref or "HEAD"
    if not _SEGMENT_RE.match(ref):
        return None
    return repo.tree_url(path, ref)


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
    fragment (the fragment wins). Raises ValueError for anything else — unknown
    hosts are refused rather than fetched blind.
    """
    if urlsplit(url.strip()).scheme != "https":
        raise ValueError("plugin source must be an https URL")
    source = parse_forge_url(url)
    if isinstance(source, ArchiveRef):
        return source.url, _check_subdir(source.subdir)
    if isinstance(source, ForgeRef):
        return source.archive_url(), _check_subdir(source.subdir)
    raise ValueError(
        "unsupported plugin source; use a github.com / gitlab.com / "
        "codeberg.org / bitbucket.org repository URL or a direct https "
        "archive URL"
    )


@dataclass(frozen=True)
class PackageSite:
    """Where a source URL's package lives, apart from the revision it names.

    Two URLs at one ``place`` are one package, whatever commit each pins and
    however each spells its folder: a catalog that re-pins an entry moves the
    version, not the package, and an install from the chooser's ``#subdir=``
    is the same package as one from the tree path.
    """

    place: tuple[str, str | None]  # repository or archive URL, and folder
    ref: str | None  # None for the default branch and for a direct archive


def package_site(url: object) -> PackageSite | None:
    """The site a source URL fetches from, or None for one install refuses."""
    if not isinstance(url, str):
        return None
    source = parse_forge_url(url)
    try:
        if isinstance(source, ForgeRef):
            # The supported forges resolve owner and project case-blind.
            return PackageSite(
                (source.repository.lower(), _check_subdir(source.subdir)),
                None if source.ref in (None, "HEAD") else source.ref,
            )
        if isinstance(source, ArchiveRef):
            return PackageSite((source.url, _check_subdir(source.subdir)), None)
    except ValueError:
        pass
    return None


def forge_repository(url: str) -> tuple[str, str | None] | None:
    """(repository URL, ref or None) for a forge URL, or None.

    The inputs compose_tree_url takes, read back off any URL
    normalize_forge_url accepts: a marketplace that pins a branch has its
    in-repo entries composed onto that same repository and branch. None for a
    direct archive URL (there is no repository to name) and for anything
    normalize_forge_url refuses.
    """
    source = parse_forge_url(url)
    if not isinstance(source, ForgeRef):
        return None
    return source.repository, source.ref


def _catalog_tree(url: str) -> tuple[str, str | None] | None:
    """(repository, ref) when ``url`` is in the marketplace's own github.com
    repository at the ref the catalog is read from, with a token to read it.

    The one tree the token is for: its in-repo entries are composed onto that
    ref. Any signed-in user can name a source URL, so matching the repository
    alone would read every other branch and commit of a private catalog on
    the deployment's token.
    """
    if not PLUGIN_MARKETPLACE_TOKEN.strip():
        return None
    home = forge_repository(PLUGIN_MARKETPLACE_URL)
    source = forge_repository(url)
    if home is None or source is None:
        return None
    repo, ref = source
    if not repo.lower().startswith("https://github.com/"):
        return None
    if repo.lower() != home[0].lower() or (ref or "HEAD") != (home[1] or "HEAD"):
        return None
    return source


def in_catalog_tree(url: str) -> bool:
    return _catalog_tree(url) is not None


def _first_hop(
    url: str, archive: str, listed: bool
) -> tuple[str, dict[str, str] | None]:
    """Where the first request goes, and the headers only it may carry.

    A team can point Discover at its own private catalog, and codeload serves
    a private repository to nobody. A source the catalog lists (``listed``,
    the caller's to establish) in the catalog's own tree is fetched through
    the API's tarball endpoint instead, which answers with a redirect to a
    codeload URL carrying GitHub's own short-lived grant. The redirect loop
    follows that without the header, so the token reaches api.github.com
    once. The tarball is the whole repository, so a folder the catalog does
    not list (a draft, a package held back) is never fetched with it.
    """
    tree = _catalog_tree(url) if listed else None
    if tree is None:
        return archive, None
    repo, ref = tree
    return (
        f"https://api.github.com/repos{urlsplit(repo).path}/tarball/"
        f"{quote(ref or 'HEAD', safe='')}"
    ), {
        "Authorization": f"Bearer {PLUGIN_MARKETPLACE_TOKEN.strip()}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def fetch_plugin_source(
    url: str, *, listed: bool = False
) -> tuple[bytes, str | None]:
    """Download a plugin package from a normalized source URL.

    Returns (bytes, subdirectory the URL deep-links to, or None). ``listed``
    says the catalog itself, or one of its entries, is being fetched, which
    is what lets a private catalog's token go out (``_first_hop``). Raises
    PluginFatal with a user-facing message on every failure class — bad URL,
    SSRF refusal, redirect chain, oversize, timeout, HTTP error.
    """
    try:
        normalized, subdir = normalize_forge_url(url)
    except ValueError as e:
        raise PluginFatal(str(e)) from e

    current, hop_headers = _first_hop(url, normalized, listed)
    try:
        async with asyncio.timeout(FETCH_DEADLINE_SECONDS):
            for _hop in range(_MAX_REDIRECTS + 1):
                target = await pin_public_url(current)
                async with pinned_stream_client(
                    target, max_bytes=MAX_PACKAGE_BYTES
                ) as client:
                    if hop_headers:
                        response = await client.get(current, headers=hop_headers)
                        # Hop 0 only: whatever the answer, no later request
                        # (a redirect included) carries the credential.
                        hop_headers = None
                    else:
                        response = await client.get(current)
                    if response.status_code in (301, 302, 303, 307, 308):
                        location = response.headers.get("location")
                        if not location:
                            raise PluginFatal(
                                "source answered a redirect without a location"
                            )
                        try:
                            current = urljoin(current, location)
                        except ValueError as e:
                            # The upstream wrote this header, so it can be
                            # unparseable (``http://[bad`` raises here). A
                            # ValueError is not PluginFatal, and the endpoint's
                            # generic handler would answer 409, telling the user
                            # their package conflicts when the source is simply
                            # broken.
                            raise PluginFatal(
                                f"source returned an invalid redirect "
                                f"location: {e}"
                            ) from e
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
