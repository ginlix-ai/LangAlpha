"""Where a plugin's mark comes from, when nothing we ship draws it.

Two readers share these rules: a Discover entry, whose fields a catalog wrote,
and an installed plugin, whose manifest its author wrote. Each answers with a
source for the brand resolver, which the listing publishes as a handle on this
origin, so the browser is never told a third-party host.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from src.server.services.brand_icons import MAX_SOURCE_CHARS, inline_icon
from src.server.services.icon_urls import icon_url, source_icon_url
from src.server.services.plugins.extension import NAMESPACE
from src.server.services.plugins.paths import split_member

# A bare domain and nothing else. The resolver reads anything with a scheme as
# a URL to fetch, so a scheme, a path, a port or an IP literal is refused here
# rather than interpreted.
_SITE_RE = re.compile(
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})"
)
_MAX_URL_CHARS = 2048
_GITHUB_HOSTS = ("github.com", "www.github.com")
_GITHUB_LOGIN_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})")
# github.com path roots that are pages rather than accounts. `orgs/` and
# `users/` prefix an account and are stepped over instead.
_GITHUB_PAGES = frozenset({"apps", "collections", "marketplace", "sponsors", "topics"})


def site(value: Any) -> str | None:
    """``value`` as a bare domain, or None when it is anything else."""
    if not isinstance(value, str) or len(value) > 253:
        return None
    host = value.lower()
    return host if _SITE_RE.fullmatch(host) else None


def _host(url: Any) -> str | None:
    if not isinstance(url, str):
        return None
    try:
        return urlsplit(url).hostname
    except ValueError:
        return None


def icon_value(value: Any) -> str | None:
    """A declared ``icon``: a bare site, or the https address of the image.

    The address is for a brand whose mark no site serves as its icon, such as
    a library whose logo lives only in its repository.
    """
    if (host := site(value)) is not None:
        return host
    if (
        isinstance(value, str)
        and len(value) <= _MAX_URL_CHARS
        and value.startswith("https://")
        and not any(c.isspace() for c in value)
        and site(_host(value)) is not None
    ):
        return value
    return None


def vendored_icon(
    value: Any, files: Mapping[str, bytes], base: str = ""
) -> str | None:
    """The image a ``./`` icon names among ``files``, as a ``data:`` URI.

    A catalog can carry a brand's mark beside the plugin it vendors, where it
    outlives the vendor's own site or account. ``base`` is the directory the
    path is relative to, inside ``files``.
    """
    if not isinstance(value, str) or not value.startswith("./"):
        return None
    segments = split_member(f"{base}/{value}" if base else value)
    data = files.get("/".join(segments)) if segments else None
    return None if data is None else inline_icon(data)


def _installed_icon(value: Any) -> str | None:
    """A declared icon as an installed manifest holds it.

    Install turns a ``./`` file into a ``data:`` URI, which the resolver
    checks again before it draws anything.
    """
    if (
        isinstance(value, str)
        and value.startswith("data:image/")
        and len(value) <= MAX_SOURCE_CHARS
    ):
        return value
    return icon_value(value)


def github_avatar(url: Any) -> str | None:
    """The avatar of the GitHub account ``url`` belongs to, if it is one.

    GitHub's own icon is the same octocat for every repository there, so it
    says nothing about whose work this is. The account's avatar is the mark
    its author chose, where a monogram tells rows apart only by first letter.
    """
    if _host(url) not in _GITHUB_HOSTS:
        return None
    segments = [s for s in urlsplit(url).path.split("/") if s]
    if segments[:1] in (["orgs"], ["users"]):
        segments = segments[1:]
    if not segments or segments[0].lower() in _GITHUB_PAGES:
        return None
    login = segments[0]
    if not _GITHUB_LOGIN_RE.fullmatch(login):
        return None
    # By login through github.com, which redirects to the account's avatar.
    # The avatar host's own by-login path answers an organization with the
    # default octocat.
    return f"https://github.com/{login}.png?size=128"


def _page_mark(url: Any) -> str | None:
    """The mark of the page ``url`` names: its account on GitHub, else its site."""
    return github_avatar(url) or site(_host(url))


def plugin_icon_source(
    manifest: Any, source_ref: str | None, server_key: str | None = None
) -> str | None:
    """The mark an installed plugin, or one of its servers, draws.

    What the author declared comes first: the server's own ``icon``, then the
    plugin's. Then its homepage, then its author's link, each the author
    saying where the plugin lives. The source it was fetched from comes last
    and only as a GitHub account, because a catalog that vendors a plugin is
    its source without being its author, and another git host's icon is that
    host's brand, not the plugin's.
    """
    if not isinstance(manifest, dict):
        return None
    extensions = manifest.get("extensions")
    namespace = extensions.get(NAMESPACE) if isinstance(extensions, dict) else None
    if not isinstance(namespace, dict):
        namespace = {}
    servers = namespace.get("servers")
    server = servers.get(server_key) if isinstance(servers, dict) and server_key else None
    author = manifest.get("author")
    return (
        _installed_icon(server.get("icon") if isinstance(server, dict) else None)
        or _installed_icon(namespace.get("icon"))
        or _page_mark(manifest.get("homepage"))
        or _page_mark(author.get("url") if isinstance(author, dict) else None)
        or github_avatar(source_ref)
    )


async def plugin_mark(plugin: Mapping[str, Any]) -> str | None:
    """This origin's URL for an installed plugin's own mark, if it has one."""
    return await source_icon_url(
        plugin_icon_source(plugin.get("manifest"), plugin.get("source_ref"))
    )


async def component_mark(server_info: dict | None, source: str | None) -> str | None:
    """This origin's URL for a server's mark: what its handshake names, else
    ``source``, the mark the package that ships it declares for it."""
    return await icon_url(server_info) or await source_icon_url(source)
