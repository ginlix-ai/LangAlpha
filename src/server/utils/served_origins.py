"""Where a browser loads this server's grant-in-path routes from, as CSP sources.

A sandboxed document on the API origin has an opaque origin, and ``'self'``
there names every API route. Its connect-src names its own route prefix
instead, and a source with a path needs an absolute origin. A request's Host
cannot supply one: behind a proxy it can be a name no browser uses. So the
origins come from configuration, and a deployment that configures none gets
no source, which fails closed.
"""

from __future__ import annotations

from urllib.parse import quote, urlsplit

from src.config.env import PDF_RENDER_INTERNAL_BASE, PUBLIC_APP_URL, SERVER_BASE_URL

__all__ = ["served_sources"]


def _origin(url: str) -> str | None:
    """``scheme://host[:port]`` of an http(s) URL, as a CSP source spells it.

    None for anything a source cannot name, an IPv6 literal included.
    """
    parts = urlsplit((url or "").strip())
    try:
        port = parts.port
    except ValueError:
        return None
    host = parts.hostname
    if parts.scheme not in ("http", "https") or not host or ":" in host:
        return None
    if port is None or port == (443 if parts.scheme == "https" else 80):
        return f"{parts.scheme}://{host}"
    return f"{parts.scheme}://{host}:{port}"


def _served_origins(host: str | None) -> list[str]:
    """The origins a browser loads these routes from.

    The web app frames a document on the API's public origin, or on its own
    when it calls the API there, so both are named. The PDF renderer loads
    one through ``PDF_RENDER_INTERNAL_BASE``, named only for a request
    addressed to it: Host picks among these and never supplies one.
    """
    origins = [_origin(SERVER_BASE_URL), _origin(PUBLIC_APP_URL)]
    renderer = _origin(PDF_RENDER_INTERNAL_BASE)
    if renderer and (host or "").strip().lower() == urlsplit(renderer).netloc:
        origins.append(renderer)
    return list(dict.fromkeys(o for o in origins if o))


def served_sources(prefix: str, host: str | None = None) -> list[str]:
    """``prefix`` on each served origin; empty when none is configured.

    ``prefix`` ends in ``/``, so each source admits that route prefix and
    every path below it, and nothing else on the origin.
    """
    path = quote(prefix, safe="/")
    return [f"{origin}{path}" for origin in _served_origins(host)]
