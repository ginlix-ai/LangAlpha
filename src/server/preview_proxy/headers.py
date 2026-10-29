"""What crosses the preview proxy in each direction, header by header.

Pure functions over raw header pairs, so the policy is one readable place and
the proxy only applies it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

RawHeaders = Iterable[tuple[bytes, bytes]]

# RFC 9110 hop-by-hop fields, plus the legacy ones clients still send.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)
# Who the client is and which edge it came through is nothing a sandbox's code
# should learn, and a forwarded scheme or host would describe the proxy's
# origin to an app that is answering on the sandbox's. E2B's gateway routes
# on e2b-* fields of its own (the SDK names a sandbox and port with them),
# which a page must not steer.
_REQUEST_DROPPED = frozenset(
    {"host", "forwarded", "x-real-ip", "true-client-ip", "cdn-loop"}
)
_REQUEST_DROPPED_PREFIXES = ("x-forwarded-", "cf-", "e2b-")
# The handshake fields a WebSocket client library writes itself.
_WEBSOCKET_OWNED_PREFIXES = ("sec-websocket-",)
# Response fields the proxy sets itself, that the server in front writes its
# own of (uvicorn adds date and server to every response, so passing the
# app's doubles them), or that describe the sandbox's host rather than the
# preview's.
_RESPONSE_DROPPED = frozenset(
    {"alt-svc", "date", "referrer-policy", "server", "x-robots-tag"}
)
_RESPONSE_DROPPED_PREFIXES = ("e2b-",)


def _connection_listed(headers: list[tuple[str, str]]) -> set[str]:
    listed: set[str] = set()
    for name, value in headers:
        if name == "connection":
            listed.update(t.strip().lower() for t in value.split(",") if t.strip())
    return listed


def _decoded(raw: RawHeaders) -> list[tuple[str, str]]:
    return [(k.decode("latin-1").lower(), v.decode("latin-1")) for k, v in raw]


def _rebase(value: str, origin: str, onto: str) -> str | None:
    """*value* moved from *origin* onto *onto*, or None when it is elsewhere."""
    if value.lower() == origin.lower():
        return onto
    if value.lower().startswith(origin.lower()) and value[len(origin)] in "/?#":
        return onto + value[len(origin) :]
    return None


def request_headers(
    raw: RawHeaders,
    *,
    capability_origin: str,
    upstream_origin: str,
    inject: Mapping[str, str],
    websocket: bool = False,
) -> list[tuple[str, str]]:
    """The headers sent to the sandbox for a browser request.

    ``Authorization`` and ``Cookie`` pass: a browser attaches them only for the
    preview's own host, which the boot check keeps off the app's site, so they
    are the preview app's own. ``Origin`` and ``Referer`` naming the capability
    origin are moved onto the sandbox's, since an app that checks Origin
    against Host (Tornado's WebSocket check, for one) would otherwise refuse
    its own page; a foreign Origin passes untouched, so the app's own
    cross-site check still sees it. A header the proxy injects replaces any
    copy the browser sent.
    """
    headers = _decoded(raw)
    listed = _connection_listed(headers)
    injected = {name.lower() for name in inject}
    out: list[tuple[str, str]] = []
    for name, value in headers:
        if (
            name in _HOP_BY_HOP
            or name in listed
            or name in injected
            or name in _REQUEST_DROPPED
            or name.startswith(_REQUEST_DROPPED_PREFIXES)
            or (websocket and name.startswith(_WEBSOCKET_OWNED_PREFIXES))
        ):
            continue
        if name in ("origin", "referer"):
            value = _rebase(value, capability_origin, upstream_origin) or value
        out.append((name, value))
    out.extend((name.lower(), value) for name, value in inject.items())
    return out


def rewrite_location(value: str, *, upstream_origin: str, capability_origin: str) -> str:
    """A redirect at the sandbox's own origin, pointed back through the proxy."""
    moved = _rebase(value, upstream_origin, capability_origin)
    if moved is not None:
        return moved
    host = upstream_origin.split("://", 1)[-1]
    moved = _rebase(value, "//" + host, capability_origin)
    return moved if moved is not None else value


def _host_only_cookie(value: str) -> str:
    """A Set-Cookie without ``Domain``, so it stays on this one preview host."""
    first, *attributes = value.split(";")
    kept = [
        a for a in attributes if a.split("=", 1)[0].strip().lower() != "domain"
    ]
    return ";".join([first, *kept])


def _private_cache(value: str) -> str:
    """Cache-Control that no shared cache may keep past a revocation."""
    directives = [d.strip() for d in value.split(",") if d.strip()]
    kept = [
        d
        for d in directives
        if d.split("=", 1)[0].strip().lower() not in ("public", "s-maxage")
    ]
    names = {d.split("=", 1)[0].strip().lower() for d in kept}
    if "private" not in names and "no-store" not in names:
        kept.insert(0, "private")
    return ", ".join(kept)


def response_headers(
    raw: RawHeaders,
    *,
    upstream_origin: str,
    capability_origin: str,
    frame_ancestors: str | None = None,
    withheld: Iterable[str] = (),
) -> list[tuple[bytes, bytes]]:
    """The headers a browser gets back from the sandbox.

    The referrer is withheld outright: the capability is the hostname, so even
    an origin-only referrer would hand it to every CDN a preview loads from.
    So is any field carrying a *withheld* value (the header the proxy added),
    which an app that reflects its request headers would otherwise return.
    """
    headers = _decoded(raw)
    listed = _connection_listed(headers)
    secrets = [v for v in withheld if v]
    out: list[tuple[str, str]] = []
    cache_control = False
    for name, value in headers:
        if (
            name in _HOP_BY_HOP
            or name in listed
            or name in _RESPONSE_DROPPED
            or name.startswith(_RESPONSE_DROPPED_PREFIXES)
            or any(secret in value for secret in secrets)
        ):
            continue
        if name in ("location", "content-location"):
            value = rewrite_location(
                value,
                upstream_origin=upstream_origin,
                capability_origin=capability_origin,
            )
        elif name == "set-cookie":
            value = _host_only_cookie(value)
        elif name == "cache-control":
            value = _private_cache(value)
            cache_control = True
        out.append((name, value))
    if not cache_control:
        out.append(("cache-control", "private"))
    out.append(("referrer-policy", "no-referrer"))
    out.append(("x-robots-tag", "noindex"))
    if frame_ancestors:
        out.append(("content-security-policy", f"frame-ancestors {frame_ancestors}"))
    return [(k.encode("latin-1"), v.encode("latin-1")) for k, v in out]
