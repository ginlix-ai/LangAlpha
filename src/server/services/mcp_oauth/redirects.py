"""Where the OAuth callback may send the browser, and how that URL is built.

Every user-visible outcome of the callback is a redirect, so the allowlisting
lives in one place: a same-app relative path, optionally prefixed by an origin
this deployment has vouched for. :func:`redirect_to` re-sanitizes both halves
on the way out, which is what makes it impossible for any path through the
callback to emit an unvetted redirect.
"""

from __future__ import annotations

from enum import StrEnum
from ipaddress import ip_address
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from src.config.env import SERVER_BASE_URL

DEFAULT_RETURN_TO = "/plugins"


class CallbackError(StrEnum):
    """Every way a connector callback can end badly, as the browser is told it.

    A closed set because the value leaves this process twice: into the redirect
    the browser follows, where the web app turns it into a sentence, and into
    the logs, where it is what an operator greps for. As bare literals at the
    call sites these were free to become a typo or a synonym, and both read
    downstream as an unknown reason with a generic apology attached.
    """

    MISSING_STATE = "missing_state"
    INVALID_STATE = "invalid_state"
    STATE_MISMATCH = "state_mismatch"
    DENIED = "denied"
    PROVIDER_ERROR = "provider_error"
    MISSING_CODE = "missing_code"
    ISSUER_MISMATCH = "issuer_mismatch"
    BLOCKED_ENDPOINT = "blocked_endpoint"
    TOKEN_EXCHANGE_FAILED = "token_exchange_failed"
    SERVER_CHANGED = "server_changed"
    INTERNAL = "internal"

# Below this a listener needs root on a POSIX box, so a redirect naming one is
# not a desktop app asking for its own port.
_MIN_LOOPBACK_PORT = 1024


def callback_uri() -> str:
    return f"{SERVER_BASE_URL.rstrip('/')}/api/v1/mcp/oauth/callback"


def _host_is_loopback(host: str | None) -> bool:
    host = (host or "").lower()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def _server_origin() -> str:
    parts = urlsplit(SERVER_BASE_URL)
    return f"{parts.scheme}://{parts.netloc}" if parts.scheme and parts.netloc else ""


def callback_is_loopback() -> bool:
    """Whether the browser-nonce binding can be honored at all here.

    The cookie only returns if the origin the user browses shares a *host* with
    this callback (cookies ignore port, not host). A loopback callback means a
    local dev box, where the UI is routinely served from some other host — a
    ``*.localhost`` dev proxy — while an AS will only accept a loopback literal
    for an ``http`` redirect_uri. Those two demands can't both be met, so
    requiring the cookie there rejects every connect. A deployed instance never
    has a loopback callback, so the binding stays mandatory in production.
    """
    return _host_is_loopback(urlsplit(SERVER_BASE_URL).hostname or "")


# The one path a desktop shell offers, spelled `MCP_CALLBACK_PATH` in
# desktop/src/oauth.js. A shell that ever names another is refused here and the
# flow falls back to the hosted callback, which is the safe direction to fail.
LOOPBACK_CALLBACK_PATH = "/mcp/callback"


def sanitize_loopback_redirect(value: str | None) -> str:
    """A native-app loopback redirect_uri (RFC 8252 §7.3), or "".

    The callback is otherwise :func:`callback_uri`, built from this deployment's
    own base URL and so underivable from anything a caller sends. This is the
    one place a caller names it instead, because some authorization servers
    allowlist only the native-app profile and refuse a hosted callback outright;
    a desktop shell holding a listener can complete those, and hands the code
    straight back to this deployment's own callback.

    The bound is what replaces the underivability: a loopback target can only
    ever deliver a code to the machine whose browser is already running the
    flow, so a forged value cannot name somewhere an attacker can read. There is
    deliberately no check that the caller *is* a desktop shell — nothing on the
    wire could prove it — and none is needed once the target is bounded.

    An IP literal, never ``localhost``: that name resolves through whatever the
    machine's resolver says, and an AS matching its allowlist as a string
    rejects it besides. The path is pinned for the same reason the host is
    bounded: leaving it free let a caller name any listener on the machine, and
    the only value a shell ever offers is the one below.
    """
    if not value:
        return ""
    parts = urlsplit(value)
    if parts.scheme != "http" or "@" in parts.netloc or parts.query or parts.fragment:
        return ""
    try:
        host, port = parts.hostname or "", parts.port
    except ValueError:  # a non-numeric or out-of-range port raises here
        return ""
    if port is None or not (_MIN_LOOPBACK_PORT <= port <= 65535):
        return ""
    try:
        if not ip_address(host).is_loopback:
            return ""
    except ValueError:
        return ""
    if parts.path != LOOPBACK_CALLBACK_PATH:
        return ""
    # Rebuilt rather than echoed, so a mixed-case host cannot make the value
    # bound into the state record differ by a byte from the one just validated.
    netloc = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    return urlunsplit(("http", netloc, parts.path, "", ""))


def sanitize_return_to(value: str | None) -> str:
    """Allowlist: a same-app relative path only ('/x', never '//x' or '/\\x').

    A leading '/' followed by '/' or '\\' is protocol-relative once the browser
    normalizes backslashes ('/\\evil.com' → '//evil.com'), which would redirect
    off-site — so the second character must be a normal path char.
    """
    if value and value.startswith("/") and value[1:2] not in ("/", "\\"):
        return value
    return DEFAULT_RETURN_TO


def sanitize_web_origin(value: str | None) -> str:
    """A vouched-for http(s) origin (scheme://host[:port]) or "".

    Captured from the browser's Origin header on the authenticated start
    request — it is where the UI actually lives, which the callback's own
    origin is not when the frontend and API run on split dev ports. It becomes
    the prefix of the post-connect redirect, so it must never be an
    attacker-forged Origin: only a local dev host (the one case where the
    callback's origin legitimately differs from the UI's) or this deployment's
    own base URL is honored. Anything else — including a well-formed but foreign
    public origin — is dropped, and the callback falls back to a relative path
    on its own origin. Anything beyond a bare origin (path, query, userinfo,
    "null") is dropped up front.
    """
    if not value:
        return ""
    parts = urlsplit(value)
    if not (
        parts.scheme in ("http", "https")
        and parts.netloc
        and "@" not in parts.netloc
        and parts.path in ("", "/")
        and not parts.query
        and not parts.fragment
    ):
        return ""
    origin = f"{parts.scheme}://{parts.netloc}"
    if _host_is_loopback(parts.hostname) or origin == _server_origin():
        return origin
    return ""


def redirect_to(
    return_to: str | None = None, web_origin: str | None = None, /, **params: str
) -> str:
    """The callback's answer: an allowlisted target carrying outcome params.

    ``params`` merge into whatever query the return path already carries, so a
    ``return_to`` of ``/plugins?tab=oauth`` gains ``&mcp_error=…`` instead of
    a second ``?``. Encoding is ``%20``-style to match what the UI parses.
    """
    target = sanitize_web_origin(web_origin) + sanitize_return_to(return_to)
    parts = urlsplit(target)
    query = urlencode(
        parse_qsl(parts.query) + list(params.items()), quote_via=quote
    )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
