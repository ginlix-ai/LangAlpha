"""The CDN hosts agent-written HTML may load from, and the one CSP a document
of it is served under.

The CDNs are the html-report skill's allowlist. One copy, and one policy, so a
served report and a published page load the same libraries and fonts.
"""

from __future__ import annotations

from collections.abc import Sequence

SCRIPT_CDNS = (
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net "
    "https://unpkg.com https://esm.sh"
)
# Google Fonts' stylesheet host serves the CJK web fonts (Noto Sans SC/JP/KR),
# which render as tofu without it.
STYLE_CDNS = (
    "https://fonts.googleapis.com "
    "https://cdnjs.cloudflare.com https://cdn.jsdelivr.net https://unpkg.com"
)
FONT_CDNS = "https://fonts.gstatic.com https://cdnjs.cloudflare.com"

# An opaque origin for a document served on the API origin, where it would
# otherwise share the app's cookies and storage. The popup tokens let a link
# the bridge opens in a new tab load as an ordinary page: one that inherited
# the sandbox would have no cookies, and real sites' bot checks would fail.
_SANDBOX = "sandbox allow-scripts allow-popups allow-popups-to-escape-sandbox"


def html_document_csp(
    own: Sequence[str],
    *,
    published: bool,
    sandboxed: bool = True,
    frame_ancestors: str | None = None,
) -> str:
    """The policy for an agent-written document and every file it pulls in.

    ``own`` is where the document may fetch its own files, and nothing else
    there: no other API route, no other grant. Script and style allow inline
    code because reports inline theirs and the server inlines the page bridge.
    Nothing frames another document, posts a form or rebases its links.

    A published version may also fetch from the CDNs and show images from any
    https host; a live file may not. A live file is served under a prefix that
    reaches the whole workspace, or a share's subtree, so a request to another
    host is a way out for anything the document read there. A version's prefix
    reaches only that artifact's files, which its publisher chose to show.
    """
    connect = " ".join([*own, SCRIPT_CDNS] if published else own) or "'none'"
    images = "'self' data: blob: https:" if published else "'self' data: blob:"
    directives = [
        *([_SANDBOX] if sandboxed else []),
        "default-src 'none'",
        f"script-src 'self' 'unsafe-inline' {SCRIPT_CDNS}",
        f"style-src 'self' 'unsafe-inline' {STYLE_CDNS}",
        f"font-src 'self' data: {FONT_CDNS}",
        f"img-src {images}",
        "media-src 'self' data: blob:",
        f"connect-src {connect}",
        "worker-src 'self' blob:",
        "frame-src 'none'",
        "form-action 'none'",
        "base-uri 'none'",
        "object-src 'none'",
    ]
    if frame_ancestors:
        directives.append(f"frame-ancestors {frame_ancestors}")
    return "; ".join(directives)
