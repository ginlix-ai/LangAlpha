"""The one CSP agent-written HTML is served under, live and published.

It limits nothing a document loads. A report that cannot reach its chart
library, a public API or an embed looks broken to the person reading it, and
an agent made to write a hostile document already holds the computer it runs
on, with easier ways out than a web page. What a document can read is settled
elsewhere: the owner's grant opens the workspace, and a version's opens what
its publisher listed.
"""

from __future__ import annotations

# default-src covers every kind of load; `*` does not cover data: or blob:, and
# 'unsafe-eval' also admits WebAssembly. form-action and base-uri do not fall
# back to it, so leaving them out leaves forms and <base> as they are anywhere.
_LOADS = "default-src * data: blob: 'unsafe-inline' 'unsafe-eval'"

# An opaque origin for a document served on the API origin, where it would
# otherwise share the app's cookies and storage. The popup tokens let a link
# the bridge opens in a new tab load as an ordinary page: one that inherited
# the sandbox would have no cookies, and real sites' bot checks would fail.
# Never allow-same-origin here, and never top navigation.
_SANDBOX = (
    "sandbox allow-scripts allow-forms allow-modals allow-downloads "
    "allow-popups allow-popups-to-escape-sandbox"
)


def html_document_csp(
    *, sandboxed: bool = True, frame_ancestors: str | None = None
) -> str:
    """The policy for an agent-written document and every file it pulls in."""
    directives = [*([_SANDBOX] if sandboxed else []), _LOADS]
    if frame_ancestors:
        directives.append(f"frame-ancestors {frame_ancestors}")
    return "; ".join(directives)
