"""The parts of the connector OAuth flow that are spelled once per language.

Nothing at runtime compares them. The shell mints a loopback ``redirect_uri``
this app either accepts or silently drops; the web app names this app's own
callback route in a string no Python ever reads back; and the reasons a failed
callback redirects with are picked here and rendered as sentences there. Each
mismatch fails quietly -- a connect that never comes home, or a user shown a
generic apology instead of the one thing that actually went wrong -- and no
side's type checker can see the other side's constant.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.server.app.mcp_oauth import router
from src.server.services.mcp_oauth.redirects import (
    LOOPBACK_CALLBACK_PATH,
    CallbackError,
)

REPO_ROOT = Path(__file__).resolve().parents[4]


def _js_const(path: Path, name: str) -> str:
    assert path.is_file(), f"{path} is missing; the contract has no other end"
    match = re.search(rf"^const {name} = '([^']*)'", path.read_text(), re.MULTILINE)
    assert match, f"{name} is no longer a plain string const in {path.name}"
    return match.group(1)


def _js_const_set(path: Path, name: str) -> set[str]:
    """The string literals of an exported `new Set([...])`."""
    assert path.is_file(), f"{path} is missing; the contract has no other end"
    match = re.search(
        rf"export const {name} = new Set\(\[(.*?)\]\)", path.read_text(), re.DOTALL
    )
    assert match, f"{name} is no longer a literal Set in {path.name}"
    return set(re.findall(r"'([^']+)'", match.group(1)))


def test_the_shell_listens_on_the_path_this_app_will_accept():
    """A shell naming any other path is refused, and the flow never returns."""
    shell_path = _js_const(REPO_ROOT / "desktop/src/oauth.js", "MCP_CALLBACK_PATH")
    assert shell_path == LOOPBACK_CALLBACK_PATH


def test_the_web_app_names_the_callback_route_this_app_actually_serves():
    """Derived from the router, so moving the route fails here rather than live."""
    served = {
        route.path  # type: ignore[attr-defined]
        for route in router.routes
        if route.path.endswith("/oauth/callback")  # type: ignore[attr-defined]
    }
    assert len(served) == 1, f"expected one callback route, found {served}"
    named = _js_const(
        REPO_ROOT / "web/src/pages/ChatAgent/utils/api/mcp.ts", "OAUTH_CALLBACK_PATH"
    )
    assert named == served.pop()


@pytest.mark.parametrize("name", ["MCP_CALLBACK_PATH", "OAUTH_CALLBACK_PATH"])
def test_both_constants_are_still_readable(name: str):
    """The pin is textual, so a refactor that hides either end must say so here
    rather than leave two tests quietly passing against nothing."""
    source = (
        REPO_ROOT / "desktop/src/oauth.js"
        if name == "MCP_CALLBACK_PATH"
        else REPO_ROOT / "web/src/pages/ChatAgent/utils/api/mcp.ts"
    )
    assert _js_const(source, name).startswith("/")


def _understood_by_the_web_app() -> set[str]:
    return _js_const_set(
        REPO_ROOT / "web/src/pages/Plugins/connectOutcome.ts",
        "CALLBACK_ERROR_REASONS",
    )


def test_the_web_app_has_a_sentence_for_every_reason_this_app_sends():
    """A reason it has not heard of is not an error, but it costs the user the
    one thing the message exists to say."""
    missing = {e.value for e in CallbackError} - _understood_by_the_web_app()
    assert not missing, f"no sentence for: {sorted(missing)}"


def test_the_web_app_claims_no_reason_this_app_cannot_send():
    """The other direction is dead copy, and it is how a rename survives in
    one file: the old value keeps its sentence and the new one gets none."""
    stale = _understood_by_the_web_app() - {e.value for e in CallbackError}
    assert not stale, f"a sentence for reasons never sent: {sorted(stale)}"
