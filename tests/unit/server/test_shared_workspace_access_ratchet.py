"""Ratchet: the routes a shared workspace's members cannot use, and why.

A shared workspace's rows are owned by its holder, so every owner-only check
(``require_workspace_owner``, ``require_thread_owner``) refuses its members,
failing closed. Every route a member needs is on ``workspace_access`` and
``thread_access``; the sites left are listed here, by file, enclosing
function and helper, each with the reason it stays the owner's alone, and
compared both ways: a new owner-only site fails until it is listed (its
author has decided members are refused there, and says why) and a site moved
onto the access helpers fails until its row is deleted. The list only
shrinks.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER = REPO_ROOT / "src" / "server"

_HELPERS = (
    "require_workspace_owner",
    "require_thread_owner",
)

# Automations fire later under the account that set them up; one aimed at a
# shared workspace would need its sponsor's access judged again at every
# firing, which is later work (plan P8).
_AUTOMATIONS = "an automation is a person's and cannot target a shared workspace"
# A member's own connector sign-ins never reach a shared computer, and the
# holder's tier is written by managers only once that exists (plan P4).
_CONNECTORS = "connector and skill settings write the holder's tier (plan P4)"
_PERSONAL = "about the caller's own things, which a shared workspace never serves"
# A shared workspace's computer is the holder's, made for it, a cloud
# computer of a standard size that never moves (plan P5, P9).
_COMPUTER = "the computer's kind, size and place are fixed for a shared workspace"

OWNER_ONLY_SITES: dict[tuple[str, str, str], str] = {
    ("src/server/app/automations.py", "_delivery_workspace", "require_workspace_owner"): _AUTOMATIONS,
    ("src/server/services/automations/lifecycle.py", "_home", "require_workspace_owner"): _AUTOMATIONS,
    ("src/server/services/automations/lifecycle.py", "require_target_ownership", "require_thread_owner"): _AUTOMATIONS,
    ("src/server/services/automations/lifecycle.py", "require_target_ownership", "require_workspace_owner"): _AUTOMATIONS,
    ("src/server/app/mcp_servers.py", "_require_owned_workspace", "require_workspace_owner"): _CONNECTORS,
    ("src/server/services/mcp_saves.py", "_owned_workspace", "require_workspace_owner"): _CONNECTORS,
    ("src/server/app/skills.py", "_require_owned_workspace", "require_workspace_owner"): _CONNECTORS,
    # The Chief of Staff runs in the caller's Home, never in a shared workspace.
    ("src/server/app/claude_code.py", "get_chief_of_staff", "require_thread_owner"): _PERSONAL,
    # Memos are the caller's own store; a shared workspace's files go to no one's.
    ("src/server/app/memo.py", "upload_user_memo", "require_workspace_owner"): _PERSONAL,
    # A chat app's conversation is the person's; it never becomes a shared thread.
    ("src/server/app/threads/crud.py", "stamp_thread_external_id_endpoint", "require_thread_owner"): _PERSONAL,
    # Offered only for a folder gone from the owner's own machine, which a
    # shared workspace is never on.
    ("src/server/app/workspaces.py", "choose_files_restore", "require_workspace_owner"): _PERSONAL,
    # A copy lands on the caller's own computer, taking shared work out of the
    # workspace its members share; a member downloads what they need instead.
    ("src/server/app/workspaces.py", "duplicate_workspace", "require_workspace_owner"): _COMPUTER,
    ("src/server/app/workspaces.py", "move_workspace", "require_workspace_owner"): _COMPUTER,
    ("src/server/app/workspaces.py", "get_workspace_move", "require_workspace_owner"): _COMPUTER,
    ("src/server/app/workspaces.py", "cancel_workspace_move", "require_workspace_owner"): _COMPUTER,
    ("src/server/app/workspaces.py", "set_workspace_always_on", "require_workspace_owner"): _COMPUTER,
    ("src/server/app/workspaces.py", "set_workspace_spec", "require_workspace_owner"): _COMPUTER,
}


def _sites() -> Counter[tuple[str, str, str]]:
    found: Counter[tuple[str, str, str]] = Counter()

    def walk(node: ast.AST, rel: str, function: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = (
                child.name
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                else function
            )
            if isinstance(child, ast.Call):
                callee = child.func
                called = callee.attr if isinstance(callee, ast.Attribute) else getattr(callee, "id", None)
                if called in _HELPERS:
                    found[(rel, function, called)] += 1
            walk(child, rel, inner)

    for path in sorted(SERVER.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if any(name in text for name in _HELPERS):
            walk(ast.parse(text), path.relative_to(REPO_ROOT).as_posix(), "<module>")
    return found


def test_every_owner_only_site_is_listed():
    assert _sites() == Counter(OWNER_ONLY_SITES.keys())
