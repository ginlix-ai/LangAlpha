"""Ratchet: the sites that still read one account where a turn has two.

A turn has a sender (``user_id``) and a workspace owner
(``TurnRoute.owner_id``); they are one account for every turn that runs
today, so nothing fails when a site reads the wrong one. This guard does it
instead. Each table below is the whole set of call sites of one kind, by file
and enclosing function, compared by equality in both directions: a new site
fails until it is listed (and its author has decided which account it
means), and a site moved onto the right account fails until its row is
deleted, so the tables only shrink.
"""

from __future__ import annotations

import ast
import functools
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC = REPO_ROOT / "src"

#: ``_keep_computer_tokens`` keeps the market-data token and the CLI's token
#: for one account, the caller at an acquire and the owner where the
#: lifecycle rebuilds a sandbox. On a computer that is not its sender's they
#: part (the data token's scopes are the person's plan; the CLI's token acts
#: as an account on the machine). The keepers split them: on a shared
#: workspace's computer the data token is the acting member's, minted again
#: when the stored one is another member's, and no CLI token is kept. Each
#: new caller is still a decision about which account it passes.
SINGLE_ACCOUNT_TOKEN_SITES: Counter[tuple[str, str]] = Counter({
    ("src/server/services/computer_manager/_machines.py", "_build_machine_session"): 2,
    ("src/server/services/computer_manager/_provisioning.py", "_recover_sandbox"): 1,
    ("src/server/services/computer_manager/_provisioning.py", "_sync_project_assets"): 1,
    ("src/server/services/workspace_manager.py", "get_session_for_workspace"): 2,
    ("src/server/services/workspace_manager.py", "reach_computer"): 1,
})

#: Turns the server posts for someone (report-backs, parked steers, wakeups)
#: name the account in their job; where a job names none, they ask
#: ``thread_person``, which never answers a shared workspace's holder. These
#: read the thread's owner directly for other ends (labels, a model pin, the
#: thread's workspace and existence): the module posts turns, so each read
#: is a decision.
THREAD_OWNER_FALLBACKS: Counter[tuple[str, str]] = Counter({
    ("src/server/services/report_back/handoff/status.py", "read_report_back_slice"): 1,
    ("src/server/services/report_back/subagent.py", "_with_owner"): 1,
    ("src/server/services/report_back/task_steer.py", "_computer_full"): 1,
})

#: Agent builds between turns that name no route, which ``_thread_agent``
#: then reads as the caller's analyst: right only where the route's own check
#: made the caller the thread's owner.
THREAD_AGENTS_WITHOUT_OWNER: Counter[tuple[str, str]] = Counter()


#: The names the tests below look for; a file naming none is not parsed.
_WATCHED = (
    "_keep_computer_tokens",
    "get_thread_auth_meta",
    "_thread_agent",
    "resolve_llm_config",
    "build_ptc_graph_with_session",
)


@functools.cache
def _all_calls() -> tuple[tuple[str, str, str, ast.Call], ...]:
    """Every call of a watched name under ``src/``, parsed once, as (file,
    enclosing function, the name called, the call node)."""
    found: list[tuple[str, str, str, ast.Call]] = []

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
                if called in _WATCHED:
                    found.append((rel, function, called, child))
            walk(child, rel, inner)

    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        if any(name in text for name in _WATCHED):
            walk(ast.parse(text), path.relative_to(REPO_ROOT).as_posix(), "<module>")
    return tuple(found)


def _calls(root: Path, name: str) -> list[tuple[str, str, ast.Call]]:
    """Each call of ``name`` (a bare name or an attribute) under ``root``."""
    prefix = root.relative_to(REPO_ROOT).as_posix() + "/"
    return [
        (rel, function, call)
        for rel, function, called, call in _all_calls()
        if called == name and rel.startswith(prefix)
    ]


def _keywords(call: ast.Call) -> set[str | None]:
    return {k.arg for k in call.keywords}


def _sites(found) -> Counter[tuple[str, str]]:
    return Counter((rel, function) for rel, function, _ in found)


def test_every_single_account_token_site_is_listed():
    assert _sites(_calls(SRC, "_keep_computer_tokens")) == SINGLE_ACCOUNT_TOKEN_SITES


def test_every_thread_owner_fallback_of_a_posted_turn_is_listed():
    found = [
        *_calls(SRC / "server" / "services" / "report_back", "get_thread_auth_meta"),
        *_calls(SRC / "server" / "services" / "wakeups", "get_thread_auth_meta"),
    ]
    assert _sites(found) == THREAD_OWNER_FALLBACKS


def test_every_agent_built_between_turns_names_its_owner_or_is_listed():
    found = [c for c in _calls(SRC, "_thread_agent") if "route" not in _keywords(c[2])]
    assert _sites(found) == THREAD_AGENTS_WITHOUT_OWNER


def test_a_config_resolved_for_a_workspace_names_its_owner():
    """The skills a workspace's computer is given are its owner's, so a
    resolve that names a workspace names the turn's identity too."""
    found = [
        (rel, function)
        for rel, function, call in _calls(SRC, "resolve_llm_config")
        if "workspace_id" in _keywords(call) and "identity" not in _keywords(call)
    ]
    assert found == []


def test_a_server_agent_build_takes_both_accounts_from_its_inputs():
    """``AgentInputs`` requires the owner, so a build that takes its
    arguments from them (``**inputs.build_kwargs()``) names both."""
    found = [
        (rel, function)
        for rel, function, call in _calls(SRC / "server", "build_ptc_graph_with_session")
        if None not in _keywords(call) and "owner_id" not in _keywords(call)
    ]
    assert found == []
