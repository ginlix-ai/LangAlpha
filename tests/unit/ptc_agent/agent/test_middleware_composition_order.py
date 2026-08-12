"""CI guard: MultimodalMiddleware must be composed INSIDE ModelResilienceMiddleware.

Position in a LangChain ``middleware`` list is composition order — index 0 is
outermost. Resilience substitutes the model inside its own ``awrap_model_call``
via ``request.override(model=...)``, so a capability sanitizer placed outside it
reads the pre-fallback client and judges the request against a model that is not
the one being called.

That failure is silent. Nothing raises; the strip simply stops matching the real
target, and a vision-primary → text-only-fallback replays image blocks into the
400 the fallback existed to avoid. A comment cannot hold this invariant, so it is
asserted against the source of every stack that wires both.
"""

from __future__ import annotations

import ast
import os

# Repo root = five levels up from tests/unit/ptc_agent/agent/<this file>.
REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), *([os.pardir] * 4))
)

# Every stack that composes both middlewares. A new agent stack wiring them must
# be added here, or its ordering goes unguarded.
SCAN_FILES = (
    "src/ptc_agent/agent/agent.py",
    "src/ptc_agent/agent/flash/agent.py",
)

# Sites expected to wire both: agent.py's subagent + deepagent lists, and flash's
# append sequence. The floor makes the guard non-vacuous — a rename that stopped
# matching would otherwise leave zero sites and pass.
MIN_SITES = 3

_RESILIENCE_NAMES = frozenset({"model_resilience", "ModelResilienceMiddleware"})
_MULTIMODAL_NAMES = frozenset({"multimodal", "MultimodalMiddleware"})


def _markers(node: ast.AST) -> set[str]:
    """Which of the two middlewares *node* refers to, by name or by constructor.

    Matches the local variable and the class alike, since agent.py splices
    pre-built locals (``*model_resilience``, ``multimodal``) while flash appends
    the constructor calls directly.
    """
    found: set[str] = set()
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name):
            name = sub.id
        elif isinstance(sub, ast.Attribute):
            name = sub.attr
        else:
            continue
        if name in _RESILIENCE_NAMES:
            found.add("resilience")
        elif name in _MULTIMODAL_NAMES:
            found.add("multimodal")
    return found


def _list_literal_sites(tree: ast.AST, rel_path: str) -> list[tuple[str, list[set[str]]]]:
    """Ordered markers for each list literal — agent.py's middleware stacks."""
    sites = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.List):
            continue
        ordered = [_markers(el) for el in node.elts]
        sites.append((f"{rel_path}:{node.lineno} (list literal)", ordered))
    return sites


def _append_sites(tree: ast.AST, rel_path: str) -> list[tuple[str, list[set[str]]]]:
    """Ordered markers for each ``<var>.append/extend`` sequence — flash's stack.

    Source line order stands in for statement order; both calls sit in one
    straight-line function body, so the two agree.
    """
    by_var: dict[str, list[tuple[int, set[str]]]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in ("append", "extend"):
            continue
        if not isinstance(node.func.value, ast.Name):
            continue
        marks = set()
        for arg in node.args:
            marks |= _markers(arg)
        by_var.setdefault(node.func.value.id, []).append((node.lineno, marks))

    sites = []
    for var, calls in by_var.items():
        ordered = [marks for _lineno, marks in sorted(calls)]
        sites.append((f"{rel_path}: {var}.append(...) sequence", ordered))
    return sites


def _assembly_sites() -> list[tuple[str, list[set[str]]]]:
    sites: list[tuple[str, list[set[str]]]] = []
    for rel_path in SCAN_FILES:
        abs_path = os.path.join(REPO_ROOT, rel_path)
        assert os.path.isfile(abs_path), f"scan target missing: {rel_path}"
        with open(abs_path, encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), filename=rel_path)
        sites.extend(_list_literal_sites(tree, rel_path))
        sites.extend(_append_sites(tree, rel_path))
    return sites


def _ordering_violations(sites: list[tuple[str, list[set[str]]]]) -> tuple[list[str], int]:
    """Messages for sites wiring both in the wrong order, and how many wired both."""
    messages: list[str] = []
    wired_both = 0
    for label, ordered in sites:
        resilience = next((i for i, m in enumerate(ordered) if "resilience" in m), None)
        multimodal = next((i for i, m in enumerate(ordered) if "multimodal" in m), None)
        if resilience is None or multimodal is None:
            continue
        wired_both += 1
        if multimodal < resilience:
            messages.append(
                f"{label}: multimodal is at index {multimodal}, model_resilience at "
                f"{resilience} — multimodal must come AFTER (inside) resilience"
            )
    return messages, wired_both


_REMEDIATION = (
    "\n\nMultimodalMiddleware reads the model off the request to decide which "
    "content blocks a target can accept. Outside ModelResilienceMiddleware it "
    "sees the pre-fallback client, so after a fallback it strips against the "
    "wrong model — silently. Move it after *model_resilience in the list."
)


def test_multimodal_is_composed_inside_model_resilience() -> None:
    messages, wired_both = _ordering_violations(_assembly_sites())
    assert not messages, "middleware composition order violations:\n" + "\n".join(
        messages
    ) + _REMEDIATION
    assert wired_both >= MIN_SITES, (
        f"expected at least {MIN_SITES} sites wiring both middlewares, found "
        f"{wired_both} — a rename likely broke this guard's matching"
    )


def test_guard_catches_an_inverted_order() -> None:
    """Self-test: the guard is not vacuous — an inverted stack is flagged."""
    snippet = "stack = [multimodal, *model_resilience]\n"
    tree = ast.parse(snippet)
    messages, wired_both = _ordering_violations(_list_literal_sites(tree, "fake.py"))
    assert wired_both == 1
    assert messages and "must come AFTER" in messages[0]


def test_guard_accepts_the_correct_order() -> None:
    """Self-test: the correct order produces no violation."""
    snippet = "stack = [*model_resilience, multimodal]\n"
    tree = ast.parse(snippet)
    messages, wired_both = _ordering_violations(_list_literal_sites(tree, "fake.py"))
    assert wired_both == 1
    assert not messages
