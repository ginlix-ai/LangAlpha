"""CI guard: MultimodalStripMiddleware must be composed INSIDE ModelResilienceMiddleware.

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
SCAN_FILES = ("src/ptc_agent/agent/agent.py",)

# Sites expected to wire both: agent.py's subagent + deepagent lists. The floor
# makes the guard non-vacuous — a rename that stopped matching would otherwise
# leave zero sites and pass.
MIN_SITES = 2

_RESILIENCE_NAMES = frozenset(
    {
        "model_resilience",
        "ModelResilienceMiddleware",
        "build_model_resilience_middleware",
    }
)
_STRIP_NAMES = frozenset({"multimodal_strip", "MultimodalStripMiddleware"})


def _markers(node: ast.AST) -> set[str]:
    """Which of the two middlewares *node* refers to, by name or by constructor.

    Matches the local variable and the class alike, since agent.py splices
    pre-built locals (``*model_resilience``, ``multimodal_strip``) while a
    stack built by appending may pass the constructor call directly.
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
        elif name in _STRIP_NAMES:
            found.add("strip")
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
    """Ordered markers for each ``<var>.append/extend`` sequence.

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
        strip = next((i for i, m in enumerate(ordered) if "strip" in m), None)
        if resilience is None or strip is None:
            continue
        wired_both += 1
        if strip < resilience:
            messages.append(
                f"{label}: multimodal_strip is at index {strip}, model_resilience "
                f"at {resilience} — the strip must come AFTER (inside) resilience"
            )
    return messages, wired_both


_REMEDIATION = (
    "\n\nMultimodalStripMiddleware reads the model off the request to decide which "
    "content blocks a target can accept. Outside ModelResilienceMiddleware it "
    "sees the pre-fallback client, so after a fallback it strips against the "
    "wrong model — silently. Move it after *model_resilience in the list."
)


def test_multimodal_strip_is_composed_inside_model_resilience() -> None:
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
    snippet = "stack = [multimodal_strip, *model_resilience]\n"
    tree = ast.parse(snippet)
    messages, wired_both = _ordering_violations(_list_literal_sites(tree, "fake.py"))
    assert wired_both == 1
    assert messages and "must come AFTER" in messages[0]


def test_guard_accepts_the_correct_order() -> None:
    """Self-test: the correct order produces no violation."""
    snippet = "stack = [*model_resilience, multimodal_strip]\n"
    tree = ast.parse(snippet)
    messages, wired_both = _ordering_violations(_list_literal_sites(tree, "fake.py"))
    assert wired_both == 1
    assert not messages


#: Nodes whose bodies a statement can be skipped by. ``Try`` is absent on
#: purpose: its body runs, only an ``ExceptHandler`` is conditional.
_BRANCHING = (ast.If, ast.IfExp, ast.While, ast.For, ast.AsyncFor, ast.ExceptHandler)


def _branch_above(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> ast.AST | None:
    """The nearest enclosing branch, or ``None`` if the node always runs."""
    current = node
    while current in parents:
        parent = parents[current]
        if isinstance(parent, _BRANCHING):
            return parent
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
            return None
        current = parent
    return None


def _strip_construction_sites(tree: ast.AST) -> list[tuple[ast.Call, ast.AST | None]]:
    """Every ``MultimodalStripMiddleware(...)`` with the branch it sits under.

    The construction itself rather than the assignment: agent.py binds it to a
    name, but a stack built by appending may put the call straight into its
    list, and a check written against assignments alone never looks at it.
    """
    parents = {
        child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)
    }
    return [
        (node, _branch_above(node, parents))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and "strip" in _markers(node.func)
    ]


def test_multimodal_strip_is_wired_unconditionally() -> None:
    """The strip is never optional while the read half is unconditional.

    They used to be one middleware built behind ``if self.config.llm``, so an
    unconfigured llm switched both halves off together and nothing attached a
    block in the first place. Split apart, the same condition on the strip alone
    leaves the read half attaching images that nothing removes, which is the
    provider rejection the strip exists to prevent, checkpointed and replayed
    every turn after. The strip needs no configured model to be useful: it reads
    the target off each request and judges an unresolvable one text-only.
    """
    for rel_path in SCAN_FILES:
        with open(os.path.join(REPO_ROOT, rel_path), encoding="utf-8") as handle:
            tree = ast.parse(handle.read(), filename=rel_path)
        sites = _strip_construction_sites(tree)
        assert sites, f"{rel_path}: no strip construction — a rename broke this guard"
        if any(branch is None for _node, branch in sites):
            continue
        node, branch = sites[0]
        raise AssertionError(
            f"{rel_path}:{node.lineno} builds the strip under "
            f"{type(branch).__name__} at line {branch.lineno}, and no site in "
            "the file is unconditional. The read half always runs, so a strip "
            "that can be skipped means blocks get attached with nothing to "
            "remove them."
        )


def _sites(snippet: str) -> list[tuple[ast.Call, ast.AST | None]]:
    return _strip_construction_sites(ast.parse(snippet))


def test_unconditional_guard_accepts_a_plain_append() -> None:
    """Self-test: the call appended straight into the list."""
    sites = _sites("def f():\n    mw.append(MultimodalStripMiddleware(a=1))\n")
    assert len(sites) == 1 and sites[0][1] is None


def test_unconditional_guard_catches_a_conditional_append() -> None:
    """Self-test: the hole an assignment-only check left open."""
    sites = _sites("def f():\n    if x:\n        mw.append(MultimodalStripMiddleware(a=1))\n")
    assert len(sites) == 1 and isinstance(sites[0][1], ast.If)


def test_unconditional_guard_catches_a_conditional_expression() -> None:
    """Self-test: agent.py's shape, the regression this guard was written for."""
    sites = _sites("def f():\n    s = MultimodalStripMiddleware(a=1) if x else None\n")
    assert len(sites) == 1 and isinstance(sites[0][1], ast.IfExp)

