"""CI guard: forbid request-time LLM constructors outside the credential-resolution chain.

AST-scans agent/tool/server-service source for calls to request-time LLM
constructors and fails on any call not on the allowlist. New code must route
through ``resolve_llm_config`` / ``LLMService.complete`` so BYOK, OAuth, and
per-user model preferences are respected.
"""

from __future__ import annotations

import ast
import os
from dataclasses import dataclass

# Repo root = two levels up from this file (tests/unit/).
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Dirs scanned for request-time constructor calls (relative to repo root).
SCAN_ROOTS = (
    "src/ptc_agent",
    "src/tools",
    "src/server/services",
)

# Bare-name constructors: create_llm("...") etc.
BARE_CONSTRUCTORS = frozenset(
    {
        "create_llm",
        "create_llm_from_custom",
        "init_chat_model",
        "get_llm_by_type",
    }
)

# Method-call constructors: the `LLM(model).get_llm()` pattern. Matched on the
# attribute name only (node.func.attr).
ATTR_CONSTRUCTORS = frozenset({"get_llm"})

# LangChain chat-model class constructors (explicit set so app data models like
# ChatRequest / ChatMessage are NOT mistaken for LLM constructors).
#
# Known scanner limitations (deliberate trade-offs — silence does NOT prove safety):
#   1. Aliased imports are NOT caught: `from x import create_llm as foo; foo(...)`
#      matches on the callee name only, so a renamed callee slips through. Keep using
#      the canonical constructor names (BARE_CONSTRUCTORS / ATTR_CONSTRUCTORS above).
#   2. New LangChain chat-model provider classes not listed below are missed. This is
#      an explicit allowlist on purpose — a Chat*-prefix heuristic false-positives on
#      app models like ChatRequest / ChatMessage. When adopting a new provider (e.g.
#      ChatXAI), add its class name here so direct construction is detected.
CHAT_MODEL_CLASSES = frozenset(
    {
        "ChatOpenAI",
        "AzureChatOpenAI",
        "ChatAnthropic",
        "ChatGoogleGenerativeAI",
        "ChatVertexAI",
        "ChatBedrock",
        "ChatBedrockConverse",
        "ChatLiteLLM",
        "ChatDeepSeek",
        "ChatGroq",
        "ChatMistralAI",
        "ChatCohere",
        "ChatFireworks",
        "ChatTogether",
        "ChatOllama",
    }
)

CONSTRUCTOR_NAMES = BARE_CONSTRUCTORS | ATTR_CONSTRUCTORS | CHAT_MODEL_CLASSES


# Allowlist of legitimate request-time constructor call sites.
# Keyed by (relative_path, callee_name) -> expected call count. The count makes
# the guard bite: a NEW call of an already-listed (file, callee) trips a count
# mismatch and forces a deliberate, justified bump here.
ALLOWLIST: dict[tuple[str, str], int] = {
    # get_llm_client lazy factory — OSS standalone path, no server resolution ran.
    ("src/ptc_agent/config/agent.py", "create_llm"): 1,
    # flash standalone build.
    ("src/ptc_agent/agent/flash/agent.py", "create_llm"): 1,
    # fallback-model name path (build_fallback_pairs, OSS standalone — server
    # runs pre-resolve fallback_llm_clients via resolve_llm_config first).
    ("src/ptc_agent/agent/middleware/model_resilience.py", "get_llm_by_type"): 1,
    # compaction summarizer name path (compact.py helper).
    ("src/ptc_agent/agent/middleware/compaction/compact.py", "get_llm_by_type"): 1,
    # compaction middleware fallback name path.
    ("src/ptc_agent/agent/middleware/compaction/middleware.py", "get_llm_by_type"): 1,
    # compaction middleware model coercion (string model -> chat model).
    ("src/ptc_agent/agent/middleware/compaction/middleware.py", "init_chat_model"): 1,
    # non-BYOK fetch name path (reached only for non-credentialed users).
    ("src/tools/fetch.py", "get_llm"): 1,
    # user_id=None system path AND resolved-None platform fallback (two calls).
    ("src/server/services/llm_service.py", "create_llm"): 2,
}


@dataclass(frozen=True)
class Violation:
    path: str  # repo-relative
    line: int
    callee: str


def _callee_name(func: ast.expr) -> str | None:
    """Return the callee name for a Call node's func, or None if not a name/attr."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def find_constructor_calls(source: str, rel_path: str) -> list[Violation]:
    """Parse source and return every request-time LLM constructor Call as a Violation.

    Because matches are AST Call nodes, docstring examples and comments are
    naturally excluded (they are str constants, not parsed calls).
    """
    tree = ast.parse(source, filename=rel_path)
    found: list[Violation] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _callee_name(node.func)
        if name is not None and name in CONSTRUCTOR_NAMES:
            found.append(Violation(path=rel_path, line=node.lineno, callee=name))
    return found


def scan_tree(roots: tuple[str, ...]) -> tuple[list[Violation], list[tuple[str, str]]]:
    """Scan roots for constructor calls. Returns (calls, parse_errors).

    Skips __pycache__ and test files; reports unparseable files instead of crashing.
    """
    calls: list[Violation] = []
    parse_errors: list[tuple[str, str]] = []
    for root in roots:
        abs_root = os.path.join(REPO_ROOT, root)
        if not os.path.isdir(abs_root):
            # A renamed/moved scan root must fail loudly, not silently drop
            # coverage for that directory.
            parse_errors.append((root, "scan root does not exist"))
            continue
        for dirpath, _dirnames, filenames in os.walk(abs_root):
            if "__pycache__" in dirpath.split(os.sep):
                continue
            for fn in filenames:
                if not fn.endswith(".py"):
                    continue
                if fn.startswith("test_") or fn.endswith("_test.py"):
                    continue
                abs_path = os.path.join(dirpath, fn)
                rel_path = os.path.relpath(abs_path, REPO_ROOT)
                try:
                    with open(abs_path, encoding="utf-8") as fh:
                        source = fh.read()
                except (OSError, UnicodeDecodeError) as exc:
                    parse_errors.append((rel_path, str(exc)))
                    continue
                try:
                    calls.extend(find_constructor_calls(source, rel_path))
                except SyntaxError as exc:
                    parse_errors.append((rel_path, str(exc)))
    return calls, parse_errors


def violations_against_allowlist(
    calls: list[Violation], allowlist: dict[tuple[str, str], int]
) -> list[str]:
    """Return human-readable violation messages for calls not matching the allowlist.

    Three ways to violate: (1) a (file, callee) not in the allowlist at all,
    (2) more or fewer calls of a listed (file, callee) than the expected count,
    (3) a stale allowlist entry whose (file, callee) is no longer present.
    """
    messages: list[str] = []

    # Group calls by (file, callee).
    grouped: dict[tuple[str, str], list[Violation]] = {}
    for call in calls:
        grouped.setdefault((call.path, call.callee), []).append(call)

    for key, group in sorted(grouped.items()):
        rel_path, callee = key
        expected = allowlist.get(key)
        if expected is None:
            for v in group:
                messages.append(
                    f"{v.path}:{v.line}: unexpected request-time LLM constructor "
                    f"'{v.callee}' (this (file, callee) is not on the allowlist)"
                )
        elif len(group) != expected:
            lines = ", ".join(str(v.line) for v in sorted(group, key=lambda x: x.line))
            messages.append(
                f"{rel_path}: '{callee}' called {len(group)} time(s) at line(s) "
                f"{lines}, but allowlist expects {expected}"
            )

    # Stale-entry check: an allowlisted (file, callee) with no matching found call
    # is dead weight after a refactor/rename/delete — flag it so it gets removed.
    for key, expected in sorted(allowlist.items()):
        if key not in grouped:
            rel_path, callee = key
            messages.append(
                f"{rel_path}: '{callee}' is allowlisted (expected {expected}) but was "
                f"not found — remove this stale entry"
            )

    return messages


_REMEDIATION = (
    "\n\nYou added (or moved) a request-time LLM constructor in a scanned dir. "
    "Route the call through resolve_llm_config / LLMService.complete instead so "
    "BYOK, OAuth, and per-user model preferences are respected. If it is a "
    "legitimate platform or lazy-fallback path, add it to the ALLOWLIST in "
    "tests/unit/test_llm_constructor_boundary.py with a one-line justification "
    "(and bump the expected count if extending an existing entry)."
)


def test_no_unallowlisted_llm_constructors() -> None:
    """The scanned tree must contain only allowlisted request-time constructors."""
    calls, parse_errors = scan_tree(SCAN_ROOTS)
    assert not parse_errors, f"files failed to parse: {parse_errors}"

    messages = violations_against_allowlist(calls, ALLOWLIST)
    assert not messages, "request-time LLM constructor boundary violations:\n" + "\n".join(
        messages
    ) + _REMEDIATION


def test_insight_service_has_no_constructors() -> None:
    """insight_service.py must route through LLMService — no direct constructors.

    Intentionally rescans the tree (rather than sharing the main test's scan) to
    stand as standalone intent-documentation: insight_service must never regain a
    request-time LLM constructor, even via `-k insight`.
    """
    calls, _ = scan_tree(SCAN_ROOTS)
    offenders = [c for c in calls if c.path.endswith("services/insight_service.py")]
    assert not offenders, (
        "insight_service.py must not call LLM constructors directly; found "
        + ", ".join(f"{c.callee}@{c.line}" for c in offenders)
        + _REMEDIATION
    )


def test_scanner_detects_stray_constructor() -> None:
    """Self-test: a synthetic off-allowlist call is flagged as a violation.

    Proves the guard is not vacuous — a real stray constructor would be caught.
    """
    snippet = "from x import create_llm\nllm = create_llm('some-model')\n"
    calls = find_constructor_calls(snippet, "src/tools/stray_example.py")
    assert any(c.callee == "create_llm" for c in calls), "scanner missed a stray call"

    # With an empty allowlist the synthetic call must register as a violation.
    messages = violations_against_allowlist(calls, allowlist={})
    assert messages, "scanner produced no violation for an off-allowlist call"
    assert any("create_llm" in m for m in messages)


def test_scanner_ignores_docstring_example() -> None:
    """AST scan must NOT flag a constructor name that appears only in a docstring."""
    snippet = '"""Example:\n\n    llm = ChatAnthropic(model="x")\n"""\n\nx = 1\n'
    calls = find_constructor_calls(snippet, "src/ptc_agent/config/example.py")
    assert not calls, f"docstring example was wrongly flagged: {calls}"
