"""The relay's error vocabulary is duplicated across the sandbox boundary.

``RelayError`` is the source of truth, but the sandbox runtime that renders a
failed relay call cannot import server code, so it keeps its own hint table.
This is the only thing holding the two halves together: a new relay error that
reaches a sandbox with no hint for it degrades into a bare code, which is
exactly the case where the user most needs the sentence telling them what to do.
"""

from __future__ import annotations

from ptc_agent.core.sandbox.mcp_client_runtime import _RELAY_ERROR_HINTS
from src.server.services.egress import RelayError


def test_every_relay_error_has_a_sandbox_hint():
    missing = sorted(e.value for e in RelayError if e.value not in _RELAY_ERROR_HINTS)
    assert not missing, f"no sandbox hint for relay error(s): {missing}"


def test_no_hint_outlives_its_relay_error():
    """A stale hint is dead weight and, worse, evidence of a removed code."""
    known = {e.value for e in RelayError}
    orphaned = sorted(code for code in _RELAY_ERROR_HINTS if code not in known)
    assert not orphaned, f"hint(s) for unknown relay error(s): {orphaned}"


def test_hints_are_actionable_sentences():
    """Guards the failure mode where a hint is added as a copy of the code."""
    for code, hint in _RELAY_ERROR_HINTS.items():
        assert hint.strip(), f"empty hint for {code}"
        assert hint != code
        assert " " in hint, f"hint for {code} is not a sentence: {hint!r}"
