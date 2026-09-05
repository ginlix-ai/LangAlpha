"""The credit-gate spellings that exist once per language.

Both are relied on across the wire and compared by nobody at runtime. The web
app decides a subagent stopped for money rather than failed by matching
``CREDIT_STOP_ERROR_TYPE`` against the ``error_type`` this service wrote, and
it routes a pause interrupt by matching the action type this service classified
it as. A drift in either fails silently and in the user's favour-losing
direction: a gate stop renders as a subagent error, or a pause never renders as
a card at all. Neither side's type checker can see the other's constant.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.server.contracts.status import (
    CREDIT_STOP_ERROR_TYPE,
    INTERRUPT_REASON_CREDIT_PAUSE,
)

REPO_ROOT = Path(__file__).resolve().parents[4]

_SSE_TYPES = REPO_ROOT / "web/src/types/sse.ts"
_PROJECTIONS = (
    REPO_ROOT / "web/src/pages/ChatAgent/session/interrupts/fromLiveEvent.ts",
    REPO_ROOT / "web/src/pages/ChatAgent/session/interrupts/fromHistoryEvent.ts",
)


def _ts_const(path: Path, name: str) -> str:
    assert path.is_file(), f"{path} is missing; the contract has no other end"
    match = re.search(
        rf"^export const {name} = '([^']*)'", path.read_text(), re.MULTILINE
    )
    assert match, f"{name} is no longer a plain string const in {path.name}"
    return match.group(1)


def test_the_web_app_mirrors_the_credit_stop_spelling():
    """status.py names this file as the mirror; this is the only thing checking.

    The pin is textual, so ``_ts_const`` asserts the literal is still findable
    rather than letting a refactor that hides it pass against nothing."""
    assert _ts_const(_SSE_TYPES, "CREDIT_STOP_ERROR_TYPE") == CREDIT_STOP_ERROR_TYPE


@pytest.mark.parametrize("path", _PROJECTIONS, ids=lambda p: p.name)
def test_both_interrupt_projections_match_the_classified_action_type(path: Path):
    """Live and history project the same interrupt; a pause the projection does
    not recognise reaches the transcript as no card at all."""
    assert path.is_file(), f"{path} is missing; the contract has no other end"
    assert (
        f"actionType === '{INTERRUPT_REASON_CREDIT_PAUSE}'" in path.read_text()
    ), f"{path.name} no longer routes on the action type this service writes"
