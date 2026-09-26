"""The automation skip and failure reasons, spelled once per language.

The server writes ``skip_reason`` and ``failure_reason`` as open strings, and
the web app words each reason from a table keyed by the same spellings. Neither
side's type checker can see the other's list, so a reason added to one and not
the other drifts silently: a failed run shows no word on why, or the web keeps
a note for a reason the server never sends.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import get_args

import pytest

from src.server.models.automation import FailureReason, SkipReason

REPO_ROOT = Path(__file__).resolve().parents[4]

_TYPES = REPO_ROOT / "web/src/types/automation.ts"
_STATUS = REPO_ROOT / "web/src/pages/Automations/utils/status.ts"


def _read(path: Path) -> str:
    assert path.is_file(), f"{path} is missing; the contract has no other end"
    return path.read_text()


def _ts_union(name: str) -> set[str]:
    match = re.search(rf"^export type {name} = ([^;]+);", _read(_TYPES), re.MULTILINE)
    assert match, f"{name} is no longer a plain string union in {_TYPES.name}"
    return set(re.findall(r"'([^']+)'", match.group(1)))


def _ts_table_keys(name: str) -> set[str]:
    match = re.search(
        rf"^const {name}: Record<\w+, [^>]+> = \{{\n(.*?)^\}};",
        _read(_STATUS),
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"{name} is no longer a Record literal in {_STATUS.name}"
    return set(re.findall(r"^\s+(\w+):", match.group(1), re.MULTILINE))


@pytest.mark.parametrize(
    ("server_literal", "web_union", "web_table"),
    [
        (SkipReason, "SkipReason", "SKIP_REASON_NOTE"),
        (FailureReason, "FailureReason", "FAILURE_REASON_NOTE"),
    ],
    ids=["skip_reason", "failure_reason"],
)
def test_the_web_words_every_reason_the_server_writes(
    server_literal, web_union: str, web_table: str
):
    """The union types the wire; the table is what renders. Both are read as
    text, so each helper asserts its literal is still findable rather than
    letting a refactor that hides it pass against an empty set."""
    spoken = set(get_args(server_literal))
    assert _ts_union(web_union) == spoken
    assert _ts_table_keys(web_table) == spoken
