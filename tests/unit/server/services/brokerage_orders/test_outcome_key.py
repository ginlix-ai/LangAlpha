"""``outcome_key`` and the order wakeup's fire read an order's end alike.

The fire classifies in SQL, inside the statement that claims the wakeup, and
a placement's note classifies in Python; the two are one mapping written
twice, so this reads the fire's CASE out of its source and holds the function
to it for every status, with and without a fill.
"""

from __future__ import annotations

import inspect
import re

import pytest

from src.server.database.wakeups import fire
from src.server.services.brokerage_orders.models import (
    TERMINAL_STATUSES,
    AttemptStatus,
    outcome_key,
)

_RULE = re.compile(
    r"WHEN a\.status (?:= '(?P<status>\w+)'|IN \(\{_ORDER_ENDS\}\))"
    r"(?P<partly>\s+AND COALESCE\(a\.filled_qty, 0\) > 0)?"
    r"(?P<listed>\s+AND a\.unlisted_at IS NULL)?"
    r"\s+THEN '(?P<outcome>\w+)'"
)


def _case() -> str:
    source = inspect.getsource(fire.fire_order_wakeups)
    return source[source.index("'outcome', CASE") : source.index("'status', a.status")]


def _sql_rules() -> tuple[list[re.Match], str]:
    case = _case()
    default = re.search(r"ELSE '(\w+)'", case)
    assert default is not None
    return list(_RULE.finditer(case)), default.group(1)


def _sql_outcome(status: AttemptStatus, filled: float, unlisted: bool) -> str:
    rules, default = _sql_rules()
    for rule in rules:
        if rule["status"] is None:
            if status not in TERMINAL_STATUSES:
                continue
        elif rule["status"] != status:
            continue
        if rule["partly"] and not filled > 0:
            continue
        if rule["listed"] and unlisted:
            continue
        return rule["outcome"]
    return default


def test_every_when_of_the_fires_case_is_read():
    rules, _ = _sql_rules()
    assert rules and len(rules) == _case().count("WHEN ")


@pytest.mark.parametrize("status", list(AttemptStatus))
@pytest.mark.parametrize("filled", [None, 0, 40])
@pytest.mark.parametrize("unlisted", [False, True])
def test_outcome_key_is_the_fires_case(status, filled, unlisted):
    assert outcome_key(status, filled, unlisted=unlisted) == _sql_outcome(
        status, float(filled or 0), unlisted
    )
