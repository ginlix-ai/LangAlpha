"""A vendor's value, read into the one the neutral shapes are built from.

Every brokerage types the same field differently: a price as a string here and
a float there, an empty string where it means absent, a quantity that comes
back ``1.0`` for the ``1`` that was sent. Each of these answers None rather
than raising when it cannot read what arrived, because a field we cannot parse
is one the record carries in ``raw`` instead, never one that loses the record.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def as_decimal(value: Any) -> Decimal | None:
    """A vendor's number, however it was typed, or None when it is not one."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def as_date(value: Any) -> date | None:
    """A date, from a date, a datetime, or the first ten characters of ISO text."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def as_text(value: Any) -> str | None:
    """A string with nothing in it read as absent, which is what a vendor means by it."""
    stripped = str(value).strip() if value is not None else ""
    return stripped or None


def norm_number(value: Any) -> str:
    """A number spelled the one way, so ``50.00`` and ``50`` compare equal."""
    number = as_decimal(value)
    return "" if number is None else format(number.normalize(), "f")


def extras(args: Mapping[str, Any], consumed: set[str]) -> dict[str, Any]:
    """What a call carried that the neutral vocabulary has no field for."""
    return {k: v for k, v in args.items() if k not in consumed and v is not None}


def target_ids(args: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    """The ids a call named, kept so a later read can address the same order."""
    return {k: args[k] for k in keys if args.get(k) is not None}
