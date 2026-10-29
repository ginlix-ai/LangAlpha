"""A computer's name says which of the user's computers it means, so no two live ones share it.

Names compare as ``_NAME_KEY`` folds them, trimmed, each run of spaces read
as one and lower-cased, so "Lab box" and " lab  box " are one name, as an
agent reads and passes back a name; migration 097's index holds each user to
that. A
name the user typed is refused while another computer holds it
(``ComputerNameTaken``); one nobody typed, a provider's default or a project's,
takes the first free "Name 2", "Name 3" instead (``insert_under_free_name``).
Migration 097 carries a frozen copy of the fold and the numbering; edit both
or its dedupe and the app disagree about what is free.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import FENCE_NOT_DELETED

logger = logging.getLogger(__name__)

COMPUTER_NAME_INDEX = "idx_computers_user_name"
# The index's fold. A holder is looked up with it rather than in Python, and
# ``fold_computer_name`` is its Python reading: lower(), not casefold(), which
# folds some letters lower() leaves apart.
_NAME_KEY = r"lower(btrim(regexp_replace({}, '\s+', ' ', 'g')))"
# The column's width.
_NAME_MAX = 255

# A name lost to another insert is numbered again from a fresh read. Each loss
# is a computer that took the name, so only a stream of creates for one user
# under one base exhausts this.
_FREE_NAME_ATTEMPTS = 8


class ComputerNameTaken(Exception):
    """Another live computer of the user already has this name.

    ``name`` is the holder's own spelling, and ``computer_id`` the holder when
    it could be read: the index refuses before the holder is known.
    """

    def __init__(self, name: str, computer_id: Optional[str] = None):
        self.name = name
        self.computer_id = computer_id
        super().__init__(f'You already have a computer named "{name}".')


def fold_computer_name(name: str) -> str:
    """A name as ``_NAME_KEY`` folds it."""
    return " ".join(name.split()).lower()


def first_free_computer_name(base: str, taken: Iterable[Optional[str]]) -> str:
    """``base``, or the first "base 2", "base 3" no name in ``taken`` holds."""
    base = base.strip()[:_NAME_MAX]
    used = {fold_computer_name(name) for name in taken if name}
    if fold_computer_name(base) not in used:
        return base
    number = 2
    while fold_computer_name(_numbered(base, number)) in used:
        number += 1
    return _numbered(base, number)


def _numbered(base: str, number: int) -> str:
    suffix = f" {number}"
    return f"{base[: _NAME_MAX - len(suffix)].rstrip()}{suffix}"


async def name_taken(user_id: str, name: str) -> Optional[ComputerNameTaken]:
    """The refusal naming the user's live computer that holds ``name``, or None
    while none does.

    A caller the index refused reaches this on a fresh connection, its own
    transaction being aborted.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT computer_id, name
                FROM computers
                WHERE user_id = %(user_id)s AND {FENCE_NOT_DELETED}
                  AND {_NAME_KEY.format("name")} = {_NAME_KEY.format("%(name)s")}
                LIMIT 1
                """,
                {"user_id": user_id, "name": name},
            )
            row = await cur.fetchone()
    return ComputerNameTaken(row["name"], str(row["computer_id"])) if row else None


async def get_computer_names(user_id: str) -> List[str]:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT name FROM computers WHERE user_id = %s AND {FENCE_NOT_DELETED}",
                (user_id,),
            )
            return [name for (name,) in await cur.fetchall()]


async def insert_under_free_name(
    user_id: str,
    base: str,
    insert: Callable[[str], Awaitable[Dict[str, Any]]],
) -> Dict[str, Any]:
    """``insert`` a computer under ``base`` numbered past the user's names.

    A name the index refuses meanwhile was won by another insert, so the next
    attempt reads the names again and skips the refused one even where the
    index folds case differently from Python.
    """
    refused: list[str] = []
    for _ in range(_FREE_NAME_ATTEMPTS):
        name = first_free_computer_name(
            base, [*await get_computer_names(user_id), *refused]
        )
        try:
            return await insert(name)
        except ComputerNameTaken:
            refused.append(name)
            logger.info(
                f"Computer name {name!r} was taken for user {user_id} meanwhile; "
                "numbering past it"
            )
    logger.warning(f"No free computer name under {base!r} for user {user_id}")
    raise RuntimeError("Could not find a free name for the new computer. Try again.")
