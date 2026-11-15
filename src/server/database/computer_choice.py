"""Where a user chose to work when they had no computer: their own machine or the cloud.

Asked once, by the desktop app's first run, and read only while the user has
no primary computer: ``own_machine`` keeps a cloud computer from being built
for them (``ComputerManager.ensure_primary_computer``). NULL is a user never
asked, who gets the cloud as before.
"""

from __future__ import annotations

from typing import Literal, Optional

from src.server.database.pool import get_db_connection

ComputerChoice = Literal["own_machine", "cloud"]

COMPUTER_CHOICE_OWN_MACHINE: ComputerChoice = "own_machine"
COMPUTER_CHOICE_CLOUD: ComputerChoice = "cloud"
COMPUTER_CHOICES: tuple[ComputerChoice, ...] = (
    COMPUTER_CHOICE_OWN_MACHINE,
    COMPUTER_CHOICE_CLOUD,
)


async def get_computer_choice(user_id: str) -> Optional[ComputerChoice]:
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT computer_choice FROM users WHERE user_id = %s", (user_id,)
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def set_computer_choice(user_id: str, choice: ComputerChoice) -> bool:
    """Record the choice; False when there is no such user."""
    if choice not in COMPUTER_CHOICES:
        raise ValueError(f"Unknown computer choice: {choice!r}")
    async with get_db_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE users SET computer_choice = %s, updated_at = NOW()
            WHERE user_id = %s
            """,
            (choice, user_id),
        )
        return cur.rowcount > 0
