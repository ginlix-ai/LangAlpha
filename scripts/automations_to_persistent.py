"""Move legacy per-run automations onto a continuous thread.

Before bot identity, a scheduled automation opened a new thread every run, so
it never built a conversation: each firing stood alone. A bot-owned
automation now defaults to 'continue', which lands its runs in the bot's own
thread (or the thread the automation already pinned). This is the opt-in
conversion for existing rows: it never runs on its own.

    /app/.venv/bin/python scripts/automations_to_persistent.py            # list only
    /app/.venv/bin/python scripts/automations_to_persistent.py --apply
    /app/.venv/bin/python scripts/automations_to_persistent.py --apply --user-id u-123
    /app/.venv/bin/python scripts/automations_to_persistent.py --apply --include-paused

Only active rows in 'new' are selected unless --include-paused says otherwise.
A row already continuing, or one whose pinned thread exists, is left alone.
Run inside the backend container, with the app's env.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from scripts.utils._thread_job import app_infra  # noqa: E402


async def _rows(conn, user_id: str | None, include_paused: bool, limit: int):
    from psycopg.rows import dict_row

    where = ["thread_strategy = 'new'"]
    params: list[object] = []
    if not include_paused:
        where.append("status = 'active'")
    if user_id:
        where.append("user_id = %s")
        params.append(user_id)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT automation_id, user_id, name, workspace_id, bot_id, status "
            f"FROM automations WHERE {' AND '.join(where)} "
            "ORDER BY created_at LIMIT %s",
            (*params, limit),
        )
        return await cur.fetchall()


async def main(args: argparse.Namespace) -> int:
    from src.server.database import automation as auto_db
    from src.server.database.pool import get_db_connection

    async with app_infra(redis=False, agent_config=False):
        async with get_db_connection() as conn:
            rows = await _rows(conn, args.user_id, args.include_paused, args.limit)
        print(f"{len(rows)} automation(s) still open a new thread each run")
        for row in rows:
            bot = "no bot" if row["bot_id"] is None else f"bot {row['bot_id']}"
            print(
                f"  {row['automation_id']}  {row['name']!r:40.40}  "
                f"workspace {row['workspace_id']}  {bot}  ({row['status']})"
            )
        if not rows or not args.apply:
            if rows:
                print("\nDry run: rerun with --apply to switch them to 'continue'.")
            return 0
        converted = 0
        for row in rows:
            updated = await auto_db.update_automation(
                str(row["automation_id"]),
                str(row["user_id"]),
                thread_strategy="continue",
            )
            if updated is not None:
                converted += 1
        print(f"\nconverted {converted}/{len(rows)} to a continuous thread")
    return 0


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write the change")
    parser.add_argument(
        "--dry-run", action="store_true", help="list only (the default)"
    )
    parser.add_argument("--user-id", default=None, help="only this user's rows")
    parser.add_argument(
        "--include-paused", action="store_true", help="also convert paused rows"
    )
    parser.add_argument("--limit", type=int, default=500)
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(_parse())))
