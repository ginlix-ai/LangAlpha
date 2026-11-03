"""Operator controls over the setup tokens users let the platform store for
Claude Code on cloud computers.

  block <user_id> --reason <text>   delete the user's stored token, set their
                                    choice to local, stop the turns that may
                                    hold it, and refuse storing one again
  unblock <user_id>                 lift that block; the user chooses again
  purge --all | --user <user_id>    delete stored tokens and set every
                                    ``stored`` choice to local, stopping
                                    the turns that may hold them

Usage:
    uv run python scripts/ops/claude_code_custody.py block <user_id> --reason "<text>"
    uv run python scripts/ops/claude_code_custody.py purge --all

Connects with DB_* (or --dsn) and the server's Redis. Logs counts only, never a token.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.ops._db import build_db_uri, point_app_pool_at  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("claude_code_custody")


async def run(args: argparse.Namespace) -> int:
    from src.server.database import pool as db_pool
    from src.server.database.claude_code_hosted import stored_users, unblock
    from src.server.services.claude_code.hosted import drop_stored
    from src.utils.cache.redis_cache import close_cache, init_cache

    pool = db_pool.get_or_create_pool()
    await pool.open()
    await init_cache()
    try:
        if args.command == "block":
            stopped = await drop_stored(args.user_id, block_reason=args.reason)
            logger.info("blocked one user; %d turns stopped", stopped)
        elif args.command == "unblock":
            logger.info("unblocked" if await unblock(args.user_id) else "no block to lift")
        else:
            users = [args.user] if args.user is not None else await stored_users()
            stopped = failed = 0
            for user_id in users:
                try:
                    stopped += await drop_stored(user_id)
                except Exception:
                    failed += 1
                    logger.warning("purge failed for user_id=%s", user_id, exc_info=True)
            logger.info("purged %d users; %d turns stopped; %d failed", len(users) - failed, stopped, failed)
            return 1 if failed else 0
    finally:
        await close_cache()
        await pool.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument("--dsn", help="Postgres URI; defaults to DB_*")
    commands = parser.add_subparsers(dest="command", required=True)
    block = commands.add_parser("block")
    block.add_argument("user_id")
    block.add_argument("--reason", required=True)
    commands.add_parser("unblock").add_argument("user_id")
    purge = commands.add_parser("purge")
    who = purge.add_mutually_exclusive_group(required=True)
    who.add_argument("--all", action="store_true")
    who.add_argument("--user")
    args = parser.parse_args()
    if args.command == "purge" and args.user is not None and not args.user.strip():
        parser.error("--user needs a user id")
    from src.config.settings import HOST_MODE

    if HOST_MODE != "platform":
        parser.error("these controls are for setup tokens stored on the platform; here they would delete the tokens users saved")
    point_app_pool_at(args.dsn or build_db_uri())
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
