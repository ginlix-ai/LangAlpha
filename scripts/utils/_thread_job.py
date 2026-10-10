"""What the thread jobs under ``scripts/utils`` share: the app's infrastructure,
opened and always closed; the thread selection flags; and a paced worker loop
that reports a failure without its message.

The importing script puts the repo root and ``src`` on ``sys.path`` first.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, TypeVar

from scripts._errors import where

T = TypeVar("T")


@contextlib.asynccontextmanager
async def app_infra(*, redis: bool, agent_config: bool) -> AsyncIterator[None]:
    """The app's pool and checkpointer, plus Redis and the agent config when
    the job needs them, closed on the way out however it ends: an open
    pool's workers outlive ``main()``, and ``asyncio.run()`` can wait on
    them at exit forever. Each close is registered before its open, so a
    failed open still closes what came up before it."""
    from src.server.app import setup
    from src.server.database import pool as db_pool
    from src.server.utils.checkpointer import (
        close_checkpointer_pool,
        get_checkpointer,
        open_checkpointer_pool,
    )

    async with contextlib.AsyncExitStack() as stack:
        pool = db_pool.get_or_create_pool()
        stack.push_async_callback(pool.close)
        await pool.open()
        if redis:
            from src.utils.cache.redis_cache import close_cache, init_cache

            stack.push_async_callback(close_cache)
            await init_cache()
        checkpointer = get_checkpointer(
            "postgres",
            db_host=os.getenv("DB_HOST", "localhost"),
            db_port=int(os.getenv("DB_PORT", "5432")),
            db_name=os.getenv("DB_NAME", "postgres"),
            db_user=os.getenv("DB_USER", "postgres"),
            db_password=os.getenv("DB_PASSWORD", "postgres"),
        )
        stack.push_async_callback(close_checkpointer_pool, checkpointer)
        await open_checkpointer_pool(checkpointer)
        setup.checkpointer = checkpointer
        if agent_config:
            from ptc_agent.config import ConfigContext, load_from_files

            setup.agent_config = await load_from_files(context=ConfigContext.SDK)
        yield


# --------------------------------------------------------------------------
# Selection
# --------------------------------------------------------------------------


def add_selection_args(parser: argparse.ArgumentParser, *, concurrency: int, pause: float) -> None:
    parser.add_argument("--days", type=int, help="only threads active in the last N days")
    parser.add_argument("--user", help="only this user's threads")
    parser.add_argument("--workspace", help="only this workspace's threads")
    parser.add_argument("--thread", action="append", help="only these threads")
    parser.add_argument("--concurrency", type=int, default=concurrency)
    parser.add_argument("--pause", type=float, default=pause, help="seconds between threads")


def selection_sql(args: argparse.Namespace) -> tuple[list[str], dict[str, Any]]:
    """WHERE clauses and their named parameters for the selection flags, over
    ``conversation_threads t JOIN workspaces w``."""
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if args.thread:
        clauses.append("t.conversation_thread_id = ANY(%(threads)s::uuid[])")
        params["threads"] = args.thread
    if args.workspace:
        clauses.append("t.workspace_id = %(workspace)s::uuid")
        params["workspace"] = args.workspace
    if args.user:
        clauses.append("w.user_id = %(user)s")
        params["user"] = args.user
    if args.days:
        clauses.append("t.updated_at >= NOW() - make_interval(days => %(days)s)")
        params["days"] = args.days
    return clauses, params


# --------------------------------------------------------------------------
# The worker loop
# --------------------------------------------------------------------------


@dataclass
class Tally:
    done: int = 0
    unavailable: int = 0  # replays from stored events instead; nothing to do
    failed: int = 0


async def for_each_thread(
    items: Iterable[T] | AsyncIterable[T],
    fn: Callable[[T], Awaitable[None]],
    *,
    concurrency: int,
    pause: float,
    total: int | None = None,
    name: Callable[[T], str] = str,
) -> Tally:
    """Run ``fn`` on each item, ``concurrency`` at a time with ``pause``
    seconds after each. An async source is drawn from only as workers free
    up, so a job can recheck each page of candidates as it comes up."""
    from src.server.services.history.replay import CheckpointReplayUnavailable

    source = _aiter(items)
    draw = asyncio.Lock()
    tally = Tally()
    began = time.monotonic()

    async def worker() -> None:
        while True:
            async with draw:
                try:
                    item = await anext(source)
                except StopAsyncIteration:
                    return
            try:
                await fn(item)
            except CheckpointReplayUnavailable as exc:
                tally.unavailable += 1
                print(f"{name(item)} unavailable: {where(exc)}")
            except Exception as exc:
                tally.failed += 1
                print(f"{name(item)} failed: {where(exc)}")
            tally.done += 1
            if tally.done % 25 == 0:
                rate = tally.done / max(time.monotonic() - began, 1e-6)
                of = f"/{total}" if total is not None else ""
                print(f"{tally.done}{of} threads, {tally.failed} failed, {rate:.2f}/s")
            await asyncio.sleep(pause)

    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    return tally


async def _aiter(items: Iterable[T] | AsyncIterable[T]) -> AsyncIterator[T]:
    if isinstance(items, AsyncIterable):
        async for item in items:
            yield item
    else:
        for item in items:
            yield item
