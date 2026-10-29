"""Rename the report-back state an earlier build left under the Flash agent's names.

The keys and the ``ptc_origin`` record were named for the Flash agent, which
handed work off before the Chief of Staff did, and nothing reads those names
any more. So each worker renames that state at startup, before it serves.
Several workers run this at once: a ``RENAME`` that finds no key lost the race
to a rival, an origin record is rewritten only while it holds the bytes this
worker read, and since ``SCAN`` returns every key that exists for the whole
iteration and nothing writes the old names, a worker that finishes has seen
every old key moved.
"""

from __future__ import annotations

import asyncio
import json
import logging

from redis.exceptions import ResponseError

from src.server.services.report_back.handoff.keys import (
    ORIGIN_KIND,
    ORIGIN_THREAD_FIELD,
    ORIGIN_WORKSPACE_FIELD,
    RB_DONE_PREFIX,
    RB_RUN_PREFIX,
    USER_PENDING_PREFIX,
    WATCH_PREFIX,
    decode,
    ptc_origin_key,
)

logger = logging.getLogger(__name__)

_OLD_PREFIXES = {
    "flash_watch:": WATCH_PREFIX,
    "flash_rb_run:": RB_RUN_PREFIX,
    "flash_rb_done:": RB_DONE_PREFIX,
    "flash_user_pending:": USER_PENDING_PREFIX,
}
_OLD_KIND = "flash"
_OLD_FIELDS = {
    "flash_thread_id": ORIGIN_THREAD_FIELD,
    "flash_workspace_id": ORIGIN_WORKSPACE_FIELD,
}

_ATTEMPTS = 5

# KEYS: 1=ptc_origin  ARGV: 1=the bytes read 2=the renamed record
_REWRITE_LUA = """
if redis.call('get', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('set', KEYS[1], ARGV[2], 'KEEPTTL')
return 1
"""


async def rename_retired_names() -> None:
    """Rename the old keys and origin records, retrying a Redis error.

    Raises once the retries run out, so a worker that cannot finish never
    serves the state it could not read.
    """
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return  # without Redis this worker reads no report-back state
    for attempt in range(1, _ATTEMPTS + 1):
        try:
            moved = await _rename_keys(cache.client)
            rewritten = await _rewrite_origins(cache.client)
        except Exception:
            if attempt == _ATTEMPTS:
                raise
            logger.warning(
                f"[report_back] renaming the Flash agent's keys failed "
                f"(attempt {attempt}/{_ATTEMPTS}); retrying",
                exc_info=True,
            )
            await asyncio.sleep(0.5 * 2 ** (attempt - 1))
            continue
        if moved or rewritten:
            logger.info(
                f"[report_back] renamed {moved} keys and {rewritten} origin "
                f"records from the Flash agent's names"
            )
        return


async def _rename_keys(client) -> int:
    moved = 0
    async for raw in client.scan_iter(match="flash_*", count=1000):
        key = decode(raw)
        new_key = next(
            (new + key[len(old) :] for old, new in _OLD_PREFIXES.items() if key.startswith(old)),
            None,
        )
        if new_key is None:
            continue
        try:
            await client.rename(key, new_key)
        except ResponseError as err:
            if "no such key" not in str(err).lower():
                raise
            continue  # a rival worker moved it first
        moved += 1
    return moved


async def _rewrite_origins(client) -> int:
    rewritten = 0
    async for raw in client.scan_iter(match=ptc_origin_key("*"), count=1000):
        key = decode(raw)
        for _ in range(_ATTEMPTS):
            value = await client.get(key)
            record = renamed_record(value)
            if record is None:
                break
            if await client.eval(_REWRITE_LUA, 1, key, value, record):
                rewritten += 1
                break
        else:
            raise RuntimeError(f"{key} kept changing while its names were renamed")
    return rewritten


def renamed_record(value) -> str | None:
    """The origin record under the new names; None when gone or nothing to rename."""
    if value is None:
        return None
    try:
        record = json.loads(value)
    except ValueError:
        return None
    if not isinstance(record, dict) or not (
        record.get("origin") == _OLD_KIND or _OLD_FIELDS.keys() & record.keys()
    ):
        return None
    if record.get("origin") == _OLD_KIND:
        record["origin"] = ORIGIN_KIND
    for old, new in _OLD_FIELDS.items():
        if old in record:
            kept = record.pop(old)
            record.setdefault(new, kept)
    return json.dumps(record, ensure_ascii=False)
