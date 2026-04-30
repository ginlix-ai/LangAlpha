"""Scrub NUL bytes from a LangGraph checkpoint that has become unresumable.

A ToolMessage with a `\\x00` byte in its content gets serialized into
`checkpoint_blobs.blob` (BYTEA — accepts NUL fine). On every resume the
message rehydrates and downstream code that re-serializes it to JSONB fails
with `psycopg.errors.UntranslatableCharacter`, making the thread permanently
unresumable.

This script identifies poisoned blobs for a given thread, deserializes via
LangGraph's own serde so message types are preserved, walks the structure
stripping NUL from every string, and rewrites the blob.

Usage:
    uv run python scripts/ops/scrub_nul_checkpoint.py THREAD_ID            # dry-run
    uv run python scripts/ops/scrub_nul_checkpoint.py THREAD_ID --apply    # mutate

Idempotent. Connects via the same MEMORY_DB_* env vars used by the
checkpointer (`src/server/utils/checkpointer.py`).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Any
from urllib.parse import quote_plus

import psycopg
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scrub_nul_checkpoint")


def _strip_nul(value: Any) -> tuple[Any, int]:
    """Recursively strip NUL bytes from any string in `value`.

    Returns (cleaned_value, count) where count is the number of strings
    that had at least one NUL stripped.
    """
    if isinstance(value, str):
        if "\x00" in value:
            return value.replace("\x00", ""), 1
        return value, 0
    if isinstance(value, bytes):
        # Bytes are legal — they never bind to TEXT.
        return value, 0
    if isinstance(value, dict):
        out: dict = {}
        n = 0
        for k, v in value.items():
            new_k, kc = _strip_nul(k)
            new_v, vc = _strip_nul(v)
            out[new_k] = new_v
            n += kc + vc
        return out, n
    if isinstance(value, list):
        out_list = []
        n = 0
        for item in value:
            new_item, ic = _strip_nul(item)
            out_list.append(new_item)
            n += ic
        return out_list, n
    if isinstance(value, tuple):
        cleaned = [_strip_nul(item) for item in value]
        return tuple(c for c, _ in cleaned), sum(n for _, n in cleaned)
    # Pydantic/LangChain message objects: walk attributes that look textual.
    # `content` is the dominant carrier. Other str attributes get same treatment.
    if hasattr(value, "__dict__"):
        n = 0
        for attr_name, attr_val in list(vars(value).items()):
            new_val, c = _strip_nul(attr_val)
            if c:
                try:
                    setattr(value, attr_name, new_val)
                    n += c
                except (AttributeError, TypeError):
                    # Slot/frozen — skip.
                    pass
        return value, n
    return value, 0


def _build_db_uri() -> str:
    db_host = os.getenv("MEMORY_DB_HOST", "localhost")
    db_port = os.getenv("MEMORY_DB_PORT", "5432")
    db_name = os.getenv("MEMORY_DB_NAME", "postgres")
    db_user = os.getenv("MEMORY_DB_USER", "postgres")
    db_password = os.getenv("MEMORY_DB_PASSWORD", "postgres")
    sslmode = "require" if "supabase.com" in db_host else "disable"
    return (
        f"postgresql://{quote_plus(db_user)}:{quote_plus(db_password)}"
        f"@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"
    )


async def _scrub_table(
    conn: psycopg.AsyncConnection,
    serde: JsonPlusSerializer,
    table: str,
    key_cols: list[str],
    thread_id: str,
    apply_changes: bool,
) -> tuple[int, int]:
    """Scrub one table. Returns (rows_inspected, rows_modified).

    SELECT runs through a server-side cursor with `fetchmany` so peak memory
    stays bounded by the chunk size regardless of thread history depth. When
    applying changes, the SELECT is `FOR UPDATE` and UPDATEs run inside the
    same transaction, serializing against any concurrent checkpoint write.
    """
    select_cols = ", ".join(key_cols + ["type", "blob"])
    where_keys = " AND ".join(f"{c} = %s" for c in key_cols)
    select_sql = f"SELECT {select_cols} FROM {table} WHERE thread_id = %s"
    if apply_changes:
        select_sql += " FOR UPDATE"
    update_sql = f"UPDATE {table} SET blob = %s, type = %s WHERE {where_keys}"

    inspected = 0
    modified = 0

    async with conn.transaction():
        # Server-side cursor name: scoped to table to stay unique within the tx.
        async with conn.cursor(name=f"scrub_{table}") as cur_read:
            await cur_read.execute(select_sql, (thread_id,))
            async with conn.cursor() as cur_write:
                while True:
                    rows = await cur_read.fetchmany(100)
                    if not rows:
                        break
                    for row in rows:
                        inspected += 1
                        *key_vals, type_str, blob = row
                        if blob is None:
                            continue
                        # Quick byte-level check: skip blobs that obviously have no NUL inside
                        # any string field. A NUL byte CAN legally appear in msgpack framing
                        # bytes (length prefixes, type codes), so this is a heuristic; only
                        # blobs without ANY 0x00 are safe to skip without deserializing.
                        if b"\x00" not in bytes(blob):
                            continue
                        try:
                            value = serde.loads_typed((type_str, bytes(blob)))
                        except Exception as e:
                            logger.warning(
                                "Skipping unparseable blob in %s key=%s type=%s err=%s",
                                table, dict(zip(key_cols, key_vals)), type_str, e,
                            )
                            continue
                        cleaned, count = _strip_nul(value)
                        if count == 0:
                            continue
                        try:
                            new_type, new_blob = serde.dumps_typed(cleaned)
                        except Exception as e:
                            logger.warning(
                                "Failed to re-serialize cleaned blob in %s key=%s err=%s",
                                table, dict(zip(key_cols, key_vals)), e,
                            )
                            continue
                        modified += 1
                        logger.info(
                            "Scrubbed %d strings in %s key=%s (was %d bytes, now %d)",
                            count, table, dict(zip(key_cols, key_vals)),
                            len(bytes(blob)), len(new_blob),
                        )
                        if apply_changes:
                            await cur_write.execute(
                                update_sql, (new_blob, new_type, *key_vals),
                            )

    if modified:
        if apply_changes:
            logger.info("Committed %d UPDATE(s) on %s", modified, table)
        else:
            logger.info(
                "Dry-run: would UPDATE %d row(s) on %s. Re-run with --apply to mutate.",
                modified, table,
            )

    return inspected, modified


async def main(thread_id: str, apply_changes: bool) -> int:
    serde = JsonPlusSerializer()
    db_uri = _build_db_uri()

    logger.info("Connecting to checkpoint DB: %s", db_uri.split("@")[-1])
    async with await psycopg.AsyncConnection.connect(db_uri) as conn:
        # checkpoint_blobs holds the channel values (incl. messages list).
        # checkpoint_writes holds pending writes from interrupted runs.
        b_inspected, b_modified = await _scrub_table(
            conn,
            serde,
            "checkpoint_blobs",
            ["thread_id", "checkpoint_ns", "channel", "version"],
            thread_id,
            apply_changes,
        )
        w_inspected, w_modified = await _scrub_table(
            conn,
            serde,
            "checkpoint_writes",
            ["thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx"],
            thread_id,
            apply_changes,
        )

    logger.info(
        "Done. checkpoint_blobs: %d inspected, %d %s. checkpoint_writes: %d inspected, %d %s.",
        b_inspected, b_modified, "modified" if apply_changes else "would be modified",
        w_inspected, w_modified, "modified" if apply_changes else "would be modified",
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("thread_id", help="Thread UUID to scrub.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write the scrubbed blobs back. Default is dry-run.",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.thread_id, args.apply)))
