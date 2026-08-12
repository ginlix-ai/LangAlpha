"""Data provenance: per-turn external-data access records and bodies."""

import hashlib


from fastapi import HTTPException, Query

# require_thread_owner is called through the module (auth_api.…) so a single
# definition-site patch governs every route — a consumer-site patch that stops
# intercepting after a move would silently bypass auth in tests.
from src.server.utils import api as auth_api
from src.server.utils.api import (
    CurrentUserId,
)
from src.server.database.provenance import (
    get_provenance_body_refs,
    get_provenance_for_thread,
    get_provenance_record,
)



from ._deps import logger, router


# =============================================================================
# DATA PROVENANCE
# =============================================================================


@router.get("/{thread_id}/provenance")
async def get_provenance(thread_id: str, x_user_id: CurrentUserId):
    """Return the external data the agent accessed in a thread, grouped by turn.

    The aggregated shape (per-turn sources + a by_source_type count summary) is
    the structured input a post-hoc verification agent consumes.
    """
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        rows = await get_provenance_for_thread(thread_id)

        turns: dict[int, dict] = {}
        by_source_type: dict[str, int] = {}
        for row in rows:
            turn_index = row["turn_index"]
            turn = turns.get(turn_index)
            if turn is None:
                turn = {
                    "turn_index": turn_index,
                    "conversation_response_id": str(row["conversation_response_id"]),
                    "sources": [],
                }
                turns[turn_index] = turn

            source_timestamp = row.get("source_timestamp")
            source = {
                # `record_id` matches the SSE/replay provenance record field so a
                # consumer can map streamed records to this REST shape directly.
                "record_id": str(row["provenance_record_id"]),
                "source_type": row["source_type"],
                "identifier": row.get("identifier"),
                "title": row.get("title"),
                "detail": row.get("detail"),
                "tool_call_id": row.get("tool_call_id"),
                "args_fingerprint": row.get("args_fingerprint"),
                "args": row.get("args"),
                "result_sha256": row.get("result_sha256"),
                "result_size": row.get("result_size"),
                "result_snippet": row.get("result_snippet"),
                "agent": row.get("agent"),
                "provider": row.get("provider"),
                "timestamp": (
                    source_timestamp.isoformat() if source_timestamp else None
                ),
            }
            turn["sources"].append(source)

            source_type = row["source_type"]
            by_source_type[source_type] = by_source_type.get(source_type, 0) + 1

        return {
            "thread_id": thread_id,
            "turns": [turns[i] for i in sorted(turns)],
            "by_source_type": by_source_type,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error getting provenance for thread {thread_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get provenance")


def _body_hashes_to(body: str, sha256: str | None) -> bool:
    """True iff ``body`` reproduces the content-address ``sha256``.

    The verifier's integrity check: a present body that hashes to its advertised
    sha is the exact content the agent reasoned over. A mismatch means the body was
    redacted (a secret was stripped) or is otherwise not the hashed bytes — the
    caller distinguishes the two via the ``truncated`` flag.
    """
    if not body or not sha256:
        return False
    return hashlib.sha256(body.encode("utf-8")).hexdigest() == sha256


@router.get("/{thread_id}/provenance/bodies")
async def get_provenance_bodies(
    thread_id: str,
    x_user_id: CurrentUserId,
    limit: int = Query(
        100,
        ge=1,
        le=200,
        description="Max bodies returned; a long thread is capped (see `capped`).",
    ),
):
    """Return stored result bodies (inline head only) for a thread's provenance records.

    Sibling to ``/provenance`` (which stays snippet-only): joins each record's
    ``result_sha256`` to the content-addressed body store and returns the inline
    head plus ``truncated`` and ``verified`` flags. Spilled objects are never
    fetched here — use the per-record ``/body?full=true`` endpoint for the full body.

    The response is bounded: each inline head is up to 64 KiB, so a long thread is
    capped at ``limit`` bodies (``capped: true`` when more were available) to keep
    one request from materializing tens of MB.
    """
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)

        from src.server.database.pool import get_db_connection
        from src.server.database.provenance_bodies import fetch_result_bodies

        # Eligible refs are filtered + capped in SQL (LIMIT limit+1) and the body
        # fetch shares the same connection, so a long thread doesn't transfer every
        # record (and its args JSON) just to discard all but `limit`.
        async with get_db_connection() as conn:
            eligible = await get_provenance_body_refs(conn, thread_id, limit)
            capped = len(eligible) > limit
            eligible = eligible[:limit]
            shas = [row["result_sha256"] for row in eligible]
            bodies = await fetch_result_bodies(conn, shas)

        records = []
        for row in eligible:
            sha = row["result_sha256"]
            body = bodies.get(sha)
            if body is None:
                continue
            body_inline = body["body_inline"] or ""
            byte_len = body["byte_len"]
            # byte_len is the length of the STORED (post-redaction) body, so the
            # inline head is incomplete exactly when the full stored body is longer
            # than what's inline — i.e. it spilled to an object, or a head was kept
            # with no bucket to spill to. A body redaction shrank below the cap is
            # stored whole (byte_len == len(inline)) and reads back complete.
            truncated = byte_len > len(body_inline.encode("utf-8"))
            records.append(
                {
                    "provenance_record_id": str(row["provenance_record_id"]),
                    "result_sha256": sha,
                    "body_inline": body_inline,
                    "byte_len": byte_len,
                    "truncated": truncated,
                    # The stored body hashes to result_sha256 (untruncated + not
                    # redacted). False on a truncated head or a redaction-modified
                    # body — the signal that "these bytes != the advertised hash."
                    "verified": (not truncated) and _body_hashes_to(body_inline, sha),
                }
            )

        return {"thread_id": thread_id, "records": records, "capped": capped}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            f"Error getting provenance bodies for thread {thread_id}: {e}"
        )
        raise HTTPException(status_code=500, detail="Failed to get provenance bodies")


@router.get("/{thread_id}/provenance/{provenance_record_id}/body")
async def get_provenance_record_body(
    thread_id: str,
    provenance_record_id: str,
    x_user_id: CurrentUserId,
    full: bool = Query(
        False,
        description="When true, read the full body (pulls the spilled object if any).",
    ),
):
    """Return the body for a single provenance record.

    With ``full=true`` the full body is read via ``fetch_full_body`` (pulling the
    spilled object when present, capped at ``FULL_BODY_READ_MAX_BYTES`` so one
    request can't serialize a ~10 MiB object — an over-cap body returns truncated);
    otherwise only the inline head is returned. The record must belong to the
    caller's thread.
    """
    try:
        await auth_api.require_thread_owner(thread_id, x_user_id)
        row = await get_provenance_record(thread_id, provenance_record_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Provenance record not found")

        sha = row.get("result_sha256")
        if not sha:
            raise HTTPException(
                status_code=404, detail="Provenance record has no stored body"
            )

        from src.server.database.pool import get_db_connection
        from src.server.database.provenance_bodies import (
            fetch_full_body,
            fetch_result_bodies,
        )

        async with get_db_connection() as conn:
            bodies = await fetch_result_bodies(conn, [sha])
        meta = bodies.get(sha)
        if meta is None:
            raise HTTPException(
                status_code=404, detail="Provenance record has no stored body"
            )

        byte_len = meta["byte_len"]
        if full:
            # meta already carries body_inline + object_key from the fetch above,
            # so pass it through — fetch_full_body skips a second connection and
            # only does the spilled-object read when there's an object_key.
            body = await fetch_full_body(sha, row=meta) or ""
            # byte_len is the full stored-body length; the read is incomplete
            # exactly when we returned fewer bytes than that — the spilled object
            # exceeded the read cap, or a head was kept with no bucket to spill to.
            truncated = byte_len > len(body.encode("utf-8"))
        else:
            body = meta["body_inline"] or ""
            # The inline head is incomplete exactly when the full stored body is
            # longer than the inline slice (spilled, or head kept with no bucket).
            # byte_len tracks the stored (post-redaction) length, so a redaction-
            # shrunk body that fits inline reads back complete.
            truncated = byte_len > len(body.encode("utf-8"))

        return {
            "provenance_record_id": str(row["provenance_record_id"]),
            "result_sha256": sha,
            "body": body,
            "byte_len": byte_len,
            "truncated": truncated,
            # With full=true and no truncation, a true value attests the body is the
            # exact bytes behind result_sha256; false means redacted or head-only.
            "verified": (not truncated) and _body_hashes_to(body, sha),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            f"Error getting provenance body for record {provenance_record_id}: {e}"
        )
        raise HTTPException(status_code=500, detail="Failed to get provenance body")
