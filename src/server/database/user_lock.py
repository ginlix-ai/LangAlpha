"""The per-user locks: one every account-level write shares, and one every
write of the user's profile shares."""

from __future__ import annotations

# Salted so the profile key cannot collide with another per-user key.
_PROFILE_LOCK_KEY_PREFIX = "userdata:profile:"


async def lock_user_writes(cur, user_id: str) -> None:
    """Take the per-user write lock; it holds until the transaction ends.

    One key for the MCP server creates and deletes, the skill, plugin and
    secret caps, and workspace inserts, so a write that touches several of
    them never waits on a second lock it could deadlock against. A workspace
    insert takes it as its own statement BEFORE the INSERT: waiting after it
    would hold the new row's name and folder slots while a rename blocked on
    those slots holds a row this holder's version bump needs.
    """
    await cur.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s::text))", (user_id,)
    )


async def lock_user_profile(cur, user_id: str) -> None:
    """Take the lock on the user's portfolio, watchlists and preferences; it
    holds until the transaction ends.

    A profile file save plans its diff from the rows its version check read
    and writes whole values back by id, so every writer of those rows takes
    this lock first: a dashboard or tool write landing between the save's
    read and its diff would otherwise be written over or cascaded away.
    """
    await cur.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        (_PROFILE_LOCK_KEY_PREFIX + user_id,),
    )
