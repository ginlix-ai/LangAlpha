"""The session a backup pass holds, where these tests have no database.

Every read and write the pass makes is patched, so a transaction only has to
open and close: ``_sync_locked`` writes a pass's rows and its prune as one.
"""

from __future__ import annotations

from contextlib import asynccontextmanager


class NoDbConn:
    @asynccontextmanager
    async def transaction(self):
        yield
