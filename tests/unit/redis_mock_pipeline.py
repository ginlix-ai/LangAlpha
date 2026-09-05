"""Pipeline shim for MagicMock-based Redis clients.

Batching two commands into one round trip does not change WHICH commands are
sent, and that is what these tests assert. Replaying a pipeline onto the
client's own mocks keeps those assertions meaningful instead of rewriting them
to inspect a queue.
"""

from __future__ import annotations

from contextlib import asynccontextmanager


def attach_pipeline(client) -> None:
    """Wire ``client.pipeline()`` to queue commands and replay them on the client."""

    class _Pipe:
        def __init__(self) -> None:
            self._ops: list = []

        def __getattr__(self, name):
            def _queue(*args, **kwargs):
                self._ops.append((name, args, kwargs))
                return self

            return _queue

        async def execute(self) -> list:
            out = []
            for name, args, kwargs in self._ops:
                out.append(await getattr(client, name)(*args, **kwargs))
            self._ops.clear()
            return out

    @asynccontextmanager
    async def _pipeline(*_args, **_kwargs):
        yield _Pipe()

    client.pipeline = _pipeline
