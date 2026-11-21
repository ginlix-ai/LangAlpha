"""The computer's browser runtime: the ``agent-browser`` wrapper, the gate in
front of each command, the bridge to the stream, and what the server asks.

Ships verbatim into ``_internal/src/browser_runtime/`` and runs there under the
machine's own Python, standard library only. The host imports ``protocol`` for
the names both sides use.
"""
