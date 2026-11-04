"""Claude Code's setup tokens, wherever one is kept.

A user saves one in Settings for the backend's own CLI on a self-hosted
server (``saved``), and each computer of the user's own keeps one on its
machine (``machine``). Both are changed by one flow (``flow``: one change
at a time, a token checked before it is kept) and checked by one kept check
(``checks``). Where a turn's CLI runs, and on which token, is
``launchers``, the one module here that loads the SDK, so nothing else does
at startup; the rest reach the runtime only from inside a function, as a
check or a change starts it.
"""
