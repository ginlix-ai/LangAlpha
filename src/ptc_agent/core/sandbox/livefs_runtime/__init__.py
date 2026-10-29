"""livefs: the user's server-held files as a directory in the sandbox.

Ships verbatim into ``_internal/src/livefs/`` and runs there as
``python3 -m livefs``. Kept on the host inside ``src/`` so it is linted and
testable like any other module. The host imports ``protocol`` for the names
both sides use, and ``boot`` and ``lifecycle`` to name the code it ships.
``tree_runtime`` is also the folder scripts' copy and delete, sent whole with
each.
"""
