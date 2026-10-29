"""Start a sandbox interpreter in the workspace the turn belongs to.

Ships into ``_internal/src/`` as ``sitecustomize.py``, which ``site`` imports
at interpreter startup because that directory is on the turn's PYTHONPATH.
The alternative was prepending ``os.chdir(...)`` to the submitted source,
which no provider needs to accept and which breaks the two things a real first
line owns: a ``from __future__`` import becomes a SyntaxError, and every line
number in a traceback is off by one.

A code run also names a file for the module its unhandled exception failed
to import (``PTC_EXCEPTION_FILE``), which the host installs. Only the
``name`` the import machinery set on a ``ModuleNotFoundError``, the raised
one or one on its cause or context chain, is written: never the run's
output, nor any exception's text, both of which carry content (what the code
printed, what its subprocesses wrote to the shared stderr, an MCP server's
error message) that could otherwise name a package to install. The variable
is removed at once, so no subprocess writes the file.

Deliberately silent. A missing or unusable directory leaves the process where
the provider started it, which is the computer root, and the paths the agent's
code uses are absolute anyway.
"""

import os

_TURN_CWD_ENV = "PTC_TURN_CWD"
_EXCEPTION_FILE_ENV = "PTC_EXCEPTION_FILE"

_target = os.environ.get(_TURN_CWD_ENV)
if _target:
    try:
        os.chdir(_target)
    except OSError:
        pass

_exception_file = os.environ.pop(_EXCEPTION_FILE_ENV, None)
if _exception_file:
    import sys

    def _missing_module(value):
        seen = set()
        while value is not None and id(value) not in seen:
            seen.add(id(value))
            if isinstance(value, ModuleNotFoundError) and isinstance(value.name, str):
                return value.name
            value = value.__cause__ or value.__context__
        return None

    def _record_exception(kind, value, tb, _path=_exception_file, _hook=sys.excepthook):
        try:
            name = _missing_module(value)
            if name:
                with open(_path, "w", encoding="utf-8", errors="replace") as f:
                    f.write(name)
        except Exception:
            pass
        _hook(kind, value, tb)

    sys.excepthook = _record_exception
