"""Unit test for ``configure_structlog``'s exception rendering.

structlog's ``ConsoleRenderer`` defaults to ``show_locals=True``, which writes
every frame local into the log. Logs never pass through
``src/server/utils/secret_redactor.py`` (that covers user-facing file content),
so the default turns any ``exc_info=True`` in a frame holding a credential into
a plaintext leak. Nothing fails if someone simplifies the renderer back to a
bare ``ConsoleRenderer()``, which is why this is pinned.
"""

from __future__ import annotations

import contextlib
import io

import pytest
import structlog

from ptc_agent.config.utils import configure_structlog

_TOKEN = "gxsa_pinned_test_token_value"  # noqa: S105 — not a real credential


@pytest.fixture(autouse=True)
def _restore_structlog_defaults():
    """``configure_structlog`` mutates global structlog state; undo it."""
    yield
    structlog.reset_defaults()


def _emit_handled_exception() -> str:
    """Log an exception from a frame whose locals hold a token; return the output."""
    configure_structlog("INFO")
    log = structlog.get_logger("test_structlog_exception_locals")
    buf = io.StringIO()
    try:
        exec_command = f"curl -H 'Authorization: Bearer {_TOKEN}'"  # noqa: F841
        raise RuntimeError("sandbox is gone")
    except RuntimeError:
        with contextlib.redirect_stdout(buf):
            log.error("Failed to execute bash command", exc_info=True)
    return buf.getvalue()


def test_exception_rendering_omits_frame_locals():
    output = _emit_handled_exception()

    assert "Traceback" in output, "the traceback itself must still be rendered"
    # The source line assigning exec_command is legitimately rendered as code
    # context; what must never appear is the resolved runtime value.
    assert _TOKEN not in output
    # rich titles the panel " locals "; match with the spaces so a path or
    # identifier that merely contains the word cannot satisfy the assertion.
    assert " locals " not in output, "rich's locals panel must not be rendered"
