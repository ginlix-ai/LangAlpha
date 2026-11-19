"""What a compact or an offload answers on a Claude Code thread, which keeps
its own session rather than the thread's messages."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from src.llms.claude_code import ClaudeTurnFailed


def refuse_claude_code(on_claude_code: bool) -> None:
    """Refuse an offload on a Claude Code thread, or a summary its own model does not make. Claude Code manages its own
    session, and the thread's messages are only the record replay and
    transcripts read, so rewriting them changes nothing Claude sees, while a
    write through the ordinary graph drops what the runtime keeps beside them.
    Called inside the mutation fence, which orders it against a model switch."""
    if on_claude_code:
        raise HTTPException(
            status_code=400,
            detail={
                "type": "claude_code_compacts_itself",
                "message": "Claude Code manages this thread's context itself.",
            },
        )


def refuse_while_awaiting_user(claude_session: Any, messages: list) -> None:
    """Refuse a compaction while a call Claude Code deferred waits for the
    user: the compaction's CLI would load the session with that call pending
    and nothing to run it, and run until the compaction budget."""
    from ptc_agent.agent.claude_code.session import awaiting_user

    if awaiting_user(claude_session, messages):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "awaiting_input",
                "verb": "compact",
                "message": "Cannot compact while a question or approval is waiting on this thread. Answer or dismiss it, then try again.",
            },
        )


def claude_code_failure(e: ClaudeTurnFailed, model: str) -> HTTPException:
    """A compaction the CLI failed, answered as a turn that failed the same
    way is: in admission's words for what admission refuses, else with the
    status its answer carried."""
    from src.server.services.llm.claude_code import claude_code_refusal
    from src.server.services.runs.stream_errors import classify_stream_exception
    from src.server.utils.error_sanitization import sanitize_error_text

    refusal = claude_code_refusal(e, model)
    if refusal is not None:
        return HTTPException(status_code=400, detail=refusal)
    failure = classify_stream_exception(e)
    status = failure["status_code"] or (502 if failure["kind"] == "upstream" else 500)
    return HTTPException(status_code=status, detail=f"Failed to trigger compaction: {sanitize_error_text(str(e))}")
