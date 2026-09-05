"""
Credit-resume context utilities for chat endpoint.

Builds the ``<system-reminder>`` state update injected when a credit-paused
turn resumes, recording which background tasks the credit gate stopped
alongside the pause so the model can re-enter the ones still needed.
"""

import logging

from langchain_core.messages import HumanMessage

from src.server.database.runs import credit_ledger

logger = logging.getLogger(__name__)


async def build_credit_resume_update(
    thread_id: str, resuming_run_id: str
) -> dict | None:
    """State update for a resume of a credit-paused turn, or None.

    A mechanical record of which subagent runs the gate stopped alongside
    the pause: their checkpoints survived the terminal settle, so the model
    re-enters the ones still needed via Task(action="resume") — or declines,
    which is a feature. Never blocks the resume: any lookup failure just
    resumes without the record.
    """
    try:
        stopped = await credit_ledger.list_credit_stopped_for_resume(
            thread_id, resuming_run_id
        )
    except Exception:
        logger.warning(
            "[CreditGate] credit-resume lookup failed for thread %s",
            thread_id,
            exc_info=True,
        )
        return None
    if not stopped:
        return None
    lines = "\n".join(f"- task_id \"{t['task_id']}\"" for t in stopped)
    return {
        "messages": [
            HumanMessage(
                content=(
                    "<system-reminder>\n"
                    "This run was paused when the account ran out of credits "
                    "and has just been resumed. The credit gate also stopped "
                    "the following background tasks at a clean checkpoint:\n"
                    f"{lines}\n"
                    "Their prior work is preserved. Resume each one still "
                    'needed with Task(action="resume", task_id=...), or '
                    "leave it stopped if its work is no longer relevant.\n"
                    "</system-reminder>"
                ),
                additional_kwargs={"lc_source": "credit_gate"},
            )
        ]
    }
