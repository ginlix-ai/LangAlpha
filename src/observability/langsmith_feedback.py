"""Where users' ratings and reports meet LangSmith: the link to a turn's
trace, and feedback on its run.

A turn's run id is its trace id, so a thumbs-up or thumbs-down, and a
failed-turn report, land on the trace an engineer opens, and the project can
be filtered for the runs users rated (``user_rating``, score 1 or 0) or
reported (``user_report``). Each feedback's id is derived from what it mirrors,
and LangSmith overwrites feedback created again under its id, so a changed
rating is written again and a removed one deleted.

Off when tracing is off. The feedback calls run after the response (FastAPI
background tasks), so LangSmith's latency or an outage never reaches the
user; a failure is logged and dropped.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Any, Mapping, Optional
from uuid import UUID, uuid5

logger = logging.getLogger(__name__)

_FEEDBACK_NAMESPACE = UUID("9a1c6e2f-4b7d-4f0e-8c3a-5d2b1e7f9a64")


def tracing_enabled() -> bool:
    from langsmith import utils as ls_utils

    return ls_utils.tracing_is_enabled()


@functools.cache
def _tracer_project() -> tuple[str, str, str]:
    """The tracing project's id, its organization and the LangSmith host,
    which hold for the life of the process. A failed read is not cached."""
    from langsmith import utils as ls_utils
    from langsmith.run_trees import get_cached_client

    client = get_cached_client()
    project = client.read_project(project_name=ls_utils.get_tracer_project())
    # The shared client is built without a web URL, so its host is the one
    # derived from the API URL, as the SDK derives it.
    return str(project.id), str(project.tenant_id), ls_utils.get_host_url(None, client.api_url)


async def trace_url(run_id: Optional[str]) -> Optional[str]:
    """The LangSmith page of a turn's trace. None when tracing is off, since
    there is no trace to open, or when LangSmith cannot say where it is."""
    if not run_id or not tracing_enabled():
        return None
    try:
        project, tenant, host = await asyncio.wait_for(
            asyncio.to_thread(_tracer_project), timeout=5
        )
    except Exception:
        logger.warning("No LangSmith trace link for run %s", run_id, exc_info=True)
        return None
    return f"{host}/o/{tenant}/projects/p/{project}/r/{run_id}?trace_id={run_id}"


def mirror_rating(row: Mapping[str, Any], vault_secrets: dict[str, str]) -> None:
    """Write a ``conversation_feedback`` row as feedback on the run it rates:
    the rating, the issue categories, the comment and the notes added since,
    scrubbed of the user's secrets (``vault_secrets``)."""
    if not tracing_enabled():
        return
    from src.server.utils.secret_redactor import scrub_outbound_text

    notes = [f.get("note") for f in row.get("report_follow_ups") or () if f.get("note")]
    categories = [scrub_outbound_text(c, vault_secrets) for c in row.get("issue_categories") or () if c]
    _create(
        _feedback_id("rating", row["conversation_feedback_id"]),
        run_id=str(row["conversation_response_id"]),
        key="user_rating",
        score=1 if row["rating"] == "thumbs_up" else 0,
        value={
            "rating": row["rating"],
            "issue_categories": categories,
            "human_review": bool(row.get("consent_human_review")),
        },
        comment="\n\n".join(t for t in (row.get("comment"), *notes) if t) or None,
        vault_secrets=vault_secrets,
    )


def remove_rating(conversation_feedback_id: str) -> None:
    if not tracing_enabled():
        return
    from langsmith.run_trees import get_cached_client
    from langsmith.utils import LangSmithNotFoundError

    try:
        get_cached_client().delete_feedback(_feedback_id("rating", conversation_feedback_id))
    except LangSmithNotFoundError:
        pass
    except Exception:
        logger.warning("LangSmith rating %s not removed", conversation_feedback_id, exc_info=True)


def flag_report(*, report_id: str, run_id: str, error_type: Optional[str], note: Optional[str]) -> None:
    """Mark the run a failed-turn report is about. ``note`` is the report's,
    already scrubbed of the user's secrets."""
    if not tracing_enabled():
        return
    _create(
        _feedback_id("report", report_id),
        run_id=run_id,
        key="user_report",
        score=0,
        value={"error_type": error_type},
        comment=note,
    )


def _feedback_id(kind: str, source_id: Any) -> UUID:
    return uuid5(_FEEDBACK_NAMESPACE, f"{kind}:{source_id}")


def _create(
    feedback_id: UUID,
    *,
    run_id: str,
    key: str,
    score: int,
    value: dict,
    comment: Optional[str],
    vault_secrets: Optional[dict[str, str]] = None,
) -> None:
    from langsmith.run_trees import get_cached_client

    from src.server.utils.secret_redactor import scrub_outbound_text

    try:
        get_cached_client().create_feedback(
            run_id,
            key,
            score=score,
            value=value,
            comment=scrub_outbound_text(comment, vault_secrets) if comment else None,
            feedback_id=feedback_id,
            session_id=_tracer_project()[0],
        )
    except Exception:
        logger.warning("LangSmith %s feedback on run %s not written", key, run_id, exc_info=True)
