"""Bug reports, feedback and failed-turn reports, forwarded to the platform.

See ``src/server/services/reports.py`` for what a report carries.
"""

from typing import Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.server.models.report import ReportCreate
from src.observability.langsmith_feedback import flag_report
from src.server.services.reports import (
    PlatformResult,
    ScrubUnavailable,
    build_report,
    post_report,
    reports_enabled,
)
from src.server.utils import api as auth_api
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1/reports", tags=["Reports"])


class ReportsConfig(BaseModel):
    enabled: bool


class ReportAccepted(BaseModel):
    report_id: str
    status: Literal["accepted", "duplicate"]


@router.get("/config", response_model=ReportsConfig)
async def get_reports_config(_user_id: CurrentUserId):
    return ReportsConfig(enabled=reports_enabled())


@router.post("", response_model=ReportAccepted, status_code=202)
async def create_report(
    body: ReportCreate, user_id: CurrentUserId, background: BackgroundTasks
):
    """Send a report. A retry with the same ``report_id`` is not posted twice.

    429 when the user has sent too many, 503 when it could not be posted now;
    both carry ``Retry-After`` when the platform gave one, and a retry keeps
    the ``report_id``. A failed turn's report also marks its run in
    LangSmith once the team has it.
    """
    if not reports_enabled():
        raise HTTPException(status_code=404, detail="Reports are not enabled")
    if body.thread_id:
        await auth_api.require_thread_owner(str(body.thread_id), user_id)

    try:
        report = await build_report(body, user_id=user_id)
    except ScrubUnavailable:
        return report_refusal(PlatformResult("unavailable"))
    result = await post_report(report)
    if result.ok:
        if report.run_id:
            background.add_task(
                flag_report,
                report_id=report.report_id,
                run_id=report.run_id,
                error_type=report.error.type if report.error else None,
                note=report.user_note,
            )
        return ReportAccepted(report_id=str(body.report_id), status=result.status)
    return report_refusal(result)


_REFUSALS = {
    "rate_limited": (429, "report_rate_limited"),
    "not_found": (409, "report_closed"),
    "closed": (409, "report_closed"),
    "note_limit": (409, "report_note_limit"),
}


def report_refusal(result: PlatformResult) -> JSONResponse:
    """What every report route answers when the platform did not take a call.
    A 429 or 503 carries ``Retry-After`` when the platform gave one."""
    status, error = _REFUSALS.get(result.status, (503, "report_unavailable"))
    headers = {"Retry-After": str(result.retry_after)} if result.retry_after else None
    return JSONResponse(
        status_code=status,
        content={"detail": {"error": error, "retry_after": result.retry_after}},
        headers=headers,
    )
