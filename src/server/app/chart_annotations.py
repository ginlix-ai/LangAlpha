"""Chart Annotation API Router.

Workspace-scoped endpoints for reading and clearing chart annotations the
agent drew. A chart instance is ``(workspace_id, chart_id)`` where
``chart_id = "{SYMBOL}:{timeframe}"`` — persisted durably in Postgres (see
``src/server/database/chart_annotation.py``).

Writes happen through the agent tool only — this router is read / bulk
delete only.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/chart-annotations?symbol=&timeframe=
- DELETE /api/v1/workspaces/{workspace_id}/chart-annotations?symbol=&timeframe=
"""

import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.server.database.chart_annotation import (
    clear_chart,
    list_charts,
    make_chart_id,
)
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.tools.chart_annotation.schemas import Timeframe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Chart Annotations"])


class ChartInstance(BaseModel):
    chart_id: str
    symbol: str
    timeframe: str
    annotations: list[dict]


class ChartAnnotationListResponse(BaseModel):
    workspace_id: str
    charts: list[ChartInstance]


class ChartAnnotationClearResponse(BaseModel):
    workspace_id: str
    chart_id: str
    cleared: int


def _normalize_symbol(symbol: str) -> str:
    sym = symbol.strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    return sym


async def _ensure_owner(workspace_id: str, user_id: str) -> None:
    """Look up the workspace and verify the caller owns it (404 / 403)."""
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)


@router.get(
    "/{workspace_id}/chart-annotations",
    response_model=ChartAnnotationListResponse,
)
async def list_chart_annotations(
    workspace_id: str,
    x_user_id: CurrentUserId,
    symbol: str = Query(..., max_length=32, description="Ticker symbol (case-insensitive)"),
    timeframe: Timeframe | None = Query(
        None,
        description="Chart interval; omit to return every timeframe for the symbol",
    ),
):
    """List chart instances for a workspace + symbol (optionally one timeframe).

    ``timeframe`` is constrained to the supported set (symmetric with DELETE): a
    typo yields 422 rather than silently returning an empty list. Omit it to get
    every timeframe for the symbol.
    """
    await _ensure_owner(workspace_id, x_user_id)
    sym = _normalize_symbol(symbol)
    # ``timeframe`` is a validated ``Timeframe`` literal (no surrounding
    # whitespace possible), so it's passed straight through.
    charts = await list_charts(workspace_id, sym, timeframe)
    return ChartAnnotationListResponse(
        workspace_id=workspace_id,
        charts=[ChartInstance(**c) for c in charts],
    )


@router.delete(
    "/{workspace_id}/chart-annotations",
    response_model=ChartAnnotationClearResponse,
)
async def clear_chart_annotations(
    workspace_id: str,
    x_user_id: CurrentUserId,
    symbol: str = Query(..., max_length=32, description="Ticker symbol (case-insensitive)"),
    timeframe: Timeframe = Query("1day", description="Chart interval"),
):
    """Delete every annotation on one chart instance (workspace + symbol + timeframe).

    ``timeframe`` is constrained to the supported set (it is part of the chart
    identity, unlike the GET filter) so a typo yields 422 rather than a silent
    no-op delete.
    """
    await _ensure_owner(workspace_id, x_user_id)
    sym = _normalize_symbol(symbol)
    chart_id = make_chart_id(sym, timeframe)
    cleared = await clear_chart(workspace_id, chart_id)
    return ChartAnnotationClearResponse(
        workspace_id=workspace_id,
        chart_id=chart_id,
        cleared=cleared,
    )
