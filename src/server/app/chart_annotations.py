"""Chart layers API router.

Workspace-scoped endpoints for reading and clearing the chart layers the
agent's code published. A chart instance is ``(workspace_id, chart_id)`` where
``chart_id = "{SYMBOL}:{timeframe}"`` — persisted durably in Postgres (see
``src/server/database/chart_annotation.py``). The paths keep their first name,
from the per-drawing annotations layers replaced.

Writes happen through the agent only — this router reads and deletes.

Endpoints:
- GET    /api/v1/workspaces/{workspace_id}/chart-annotations?symbol=&timeframe=
- DELETE /api/v1/workspaces/{workspace_id}/chart-annotations?symbol=&timeframe=
- DELETE /api/v1/workspaces/{workspace_id}/chart-annotations/items/{layer_id}?symbol=&timeframe=
"""

import logging

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel

from src.server.database.chart_annotation import clear_chart, list_charts, remove_layers
from src.server.models.chart_layer import make_chart_id
from src.server.database.workspace import get_workspace as db_get_workspace
from src.server.dependencies.principal import DataReadUserId
from src.server.models.user import normalize_symbol
from src.server.utils.api import CurrentUserId, require_workspace_owner
from src.tools.chart_annotation.schemas import Timeframe

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/workspaces", tags=["Chart Layers"])


class StoredLayer(BaseModel):
    """One layer: its address (``LayerRef``) and the layer body."""

    workspace_id: str
    chart_id: str
    symbol: str
    timeframe: str
    layer_id: str
    layer: dict


class ChartInstance(BaseModel):
    chart_id: str
    symbol: str
    timeframe: str
    layers: list[StoredLayer]


class ChartLayerListResponse(BaseModel):
    workspace_id: str
    charts: list[ChartInstance]


class ChartLayerClearResponse(BaseModel):
    workspace_id: str
    chart_id: str
    cleared: int


class ChartLayerRemoveResponse(BaseModel):
    workspace_id: str
    chart_id: str
    layer_id: str
    removed: int


def _normalize_symbol(symbol: str) -> str:
    sym = normalize_symbol(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required")
    return sym


async def _ensure_owner(workspace_id: str, user_id: str) -> None:
    """Look up the workspace and verify the caller owns it (404 / 403)."""
    workspace = await db_get_workspace(workspace_id)
    require_workspace_owner(workspace, user_id=user_id)


@router.get(
    "/{workspace_id}/chart-annotations",
    response_model=ChartLayerListResponse,
)
async def list_chart_layers(
    workspace_id: str,
    x_user_id: DataReadUserId,
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
    return ChartLayerListResponse(
        workspace_id=workspace_id,
        charts=[ChartInstance(**c) for c in charts],
    )


@router.delete(
    "/{workspace_id}/chart-annotations",
    response_model=ChartLayerClearResponse,
)
async def clear_chart_layers(
    workspace_id: str,
    x_user_id: CurrentUserId,
    symbol: str = Query(..., max_length=32, description="Ticker symbol (case-insensitive)"),
    timeframe: Timeframe = Query("1day", description="Chart interval"),
):
    """Delete every layer on one chart instance (workspace + symbol + timeframe).

    ``timeframe`` is constrained to the supported set (it is part of the chart
    identity, unlike the GET filter) so a typo yields 422 rather than a silent
    no-op delete.
    """
    await _ensure_owner(workspace_id, x_user_id)
    sym = _normalize_symbol(symbol)
    chart_id = make_chart_id(sym, timeframe)
    cleared = await clear_chart(workspace_id, chart_id)
    return ChartLayerClearResponse(
        workspace_id=workspace_id,
        chart_id=chart_id,
        cleared=cleared,
    )


@router.delete(
    "/{workspace_id}/chart-annotations/items/{layer_id}",
    response_model=ChartLayerRemoveResponse,
)
async def remove_chart_layer(
    workspace_id: str,
    x_user_id: CurrentUserId,
    layer_id: str = Path(..., min_length=1, max_length=64),
    symbol: str = Query(..., max_length=32, description="Ticker symbol (case-insensitive)"),
    timeframe: Timeframe = Query("1day", description="Chart interval"),
):
    """Delete one chart layer, by its id, from one chart instance.

    ``removed`` is 0 when it was already gone, so a repeated delete (another
    tab, a retry) succeeds rather than erroring.
    """
    await _ensure_owner(workspace_id, x_user_id)
    sym = _normalize_symbol(symbol)
    chart_id = make_chart_id(sym, timeframe)
    removed = await remove_layers(workspace_id, chart_id, [layer_id])
    return ChartLayerRemoveResponse(
        workspace_id=workspace_id,
        chart_id=chart_id,
        layer_id=layer_id,
        removed=len(removed),
    )
