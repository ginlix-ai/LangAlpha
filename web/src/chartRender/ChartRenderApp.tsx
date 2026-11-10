/**
 * The MarketView chart, alone, for a snapshot taken in the sandbox. It is the
 * page's own `MarketChart` wired the way `MarketChartSurface` wires it (no live
 * feed or quote: a snapshot is one frame), so what the agent sees is what the
 * user's chart paints. Every request the chart makes is answered by the
 * snapshot driver (chartkit's `_renderer.py`, through Playwright routing), so
 * this page holds no data of its own.
 */
import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';

import { displaySpelling } from '@/lib/bars/exchanges';
import MarketChart, { type MarketChartHandle } from '@/pages/MarketView/components/MarketChart';
import type { LayerFrameStats } from '@/pages/MarketView/utils/chartLayer/layerReport';

import type { ChartRenderFrame, ChartRenderFrameStatus, ChartRenderPayload, ChartRenderView } from './payload';

/**
 * Frame the chart through its own handle, the calls the live page makes. One
 * whose bars or layer are not in yet is `pending`, which the driver retries.
 */
function applyFrame(handle: MarketChartHandle | null, frame: ChartRenderFrame): ChartRenderFrameStatus {
  if (!handle) return 'pending';
  if (frame.kind === 'layer') return handle.frameLayer([frame.layerId]) ? 'ok' : 'pending';
  return handle.showTimeRange(frame.from, frame.to) ? 'ok' : 'pending';
}

function currentView(handle: MarketChartHandle | null): ChartRenderView | null {
  const ctx = handle?.getViewForContext() ?? null;
  return ctx ? { from: ctx.from, to: ctx.to, width: ctx.width, height: ctx.height } : null;
}

export function ChartRenderApp({ payload }: { payload: ChartRenderPayload }): React.ReactElement {
  const { timeframe, workspaceId, width, height } = payload;

  // The layer renderer's report, numbered and published on
  // `window.__LA_CHART_REPORT__` for the driver to read with the picture. Only
  // this page asks the chart for one.
  const reportFrame = useRef(0);
  const publishReport = React.useCallback((stats: LayerFrameStats | null) => {
    if (!stats) {
      delete window.__LA_CHART_REPORT__;
      return;
    }
    reportFrame.current += 1;
    window.__LA_CHART_REPORT__ = { frame: reportFrame.current, ...stats };
  }, []);
  // The spelling the server keys a chart by (`600519.SH`, never `.SS`), which
  // the driver echoes back as the chart's id.
  const symbol = displaySpelling(payload.symbol);

  const [stockMeta, setStockMeta] = useState<Record<string, unknown> | null>(null);
  const handleStockMeta = React.useCallback((meta: unknown) => {
    setStockMeta(meta as Record<string, unknown> | null);
  }, []);

  // The driver frames the chart once its bars and layer are drawn.
  const chartRef = useRef<MarketChartHandle>(null);
  useEffect(() => {
    window.__LA_CHART_RENDER_FRAME__ = (frame) => applyFrame(chartRef.current, frame);
    window.__LA_CHART_RENDER_VIEW__ = () => currentView(chartRef.current);
    return () => {
      delete window.__LA_CHART_RENDER_FRAME__;
      delete window.__LA_CHART_RENDER_VIEW__;
    };
  }, []);

  // `height` is the chart region's, which is what the snapshot keeps: the host
  // grows by the toolbar and bottom bar around it. Measured before the chart's
  // bars can arrive, so the default view is framed at the final size.
  const hostRef = useRef<HTMLDivElement>(null);
  const [hostHeight, setHostHeight] = useState(height);
  useLayoutEffect(() => {
    const region = hostRef.current?.querySelector<HTMLElement>('.charts-container');
    if (!region) return;
    const chrome = hostHeight - region.getBoundingClientRect().height;
    if (chrome > 0 && hostHeight !== height + chrome) setHostHeight(height + chrome);
  }, [height, hostHeight]);

  return (
    <div
      ref={hostRef}
      data-chart-render-host=""
      style={{ width, height: hostHeight, display: 'flex', background: 'var(--color-bg-card)' }}
    >
      <MarketChart
        ref={chartRef}
        symbol={symbol}
        interval={timeframe}
        workspaceId={workspaceId}
        onStockMeta={handleStockMeta}
        quoteData={null}
        earningsData={null}
        overlayData={null}
        stockMeta={stockMeta}
        liveTick={null}
        wsStatus="disconnected"
        marketStatus={null}
        snapshot={null}
        defaultView="centered"
        onLayerFrame={publishReport}
      />
    </div>
  );
}
