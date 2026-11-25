/**
 * The owner's comments on MarketChart in one place: whether comment mode is
 * on, the Comment tool for the toolbar, and the layer over the chart. Only
 * the Light chart draws them, so the Advanced chart turns the mode off.
 */
import { useEffect, useMemo, type ReactNode, type RefObject } from 'react';
import type { IChartApi, ISeriesApi } from 'lightweight-charts';
import { useTranslation } from 'react-i18next';
import { ChartComments, type ChartCommentsHost } from '@/pages/ChatAgent/components/viewers/comments/chart/ChartComments';
import { CommentTools } from '@/pages/ChatAgent/components/viewers/html/comments/CommentTools';
import type { ChartLayerDrawing } from './useChartLayerIntegration';

/** What the layer reads off the chart as it is drawn. */
interface ChartOn {
  chart: IChartApi | null;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  content: HTMLElement | null;
  drawing: ChartLayerDrawing;
  interval: string;
}

export interface ChartCommentsWiring {
  /** Comment mode is on: the chart's own explain card stands down. */
  mode: boolean;
  /** The Comment tool, or nothing where comments cannot be kept. */
  tools: ReactNode;
  /** The comment layer over the chart, or nothing off the Light chart. */
  layer: (on: ChartOn) => ReactNode;
}

export function useChartComments(
  comments: ChartCommentsHost | null,
  isCustom: boolean,
  { workspaceId, chartId }: { workspaceId: string | null | undefined; chartId: string | null },
): ChartCommentsWiring {
  const { t } = useTranslation();
  const mode = isCustom && !!comments && comments.ui.modeTab === comments.tabId;
  const { setMode } = comments?.ui ?? {};
  const tab = comments?.tabId;
  useEffect(() => {
    if (!isCustom && tab) setMode?.(tab, false);
  }, [isCustom, tab, setMode]);

  const kept = comments && workspaceId && chartId ? { comments, workspaceId, chartId } : null;
  // The same element while nothing it shows changes, so a live bar repainting the chart passes it by.
  const tools = useMemo(() => comments && workspaceId && chartId && (
    <div className="chart-tool-buttons">
      <CommentTools
        tabId={comments.tabId}
        workspaceId={workspaceId}
        target={{ kind: 'chart', path: chartId }}
        ui={comments.ui}
        variant="chart"
        modeTitle={t('pageViewer:comments.modeTitleChart')}
      />
    </div>
  ), [comments, workspaceId, chartId, t]);
  // Only the parts of the drawing the layer reads, each stable across live bars, reach the memoized layer.
  const layer = ({ chart, seriesRef, content, drawing: { render }, interval }: ChartOn) => kept && isCustom && (
    <ChartComments
      host={kept.comments}
      workspaceId={kept.workspaceId}
      chartId={kept.chartId}
      chart={chart}
      seriesRef={seriesRef}
      content={content}
      grid={render.grid}
      identities={render.identities}
      primitiveRef={render.primitiveRef}
      seriesKeysRef={render.seriesKeysRef}
      interval={interval}
    />
  );
  return { mode, tools, layer };
}
