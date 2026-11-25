import { memo, useEffect, useMemo, useState, type RefObject } from 'react';
import { useTranslation } from 'react-i18next';
import type { IChartApi, ISeriesApi } from 'lightweight-charts';
import { useLatestRef } from '@/hooks/useLatestRef';
import type { ChartLayersRender } from '@/pages/MarketView/hooks/useChartLayers';
import type { CommentUi } from '../../html/comments/useCommentUi';
import { ViewerComments } from '../ViewerComments';
import { chartPictures } from './chartPictures';
import { chartPlane, type LiveChartPlane } from './chartPlane';
import { chartSpots } from './chartSpots';

/** Where a chart's comments are kept and sent from: the controls of the surface it is on. */
export interface ChartCommentsHost {
  ui: CommentUi;
  /** The chart's own id among the surface's comment targets (a panel tab's id). */
  tabId: string;
  /** The chart is in front: keys and asks are its. */
  active: boolean;
}

/**
 * The owner's comments on a chart: the comment layer every viewer has, over
 * the chart's price pane. In comment mode a drag draws a region, a click on
 * a shape a layer drew picks that shape, and a click anywhere else on the
 * chart pins a point; each is saved in the chart's own times and prices, and
 * a region's picture is written for the agent as its comment is saved.
 *
 * Memoized, and handed only what changes with the chart's grid or layers:
 * its host repaints on every live bar, and the layer under it need not.
 */
export const ChartComments = memo(function ChartComments({
  host, workspaceId, chartId, chart, seriesRef, content, grid, identities, primitiveRef, seriesKeysRef, interval,
}: {
  host: ChartCommentsHost;
  /** Whose comments they are: the workspace whose layers the chart draws. */
  workspaceId: string;
  /** `SYMBOL:timeframe`, as its layers are keyed. */
  chartId: string;
  chart: IChartApi | null;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  /** The box the chart is drawn in, which the comment layer covers. */
  content: HTMLElement | null;
  grid: ChartLayersRender['grid'];
  identities: ChartLayersRender['identities'];
  primitiveRef: ChartLayersRender['primitiveRef'];
  seriesKeysRef: ChartLayersRender['seriesKeysRef'];
  interval: string;
}) {
  const { t } = useTranslation();
  // Read as each mark is placed: the bars and the layers move under the plane.
  const live = useLatestRef({ grid, identities, interval });
  const [plane, setPlane] = useState<LiveChartPlane | null>(null);
  useEffect(() => {
    const series = seriesRef.current;
    if (!chart || !series) return;
    const made = chartPlane({
      chart,
      series,
      grid: () => live.current.grid,
      identities: () => live.current.identities,
      interval: () => live.current.interval,
      primitive: () => primitiveRef.current,
      seriesKeys: () => seriesKeysRef.current,
    });
    setPlane(made);
    return () => {
      made.dispose();
      setPlane(null);
    };
  }, [chart, seriesRef, live, primitiveRef, seriesKeysRef]);

  const strategy = useMemo(() => (plane && content ? chartSpots(plane, content) : null), [plane, content]);
  const pictures = useMemo(() => {
    if (!plane || !chart || !content) return null;
    return chartPictures(plane, () => {
      try {
        const canvas = chart.takeScreenshot();
        return content.clientWidth ? { canvas, scale: canvas.width / content.clientWidth } : null;
      } catch {
        return null;
      }
    });
  }, [plane, chart, content]);

  return (
    <ViewerComments
      scroller={content}
      content={content}
      strategy={strategy}
      target={{ workspaceId, path: chartId, kind: 'chart', tabId: host.tabId, ui: host.ui, active: host.active }}
      modeHint={t('pageViewer:comments.modeHintChart')}
      pictures={pictures}
      // The region drawn stays in view.
      keepSpotClear
    />
  );
});
