import React, { Suspense, useCallback, useMemo } from 'react';
import { ExternalLink, TextSelect } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { ChartToolButton } from '@/pages/MarketView/components/ChartToolButton';
import type { ChartTabSpec, ContextPayload } from './types';
import type { FileTab, FileTabsApi } from './useFileTabs';
import './ChartTab.css';

// The chart stack (lightweight-charts, html2canvas) stays out of the panel's
// bundle until a chart tab is looked at.
const MarketChartSurface = React.lazy(() =>
  import('@/pages/MarketView/components/MarketChartSurface').then((m) => ({ default: m.MarketChartSurface })),
);

interface ChartTabProps {
  tab: Extract<FileTab, { kind: 'chart' }>;
  tabs: FileTabsApi;
  workspaceId: string;
  onAddContext: ((ctx: ContextPayload) => void) | null;
  /** Leaves the panel for the full MarketView page on this symbol. */
  onOpenInMarketView: ((spec: ChartTabSpec) => void) | null;
}

/**
 * One surface per active chart: its websocket and drawn state are its own, so
 * a tab switch away tears it down and coming back reopens it on the remembered
 * interval. A symbol switch is followed in place, the way the MarketView page
 * does it: the surface resubscribes, reloads bars and resyncs annotations off
 * the prop, so nothing here remounts it. The chart's own header carries the
 * ticker (which changes the symbol in place) and the panel's two actions, so
 * it needs no crumb row.
 */
export function ChartTab({ tab, tabs, workspaceId, onAddContext, onOpenInMarketView }: ChartTabProps): React.ReactElement {
  const { t } = useTranslation();
  const { id, symbol, timeframe } = tab;
  // An artifact from another workspace opens on that workspace's drawings.
  const chartWorkspaceId = tab.workspaceId ?? workspaceId;
  const { patchTab, retargetChart } = tabs;

  // The chart is a live view, so the context it hands over is a pointer to
  // what the user is looking at, not a copy of the data behind it.
  const addToContext = useCallback(() => {
    onAddContext?.({
      label: `${symbol} · ${timeframe}`,
      snippet: t('filePanel.chartContextHint', { symbol, timeframe }),
      source: 'chart',
    });
  }, [onAddContext, symbol, timeframe, t]);

  // The toolbar's interval switch is remembered on the tab, so coming back to
  // the chart finds it where it was left.
  const rememberInterval = useCallback((interval: string) => {
    patchTab(id, (x) => (x.kind === 'chart' ? { ...x, timeframe: interval } : x));
  }, [patchTab, id]);

  const onSwitchSymbol = useCallback((next: string) => retargetChart(id, next), [retargetChart, id]);

  // The page opens on the same drawings the tab shows.
  const openInMarketView = useCallback(
    () => onOpenInMarketView?.({ symbol, timeframe, workspaceId: tab.workspaceId }),
    [onOpenInMarketView, symbol, timeframe, tab.workspaceId],
  );

  const headerActions = useMemo(() => (
    <>
      {onAddContext && (
        <ChartToolButton onClick={addToContext} title={t('filePanel.addChartToContext')}>
          <TextSelect size={14} />
        </ChartToolButton>
      )}
      {onOpenInMarketView && (
        <ChartToolButton onClick={openInMarketView} title={t('filePanel.openInMarketView')}>
          <ExternalLink size={14} />
        </ChartToolButton>
      )}
    </>
  ), [onAddContext, addToContext, onOpenInMarketView, openInMarketView, t]);

  return (
    <Suspense fallback={null}>
      <div className="file-panel-chart-pane">
        <MarketChartSurface
          symbol={symbol}
          timeframe={timeframe}
          workspaceId={chartWorkspaceId}
          onIntervalChange={rememberInterval}
          onSwitchSymbol={onSwitchSymbol}
          headerActions={headerActions}
          // Picks go out with the thread's composer, so only a host with one gets the tools.
          selectionTools={onAddContext != null}
          variant="compact"
        />
      </div>
    </Suspense>
  );
}
