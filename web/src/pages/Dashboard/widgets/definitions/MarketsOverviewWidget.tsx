import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { LineChart } from 'lucide-react';
import IndexMovementCard from '../../components/IndexMovementCard';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import {
  serializeRowsToMarkdown,
  wrapWidgetContext,
} from '../framework/snapshotSerializers';
import { MarketsOverviewConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';

type MarketsOverviewConfig = { indices?: string[] };

// Below this cell width the 5-tile desktop grid wraps to ≥3 rows and looks
// worse than the mobile swipe stack, so switch to the compact variant.
const COMPACT_WIDTH_PX = 640;

function MarketsOverviewWidget({ instance }: WidgetRenderProps<MarketsOverviewConfig>) {
  const { t } = useTranslation();
  const { dashboard } = useDashboardContext();

  useWidgetContextExport(instance.id, {
    full: () => {
      const indices = dashboard.indices ?? [];
      const rows = indices.map((idx) => ({
        symbol: idx.symbol,
        name: idx.name,
        price: idx.price,
        change: idx.change,
        changePercent: idx.changePercent,
      }));
      const fmtNum = (v: unknown) => (typeof v === 'number' ? v.toFixed(2) : '');
      const fmtPct = (v: unknown) => (typeof v === 'number' ? `${v.toFixed(2)}%` : '');
      const body = rows.length
        ? serializeRowsToMarkdown(rows, [
            { key: 'symbol', label: 'symbol' },
            { key: 'name', label: 'name' },
            { key: 'price', label: 'price', format: fmtNum },
            { key: 'change', label: 'change', format: fmtNum },
            { key: 'changePercent', label: 'change%', format: fmtPct },
          ])
        : '_no indices_';
      const text = wrapWidgetContext('markets.overview', { count: rows.length }, body);
      return {
        widget_type: 'markets.overview',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.marketsOverview.title')} · ${rows.length}`,
        description: rows.length ? `${rows.length} ${rows.length === 1 ? 'index' : 'indices'}` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { indices },
      };
    },
  });
  const containerRef = useRef<HTMLDivElement>(null);
  const [compact, setCompact] = useState(false);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const update = () => setCompact(el.clientWidth > 0 && el.clientWidth < COMPACT_WIDTH_PX);
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return (
    <div ref={containerRef} className="h-full">
      <IndexMovementCard
        indices={dashboard.indices}
        loading={dashboard.indicesLoading}
        forceMobile={compact}
      />
    </div>
  );
}

registerWidget<MarketsOverviewConfig>({
  type: 'markets.overview',
  titleKey: 'dashboard.widgets.marketsOverview.title',
  descriptionKey: 'dashboard.widgets.marketsOverview.description',
  category: 'markets',
  icon: LineChart,
  component: MarketsOverviewWidget,
  defaultConfig: {},
  configSchema: MarketsOverviewConfigSchema,
  defaultSize: { w: 12, h: 11 },
  minSize: { w: 3, h: 11 },
  maxSize: { w: 12, h: 11 },
  singleton: true,
});

export default MarketsOverviewWidget;
