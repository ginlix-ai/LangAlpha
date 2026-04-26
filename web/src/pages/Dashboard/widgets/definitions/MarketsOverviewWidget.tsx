import { useEffect, useRef, useState } from 'react';
import { LineChart } from 'lucide-react';
import IndexMovementCard from '../../components/IndexMovementCard';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { MarketsOverviewConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';

type MarketsOverviewConfig = { indices?: string[] };

// Below this cell width the 5-tile desktop grid wraps to ≥3 rows and looks
// worse than the mobile swipe stack, so switch to the compact variant.
const COMPACT_WIDTH_PX = 640;

function MarketsOverviewWidget(_props: WidgetRenderProps<MarketsOverviewConfig>) {
  const { dashboard } = useDashboardContext();
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
