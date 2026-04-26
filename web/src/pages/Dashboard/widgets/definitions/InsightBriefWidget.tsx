import { Sparkles } from 'lucide-react';
import AIDailyBriefCard from '../../components/AIDailyBriefCard';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { InsightBriefConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';
import './InsightBriefWidget.css';

type InsightBriefConfig = { variant?: 'latest' | 'personalized' };

function InsightBriefWidget(_props: WidgetRenderProps<InsightBriefConfig>) {
  const { modals } = useDashboardContext();
  return (
    <div className="insight-brief-widget">
      <AIDailyBriefCard onReadFull={modals.openInsight} />
    </div>
  );
}

registerWidget<InsightBriefConfig>({
  type: 'insight.brief',
  titleKey: 'dashboard.widgets.insightBrief.title',
  descriptionKey: 'dashboard.widgets.insightBrief.description',
  category: 'intel',
  icon: Sparkles,
  component: InsightBriefWidget,
  defaultConfig: { variant: 'latest' },
  configSchema: InsightBriefConfigSchema,
  defaultSize: { w: 8, h: 18 },
  minSize: { w: 4, h: 15 },
  maxSize: { w: 12, h: 44 },
  singleton: true,
  // fitToContent: cell height tracks the card's natural content height so
  // nothing ever clips regardless of the user's chosen width. WidgetFrame
  // debounces fit-height commits so AnimatePresence's expand/collapse
  // animation inside AIDailyBriefCard doesn't cause per-frame cell
  // re-layouts (which would race with RGL's own height transitions).
  fitToContent: true,
});

export default InsightBriefWidget;
