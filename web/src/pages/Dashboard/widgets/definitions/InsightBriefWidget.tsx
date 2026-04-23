import { Sparkles } from 'lucide-react';
import AIDailyBriefCard from '../../components/AIDailyBriefCard';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
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
  title: 'AI Daily Brief',
  description: 'Auto-generated market insight with a "Read Full Brief" CTA.',
  category: 'intel',
  icon: Sparkles,
  component: InsightBriefWidget,
  defaultConfig: { variant: 'latest' },
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
