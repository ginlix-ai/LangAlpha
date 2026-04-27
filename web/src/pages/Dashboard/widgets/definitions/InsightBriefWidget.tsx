import { Sparkles } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import AIDailyBriefCard, { getCachedInsights } from '../../components/AIDailyBriefCard';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import {
  useWidgetContextExport,
  type WidgetContextSnapshot,
} from '../framework/contextSnapshot';
import {
  serializeInsightDayToMarkdown,
  wrapWidgetContext,
} from '../framework/snapshotSerializers';
import { buildInsightSnapshot, normalizeInsight } from '../../utils/insightFetch';
import { InsightBriefConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';
import './InsightBriefWidget.css';

type InsightBriefConfig = { variant?: 'latest' | 'personalized' };

interface CachedInsight {
  market_insight_id: string;
  type: string;
  headline: string;
  summary: string;
  completed_at?: string;
  topics?: Array<{ text: string; trend: 'up' | 'down' | 'neutral' }>;
  [key: string]: unknown;
}

function InsightBriefWidget({ instance }: WidgetRenderProps<InsightBriefConfig>) {
  const { t } = useTranslation();
  const titleKey = 'dashboard.widgets.insightBrief.title';
  useWidgetContextExport(instance.id, {
    full: (): WidgetContextSnapshot => {
      const cached = (getCachedInsights() as CachedInsight[] | null) ?? [];
      const titleResolved = t(titleKey);

      if (!cached.length) {
        // Brief hasn't loaded yet. Emit a config-only directive so the agent
        // at least knows what the user is looking at.
        return {
          widget_type: 'insight.brief',
          widget_id: instance.id,
          label: titleResolved,
          captured_at: new Date().toISOString(),
          text: wrapWidgetContext('insight.brief', {}, `Widget: ${titleResolved}\n_(Brief data not yet loaded.)_`),
          data: { config: instance.config },
        };
      }

      const items = cached.map((it) => normalizeInsight(it));
      const personalizedCount = items.filter((it) => it.type === 'personalized').length;
      const body = `Widget: ${titleResolved}\n\n${serializeInsightDayToMarkdown(items)}`;
      const latest = items[0];
      const descParts = [`${items.length} brief${items.length === 1 ? '' : 's'}`];
      if (personalizedCount) descParts.push(`${personalizedCount} personalized`);

      return {
        widget_type: 'insight.brief',
        widget_id: instance.id,
        label: latest.headline,
        description: descParts.join(' · '),
        captured_at: new Date().toISOString(),
        text: wrapWidgetContext(
          'insight.brief',
          {
            count: items.length,
            personalized: personalizedCount || undefined,
            latestType: latest.type,
            completedAt: latest.completed_at,
          },
          body,
        ),
        data: { items, count: items.length, personalizedCount },
      };
    },
    rows: async (rowId: string) => {
      const cached = (getCachedInsights() as CachedInsight[] | null) ?? [];
      const item = cached.find((i) => i.market_insight_id === rowId);
      if (!item) return null;
      return buildInsightSnapshot({
        instanceId: instance.id,
        rowId,
        insightId: rowId,
        fallback: normalizeInsight(item),
      });
    },
  });

  const { modals } = useDashboardContext();
  return (
    <div className="insight-brief-widget">
      <AIDailyBriefCard onReadFull={modals.openInsight} instanceId={instance.id} />
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
  fitToContent: true,
});

export default InsightBriefWidget;
