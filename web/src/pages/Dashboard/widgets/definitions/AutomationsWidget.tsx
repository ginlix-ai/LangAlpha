import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { relativeTime } from '@/lib/format';
import {
  Workflow,
  ArrowUpRight,
  Pause,
  Play,
  Zap,
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { StatusGlyph } from '@/pages/Automations/components/StatusMark';
import { useAutomations } from '@/pages/Automations/hooks/useAutomations';
import { useAutomationMutations } from '@/pages/Automations/hooks/useAutomationMutations';
import { useOrderedGroups, type OrderedGroup } from '@/pages/Automations/hooks/useOrderedGroups';
import {
  automationActions,
  automationCensus,
  automationState,
  automationStatusUi,
  GROUP_LABEL_KEY,
  rowTrailing,
  type AutomationCensus,
  type AutomationGroup,
} from '@/pages/Automations/utils/status';
import type { Automation } from '@/types/automation';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import { serializeRowsToMarkdown, wrapWidgetContext } from '../framework/snapshotSerializers';
import { AutomationsConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';

type AutomationsConfig = { limit?: number };

/** The census in the page's own words and order, for the agent's copy. */
const CENSUS_WORDS: ReadonlyArray<[keyof AutomationCensus, string]> = [
  ['attention', 'need attention'],
  ['running', 'running'],
  ['scheduled', 'scheduled'],
  ['watching', 'watching'],
  ['paused', 'paused'],
  ['off', 'switched off'],
  ['finished', 'finished'],
];

function triggerLabel(a: Automation): string {
  if (a.trigger_type === 'price') {
    const sym = a.trigger_config?.symbol;
    return sym
      ? i18n.t('dashboard.widgets.automations.trigger.priceSymbol', { symbol: sym })
      : i18n.t('dashboard.widgets.automations.trigger.price');
  }
  if (a.trigger_type === 'once') return i18n.t('dashboard.widgets.automations.trigger.once');
  if (a.cron_expression) return i18n.t('dashboard.widgets.automations.trigger.cron');
  return i18n.t('dashboard.widgets.automations.trigger.auto');
}

function AutomationRow({
  automation,
  group,
  onOpen,
  onToggle,
  onRun,
  busy,
}: {
  automation: Automation;
  group: AutomationGroup;
  onOpen: () => void;
  onToggle: () => void;
  onRun: () => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  const { canPause, canResume, canRun, runBusy } = automationActions(automation);

  // The Automations list's rule, so the two never read differently; a watch
  // has no quote here and reads as its last run.
  const ui = automationStatusUi(automation);
  const row = rowTrailing(automation, group);
  const rightText = !row
    ? ''
    : row.kind === 'state'
      ? t(row.labelKey)
      : row.kind === 'next'
        ? relativeTime(row.at)
        : row.at
          ? t('dashboard.widgets.automations.lastRun', { when: relativeTime(row.at) })
          : '';

  return (
    <div
      className="group relative w-full flex items-center gap-3 py-2 pr-2 pl-3 rounded-md transition-colors duration-150"
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
      }}
    >
      <button
        type="button"
        onClick={onOpen}
        className="flex-1 min-w-0 flex items-center gap-3 text-left"
      >
        <span className="flex w-3 flex-shrink-0 justify-center">
          <StatusGlyph ui={ui} label={t(ui.labelKey)} size={12} />
        </span>
        <span className="flex-1 min-w-0 flex flex-col gap-0.5 overflow-hidden">
          <span
            className="text-[0.8125rem] truncate leading-tight font-medium"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {automation.name || t('dashboard.widgets.automations.untitled')}
          </span>
          <span
            className="text-[0.625rem] uppercase tracking-wider truncate"
            style={{ color: 'var(--color-text-tertiary)', opacity: 0.85 }}
          >
            {triggerLabel(automation)}
          </span>
        </span>
      </button>

      {rightText ? (
        <span
          className="text-[0.6563rem] dashboard-mono uppercase tracking-wider tabular-nums flex-shrink-0 group-hover:opacity-0 transition-opacity"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {rightText}
        </span>
      ) : null}

      <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
        {canRun && (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onRun();
            }}
            disabled={busy || runBusy}
            title={t('dashboard.widgets.automations.runNow')}
            aria-label={t('dashboard.widgets.automations.runNow')}
            className="p-1 rounded-md transition-colors disabled:opacity-50"
            style={{ color: 'var(--color-text-secondary)' }}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = 'var(--color-bg-subtle)';
              e.currentTarget.style.color = 'var(--color-text-primary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = 'transparent';
              e.currentTarget.style.color = 'var(--color-text-secondary)';
            }}
          >
            <Zap className="h-3.5 w-3.5" fill="currentColor" />
          </button>
        )}
        {(canPause || canResume) && (
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
            disabled={busy}
            title={canPause ? t('dashboard.widgets.automations.pause') : t('dashboard.widgets.automations.resume')}
            aria-label={canPause ? t('dashboard.widgets.automations.pauseAria') : t('dashboard.widgets.automations.resumeAria')}
            className="p-1 rounded-md transition-colors disabled:opacity-50"
            style={{ color: 'var(--color-text-secondary)' }}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = 'var(--color-bg-subtle)';
              e.currentTarget.style.color = 'var(--color-text-primary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = 'transparent';
              e.currentTarget.style.color = 'var(--color-text-secondary)';
            }}
          >
            {canPause ? <Pause className="h-3.5 w-3.5" /> : <Play className="h-3.5 w-3.5" />}
          </button>
        )}
      </div>
    </div>
  );
}

function BucketSection({
  group,
  items,
  onOpen,
  onToggle,
  onRun,
  busy,
}: {
  group: AutomationGroup;
  items: Automation[];
  onOpen: (a: Automation) => void;
  onToggle: (a: Automation) => void;
  onRun: (a: Automation) => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  if (items.length === 0) return null;
  return (
    <div className="flex flex-col">
      <div
        className="flex items-baseline gap-2 px-3 mb-0.5 sticky top-0 z-10 py-1 -mx-1"
        style={{ backgroundColor: 'var(--color-bg-card)' }}
      >
        <span
          className="text-[0.5938rem] font-semibold uppercase tracking-[0.16em]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t(GROUP_LABEL_KEY[group])}
        </span>
        <span
          className="flex-1 h-px"
          style={{ backgroundColor: 'var(--color-border-muted)' }}
        />
        <span
          className="text-[0.625rem] dashboard-mono tabular-nums"
          style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}
        >
          {String(items.length).padStart(2, '0')}
        </span>
      </div>
      <div className="flex flex-col">
        {items.map((a) => (
          <AutomationRow
            key={a.automation_id}
            automation={a}
            group={group}
            busy={busy}
            onOpen={() => onOpen(a)}
            onToggle={() => onToggle(a)}
            onRun={() => onRun(a)}
          />
        ))}
      </div>
    </div>
  );
}

function AutomationsWidget({ instance }: WidgetRenderProps<AutomationsConfig>) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const limit = instance.config.limit ?? 8;

  const { automations, loading } = useAutomations();
  const { pause, resume, trigger, busy } = useAutomationMutations();

  useWidgetContextExport(instance.id, {
    full: () => {
      const tableRows = automations.map((a) => ({
        name: a.name || t('dashboard.widgets.automations.untitled'),
        trigger: triggerLabel(a),
        state: automationState(a),
        next_run: a.next_run_at ?? '',
        last_run: a.last_run_at ?? '',
      }));
      // The page header's census, so the agent reads the states the reader sees.
      const counts = { total: automations.length, ...automationCensus(automations) };
      const summary = CENSUS_WORDS.filter(([key]) => counts[key])
        .map(([key, word]) => `${counts[key]} ${word}`)
        .join(', ');
      const body = automations.length
        ? (summary ? `**${summary}**\n\n` : '') +
          serializeRowsToMarkdown(tableRows, [
            { key: 'name', label: 'name' },
            { key: 'trigger', label: 'trigger' },
            { key: 'state', label: 'state' },
            { key: 'next_run', label: 'next run' },
            { key: 'last_run', label: 'last run' },
          ])
        : '_no automations_';
      const text = wrapWidgetContext('automations.list', counts, body);
      return {
        widget_type: 'automations.list',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.automations.title')} · ${counts.total}`,
        description: counts.total ? summary || `${counts.total} automations` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { counts, automations },
      };
    },
  });

  // The Automations page's own groups, in its order, so what needs attention
  // leads and the limit trims from the finished end.
  const groups = useOrderedGroups(automations);
  const shownGroups = useMemo(() => {
    const out: OrderedGroup[] = [];
    let room = limit;
    for (const { group, items } of groups) {
      if (room <= 0) break;
      out.push({ group, items: items.slice(0, room) });
      room -= items.length;
    }
    return out;
  }, [groups, limit]);

  const total = automations.length;

  const handleOpen = (a: Automation) => {
    navigate(`/automations?id=${encodeURIComponent(a.automation_id)}`);
  };

  const handleToggle = (a: Automation) => {
    if (automationActions(a).canPause) pause.mutate(a.automation_id);
    else resume.mutate(a.automation_id);
  };

  const handleRun = (a: Automation) => trigger.mutate(a.automation_id);

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Workflow
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[0.625rem] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.automations.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {total}
          </span>
        </div>
        <button
          type="button"
          onClick={() => navigate('/automations')}
          className="group flex items-center gap-1 text-[0.6875rem] uppercase tracking-wider transition-colors"
          style={{ color: 'var(--color-text-tertiary)' }}
          onMouseEnter={(e) => {
            e.currentTarget.style.color = 'var(--color-text-primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.color = 'var(--color-text-tertiary)';
          }}
        >
          <span>{t('dashboard.widgets.automations.viewAll')}</span>
          <ArrowUpRight className="h-3 w-3 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
        </button>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
        {loading && total === 0 ? (
          <div className="space-y-1.5 py-1">
            {Array.from({ length: 5 }).map((_, i) => (
              <div
                key={i}
                className="h-8 rounded animate-pulse"
                style={{
                  backgroundColor: 'var(--color-bg-subtle)',
                  opacity: 1 - i * 0.1,
                }}
              />
            ))}
          </div>
        ) : total === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-3 py-6">
            <div
              className="h-9 w-9 rounded-full flex items-center justify-center"
              style={{ backgroundColor: 'var(--color-bg-subtle)' }}
            >
              <Workflow className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <div className="text-center">
              <div
                className="dashboard-mono text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('dashboard.widgets.automations.empty')}
              </div>
              <button
                type="button"
                onClick={() => navigate('/automations')}
                className="text-[0.6875rem] uppercase tracking-wider underline-offset-4 hover:underline"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {t('dashboard.widgets.automations.emptyCta')}
              </button>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            {shownGroups.map(({ group, items }) => (
              <BucketSection
                key={group}
                group={group}
                items={items}
                busy={busy}
                onOpen={handleOpen}
                onToggle={handleToggle}
                onRun={handleRun}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

registerWidget<AutomationsConfig>({
  type: 'automations.list',
  titleKey: 'dashboard.widgets.automations.title',
  descriptionKey: 'dashboard.widgets.automations.description',
  category: 'personal',
  icon: Workflow,
  component: AutomationsWidget,
  defaultConfig: { limit: 8 },
  configSchema: AutomationsConfigSchema,
  defaultSize: { w: 4, h: 22 },
  minSize: { w: 3, h: 14 },
});

export default AutomationsWidget;
