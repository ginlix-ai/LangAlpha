import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { HeaderButton, ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import type { Automation, AutomationRun } from '@/types/automation';
import { useAgentModeLabels, useRunsAs, type AgentModeLabels } from '../hooks/useAgentModeLabels';
import { useAutomationMutations, type AutomationMutations } from '../hooks/useAutomationMutations';
import { useOpenThread } from '../hooks/useOpenThread';
import { useRecentRuns } from '../hooks/useRecentRuns';
import type { TemplateId } from '../utils/templates';
import { deliveryAttemptLabel } from '../utils/delivery';
import { groupRunsByDay, nearDayKeys, runTime } from '../utils/feed';
import { automationActions, describeRun } from '../utils/status';
import { formatClock, formatDayHeading, formatDuration } from '../utils/time';
import { StatusGlyph } from './StatusMark';
import { FeedRail } from './FeedRail';
import { RunReport } from './RunReport';
import type { WatchedReading } from '../hooks/useWatchedReadings';
import './FeedView.css';

interface FeedViewProps {
  automations: Automation[];
  readings: Map<string, WatchedReading>;
  onOpenAutomation: (id: string) => void;
  onOpenRun: (automationId: string, runId: string) => void;
  onManage: () => void;
  onNew: (template: TemplateId) => void;
}

/**
 * What the automations produced, newest first, grouped by the day the reader
 * lived it. Each entry leads with the start of the answer rather than with
 * the run's bookkeeping, since the answer is why the automation exists; the
 * rail beside it holds what is coming and what is waiting on the reader.
 */
export default function FeedView({ automations, readings, onOpenAutomation, onOpenRun, onManage, onNew }: FeedViewProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { runs, isLoading, error, hasNextPage, fetchNextPage, isFetchingNextPage } = useRecentRuns();
  // Once for the feed, its entries and its rail: each would otherwise hold
  // its own observer for each verb.
  const mutations = useAutomationMutations();
  const { label: modeLabel } = useAgentModeLabels();
  const byId = useMemo(() => new Map(automations.map((a) => [a.automation_id, a])), [automations]);

  const days = useMemo(() => groupRunsByDay(runs), [runs]);

  const near = nearDayKeys(new Date(now));
  const dayLabel = (key: string, date: Date) =>
    key === near.today ? t('automation.today') : key === near.yesterday ? t('automation.yesterday') : formatDayHeading(date, locale);

  return (
    <div className="automations-feed">
      <section className="automations-feed-main" aria-label={t('automation.viewFeed')}>
        {error && runs.length === 0 ? (
          <ListError>{t('automation.loadFailed')}</ListError>
        ) : isLoading ? (
          <ListSkeleton rows={4} />
        ) : runs.length === 0 ? (
          <p className="automations-quiet-note">{t('automation.noRunsYet')}</p>
        ) : (
          <>
            {days.map((day) => (
              <div key={day.key} className="automations-day">
                <h2 className="automations-kicker automations-day-heading">{dayLabel(day.key, day.date)}</h2>
                {day.runs.map((run) => (
                  <RunEntry
                    key={run.automation_execution_id}
                    run={run}
                    automation={byId.get(run.automation_id)}
                    mutations={mutations}
                    modeLabel={modeLabel}
                    onOpenRun={onOpenRun}
                  />
                ))}
              </div>
            ))}
            {hasNextPage && (
              <div className="flex justify-center pt-4">
                <HeaderButton variant="secondary" onClick={() => void fetchNextPage()} disabled={isFetchingNextPage}>
                  {t(isFetchingNextPage ? 'common.loading' : 'automation.loadMore')}
                </HeaderButton>
              </div>
            )}
          </>
        )}
      </section>
      <FeedRail
        automations={automations}
        readings={readings}
        mutations={mutations}
        onOpenAutomation={onOpenAutomation}
        onManage={onManage}
        onNew={onNew}
      />
    </div>
  );
}

function RunEntry({
  run,
  automation,
  mutations,
  modeLabel,
  onOpenRun,
}: {
  run: AutomationRun;
  automation: Automation | undefined;
  mutations: AutomationMutations;
  modeLabel: AgentModeLabels['label'];
  onOpenRun: (automationId: string, runId: string) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const openThread = useOpenThread();
  const runsAs = useRunsAs();
  const view = describeRun(run);
  const { ui } = view;
  // Only a live run shows a time since, so only it needs the clock to move.
  const now = useNow(60_000, !!ui.live);

  const meta: string[] = [];
  if (run.status !== 'completed') meta.push(t(ui.labelKey));
  meta.push(modeLabel(runsAs(run.agent_mode, run.workspace_id)));
  if (ui.live) {
    if (run.started_at) meta.push(t('automation.startedAgo', { when: relativeTime(run.started_at, locale, now) }));
  } else if (view.showDuration) {
    meta.push(formatDuration(run.started_at, run.completed_at, t));
  }
  // Where each delivery landed and how, by the name the run recorded.
  for (const attempt of run.delivery_result ?? []) meta.push(deliveryAttemptLabel(attempt, t));

  // Retry is a fresh run, offered as the inspector's Run now is; a disabled
  // automation has to be resumed first.
  const remedy = ui.danger && automation ? automationActions(automation).remedy : null;

  return (
    <article className="automation-run">
      <div className="automation-mono automation-run-time">{formatClock(runTime(run), locale)}</div>
      <div className="min-w-0 flex-1">
        <div className="automation-run-head">
          {ui.live || ui.Icon ? (
            <span className="automation-run-glyph">
              <StatusGlyph ui={ui} label={t(ui.labelKey)} size={14} />
            </span>
          ) : null}
          {/* Opens the automation on this run's report, not its newest. */}
          <button
            type="button"
            className="automation-name automation-run-name"
            onClick={() => onOpenRun(run.automation_id, run.automation_execution_id)}
          >
            {run.automation_name}
          </button>
        </div>
        <RunReport
          run={run}
          view={view}
          meta={meta}
          layout="entry"
          busy={mutations.busy}
          onSkip={() => mutations.skip.mutate({ automationId: run.automation_id, executionId: run.automation_execution_id })}
          onOpenThread={() => openThread(run.conversation_thread_id, run.workspace_id)}
          onRetry={remedy === 'retry' ? () => mutations.trigger.mutate(run.automation_id) : undefined}
          onResume={remedy === 'resume' ? () => mutations.resume.mutate(run.automation_id) : undefined}
        />
      </div>
    </article>
  );
}
