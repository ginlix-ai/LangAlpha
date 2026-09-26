import React, { Fragment, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, ArrowUpRight, Pause, Pencil, Play, Trash2, Zap } from 'lucide-react';
import { HeaderButton, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { cn } from '@/lib/utils';
import type { Automation, AutomationExecution } from '@/types/automation';
import type { ErrorLinkSpec } from '@/utils/rateLimitError';
import { useAutomationMutations } from '../hooks/useAutomationMutations';
import { useExecutions } from '../hooks/useExecutions';
import { useOpenThread } from '../hooks/useOpenThread';
import type { WatchedReading } from '../hooks/useWatchedReadings';
import { useWorkspaceOptions, workspaceNameOf } from '../hooks/useWorkspaceOptions';
import { deliveryMethodName } from '../utils/delivery';
import { distanceLabel } from '../utils/price';
import { scheduleSentence } from '../utils/schedule';
import { attentionLinks, automationActions, automationStatusUi, describeRun } from '../utils/status';
import { formatDateTimeShort, formatDuration } from '../utils/time';
import { PriceMeter } from './PriceMeter';
import { RunReport } from './RunReport';
import { StatusGlyph } from './StatusMark';
import './AutomationInspector.css';

/** Past this an instruction starts folded: it is what the agent was told
 *  once, and a long one should not push the run history off the pane. */
const INSTRUCTION_FOLD_CHARS = 480;
const INSTRUCTION_FOLD_LINES = 10;

interface AutomationInspectorProps {
  automation: Automation;
  reading: WatchedReading | undefined;
  /** The run a link or a history row opened; null reports the newest. */
  runId: string | null;
  onOpenRun: (runId: string) => void;
  onEdit: (a: Automation) => void;
  onDelete: (a: Automation) => void;
}

/**
 * One automation, read top to bottom in the order a person asks about it:
 * what is it and when does it run, what did it last find, what was it told to
 * do, how have its runs gone, and the settings nobody needs until they do.
 */
export default function AutomationInspector({
  automation: a,
  reading,
  runId,
  onOpenRun,
  onEdit,
  onDelete,
}: AutomationInspectorProps) {
  const { t } = useTranslation();
  const openThread = useOpenThread();
  const { pause, resume, trigger, skip, busy } = useAutomationMutations();
  const { executions, loading } = useExecutions(a.automation_id);
  const workspaceName = workspaceNameOf(useWorkspaceOptions(), a.workspace_id);
  const deliveryMethods = a.delivery_config?.methods ?? [];

  const ui = automationStatusUi(a);
  const { canPause, canResume, canRun, runBusy } = automationActions(a);
  const last = a.last_execution;
  // The opened run is looked for on the list row and in the history this
  // pane loads. There is no reading one run by its id, so one older than
  // that history is not fetched: the newest stands in, and says so.
  const opened = runId
    ? runId === last?.automation_execution_id
      ? last
      : executions.find((e) => e.automation_execution_id === runId) ?? null
    : null;
  const placing = !!runId && !opened && loading;
  const runMissing = !!runId && !opened && !loading;
  const shownRun = opened ?? last;
  const isLatest = shownRun === last;

  // A run opened from the history below brings its report into view.
  const reportRef = useRef<HTMLElement>(null);
  const openedOnMount = useRef(runId);
  useEffect(() => {
    if (openedOnMount.current === runId) return;
    openedOnMount.current = runId;
    reportRef.current?.scrollIntoView?.({ block: 'nearest', behavior: 'smooth' });
  }, [runId]);

  const kicker = [t(ui.labelKey), t(a.agent_mode === 'ptc' ? 'automation.ptc' : 'automation.flash')];
  if (workspaceName) kicker.push(workspaceName);

  return (
    <article className="automation-inspector">
      <div className="automations-kicker flex items-center gap-1.5">
        <StatusGlyph ui={ui} size={12} />
        <span className="truncate">{kicker.join(' · ')}</span>
      </div>
      <h2 className="title-font automation-inspector-title">{a.name}</h2>
      {a.description && <p className="automation-inspector-desc">{a.description}</p>}
      <p className="automation-inspector-schedule">{scheduleSentence(a, t)}</p>

      {reading?.reading && (
        <div className="automation-inspector-meter">
          <div className="automation-mono automation-inspector-reading mb-1">{distanceLabel(reading.reading, t)}</div>
          <PriceMeter reading={reading.reading} symbol={reading.symbol} />
        </div>
      )}

      <div className="automation-inspector-actions">
        <HeaderButton
          variant="secondary"
          icon={Zap}
          disabled={busy || runBusy || !canRun}
          onClick={() => trigger.mutate(a.automation_id)}
        >
          {t('automation.runNow')}
        </HeaderButton>
        {canPause && (
          <HeaderButton variant="secondary" icon={Pause} disabled={busy} onClick={() => pause.mutate(a.automation_id)}>
            {t('automation.pause')}
          </HeaderButton>
        )}
        {canResume && (
          <HeaderButton variant="secondary" icon={Play} disabled={busy} onClick={() => resume.mutate(a.automation_id)}>
            {t('automation.resume')}
          </HeaderButton>
        )}
        <HeaderButton variant="secondary" icon={Pencil} onClick={() => onEdit(a)}>
          {t('automation.edit')}
        </HeaderButton>
        <HeaderButton variant="ghost" icon={Trash2} className="ml-auto hover:text-[color:var(--color-icon-danger)]" onClick={() => onDelete(a)}>
          {t('common.delete')}
        </HeaderButton>
      </div>

      <section ref={reportRef} className="automation-inspector-section">
        <h3 className="automations-kicker">{t(isLatest ? 'automation.latestRun' : 'automation.selectedRun')}</h3>
        {runMissing && <p className="automations-quiet-note automation-run-missing">{t('automation.runNotFound')}</p>}
        {placing ? (
          <ListSkeleton rows={2} />
        ) : shownRun ? (
          <RunCard
            execution={shownRun}
            // Only the newest run links to a fix: an older limit may be long
            // lifted, and in the feed the rail already offers the links.
            links={isLatest ? attentionLinks(a) : []}
            busy={busy}
            onOpen={() => openThread(shownRun.conversation_thread_id)}
            onSkip={() => skip.mutate({ automationId: a.automation_id, executionId: shownRun.automation_execution_id })}
          />
        ) : (
          <p className="automations-quiet-note">{t('automation.noRunsForThis')}</p>
        )}
      </section>

      <section className="automation-inspector-section">
        <h3 className="automations-kicker">{t('automation.instruction')}</h3>
        <Instruction text={a.instruction} />
      </section>

      <section className="automation-inspector-section">
        <h3 className="automations-kicker">{t('automation.runHistory')}</h3>
        {loading ? (
          <ListSkeleton rows={2} />
        ) : executions.length === 0 ? (
          <p className="automations-quiet-note">{t('automation.noRunsForThis')}</p>
        ) : (
          <RunHistory
            executions={executions}
            shownId={placing ? null : shownRun?.automation_execution_id ?? null}
            onOpenRun={onOpenRun}
            onOpen={(threadId) => openThread(threadId)}
          />
        )}
      </section>

      <section className="automation-inspector-section">
        <h3 className="automations-kicker">{t('automation.details')}</h3>
        <dl className="automation-details">
          <dt>{t('automation.detailModel')}</dt>
          <dd>{a.llm_model || t('automation.defaultModel')}</dd>
          <dt>{t('automation.threadStrategy')}</dt>
          <dd>{t(a.thread_strategy === 'continue' ? 'automation.continueExisting' : 'automation.newThreadEachRun')}</dd>
          {workspaceName && (
            <>
              <dt>{t('automation.detailWorkspace')}</dt>
              <dd>{workspaceName}</dd>
            </>
          )}
          {deliveryMethods.length > 0 && (
            <>
              <dt>{t('automation.delivery')}</dt>
              <dd>{deliveryMethods.map((m) => deliveryMethodName(m, t)).join(', ')}</dd>
            </>
          )}
          <dt>{t('automation.detailFailures')}</dt>
          <dd>{t('automation.failuresOf', { count: a.failure_count, max: a.max_failures })}</dd>
          <dt>{t('automation.detailCreated')}</dt>
          <dd>{formatDateTimeShort(a.created_at)}</dd>
        </dl>
      </section>
    </article>
  );
}

function Instruction({ text }: { text: string }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const long = text.length > INSTRUCTION_FOLD_CHARS || text.split('\n').length > INSTRUCTION_FOLD_LINES;
  const folded = long && !open;
  return (
    <>
      <div className={cn('automation-instruction', folded && 'is-folded')}>{text}</div>
      {long && (
        <button type="button" className="automation-link mt-2" aria-expanded={open} onClick={() => setOpen((v) => !v)}>
          {t(open ? 'automation.showLess' : 'automation.showAll')}
        </button>
      )}
    </>
  );
}

function RunCard({
  execution: e,
  links,
  busy,
  onOpen,
  onSkip,
}: {
  execution: AutomationExecution;
  links: ErrorLinkSpec[];
  busy: boolean;
  onOpen: () => void;
  onSkip: () => void;
}) {
  const { t } = useTranslation();
  const view = describeRun(e);
  const meta = [t(view.ui.labelKey), formatDateTimeShort(e.started_at ?? e.scheduled_at)];
  if (view.showDuration) meta.push(formatDuration(e.started_at, e.completed_at));
  return (
    <div className="automation-report">
      <RunReport
        run={e}
        view={view}
        meta={meta}
        layout="report"
        busy={busy}
        onSkip={onSkip}
        onOpenThread={onOpen}
        links={links}
      />
    </div>
  );
}

function RunHistory({
  executions,
  shownId,
  onOpenRun,
  onOpen,
}: {
  executions: AutomationExecution[];
  /** The run the report above shows. */
  shownId: string | null;
  onOpenRun: (runId: string) => void;
  onOpen: (threadId: string | null) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="automation-history-scroll">
      <table className="automation-history">
        <thead>
          <tr>
            <th>{t('automation.colResult')}</th>
            <th>{t('automation.colStarted')}</th>
            <th>{t('automation.colTook')}</th>
            <th>{t('automation.delivery')}</th>
            <th aria-label={t('automation.openThread')} />
          </tr>
        </thead>
        <tbody>
          {executions.map((e) => {
            const { ui, showDuration } = describeRun(e);
            // Only a failure's label takes the glyph's color: amber marks
            // liveness on the glyph and never tints words.
            const error = ui.danger ? e.error_message : null;
            const shown = e.automation_execution_id === shownId;
            return (
              <Fragment key={e.automation_execution_id}>
                <tr className={error ? 'has-error' : undefined}>
                  <td>
                    <span className="inline-flex items-center gap-1.5">
                      <StatusGlyph ui={ui} size={12} />
                      <span style={{ color: ui.danger ? ui.color : undefined }}>{t(ui.labelKey)}</span>
                    </span>
                  </td>
                  <td className="automation-mono">
                    {/* Opens this run in the report above, and in the link. */}
                    <button
                      type="button"
                      className="automation-link automation-history-run"
                      aria-current={shown ? 'true' : undefined}
                      onClick={() => onOpenRun(e.automation_execution_id)}
                    >
                      {formatDateTimeShort(e.started_at ?? e.scheduled_at)}
                    </button>
                  </td>
                  <td className="automation-mono">{showDuration ? formatDuration(e.started_at, e.completed_at) : ''}</td>
                  <td>
                    {(e.delivery_result ?? []).map((d) => (
                      <span key={d.method} className="mr-2 inline-flex items-center gap-1" title={d.error ?? undefined}>
                        {!d.success && (
                          <AlertCircle className="h-3 w-3" style={{ color: 'var(--color-icon-danger)' }} aria-label={t('automation.runFailed')} />
                        )}
                        {deliveryMethodName(d.method, t)}
                      </span>
                    ))}
                  </td>
                  <td className="text-right">
                    {e.conversation_thread_id && (
                      <button type="button" className="automation-link" onClick={() => onOpen(e.conversation_thread_id)}>
                        {t('automation.open')}
                        <ArrowUpRight className="h-3 w-3" />
                      </button>
                    )}
                  </td>
                </tr>
                {error && (
                  <tr className="automation-history-error">
                    <td colSpan={5}>
                      <p className="automation-mono automation-run-error" title={error}>
                        {error}
                      </p>
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
