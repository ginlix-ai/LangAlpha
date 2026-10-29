import React from 'react';
import type { TFunction } from 'i18next';
import { relativeTime } from '@/lib/format';
import { useNavigate } from 'react-router';
import { useRouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { cronToHuman } from '../../../Automations/utils/cron';
import { formatPriceTrigger, formatRetriggerMode } from '../../../Automations/utils/price';
import { describeRun, isRunFailed } from '../../../Automations/utils/status';
import type { AutomationExecution, ExecutionStatus, PriceTriggerConfig } from '@/types/automation';
import { formatDateTime, formatDuration } from '../../../Automations/utils/time';
import { AutomationStatus } from './InlineAutomationCards';
import { readAutomations, records, type Automation, type PanelProps } from './cardReaders';
import { scheduleLabel, triggerKind } from './cardWords';
import { TermCard, TermKV, TermStatus, TermTag } from './TermCard';
import { useCardNarrow } from './termContext';
import { TermLink, TermPanelHead } from './TermPanel';

// ─── Helpers ─────────────────────────────────────────────────────────

function marketI18nKey(triggerConfig: PriceTriggerConfig | null | undefined): string | null {
  if (!triggerConfig) return null;
  return triggerConfig.market === 'index' ? 'automation.marketIndex' : null;
}

/** Where the card's automation ran, or null when the card does not say.
 *  The tool that drew these cards is gone, so every one is kept history, and
 *  names the agent it ran on rather than a workspace: `ptc` ran in one, and
 *  the other, outside any, is the Chief of Staff now. */
function cardPlace(auto: Automation, t: TFunction): string | null {
  if (!auto.agentMode) return null;
  return auto.agentMode === 'ptc' ? t('automation.detailWorkspace') : t('agents.chiefOfStaff');
}

/** A run's state in the cards' tones: one that ran out of time failed too,
 *  and one still going reads as an executing automation. */
function runTone(status: ExecutionStatus): string {
  if (isRunFailed(status)) return 'failed';
  return status === 'running' ? 'executing' : status;
}

/** A value with what qualifies it set after it in gray: a time and its date,
 *  a schedule and its cron line. */
function Qualified({ value, aside }: { value: React.ReactNode; aside?: React.ReactNode }): React.ReactElement {
  return (
    <>
      {value}
      {aside && <span className="term-t3" style={{ marginLeft: '1.25ch' }}>{aside}</span>}
    </>
  );
}

function When({ at }: { at: string }): React.ReactElement {
  const locale = useLocale();
  const now = useNow();
  return <Qualified value={relativeTime(at, locale, now)} aside={formatDateTime(at, locale)} />;
}

type Pairs = Array<readonly [string, React.ReactNode] | null | false>;

/** What sets the automation off: a price trigger and how it re-arms, with the
 *  market when that is not stocks, or a schedule with its cron line and the
 *  next run. */
function planPairs(auto: Automation, t: TFunction, locale: string): Pairs {
  if (auto.triggerType === 'price') {
    const trigger = auto.priceTrigger;
    const market = marketI18nKey(trigger);
    const condition = <Qualified value={formatPriceTrigger(trigger, t)} aside={market && <TermTag>{t(market)}</TermTag>} />;
    return [
      [t('toolArtifact.trigger'), condition],
      [t('toolArtifact.retrigger'), formatRetriggerMode(trigger, t)],
    ];
  }
  const { schedule, nextRunAt } = auto;
  const cron = auto.triggerType === 'cron' && schedule ? <Qualified value={cronToHuman(schedule, t, locale)} aside={schedule} /> : null;
  const next = nextRunAt ? <When at={nextRunAt} /> : null;
  return [cron && [t('toolArtifact.schedule'), cron], next && [t('toolArtifact.nextRun'), next]];
}

function AutomationsPageLink({ automationId }: { automationId?: string }): React.ReactElement {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  const path = automationId ? `/automations?id=${automationId}` : '/automations';
  return <TermLink onClick={() => guardLeave(() => navigate(path))}>{t('toolArtifact.viewInAutomations')}</TermLink>;
}

/** `[ Weekly earnings recap ] RECURRING ...... [PAUSED]  VIEW IN AUTOMATIONS`:
 *  the card's edge, with the way to the automation itself. */
function AutomationHead({ auto, status }: { auto: Automation; status: React.ReactNode }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <TermPanelHead
      title={<span className="term-auto-name">{auto.name}</span>}
      sub={triggerKind(auto, t) ?? t('toolArtifact.oneTime')}
      aside={
        <>
          {status}
          <AutomationsPageLink automationId={auto.id} />
        </>
      }
    />
  );
}

// ─── Router ──────────────────────────────────────────────────────────

export default function AutomationDetailPanel({ data }: PanelProps): React.ReactElement | null {
  const read = readAutomations(data);
  switch (read?.mode) {
    case 'list':
      return <ListPanel automations={read.automations} total={read.total} />;
    case 'detail':
      return read.automation && (
        <DetailPanel automation={read.automation} executions={records(data.executions)} totalExecutions={read.totalExecutions} />
      );
    case 'created':
      return <CreatedPanel automation={read.automation} />;
    default:
      return null;
  }
}

// ─── List Panel ──────────────────────────────────────────────────────

interface ListPanelProps {
  automations: Automation[];
  total: number;
}

/** The list card a size up: every automation, with when it runs next and
 *  where, and its state in its own column. */
function ListPanel({ automations, total }: ListPanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const narrow = useCardNarrow();
  const rows = automations.map((a, i) => ({
    key: a.id || String(i),
    name: a.name,
    status: a.status,
    schedule: scheduleLabel(a, t, locale, now),
    next: a.nextRunAt ? relativeTime(a.nextRunAt, locale, now) : '',
    place: cardPlace(a, t),
  }));
  // A column no row fills costs the table its width and says nothing.
  const showNext = rows.some((r) => r.next);
  const showPlace = rows.some((r) => r.place);

  let body: React.ReactNode;
  if (rows.length === 0) {
    body = <div className="term-t3">{t('toolArtifact.noAutomationsFound')}</div>;
  } else if (narrow) {
    // A phone has no width for five columns: each name over its schedule.
    body = (
      <table className="term-table">
        <tbody>
          {rows.map((r) => (
            <tr key={r.key}>
              <td className="term-fill term-auto-wrap">
                <div>{r.name}</div>
                <div className="term-t3">
                  {[r.schedule, r.next && t('toolArtifact.next', { time: r.next }), r.place].filter(Boolean).join(' · ')}
                </div>
              </td>
              <td className="term-num">
                <AutomationStatus status={r.status} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  } else {
    body = (
      <div style={{ overflowX: 'auto' }}>
        <table className="term-table">
          <thead>
            <tr>
              <th>{t('toolArtifact.termHead.name')}</th>
              <th>{t('toolArtifact.schedule')}</th>
              {showNext && <th>{t('toolArtifact.nextRun')}</th>}
              {showPlace && <th>{t('automation.agentMode')}</th>}
              <th className="term-num">{t('toolArtifact.status')}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.key}>
                <td className="term-fill term-auto-wrap">{r.name}</td>
                <td className="term-t2">{r.schedule}</td>
                {showNext && <td className="term-t3">{r.next}</td>}
                {showPlace && <td className="term-t3">{r.place}</td>}
                <td className="term-num">
                  <AutomationStatus status={r.status} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  return (
    <TermCard
      title={t('toolArtifact.automations')}
      meta={
        <span style={{ display: 'inline-flex', alignItems: 'baseline', gap: '1.25ch' }}>
          {rows.length > 0 && <span className="term-auto-count">{t('toolArtifact.nTotalItems', { count: total })}</span>}
          <AutomationsPageLink />
        </span>
      }
    >
      {body}
    </TermCard>
  );
}

// ─── Detail Panel ────────────────────────────────────────────────────

interface DetailPanelProps {
  automation: Automation;
  executions: Record<string, unknown>[];
  totalExecutions: number;
}

/** The detail card a size up, with what the card leaves out: the cron line,
 *  the dates, the agent, the instruction and the latest runs. */
function DetailPanel({ automation, executions, totalExecutions }: DetailPanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const { lastRunAt, instruction } = automation;
  const lastRun = lastRunAt ? <When at={lastRunAt} /> : null;
  const place = cardPlace(automation, t);

  return (
    <>
      <AutomationHead auto={automation} status={<AutomationStatus status={automation.status} />} />

      <TermCard title={t('toolArtifact.configuration')}>
        <TermKV
          cols={narrow ? 1 : 2}
          pairs={[
            ...planPairs(automation, t, locale),
            lastRun && [t('toolArtifact.lastRun'), lastRun],
            !!place && [t('automation.agentMode'), place],
            // With no runs listed below, the count is said here, as the card says it.
            !executions.length && totalExecutions > 0 && [t('toolArtifact.executions'), t('toolArtifact.nTotal', { count: totalExecutions })],
          ]}
        />
      </TermCard>

      {instruction && (
        <TermCard title={t('toolArtifact.instruction')}>
          <p className="term-prose term-auto-prose">{instruction}</p>
        </TermCard>
      )}

      {executions.length > 0 && (
        <TermCard title={t('toolArtifact.executions')} meta={t('toolArtifact.nTotal', { count: totalExecutions })}>
          <table className="term-table">
            <tbody>
              {executions.map((e, i) => {
                const run = describeRun(e as unknown as AutomationExecution);
                const error = e.error_message as string | undefined;
                return (
                  <React.Fragment key={(e.execution_id as string) || i}>
                    <tr>
                      <td className="term-fill">
                        {e.scheduled_at ? formatDateTime(e.scheduled_at as string, locale) : t('toolArtifact.manualTrigger')}
                      </td>
                      <td className="term-num term-t3">
                        {!!(e.started_at && e.completed_at) && formatDuration(e.started_at as string, e.completed_at as string, t)}
                      </td>
                      <td className="term-num">
                        <AutomationStatus status={runTone(e.status as ExecutionStatus)}>{t(run.ui.labelKey)}</AutomationStatus>
                      </td>
                    </tr>
                    {error && (
                      <tr className="term-auto-error">
                        <td colSpan={3}>
                          <span className="term-auto-error-text" title={error}>{error}</span>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </TermCard>
      )}
    </>
  );
}

// ─── Created Panel ───────────────────────────────────────────────────

/** The created card a size up: what was set, and the automation's state. */
function CreatedPanel({ automation }: { automation: Automation }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const status = <AutomationStatus status={automation.status || 'active'} />;

  return (
    <>
      <AutomationHead
        auto={automation}
        status={<TermStatus tone="profit">{t('toolArtifact.automationStatus.created')}</TermStatus>}
      />

      <TermCard title={t('toolArtifact.configuration')}>
        <TermKV
          cols={narrow ? 1 : 2}
          pairs={[...planPairs(automation, t, locale), [t('toolArtifact.status'), status]]}
        />
      </TermCard>
    </>
  );
}
