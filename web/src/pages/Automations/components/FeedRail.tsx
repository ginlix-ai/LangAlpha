import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRight, ArrowUpRight } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { ErrorLink } from '@/components/ui/error-banner';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { Automation } from '@/types/automation';
import type { AutomationMutations } from '../hooks/useAutomationMutations';
import { useOpenThread } from '../hooks/useOpenThread';
import type { WatchedReading } from '../hooks/useWatchedReadings';
import { distanceLabel } from '../utils/price';
import {
  attentionKind,
  attentionLinks,
  automationActions,
  automationStatusUi,
  isUpcoming,
  isWatching,
  needsAttention,
} from '../utils/status';
import { templatesById, type TemplateId } from '../utils/templates';
import { formatUpcoming } from '../utils/time';
import { PriceMeter } from './PriceMeter';
import { Cadence } from './CadenceMarks';
import { StatusGlyph } from './StatusMark';
import './FeedRail.css';

const UP_NEXT_LIMIT = 5;

/** The kinds that keep coming back, offered when nothing is set to. */
const RECURRING_STARTERS = templatesById(['morning_briefing', 'weekly_review', 'price_alert']);

interface FeedRailProps {
  automations: Automation[];
  readings: Map<string, WatchedReading>;
  mutations: AutomationMutations;
  onOpenAutomation: (id: string) => void;
  onManage: () => void;
  onNew: (template: TemplateId) => void;
}

/** What is coming, what is being watched, and what is waiting on the reader.
 *  A section with nothing in it is left out rather than shown empty, except
 *  that when nothing at all is set to run again the rail says so and offers
 *  the recurring kinds. */
export function FeedRail({ automations, readings, mutations, onOpenAutomation, onManage, onNew }: FeedRailProps) {
  const { t } = useTranslation();

  const upNext = useMemo(
    () =>
      automations
        .filter(isUpcoming)
        .sort((a, b) => Date.parse(a.next_run_at!) - Date.parse(b.next_run_at!))
        .slice(0, UP_NEXT_LIMIT),
    [automations],
  );
  const watching = useMemo(() => automations.filter(isWatching), [automations]);
  const attention = useMemo(() => automations.filter(needsAttention), [automations]);

  if (automations.length === 0) return null;

  return (
    <aside className="automations-rail">
      {attention.length > 0 && (
        <section className="automations-rail-section">
          <h2 className="automations-kicker">{t('automation.groupAttention')}</h2>
          {attention.map((a) => (
            <AttentionRow
              key={a.automation_id}
              automation={a}
              mutations={mutations}
              onOpen={() => onOpenAutomation(a.automation_id)}
            />
          ))}
        </section>
      )}

      {upNext.length > 0 && (
        <section className="automations-rail-section automations-rail-schedule">
          <h2 className="automations-kicker">{t('automation.upNext')}</h2>
          {upNext.map((a) => (
            <div key={a.automation_id} className="automations-rail-row">
              <span className="automation-mono automations-rail-when" title={relativeTime(a.next_run_at)}>
                {formatUpcoming(a.next_run_at)}
              </span>
              <button type="button" className="automation-name automations-rail-name" onClick={() => onOpenAutomation(a.automation_id)}>
                {a.name}
              </button>
            </div>
          ))}
        </section>
      )}

      {upNext.length === 0 && watching.length === 0 && (
        <section className="automations-rail-section">
          <h2 className="automations-kicker">{t('automation.upNext')}</h2>
          <p className="automations-quiet-note">{t('automation.nothingScheduled')}</p>
          <ul className="automations-rail-starters">
            {RECURRING_STARTERS.map((tpl) => (
              <li key={tpl.id}>
                <button type="button" className="automations-starter automations-rail-starter" onClick={() => onNew(tpl.id)}>
                  <span className="automations-starter-when">
                    <Cadence template={tpl} />
                  </span>
                  <span className="automation-name automations-rail-name">{t(tpl.nameKey)}</span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      )}

      {watching.length > 0 && (
        <section className="automations-rail-section">
          <h2 className="automations-kicker">{t('automation.groupWatching')}</h2>
          {watching.map((a) => {
            const w = readings.get(a.automation_id);
            return (
              <div key={a.automation_id} className="automations-rail-watch">
                <div className="flex items-baseline justify-between gap-3">
                  <button type="button" className="automation-name automations-rail-name" onClick={() => onOpenAutomation(a.automation_id)}>
                    {a.name}
                  </button>
                  <span className="automation-mono automations-rail-sub shrink-0">
                    {w?.reading ? distanceLabel(w.reading, t) : !w?.settled ? <span aria-hidden="true">…</span> : null}
                  </span>
                </div>
                {w?.reading && <PriceMeter reading={w.reading} symbol={w.symbol} />}
                {/* A sentence, not a figure, so it takes the meter's line
                    rather than squeezing the name beside it. */}
                {w && !w.reading && w.settled && !w.quoted && (
                  <div className="automations-rail-sub">{t('automation.noQuote', { symbol: w.symbol })}</div>
                )}
              </div>
            );
          })}
        </section>
      )}

      <button type="button" className="automation-link self-start" onClick={onManage}>
        {t('automation.manageAll', { count: automations.length })}
        <ArrowRight className="h-3 w-3" />
      </button>
    </aside>
  );
}

/**
 * One automation waiting on the reader: what went wrong, then the choices,
 * under it so the reason has the rail's full width. A usage limit is told in
 * the quota service's own words, and every failure that leaves the schedule
 * on offers the same two answers, another try or a pause. Dismiss, the
 * quietest, takes the row away and changes nothing else. The run's thread is
 * one step away, since a run a usage limit paused midway resumes there.
 */
function AttentionRow({
  automation: a,
  mutations: { pause, resume, trigger, dismiss, busy },
  onOpen,
}: {
  automation: Automation;
  mutations: AutomationMutations;
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  const openThread = useOpenThread();
  const kind = attentionKind(a);
  const ui = automationStatusUi(a);
  const { canPause, canDismiss, remedy } = automationActions(a);
  const last = a.last_execution;

  const links = attentionLinks(a);
  let reason: string;
  // The service's message runs as long as it likes, so only it is clamped.
  let quoted = false;
  switch (kind) {
    case 'key_rejected':
      reason = t('automation.keyRejectedReason');
      break;
    case 'switched_off':
      reason = t('automation.disabledAfter', { count: a.failure_count });
      break;
    case 'usage_limit':
      // No message means the limit paused the run midway, and its thread
      // holds the pause.
      quoted = !!last?.error_message;
      reason = last?.error_message || t('automation.stateUsageLimit');
      break;
    default:
      reason = t('automation.lastRunFailedAgo', { when: relativeTime(last?.completed_at) });
  }

  return (
    <div className="automations-rail-row items-start">
      <span className="automations-rail-glyph">
        <StatusGlyph ui={ui} label={t(ui.labelKey)} size={14} />
      </span>
      <div className="min-w-0 flex-1">
        <button type="button" className="automation-name automations-rail-name" onClick={onOpen}>
          {a.name}
        </button>
        <div className={cn('automations-rail-sub', quoted && 'automations-rail-quote')} title={quoted ? reason : undefined}>
          {reason}
        </div>
        <div className="automations-rail-actions">
          {remedy === 'resume' ? (
            <HeaderButton variant="secondary" disabled={busy} onClick={() => resume.mutate(a.automation_id)}>
              {t('automation.resume')}
            </HeaderButton>
          ) : (
            <>
              <HeaderButton variant="secondary" disabled={busy} onClick={() => trigger.mutate(a.automation_id)}>
                {t('common.retry')}
              </HeaderButton>
              {canPause && (
                <HeaderButton variant="secondary" disabled={busy} onClick={() => pause.mutate(a.automation_id)}>
                  {t('automation.pause')}
                </HeaderButton>
              )}
            </>
          )}
          {canDismiss && last && (
            <HeaderButton
              variant="ghost"
              disabled={busy}
              title={t('automation.dismissHint')}
              onClick={() => dismiss.mutate({ automationId: a.automation_id, executionId: last.automation_execution_id })}
            >
              {t('automation.dismiss')}
            </HeaderButton>
          )}
          {links.length > 0 && (
            <span className="automations-rail-links">
              {links.map((l) => (
                <ErrorLink key={`${l.url}|${l.label}`} {...l} />
              ))}
            </span>
          )}
          {last?.conversation_thread_id && (
            <button
              type="button"
              className="automation-link automations-rail-thread"
              onClick={() => openThread(last.conversation_thread_id)}
            >
              {t('automation.openThread')}
              <ArrowUpRight className="h-3 w-3" />
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
