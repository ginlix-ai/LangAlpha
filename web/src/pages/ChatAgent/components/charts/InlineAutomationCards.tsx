import React from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import { readAutomations, type Automation, type InlineCardProps } from './cardReaders';
import { scheduleLabel, triggerKind } from './cardWords';
import { TermCard, TermKV, TermStatus, type TermTone } from './TermCard';
import { useCardNarrow } from './termContext';
import { TermTable } from './TermTable';

const STATUS_TONE: Record<string, TermTone> = {
  active: 'profit',
  paused: 'warning',
  failed: 'loss',
  completed: 'info',
  disabled: 'loss',
};

const STATUS_LABEL_KEYS: Record<string, string> = {
  active: 'toolArtifact.automationStatus.active',
  paused: 'toolArtifact.automationStatus.paused',
  failed: 'toolArtifact.automationStatus.failed',
  completed: 'toolArtifact.automationStatus.completed',
  disabled: 'toolArtifact.automationStatus.disabled',
  executing: 'toolArtifact.automationStatus.executing',
};

const statusTone = (status: string): TermTone => STATUS_TONE[status] ?? 'faint';

function statusLabel(status: string, t: TFunction): string {
  const key = STATUS_LABEL_KEYS[status];
  return key ? t(key) : status;
}

/** An automation's state as the cards say it, shared with the detail panel,
 *  which says a run's state in the same tones under its own word. */
export function AutomationStatus({ status, children }: { status: string; children?: React.ReactNode }): React.ReactElement {
  const { t } = useTranslation();
  return <TermStatus tone={statusTone(status)}>{children ?? statusLabel(status, t)}</TermStatus>;
}

export function InlineAutomationCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const read = readAutomations(artifact);
  switch (read?.mode) {
    case 'list':
      return read.automations.length ? <ListCard automations={read.automations} total={read.total} onClick={onClick} /> : null;
    case 'detail':
      return read.automation && <OneCard automation={read.automation} executions={read.totalExecutions} onClick={onClick} />;
    case 'created':
      return <OneCard automation={read.automation} created onClick={onClick} />;
    default:
      return null;
  }
}

const MAX_INLINE = 4;

function ListCard({
  automations,
  total,
  onClick,
}: {
  automations: Automation[];
  total: number;
  onClick?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  return (
    <TermCard title={t('toolArtifact.automations')} meta={t('toolArtifact.nTotalItems', { count: total })} onClick={onClick}>
      <TermTable
        rows={automations}
        limit={MAX_INLINE}
        total={total}
        more={(n) => t('toolArtifact.nMoreAutomations', { count: n })}
        rowKey={(a, i) => a.id || i}
        columns={[
          { key: 'state', cell: (a) => <span className="term-dot" data-size="row" data-tone={statusTone(a.status)} aria-hidden="true" /> },
          { key: 'name', fill: 'clip', cell: (a) => a.name },
          {
            key: 'when',
            num: true,
            className: 'term-t3',
            cell: (a) => {
              const schedule = scheduleLabel(a, t, locale, now);
              // A state other than active is said after the schedule, and a
              // long schedule is cut before the state is.
              return (
                <span className="term-sched">
                  <span>{schedule}</span>
                  {a.status !== 'active' && (
                    <span className="term-word term-caps">
                      {schedule ? ' · ' : ''}
                      {statusLabel(a.status, t)}
                    </span>
                  )}
                </span>
              );
            },
          },
        ]}
      />
    </TermCard>
  );
}

/**
 * One automation: what a detail lookup found, or what was just created. A
 * phone's edge keeps its room for the name and the state, so the kind of
 * trigger is said only on a wider one.
 */
function OneCard({
  automation,
  executions = 0,
  created = false,
  onClick,
}: {
  automation: Automation;
  executions?: number;
  created?: boolean;
  onClick?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const narrow = useCardNarrow();
  const { nextRunAt, lastRunAt } = automation;
  return (
    <TermCard
      title={automation.name}
      sub={narrow ? undefined : triggerKind(automation, t)}
      meta={
        created
          ? <TermStatus tone="profit">{t('toolArtifact.automationStatus.created')}</TermStatus>
          : <AutomationStatus status={automation.status} />
      }
      onClick={onClick}
    >
      <TermKV
        cols={narrow ? 1 : 2}
        pairs={[
          [t('toolArtifact.schedule'), scheduleLabel(automation, t, locale, now)],
          !!nextRunAt && [t('toolArtifact.nextRun'), relativeTime(nextRunAt, locale, now)],
          !created && !!lastRunAt && [t('toolArtifact.lastRun'), relativeTime(lastRunAt, locale, now)],
          executions > 0 && [t('toolArtifact.executions'), t('toolArtifact.nTotal', { count: executions })],
        ]}
      />
    </TermCard>
  );
}
