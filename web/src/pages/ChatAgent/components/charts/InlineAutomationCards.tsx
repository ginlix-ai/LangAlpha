import React from 'react';
import { Clock, Timer, TrendingUp, CheckCircle2 } from 'lucide-react';
import { cronToHuman } from '../../../Automations/utils/cron';
import { formatPriceTrigger } from '../../../Automations/utils/price';
import { formatRelativeTime } from '../../../Automations/utils/time';
import { useTranslation } from 'react-i18next';

// ─── Constants (matching InlineMarketCharts) ─────────────────────────

const GREEN = 'var(--color-profit)';
const YELLOW = 'var(--color-warning)';
const RED = 'var(--color-loss)';
const BLUE = 'var(--color-info)';
const TEXT_COLOR = 'var(--color-text-tertiary)';
const CARD_BG = 'var(--color-bg-tool-card)';
const CARD_BORDER = 'var(--color-border-muted)';

const cardStyle: React.CSSProperties = {
  background: CARD_BG,
  border: `1px solid ${CARD_BORDER}`,
  borderRadius: 8,
  padding: '12px 14px',
  cursor: 'pointer',
  transition: 'border-color 0.15s',
};

// ─── Helpers ─────────────────────────────────────────────────────────

const STATUS_COLORS: Record<string, string> = {
  active: GREEN,
  paused: YELLOW,
  failed: RED,
  completed: BLUE,
  disabled: RED,
};

function statusColor(status: string): string {
  return STATUS_COLORS[status] || TEXT_COLOR;
}

function scheduleLabel(automation: Record<string, unknown> | null | undefined): string {
  if (!automation) return '';
  if (automation.trigger_type === 'price') {
    return formatPriceTrigger(automation.trigger_config as any) || '';
  }
  if (automation.trigger_type === 'cron' && automation.schedule) {
    return cronToHuman(automation.schedule as string);
  }
  if (automation.next_run_at) {
    return formatRelativeTime(automation.next_run_at as string);
  }
  return (automation.schedule as string) || '';
}

interface StatusDotProps {
  status: string;
}

function StatusDot({ status }: StatusDotProps): React.ReactElement {
  return (
    <span
      style={{
        display: 'inline-block',
        width: 7,
        height: 7,
        borderRadius: '50%',
        backgroundColor: statusColor(status),
        flexShrink: 0,
      }}
    />
  );
}

interface QuoteRowProps {
  label: string;
  value: string;
}

function QuoteRow({ label, value }: QuoteRowProps): React.ReactElement {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0' }}>
      <span style={{ opacity: 0.7 }}>{label}</span>
      <span style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}>{value}</span>
    </div>
  );
}

// ─── Router ──────────────────────────────────────────────────────────

interface InlineAutomationCardProps {
  artifact: Record<string, unknown> | null | undefined;
  onClick?: () => void;
}

export function InlineAutomationCard({ artifact, onClick }: InlineAutomationCardProps): React.ReactElement | null {
  if (!artifact || artifact.type !== 'automations') return null;

  switch (artifact.mode) {
    case 'list':
      return <InlineAutomationListCard artifact={artifact} onClick={onClick} />;
    case 'detail':
      return <InlineAutomationDetailCard artifact={artifact} onClick={onClick} />;
    case 'created':
      return <InlineAutomationCreatedCard artifact={artifact} onClick={onClick} />;
    default:
      return null;
  }
}

// ─── List Card ───────────────────────────────────────────────────────

const MAX_INLINE = 4;

interface InlineAutomationListCardProps {
  artifact: Record<string, unknown>;
  onClick?: () => void;
}

function InlineAutomationListCard({ artifact, onClick }: InlineAutomationListCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const { automations = [], total = 0 } = artifact as { automations?: Record<string, unknown>[]; total?: number };
  if ((automations as Record<string, unknown>[]).length === 0) return null;

  const shown = (automations as Record<string, unknown>[]).slice(0, MAX_INLINE);
  const remaining = (total as number) - shown.length;

  return (
    <div
      style={cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
        <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: 13 }}>{t('toolArtifact.automations')}</span>
        <span
          style={{
            fontSize: 11,
            color: TEXT_COLOR,
            backgroundColor: 'var(--color-bg-surface)',
            padding: '1px 6px',
            borderRadius: 10,
          }}
        >
          {total as number}
        </span>
      </div>

      {/* Rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {shown.map((a, i) => (
          <div
            key={(a.automation_id as string) || i}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              padding: '3px 0',
              fontSize: 12,
            }}
          >
            <StatusDot status={a.status as string} />
            <span style={{ color: 'var(--color-text-primary)', fontWeight: 500, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {a.name as string}
            </span>
            <span style={{ color: TEXT_COLOR, flexShrink: 0, fontSize: 11 }}>
              {scheduleLabel(a)}
            </span>
          </div>
        ))}
      </div>

      {/* +N more */}
      {remaining > 0 && (
        <div style={{ marginTop: 4, fontSize: 11, color: TEXT_COLOR }}>
          {t('toolArtifact.nMoreAutomations', { count: remaining })}
        </div>
      )}
    </div>
  );
}

// ─── Detail Card ─────────────────────────────────────────────────────

interface InlineAutomationDetailCardProps {
  artifact: Record<string, unknown>;
  onClick?: () => void;
}

function InlineAutomationDetailCard({ artifact, onClick }: InlineAutomationDetailCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const { automation, executions: _executions = [], total_executions = 0 } = artifact as {
    automation?: Record<string, unknown>;
    executions?: Record<string, unknown>[];
    total_executions?: number;
  };
  if (!automation) return null;

  const isCron = automation.trigger_type === 'cron';
  const isPrice = automation.trigger_type === 'price';
  const Icon = isPrice ? TrendingUp : isCron ? Clock : Timer;

  return (
    <div
      style={cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header: icon + name + status */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
        <Icon size={14} style={{ color: TEXT_COLOR, flexShrink: 0 }} />
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: 14, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {automation.name as string}
        </span>
        <span style={{ fontSize: 12, fontWeight: 500, color: statusColor(automation.status as string), flexShrink: 0 }}>
          {automation.status as string}
        </span>
      </div>

      {/* Key-value rows */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2px 20px', fontSize: 12, color: TEXT_COLOR }}>
        <QuoteRow label={t('toolArtifact.schedule')} value={scheduleLabel(automation)} />
        <QuoteRow label={t('toolArtifact.nextRun')} value={automation.next_run_at ? formatRelativeTime(automation.next_run_at as string) : '\u2014'} />
        {!!automation.last_run_at && (
          <QuoteRow label={t('toolArtifact.lastRun')} value={formatRelativeTime(automation.last_run_at as string)} />
        )}
        {(total_executions as number) > 0 && (
          <QuoteRow label={t('toolArtifact.executions')} value={t('toolArtifact.nTotal', { count: total_executions as number })} />
        )}
      </div>
    </div>
  );
}

// ─── Created Card ────────────────────────────────────────────────────

interface InlineAutomationCreatedCardProps {
  artifact: Record<string, unknown>;
  onClick?: () => void;
}

function InlineAutomationCreatedCard({ artifact, onClick }: InlineAutomationCreatedCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!artifact) return null;

  return (
    <div
      style={{
        ...cardStyle,
        borderColor: 'var(--color-profit-border)',
      }}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-profit-border-hover)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = 'var(--color-profit-border)')}
    >
      {/* Header: checkmark + "Automation Created" */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
        <CheckCircle2 size={14} style={{ color: GREEN }} />
        <span style={{ fontWeight: 600, color: GREEN, fontSize: 13 }}>{t('toolArtifact.automationCreated')}</span>
      </div>

      {/* Name */}
      <div style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: 14, marginBottom: 8 }}>
        {artifact.name as string}
      </div>

      {/* Details */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '2px 20px', fontSize: 12, color: TEXT_COLOR }}>
        <QuoteRow label={t('toolArtifact.schedule')} value={scheduleLabel(artifact)} />
        {!!artifact.next_run_at && (
          <QuoteRow label={t('toolArtifact.nextRun')} value={formatRelativeTime(artifact.next_run_at as string)} />
        )}
      </div>
    </div>
  );
}
