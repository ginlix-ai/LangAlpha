import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Clock, Timer, TrendingUp, ExternalLink } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { cronToHuman } from '../../../Automations/utils/cron';
import { isPriceTriggerConfig, formatPriceTrigger, formatRetriggerMode } from '../../../Automations/utils/price';
import type { PriceTriggerConfig } from '@/types/automation';
import { formatRelativeTime, formatDateTime, formatDuration } from '../../../Automations/utils/time';

// ─── Constants ───────────────────────────────────────────────────────

const GREEN = 'var(--color-profit)';
const YELLOW = 'var(--color-warning)';
const RED = 'var(--color-loss)';
const BLUE = 'var(--color-info)';
const TEXT_SECONDARY = 'var(--color-text-tertiary)';

const STATUS_COLORS: Record<string, string> = {
  active: GREEN,
  running: GREEN,
  paused: YELLOW,
  failed: RED,
  completed: BLUE,
  disabled: RED,
};

function statusColor(status: string): string {
  return STATUS_COLORS[status] || TEXT_SECONDARY;
}

// ─── Shared sub-components ───────────────────────────────────────────

const STATUS_BG: Record<string, string> = {
  active: 'var(--color-profit-soft)',
  running: 'var(--color-profit-soft)',
  paused: 'var(--color-warning-soft)',
  failed: 'var(--color-loss-soft)',
  completed: 'var(--color-info-soft)',
  disabled: 'var(--color-loss-soft)',
};

interface StatusBadgeProps {
  status: string;
}

function StatusBadge({ status }: StatusBadgeProps): React.ReactElement {
  const color = statusColor(status);
  return (
    <span
      style={{
        fontSize: 11,
        fontWeight: 600,
        padding: '2px 8px',
        borderRadius: 10,
        backgroundColor: STATUS_BG[status] || 'var(--color-border-muted)',
        color,
        textTransform: 'capitalize',
      }}
    >
      {status}
    </span>
  );
}

interface StatCardProps {
  label: string;
  value: string;
  sub?: string | null;
  badge?: string | null;
}

function StatCard({ label, value, sub, badge }: StatCardProps): React.ReactElement {
  return (
    <div
      style={{
        backgroundColor: 'var(--color-bg-surface)',
        border: '1px solid var(--color-border-muted)',
        borderRadius: 8,
        padding: '8px 12px',
      }}
    >
      <p style={{ fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em', color: TEXT_SECONDARY, marginBottom: 4 }}>
        {label}
      </p>
      <p style={{ fontSize: 14, color: 'var(--color-text-primary)', fontWeight: 500 }}>{value}</p>
      {(sub || badge) && (
        <p style={{ fontSize: 11, color: TEXT_SECONDARY, marginTop: 2, display: 'flex', alignItems: 'center', gap: 4 }}>
          {sub && <span>{sub}</span>}
          {badge && (
            <span
              style={{
                fontSize: 9,
                fontWeight: 600,
                padding: '1px 5px',
                borderRadius: 4,
                backgroundColor: 'var(--color-info-soft)',
                color: BLUE,
                textTransform: 'uppercase',
                letterSpacing: '0.03em',
              }}
            >
              {badge}
            </span>
          )}
        </p>
      )}
    </div>
  );
}

interface ConfigRowProps {
  label: string;
  value: string;
}

function ConfigRow({ label, value }: ConfigRowProps): React.ReactElement {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '4px 0', fontSize: 13 }}>
      <span style={{ color: TEXT_SECONDARY }}>{label}</span>
      <span style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}>{value}</span>
    </div>
  );
}

function asPriceTrigger(v: unknown): PriceTriggerConfig | null {
  return isPriceTriggerConfig(v) ? v : null;
}

function marketI18nKey(triggerConfig: PriceTriggerConfig | null | undefined): string | null {
  if (!triggerConfig) return null;
  return triggerConfig.market === 'index' ? 'automation.marketIndex' : null;
}

function scheduleLabel(auto: Record<string, unknown> | null | undefined): string {
  if (!auto) return '\u2014';
  if (auto.trigger_type === 'price') return formatPriceTrigger(asPriceTrigger(auto.trigger_config)) || '\u2014';
  if (auto.trigger_type === 'cron' && auto.schedule) return cronToHuman(auto.schedule as string);
  if (auto.next_run_at) return formatRelativeTime(auto.next_run_at as string);
  return (auto.schedule as string) || '\u2014';
}

const ACCENT = 'var(--color-accent-primary)';

interface AutomationsPageLinkProps {
  automationId?: string;
}

function AutomationsPageLink({ automationId }: AutomationsPageLinkProps): React.ReactElement {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const path = automationId ? `/automations?id=${automationId}` : '/automations';
  return (
    <button
      onClick={() => navigate(path)}
      className="flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors hover:bg-foreground/5"
      style={{ color: ACCENT, border: '1px solid var(--color-accent-soft)' }}
    >
      <ExternalLink size={14} />
      {t('toolArtifact.viewInAutomations')}
    </button>
  );
}

// ─── Router ──────────────────────────────────────────────────────────

interface AutomationDetailPanelProps {
  data: Record<string, unknown>;
}

export default function AutomationDetailPanel({ data }: AutomationDetailPanelProps): React.ReactElement | null {
  if (!data || data.type !== 'automations') return null;

  switch (data.mode) {
    case 'list':
      return <ListPanel automations={(data.automations || []) as Record<string, unknown>[]} total={(data.total || 0) as number} />;
    case 'detail':
      return <DetailPanel automation={data.automation as Record<string, unknown>} executions={(data.executions || []) as Record<string, unknown>[]} totalExecutions={(data.total_executions || 0) as number} />;
    case 'created':
      return <CreatedPanel data={data} />;
    default:
      return null;
  }
}

// ─── List Panel ──────────────────────────────────────────────────────

interface ListPanelProps {
  automations: Record<string, unknown>[];
  total: number;
}

function ListPanel({ automations, total }: ListPanelProps): React.ReactElement {
  const { t } = useTranslation();
  if (automations.length === 0) {
    return (
      <div style={{ padding: 16, color: TEXT_SECONDARY, fontSize: 14 }}>
        {t('toolArtifact.noAutomationsFound')}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <p style={{ fontSize: 12, color: TEXT_SECONDARY }}>
        {t('toolArtifact.nAutomations', { count: total })}
      </p>
      {automations.map((a, i) => {
        const isCron = a.trigger_type === 'cron';
        const isPrice = a.trigger_type === 'price';
        const Icon = isPrice ? TrendingUp : isCron ? Clock : Timer;
        return (
          <div
            key={(a.automation_id as string) || i}
            style={{
              backgroundColor: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-muted)',
              borderRadius: 8,
              padding: '10px 14px',
            }}
          >
            {/* Row 1: icon + name + status badge */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
              <Icon size={14} style={{ color: TEXT_SECONDARY, flexShrink: 0 }} />
              <span style={{ color: 'var(--color-text-primary)', fontWeight: 600, fontSize: 14, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {a.name as string}
              </span>
              <StatusBadge status={a.status as string} />
            </div>
            {/* Row 2: schedule + next run + agent mode */}
            <div style={{ display: 'flex', gap: 16, fontSize: 12, color: TEXT_SECONDARY, flexWrap: 'wrap' }}>
              <span>{scheduleLabel(a)}</span>
              {!!a.next_run_at && (
                <span>{t('toolArtifact.next', { time: formatRelativeTime(a.next_run_at as string) })}</span>
              )}
              {!!a.agent_mode && (
                <span
                  style={{
                    fontSize: 10,
                    fontWeight: 600,
                    padding: '1px 6px',
                    borderRadius: 4,
                    backgroundColor: 'var(--color-bg-surface)',
                    color: 'var(--color-text-tertiary)',
                    textTransform: 'uppercase',
                    letterSpacing: '0.03em',
                  }}
                >
                  {a.agent_mode as string}
                </span>
              )}
            </div>
          </div>
        );
      })}
      <AutomationsPageLink />
    </div>
  );
}

// ─── Detail Panel ────────────────────────────────────────────────────

interface DetailPanelProps {
  automation: Record<string, unknown> | undefined;
  executions: Record<string, unknown>[];
  totalExecutions: number;
}

function DetailPanel({ automation, executions, totalExecutions }: DetailPanelProps): React.ReactElement | null {
  const { t } = useTranslation();
  if (!automation) return null;

  const isCron = automation.trigger_type === 'cron';
  const isPrice = automation.trigger_type === 'price';
  const Icon = isPrice ? TrendingUp : isCron ? Clock : Timer;

  return (
    <div className="space-y-4">
      {/* Header: icon + name + status */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <Icon size={16} style={{ color: TEXT_SECONDARY, flexShrink: 0 }} />
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: 16, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {automation.name as string}
        </span>
        <StatusBadge status={automation.status as string} />
      </div>

      {/* Stat grid */}
      {isPrice ? (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10 }}>
          <StatCard
            label={t('toolArtifact.trigger')}
            value={formatPriceTrigger(asPriceTrigger(automation.trigger_config)) || '\u2014'}
            sub={asPriceTrigger(automation.trigger_config)?.symbol || null}
            badge={(() => { const k = marketI18nKey(asPriceTrigger(automation.trigger_config)); return k ? t(k) : null; })()}
          />
          <StatCard
            label={t('toolArtifact.retrigger')}
            value={formatRetriggerMode(asPriceTrigger(automation.trigger_config))}
          />
          <StatCard
            label={t('toolArtifact.lastRun')}
            value={automation.last_run_at ? formatRelativeTime(automation.last_run_at as string) : '\u2014'}
            sub={automation.last_run_at ? formatDateTime(automation.last_run_at as string) : null}
          />
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10 }}>
          <StatCard
            label={t('toolArtifact.schedule')}
            value={isCron ? cronToHuman(automation.schedule as string) : t('toolArtifact.oneTime')}
            sub={isCron ? (automation.schedule as string) : null}
          />
          <StatCard
            label={t('toolArtifact.nextRun')}
            value={automation.next_run_at ? formatRelativeTime(automation.next_run_at as string) : '\u2014'}
            sub={automation.next_run_at ? formatDateTime(automation.next_run_at as string) : null}
          />
          <StatCard
            label={t('toolArtifact.lastRun')}
            value={automation.last_run_at ? formatRelativeTime(automation.last_run_at as string) : '\u2014'}
            sub={automation.last_run_at ? formatDateTime(automation.last_run_at as string) : null}
          />
        </div>
      )}

      {/* Instruction */}
      {!!automation.instruction && (
        <div>
          <p style={{ fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em', color: TEXT_SECONDARY, marginBottom: 6 }}>
            {t('toolArtifact.instruction')}
          </p>
          <div
            style={{
              backgroundColor: 'var(--color-bg-surface)',
              borderRadius: 8,
              padding: '10px 12px',
              fontSize: 13,
              color: 'var(--color-text-tertiary)',
              whiteSpace: 'pre-wrap',
              lineHeight: 1.5,
            }}
          >
            {automation.instruction as string}
          </div>
        </div>
      )}

      {/* Configuration */}
      <div>
        <p style={{ fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em', color: TEXT_SECONDARY, marginBottom: 6 }}>
          {t('toolArtifact.configuration')}
        </p>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 24px' }}>
          {!!automation.agent_mode && (
            <ConfigRow label={t('toolArtifact.agentMode')} value={(automation.agent_mode as string).toUpperCase()} />
          )}
          <ConfigRow label={t('toolArtifact.triggerType')} value={isPrice ? t('toolArtifact.priceTrigger') : isCron ? t('toolArtifact.recurringCron') : t('toolArtifact.oneTime')} />
        </div>
      </div>

      {/* Execution History */}
      {executions.length > 0 && (
        <div>
          <p style={{ fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em', color: TEXT_SECONDARY, marginBottom: 6 }}>
            {t('toolArtifact.executionHistory', { count: totalExecutions })}
          </p>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {executions.map((e, i) => (
              <div
                key={(e.execution_id as string) || i}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  padding: '6px 10px',
                  fontSize: 12,
                  backgroundColor: 'var(--color-bg-surface)',
                  borderRadius: 6,
                }}
              >
                <span
                  style={{
                    display: 'inline-block',
                    width: 7,
                    height: 7,
                    borderRadius: '50%',
                    backgroundColor: statusColor(e.status as string),
                    flexShrink: 0,
                  }}
                />
                <span style={{ color: 'var(--color-text-primary)', fontWeight: 500, flex: 1 }}>
                  {e.scheduled_at ? formatDateTime(e.scheduled_at as string) : t('toolArtifact.manualTrigger')}
                </span>
                {!!(e.started_at && e.completed_at) && (
                  <span style={{ color: TEXT_SECONDARY, fontSize: 11 }}>
                    {formatDuration(e.started_at as string, e.completed_at as string)}
                  </span>
                )}
                {!!e.error_message && (
                  <span style={{ color: RED, fontSize: 11, maxWidth: 150, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={e.error_message as string}>
                    {e.error_message as string}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      <AutomationsPageLink automationId={automation.automation_id as string} />
    </div>
  );
}

// ─── Created Panel ───────────────────────────────────────────────────

interface CreatedPanelProps {
  data: Record<string, unknown>;
}

function CreatedPanel({ data }: CreatedPanelProps): React.ReactElement {
  const { t } = useTranslation();
  const isCron = data.trigger_type === 'cron';
  const isPrice = data.trigger_type === 'price';

  return (
    <div className="space-y-4">
      {/* Confirmation header */}
      <div
        style={{
          backgroundColor: 'var(--color-profit-soft)',
          border: '1px solid var(--color-profit-border)',
          borderRadius: 8,
          padding: '12px 14px',
        }}
      >
        <p style={{ color: GREEN, fontWeight: 600, fontSize: 14, marginBottom: 4 }}>
          {t('toolArtifact.automationCreatedSuccess')}
        </p>
        <p style={{ color: 'var(--color-text-primary)', fontWeight: 600, fontSize: 16 }}>
          {data.name as string}
        </p>
      </div>

      {/* Details */}
      {isPrice ? (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          <StatCard label={t('toolArtifact.trigger')} value={formatPriceTrigger(asPriceTrigger(data.trigger_config)) || '\u2014'} sub={asPriceTrigger(data.trigger_config)?.symbol || null} badge={(() => { const k = marketI18nKey(asPriceTrigger(data.trigger_config)); return k ? t(k) : null; })()} />
          <StatCard label={t('toolArtifact.retrigger')} value={formatRetriggerMode(asPriceTrigger(data.trigger_config))} />
        </div>
      ) : (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }}>
          <StatCard label={t('toolArtifact.schedule')} value={isCron ? cronToHuman(data.schedule as string) : t('toolArtifact.oneTime')} sub={isCron ? (data.schedule as string) : null} />
          <StatCard
            label={t('toolArtifact.nextRun')}
            value={data.next_run_at ? formatRelativeTime(data.next_run_at as string) : '\u2014'}
            sub={data.next_run_at ? formatDateTime(data.next_run_at as string) : null}
          />
        </div>
      )}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 24px' }}>
        <ConfigRow label={t('toolArtifact.status')} value={(data.status as string) || 'active'} />
        <ConfigRow label={t('toolArtifact.triggerType')} value={isPrice ? t('toolArtifact.priceTrigger') : isCron ? t('toolArtifact.recurring') : t('toolArtifact.oneTime')} />
      </div>

      <AutomationsPageLink automationId={data.automation_id as string} />
    </div>
  );
}
