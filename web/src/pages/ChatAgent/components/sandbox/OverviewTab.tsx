import {
  Archive, Cpu, Folder, HardDrive, MemoryStick, MonitorCog, Play, RefreshCw, Server, Square,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useLocale } from '@/hooks/useLocale';
import { createDateFormatter } from '@/lib/format';
import type { ComputerStatus } from '@/types/api';
import { COMPUTER_STATUS_UI, computerStatusUi } from '../computerStatusUi';
import type { SandboxStats } from './sandboxTypes';

// The fields Date#toLocaleDateString() prints by default.
const createdDate = createDateFormatter({ year: 'numeric', month: 'numeric', day: 'numeric' });

// States where the sandbox has settled. Deliberately an allowlist of *terminal*
// values rather than of in-progress ones: the provider has ~23 states and keeps
// adding them, so anything unrecognized must fail safe to "in progress" (spinner,
// actions disabled) instead of rendering as a settled failure with a live Start.
// Provider synonyms are deliberately absent: the API canonicalizes them, and
// compensating here too would let the two vocabularies drift apart silently.
const TERMINAL_STATES = new Set([
  'running',
  'unknown',
  'stopped',
  'archived',
  'error',
  'paused',
  'destroyed',
  'build_failed',
  'deleted',
]);

// Wire values are provider identifiers, not user copy. States a computer can
// be in read `computerStatusUi`; these are the ones only the sandbox provider
// reports. Literal keys keep them visible to the catalog key test. Anything
// else, such as daytona's 'unknown_default_open_api' coercion, must not reach
// the screen verbatim.
const PROVIDER_STATE_LABEL_KEY: Record<string, { labelKey: string }> = {
  archiving: { labelKey: 'computer.overview.state.archiving' },
  archived: { labelKey: 'computer.overview.state.archived' },
  restoring: { labelKey: 'computer.overview.state.restoring' },
  resizing: { labelKey: 'computer.overview.state.resizing' },
  destroying: { labelKey: 'computer.overview.state.destroying' },
  destroyed: { labelKey: 'computer.overview.state.destroyed' },
  pausing: { labelKey: 'computer.overview.state.pausing' },
  paused: { labelKey: 'computer.overview.state.paused' },
  resuming: { labelKey: 'computer.overview.state.resuming' },
  snapshotting: { labelKey: 'computer.overview.state.snapshotting' },
  forking: { labelKey: 'computer.overview.state.forking' },
  build_failed: { labelKey: 'computer.overview.state.build_failed' },
  pending_build: { labelKey: 'computer.overview.state.pending_build' },
  building_snapshot: { labelKey: 'computer.overview.state.building_snapshot' },
  pulling_snapshot: { labelKey: 'computer.overview.state.pulling_snapshot' },
  unknown: { labelKey: 'computer.overview.state.unknown' },
};

function stateLabelUi(state: string): { labelKey: string; fallback?: string } {
  // hasOwn: a wire value like '__proto__' must not resolve to a prototype member.
  if (Object.hasOwn(COMPUTER_STATUS_UI, state)) return COMPUTER_STATUS_UI[state as ComputerStatus];
  if (Object.hasOwn(PROVIDER_STATE_LABEL_KEY, state)) return PROVIDER_STATE_LABEL_KEY[state];
  return computerStatusUi(null);
}

interface OverviewTabProps {
  stats: SandboxStats;
  isRunning: boolean;
  actionLoading: boolean;
  refreshing: boolean;
  onStartStop: (action: string) => void;
  /** Only a computer stops; a workspace row that names none offers no Stop. */
  canStop?: boolean;
  onRefresh: () => void;
  /** The machine this workspace lives on, when it names one. */
  computerName?: string | null;
  recoverableCreating?: boolean;
  /** The workspace's folder on that machine. */
  dirName?: string | null;
}

export function OverviewTab({ stats, isRunning, actionLoading, refreshing, onStartStop, canStop = true, onRefresh, computerName, dirName, recoverableCreating = false }: OverviewTabProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const isTransitioning =
    actionLoading || (!!stats.state && !TERMINAL_STATES.has(stats.state) &&
      !(stats.state === 'creating' && recoverableCreating));
  const stateUi = stateLabelUi(stats.state || 'unknown');
  const stateLabel = t(stateUi.labelKey, stateUi.fallback ?? '');
  const resourceCards = [
    { icon: Cpu, label: 'CPU', value: stats.resources.cpu != null ? `${stats.resources.cpu} vCPU` : '---' },
    { icon: MemoryStick, label: t('computer.overview.memory'), value: stats.resources.memory != null ? `${stats.resources.memory} GiB` : '---' },
    { icon: HardDrive, label: t('computer.overview.disk'), value: stats.resources.disk != null ? `${stats.resources.disk} GiB` : '---' },
    { icon: MonitorCog, label: 'GPU', value: stats.resources.gpu != null ? `${stats.resources.gpu} GPU` : '---' },
  ];

  return (
    <div className="flex flex-col gap-5">
      {/* Resource cards -- 2x2 grid */}
      <div className="grid grid-cols-2 gap-3">
        {resourceCards.map(({ icon: Icon, label, value }) => (
          <div
            key={label}
            className="flex items-center gap-3 p-3 rounded-lg"
            style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
          >
            <Icon className="h-5 w-5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
            <div>
              <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{label}</div>
              <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{value}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Status + control. The machine is shared, so the rows that say where
          this workspace lives and what a stop reaches sit in the same card as
          the button, read before it is pressed rather than found beneath it. */}
      <div
        className="flex flex-col gap-3 p-3 rounded-lg"
        style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
      >
      {(computerName || dirName) && (
        <div className="flex flex-col gap-1 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {computerName && (
            <div className="flex items-center gap-1.5">
              <Server className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
              <span className="font-medium">{t('computer.onComputer', { name: computerName })}</span>
            </div>
          )}
          {dirName && (
            <div className="flex items-center gap-1.5">
              <Folder className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
              <span className="font-mono">{dirName}</span>
            </div>
          )}
          {computerName && (
            <p style={{ color: 'var(--color-text-tertiary)' }}>
              {t('computer.sharedActionWarning', 'Starting or stopping this computer affects every workspace on it.')}
            </p>
          )}
        </div>
      )}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3" role="status" aria-live="polite">
          {isTransitioning ? (
            <span aria-hidden="true" className="shrink-0">
              <Loader size={14} className="text-(--color-text-tertiary)" />
            </span>
          ) : (
            <div
              aria-hidden="true"
              className="w-2.5 h-2.5 rounded-full shrink-0"
              style={{ backgroundColor: isRunning ? 'var(--color-profit)' : 'var(--color-loss)' }}
            />
          )}
          <div>
            <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {isTransitioning
                ? (actionLoading ? t('computer.overview.transition', { state: t('computer.statusUpdating') }) : t('computer.overview.transition', { state: stateLabel }))
                : stateLabel}
            </div>
            {stats.created_at && (
              <div className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('computer.overview.created', { date: createdDate(new Date(stats.created_at), locale) })}
              </div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {stats.auto_stop_interval != null && (
            <span className="text-xs px-2 py-1 rounded" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}>
              {/* 0 disables auto-stop entirely, so rendering "0m" states the opposite */}
              {stats.auto_stop_interval === 0
                ? t('computer.alwaysOn')
                : t('computer.overview.autoStop', { minutes: stats.auto_stop_interval })}
            </span>
          )}
          {/* Never disabled. In a transitional state every other control here is,
              and nothing polls — without this the panel has no way to advance. */}
          <button
            onClick={onRefresh}
            aria-label={t('computer.overview.refreshAria')}
            title={t('computer.overview.refresh')}
            className="flex items-center gap-1.5 px-2 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
          >
            {refreshing
              ? (
                <span aria-hidden="true" className="shrink-0">
                  <Loader size={12} className="text-current" />
                </span>
              )
              : <RefreshCw className="h-3 w-3" aria-hidden="true" />}
          </button>
          {!isRunning && stats.state === 'stopped' && (
            <button
              onClick={() => onStartStop('archive')}
              disabled={isTransitioning}
              data-computer-power
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
            >
              <Archive className="h-3 w-3" />
              {t('computer.overview.archive')}
            </button>
          )}
          {isRunning ? (canStop && (
            <button
              onClick={() => onStartStop('stop')}
              disabled={isTransitioning}
              data-computer-power
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-loss)', border: '1px solid var(--color-border-loss)' }}
            >
              <Square className="h-3 w-3" />
              {computerName ? t('computer.stopComputer', 'Stop computer') : t('computer.stop', 'Stop')}
            </button>
          )) : (
            <button
              onClick={() => onStartStop('start')}
              disabled={isTransitioning}
              data-computer-power
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-profit)', border: '1px solid var(--color-profit-border)' }}
            >
              <Play className="h-3 w-3" />
              {computerName ? t('computer.startComputer', 'Start computer') : t('computer.start', 'Start')}
            </button>
          )}
        </div>
      </div>
      </div>

      {/* Sandbox ID */}
      {stats.sandbox_id && (
        <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('computer.overview.sandboxId')} <span className="font-mono">{stats.sandbox_id}</span>
        </div>
      )}
    </div>
  );
}
