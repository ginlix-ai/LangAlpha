import {
  Archive, Cpu, HardDrive, MemoryStick, MonitorCog, Play, RefreshCw, Square,
} from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { SandboxStats } from './sandboxTypes';

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

// Wire values are provider identifiers, not user copy. Unmapped values must not
// reach the screen: daytona's SDK coerces anything it doesn't recognize to
// 'unknown_default_open_api', and a bare capitalize would render that verbatim.
const STATE_LABELS: Record<string, string> = {
  running: 'Running',
  stopped: 'Stopped',
  starting: 'Starting',
  stopping: 'Stopping',
  archiving: 'Archiving',
  archived: 'Archived',
  restoring: 'Restoring',
  resizing: 'Resizing',
  creating: 'Creating',
  destroying: 'Destroying',
  destroyed: 'Destroyed',
  pausing: 'Pausing',
  paused: 'Paused',
  resuming: 'Resuming',
  snapshotting: 'Snapshotting',
  forking: 'Forking',
  error: 'Error',
  deleted: 'Deleted',
  build_failed: 'Build failed',
  pending_build: 'Pending build',
  building_snapshot: 'Building snapshot',
  pulling_snapshot: 'Pulling snapshot',
  unknown: 'Unknown',
};

interface OverviewTabProps {
  stats: SandboxStats;
  isRunning: boolean;
  actionLoading: boolean;
  refreshing: boolean;
  onStartStop: (action: string) => void;
  onRefresh: () => void;
}

export function OverviewTab({ stats, isRunning, actionLoading, refreshing, onStartStop, onRefresh }: OverviewTabProps) {
  const isTransitioning =
    actionLoading || (!!stats.state && !TERMINAL_STATES.has(stats.state));
  const stateLabel = stats.state
    ? (STATE_LABELS[stats.state] ?? 'Updating')
    : 'Unknown';
  const resourceCards = [
    { icon: Cpu, label: 'CPU', value: stats.resources.cpu != null ? `${stats.resources.cpu} vCPU` : '---' },
    { icon: MemoryStick, label: 'Memory', value: stats.resources.memory != null ? `${stats.resources.memory} GiB` : '---' },
    { icon: HardDrive, label: 'Disk', value: stats.resources.disk != null ? `${stats.resources.disk} GiB` : '---' },
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
            <Icon className="h-5 w-5 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
            <div>
              <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{label}</div>
              <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{value}</div>
            </div>
          </div>
        ))}
      </div>

      {/* Status + metadata */}
      <div
        className="flex items-center justify-between p-3 rounded-lg"
        style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
      >
        <div className="flex items-center gap-3" role="status" aria-live="polite">
          {isTransitioning ? (
            <span aria-hidden="true" className="flex-shrink-0">
              <Loader size={14} className="text-[color:var(--color-text-tertiary)]" />
            </span>
          ) : (
            <div
              aria-hidden="true"
              className="w-2.5 h-2.5 rounded-full flex-shrink-0"
              style={{ backgroundColor: isRunning ? 'var(--color-profit)' : 'var(--color-loss)' }}
            />
          )}
          <div>
            <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {isTransitioning
                ? (actionLoading ? 'Updating...' : `${stateLabel}...`)
                : stateLabel}
            </div>
            {stats.created_at && (
              <div className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                Created {new Date(stats.created_at).toLocaleDateString()}
              </div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {stats.auto_stop_interval != null && (
            <span className="text-xs px-2 py-1 rounded" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}>
              {/* 0 disables auto-stop entirely, so rendering "0m" states the opposite */}
              {stats.auto_stop_interval === 0
                ? 'Always on'
                : `Auto-stop: ${stats.auto_stop_interval}m`}
            </span>
          )}
          {/* Never disabled. In a transitional state every other control here is,
              and nothing polls — without this the panel has no way to advance. */}
          <button
            onClick={onRefresh}
            aria-label="Refresh sandbox status"
            title="Refresh status"
            className="flex items-center gap-1.5 px-2 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
          >
            {refreshing
              ? (
                <span aria-hidden="true" className="flex-shrink-0">
                  <Loader size={12} className="text-current" />
                </span>
              )
              : <RefreshCw className="h-3 w-3" aria-hidden="true" />}
          </button>
          {!isRunning && stats.state === 'stopped' && (
            <button
              onClick={() => onStartStop('archive')}
              disabled={isTransitioning}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-text-tertiary)', border: '1px solid var(--color-border-muted)' }}
            >
              <Archive className="h-3 w-3" />
              Archive
            </button>
          )}
          {isRunning ? (
            <button
              onClick={() => onStartStop('stop')}
              disabled={isTransitioning}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-loss)', border: '1px solid var(--color-border-loss)' }}
            >
              <Square className="h-3 w-3" />
              Stop
            </button>
          ) : (
            <button
              onClick={() => onStartStop('start')}
              disabled={isTransitioning}
              className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
              style={{ color: 'var(--color-profit)', border: '1px solid var(--color-profit-border)' }}
            >
              <Play className="h-3 w-3" />
              Start
            </button>
          )}
        </div>
      </div>

      {/* Sandbox ID */}
      {stats.sandbox_id && (
        <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          Sandbox ID: <span className="font-mono">{stats.sandbox_id}</span>
        </div>
      )}
    </div>
  );
}
