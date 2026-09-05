import { ChevronDown, ChevronRight } from 'lucide-react';
import { ListEmpty } from '../mcp/McpPrimitives';
import type { SandboxStats } from './sandboxTypes';

interface StorageTabProps {
  stats: SandboxStats;
  showDirBreakdown: boolean;
  onToggleBreakdown: () => void;
}

export function StorageTab({ stats, showDirBreakdown, onToggleBreakdown }: StorageTabProps) {
  const disk = stats.disk_usage;

  if (!disk) {
    return <ListEmpty>Disk usage information unavailable</ListEmpty>;
  }

  // Parse use_percent for the progress bar
  const pct = parseInt(disk.use_percent, 10) || 0;

  return (
    <div className="flex flex-col gap-5">
      {/* Usage bar. Docker sets no size quota, so df(1) inside the container reports
          the host filesystem — showing it as the sandbox's disk would be a wrong number. */}
      {stats.provider === 'docker' ? (
        <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          Local sandboxes run without a disk quota, so total usage would describe the
          host filesystem rather than this sandbox. The per-directory sizes below are
          sandbox-scoped.
        </div>
      ) : (
        <div className="flex flex-col gap-2">
          <div className="flex justify-between text-sm" style={{ color: 'var(--color-text-primary)' }}>
            <span>{disk.used} used</span>
            <span>{disk.available} available</span>
          </div>
          <div className="h-3 rounded-full overflow-hidden" style={{ backgroundColor: 'var(--color-bg-card)' }}>
            <div
              className="h-full rounded-full transition-all"
              style={{
                width: `${pct}%`,
                backgroundColor: pct > 80 ? 'var(--color-loss)' : 'var(--color-accent-primary)',
              }}
            />
          </div>
          <div className="flex justify-between text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            <span>{disk.use_percent} used</span>
            <span>{disk.total} total</span>
          </div>
        </div>
      )}

      {/* Directory breakdown toggle */}
      {stats.directory_breakdown && stats.directory_breakdown.length > 0 && (
        <div>
          <button
            onClick={onToggleBreakdown}
            className="flex items-center gap-1.5 text-sm transition-colors hover:opacity-80"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {showDirBreakdown ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            Details ({stats.directory_breakdown.length} directories)
          </button>

          {showDirBreakdown && (
            <div className="mt-3 flex flex-col gap-1">
              {stats.directory_breakdown.map((d) => (
                <div
                  key={d.path}
                  className="flex justify-between py-1.5 px-3 rounded text-sm"
                  style={{ backgroundColor: 'var(--color-bg-card)' }}
                >
                  <span className="font-mono truncate" style={{ color: 'var(--color-text-primary)' }}>{d.path}/</span>
                  <span className="flex-shrink-0 ml-4" style={{ color: 'var(--color-text-tertiary)' }}>{d.size}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
