import React from 'react';
import { Trash2, Edit2, Globe, Archive, ArchiveRestore } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useTitleFade } from '@/hooks/useTitleFade';
import { useThreadFlags } from '@/lib/threadLifecycle/store';

interface ThreadCardProps {
  thread: Record<string, unknown>;
  onClick: (thread: Record<string, unknown>) => void;
  onDelete?: (thread: Record<string, unknown>) => void;
  onRename?: (thread: Record<string, unknown>) => void;
  /** Archive (active view) — mutually exclusive with onUnarchive (archived view). */
  onArchive?: (thread: Record<string, unknown>) => void;
  onUnarchive?: (thread: Record<string, unknown>) => void;
}

/**
 * ThreadCard Component
 *
 * Displays a single thread as a card with:
 * - Thread title or index as the name
 * - Status badge
 * - Edit icon that triggers rename modal
 * - Delete icon that triggers deletion confirmation
 * - Click handler to navigate to the thread conversation
 */
function ThreadCard({ thread, onClick, onDelete, onRename, onArchive, onUnarchive }: ThreadCardProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  // Lifecycle indicator: live store state first (updates in place via the
  // user feed), falling back to the row's own enrichment for threads the
  // store hasn't observed yet.
  const tid = (thread.thread_id as string) || '';
  const { isRunning, needsInput, isUnseen, status: storeStatus } = useThreadFlags(tid);
  const runStatus =
    storeStatus !== 'idle' ? storeStatus : ((thread.run_status as string) ?? 'idle');
  const dotColor =
    isRunning || isUnseen
      ? 'var(--color-accent-primary)'
      : runStatus === 'completed'
      ? 'var(--color-profit)'
      : runStatus === 'failed'
      ? 'var(--color-loss)'
      : 'var(--color-text-tertiary)';
  const cardTitle = (thread.title as string) || `Thread ${(thread.thread_index as number | undefined) !== undefined ? (thread.thread_index as number) + 1 : ''}`;
  const titleFading = useTitleFade(cardTitle);
  const handleDeleteClick = (e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent card click when clicking delete icon
    if (onDelete) {
      onDelete(thread);
    }
  };

  const handleRenameClick = (e: React.MouseEvent) => {
    e.stopPropagation(); // Prevent card click when clicking edit icon
    if (onRename) {
      onRename(thread);
    }
  };
  return (
    <div
      className="group relative cursor-pointer transition-colors rounded-lg px-4 py-3 flex items-center gap-3 hover:bg-foreground/5"
      onClick={() => onClick(thread)}
      style={{
        borderBottom: '1px solid var(--color-border-muted)',
      }}
    >
      {/* Thread lifecycle indicator: pulsing amber = running, hollow amber
          ring = waiting for input, solid amber = finished unseen, green/red =
          settled outcome, grey = idle. */}
      {needsInput ? (
        <span
          role="img"
          aria-label="Waiting for your input"
          className="w-2 h-2 rounded-full flex-shrink-0"
          style={{ border: '1.5px solid var(--color-accent-primary)' }}
        />
      ) : (
        <div
          className={`w-2 h-2 rounded-full flex-shrink-0${isRunning ? ' animate-pulse' : ''}`}
          style={{ backgroundColor: dotColor }}
        />
      )}

      {/* Thread title and info */}
      <div className="flex-1 min-w-0">
        <h3 className={`text-sm font-normal truncate${titleFading ? ' animate-fade-in' : ''}`} style={{ color: 'var(--color-text-primary)' }}>
          {cardTitle}
        </h3>
        {!!thread.updated_at && (
          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
            {new Date(thread.updated_at as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}
          </p>
        )}
      </div>

      {/* Shared indicator */}
      {!!thread.is_shared && (
        <Globe
          className="h-3.5 w-3.5 flex-shrink-0"
          style={{ color: 'var(--color-accent-primary)' }}
        />
      )}

      {/* Action icons - Show on hover */}
      {(onRename || onDelete || onArchive || onUnarchive) && (
        <div className={`flex items-center gap-1 transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}>
          {/* Edit/Rename icon */}
          {onRename && (
            <button
              onClick={handleRenameClick}
              className="p-1.5 rounded-md transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
              title="Rename thread"
            >
              <Edit2 className="h-3.5 w-3.5" />
            </button>
          )}
          {/* Archive / Unarchive */}
          {onArchive && (
            <button
              onClick={(e) => { e.stopPropagation(); onArchive(thread); }}
              className="p-1.5 rounded-md transition hover:bg-foreground/10 active:scale-90"
              style={{ color: 'var(--color-text-tertiary)' }}
              title={t('thread.archive')}
            >
              <Archive className="h-3.5 w-3.5" />
            </button>
          )}
          {onUnarchive && (
            <button
              onClick={(e) => { e.stopPropagation(); onUnarchive(thread); }}
              className="p-1.5 rounded-md transition hover:bg-foreground/10 active:scale-90"
              style={{ color: 'var(--color-text-tertiary)' }}
              title={t('thread.unarchive')}
            >
              <ArchiveRestore className="h-3.5 w-3.5" />
            </button>
          )}
          {/* Delete icon */}
          {onDelete && (
            <button
              onClick={handleDeleteClick}
              className="p-1.5 rounded-md transition-colors hover:bg-[var(--color-danger-hover-bg)]"
              style={{ color: 'var(--color-loss)' }}
              title="Delete thread"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      )}
    </div>
  );
}

// Memoized so parent list re-renders (search, pagination, modals) skip
// unchanged rows — lifecycle flags arrive via the per-thread store hooks
// above, so a memo hit never suppresses a status update.
export default React.memo(ThreadCard);
