import React from 'react';
import { Trash2, Edit2, ExternalLink, Link2, Archive, ArchiveRestore, Users } from 'lucide-react';
import { useOpenInNewTab } from '@/components/PageTabs/useOpenInNewTab';
import { threadPath } from '../utils/chatRoutes';
import { useTranslation } from 'react-i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { useTitleFade } from '@/hooks/useTitleFade';
import { shortMonthDay } from '@/lib/format';
import { useThreadFlags } from '@/lib/threadLifecycle/store';
import { draftKeys, useWaitingDraft } from '@/lib/composerDrafts';
import { DraftMark } from '@/components/ui/draft-mark';
import { cn } from '@/lib/utils';
import { PersonAvatar } from '@/components/organization/PersonAvatar';
import { personLabel, threadByline } from '@/lib/sharedThreads';
import type { Thread } from '@/types/api';


interface ThreadCardProps {
  thread: Thread;
  onClick: (thread: Thread) => void;
  onDelete?: (thread: Thread) => void;
  onRename?: (thread: Thread) => void;
  /** Archive (active view) — mutually exclusive with onUnarchive (archived view). */
  onArchive?: (thread: Thread) => void;
  onUnarchive?: (thread: Thread) => void;
  /** The reader's user id, in a shared workspace: each card then names who
   *  started the thread and who wrote in it last. */
  sharedMe?: string;
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
function ThreadCard({ thread, onClick, onDelete, onRename, onArchive, onUnarchive, sharedMe }: ThreadCardProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const openInNewTab = useOpenInNewTab();
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
  const cardTitle = (thread.title as string) || t('nav.untitledThread');
  const titleFading = useTitleFade(cardTitle);
  const byline = threadByline(sharedMe ? thread.shared : undefined, sharedMe);
  // Left unsent in its composer: the line under the title leads with it.
  const hasDraft = useWaitingDraft(draftKeys.thread(tid)) !== null;
  const date = thread.updated_at ? shortMonthDay(new Date(thread.updated_at as string), locale) : null;
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
          className="w-2 h-2 rounded-full shrink-0"
          style={{ border: '1.5px solid var(--color-accent-primary)' }}
        />
      ) : (
        <div
          className={`w-2 h-2 rounded-full shrink-0${isRunning ? ' animate-pulse' : ''}`}
          style={{ backgroundColor: dotColor }}
        />
      )}

      {/* Thread title and info */}
      <div className="flex-1 min-w-0">
        <h3 className={cn('flex items-center gap-1.5 text-sm font-normal', titleFading && 'animate-fade-in')} style={{ color: 'var(--color-text-primary)' }}>
          <span className="truncate min-w-0">{cardTitle}</span>
          {!!thread.is_shared && (
            <span
              role="img"
              aria-label={t('share.anyoneWithLink')}
              title={t('share.anyoneWithLink')}
              className="inline-flex shrink-0"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              <Link2 className="h-3.5 w-3.5" aria-hidden />
            </span>
          )}
        </h3>
        {byline.starter ? (
          <p className="mt-1 flex min-w-0 items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }} data-testid="thread-byline">
            <PersonAvatar name={byline.starter.name} size={14} />
            <span className="min-w-0 truncate">
              {hasDraft && <><DraftMark /> · </>}
              {byline.starter.mine ? t('sharedWorkspace.threads.startedByMe') : t('sharedWorkspace.threads.startedBy', { name: personLabel(t, byline.starter) })}
              {byline.last && <> · {byline.last.mine ? t('sharedWorkspace.threads.lastByMe') : t('sharedWorkspace.threads.lastBy', { name: personLabel(t, byline.last) })}</>}
              {date && <> · {date}</>}
            </span>
          </p>
        ) : (hasDraft || date) && (
          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
            {hasDraft && <DraftMark />}
            {hasDraft && date && ' · '}
            {date}
          </p>
        )}
      </div>

      {/* Action icons - Show on hover, or while one has keyboard focus */}
      {(openInNewTab || onRename || onDelete || onArchive || onUnarchive) && (
        <div className={`flex items-center gap-1 transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100 focus-within:opacity-100'}`}>
          {openInNewTab && (
            <button
              type="button"
              onClick={(e) => { e.stopPropagation(); openInNewTab(threadPath(tid)); }}
              className="p-1.5 rounded-md transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
              title={t('nav.openInNewTab')}
              aria-label={t('nav.openInNewTab')}
            >
              <ExternalLink className="h-3.5 w-3.5" />
            </button>
          )}
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
              className="p-1.5 rounded-md press hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
              title={t('thread.archive')}
            >
              <Archive className="h-3.5 w-3.5" />
            </button>
          )}
          {onUnarchive && (
            <button
              onClick={(e) => { e.stopPropagation(); onUnarchive(thread); }}
              className="p-1.5 rounded-md press hover:bg-foreground/10"
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
              className="p-1.5 rounded-md transition-colors hover:bg-(--color-danger-hover-bg)"
              style={{ color: 'var(--color-loss)' }}
              title="Delete thread"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      )}

      {/* A shared workspace's thread its whole workspace sees; one its
          starter keeps private carries nothing, as one's own threads do. Last,
          so it stays flush right while the hover actions keep their room. */}
      {sharedMe && thread.shared?.shared_with_workspace && (
        <span
          role="img"
          aria-label={t('sharedWorkspace.privacy.sharedMark')}
          title={t('sharedWorkspace.privacy.sharedMark')}
          className="inline-flex shrink-0"
          style={{ color: 'var(--color-text-tertiary)' }}
          data-testid="thread-shared-mark"
        >
          <Users className="h-3.5 w-3.5" aria-hidden />
        </span>
      )}
    </div>
  );
}

// Memoized so parent list re-renders (search, pagination, modals) skip
// unchanged rows — lifecycle flags arrive via the per-thread store hooks
// above, so a memo hit never suppresses a status update.
export default React.memo(ThreadCard);
