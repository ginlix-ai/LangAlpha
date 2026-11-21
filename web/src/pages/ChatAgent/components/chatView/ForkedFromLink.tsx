/**
 * "Forked from <title>" in the chat header, for a thread copied from another.
 * Opens the source; a source that is gone reads as a deleted chat with nothing
 * to open. Nothing shows while the source is read or when it cannot be.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery } from '@tanstack/react-query';
import { GitBranch } from 'lucide-react';
import { apiErrorStatus } from '@/api/errors';
import { cn } from '@/lib/utils';
import { threadDetailQuery } from '../../utils/threadQueries';

const QUIET = 'flex min-w-0 shrink items-center gap-1 text-xs whitespace-nowrap text-(--color-text-tertiary)';

export function ForkedFromLink({ threadId, onOpen }: {
  /** The thread in view; null or `__default__` before it exists. */
  threadId: string | null | undefined;
  /** Leaves the view for the source, as the view's other exits do. */
  onOpen: (sourceThreadId: string, state: { workspaceId: string }) => void;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const liveId = threadId && threadId !== '__default__' ? threadId : null;
  const { data: thread } = useQuery({ ...threadDetailQuery(liveId ?? ''), enabled: !!liveId });
  const sourceId = thread?.metadata?.fork?.thread_id || null;
  const { data: source, error } = useQuery({ ...threadDetailQuery(sourceId ?? ''), enabled: !!sourceId });

  if (!sourceId) return null;
  if (error) {
    if (apiErrorStatus(error) !== 404) return null;
    return (
      <span className={QUIET} data-testid="forked-from">
        <GitBranch aria-hidden className="h-3 w-3 shrink-0" />
        <span className="truncate">{t('chat.forkedFromDeleted')}</span>
      </span>
    );
  }
  if (!source) return null;
  const title = source.title || t('nav.untitledThread');
  return (
    <button
      type="button"
      onClick={() => onOpen(source.thread_id, { workspaceId: source.workspace_id })}
      className={cn(QUIET, 'tap-target rounded px-1 -mx-1 py-0.5 transition-colors hover:text-(--color-text-secondary) hover:bg-(--color-border-muted)')}
      title={title}
      data-testid="forked-from"
    >
      <GitBranch aria-hidden className="h-3 w-3 shrink-0" />
      <span className="truncate">{t('chat.forkedFrom', { title })}</span>
    </button>
  );
}
