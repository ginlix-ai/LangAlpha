/**
 * The row between a fork's borrowed turns and its own. Above it the turns are
 * the original chat's, shown from there and read-only here. The line names the
 * original and links to it while there is one to open; what that means for
 * the turns above waits in its tooltip, so the line stays one line.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery } from '@tanstack/react-query';
import { GitBranch } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import type { ForkDividerEvent } from '@/types/api';
import { threadDetailQuery } from '../../utils/threadQueries';
import { useMessageActions } from './MessageActionsContext';

const RULE: React.CSSProperties = { borderTop: '1px solid var(--color-border-muted)' };

export function ForkDivider({ divider }: { divider: ForkDividerEvent }): React.ReactElement {
  const { t } = useTranslation();
  const { onOpenThread } = useMessageActions();
  const { source_thread_id: sourceThreadId, state } = divider;
  const linked = state === 'shown' || state === 'changed';
  // Only a view that can leave for the original reads its title: a shared
  // chat's visitor could not open it either.
  const readsSource = linked && !!onOpenThread;
  const { data, error, isPending } = useQuery({
    ...threadDetailQuery(sourceThreadId),
    enabled: readsSource,
  });
  // A disabled query still hands back what the cache holds, which may be
  // the original before it was deleted or closed to this reader.
  const source = readsSource ? data : undefined;

  let text: string;
  if (state === 'deleted') text = t('chat.forkDivider.deleted');
  else if (state === 'unavailable') text = t('chat.forkDivider.unavailable');
  else if (source) {
    const title = source.title || t('nav.untitledThread');
    text = t(state === 'changed' ? 'chat.forkDivider.changed' : 'chat.forkDivider.shown', { title });
  } else text = t(state === 'changed' ? 'chat.forkDivider.changedUntitled' : 'chat.forkDivider.shownUntitled');
  const hint = t(`chat.forkDivider.hint.${state}`);
  // The line waits for the title rather than changing under the reader.
  const waiting = readsSource && isPending && !error;

  return (
    <div className="my-1" data-testid="fork-divider" data-state={state}>
      <div style={RULE} />
      <p
        className="mx-auto mt-3 flex max-w-2xl items-center justify-center gap-1.5 text-xs"
        style={{ color: 'var(--color-text-tertiary)', visibility: waiting ? 'hidden' : undefined }}
      >
        <TooltipProvider delayDuration={300}>
          <Tooltip>
            <TooltipTrigger asChild>
              <span
                tabIndex={0}
                className="inline-flex min-w-0 cursor-default items-center gap-1 rounded-sm outline-none focus-visible:outline-2 focus-visible:outline-offset-2"
                style={{ outlineColor: 'var(--color-text-primary)' }}
              >
                <GitBranch aria-hidden className="h-3 w-3 shrink-0" />
                <span className="truncate">{text}</span>
              </span>
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-72">
              {hint}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>
        {source && onOpenThread && (
          <button
            type="button"
            onClick={() => onOpenThread(source.thread_id, { workspaceId: source.workspace_id })}
            className="tap-target shrink-0 whitespace-nowrap underline-offset-2 hover:underline"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('chat.forkDivider.open')}
          </button>
        )}
      </p>
    </div>
  );
}
