/**
 * Copy the thread in view up to a turn into a new chat and open it. A
 * `before_turn` copy stops short of the turn and leaves its message in the new
 * chat's composer, unsent, through the same `draftMessage` route state other
 * surfaces hand the composer words with.
 */
import { useCallback, useEffect, useRef } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from '@/components/ui/use-toast';
import { apiErrorDetailMessage } from '@/api/errors';
import { queryKeys } from '@/lib/queryKeys';
import { authSessionCheck } from '@/lib/authResets';
import { cachedThreadTitle, refreshThreadLists } from '@/lib/threadListCache';
import type { Thread, ThreadForkCut } from '@/types/api';
import { forkThread } from '../../utils/api';
import { showNewThread } from '../../session/threadCreation';

/**
 * The title a fork asks for: its source's, marked apart from it, since the
 * sidebar lists the two side by side. A fork of a fork keeps one mark. With
 * no title to go on it asks for none, and the server keeps the source's.
 */
export function forkTitle(sourceTitle: string | null | undefined, t: TFunction): string | undefined {
  const title = sourceTitle?.trim();
  if (!title) return undefined;
  if (title.endsWith(t('chat.forkTitle', { title: '' }))) return clip(title, FORK_TITLE_MAX);
  // The server refuses a title over its limit, so the base gives up room for the mark.
  const markLength = [...t('chat.forkTitle', { title: '' })].length;
  const base = clip(title, FORK_TITLE_MAX - markLength).trimEnd();
  return t('chat.forkTitle', { title: base });
}

const FORK_TITLE_MAX = 255;

/** At most `max` code points, never splitting a surrogate pair. */
function clip(text: string, max: number): string {
  const points = [...text];
  return points.length > max ? points.slice(0, max).join('') : text;
}

export function useForkThread({ threadId, open }: {
  /** The thread in view; null or `__default__` before it exists. */
  threadId: string | null | undefined;
  /** Leaves the view for the new chat, as the view's other exits do. */
  open: (threadId: string, state: { workspaceId: string; draftMessage?: string }) => void;
}): (turnIndex: number, cut: ThreadForkCut) => Promise<void> {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  // One copy at a time across the transcript: a second button pressed while
  // the first request runs would open a second chat behind the first.
  const inFlight = useRef(false);
  // A view gone by the time the copy lands opens nothing; the fork waits in the lists.
  const mounted = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);

  return useCallback(async (turnIndex: number, cut: ThreadForkCut) => {
    if (!threadId || threadId === '__default__' || inFlight.current) return;
    inFlight.current = true;
    // Dropped if the user signs out or switches accounts while the copy runs.
    const sameSession = authSessionCheck();
    try {
      const title = forkTitle(cachedThreadTitle(queryClient, threadId), t);
      const { thread, turns_copied, prefill } = await forkThread(threadId, { cut, turnIndex, platform: 'web', title });
      if (!sameSession()) return;
      // The new chat's header reads its own row from here rather than asking.
      queryClient.setQueryData<Thread>(queryKeys.threads.detail(thread.thread_id), thread);
      showNewThread(queryClient, { ...thread, turn_count: turns_copied });
      refreshThreadLists(queryClient, thread.workspace_id);
      if (!mounted.current) return;
      open(thread.thread_id, {
        workspaceId: thread.workspace_id,
        ...(cut === 'before_turn' && prefill ? { draftMessage: prefill } : {}),
      });
    } catch (err) {
      if (!sameSession()) return;
      toast({ variant: 'destructive', title: apiErrorDetailMessage(err) ?? t('chat.forkFailed') });
    } finally {
      inFlight.current = false;
    }
  }, [threadId, open, queryClient, t]);
}
