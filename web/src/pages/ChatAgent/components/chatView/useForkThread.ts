/**
 * Make a new chat that carries on from the thread in view through a reply,
 * and open it. It runs as the fork mutation (`hooks/forkMutation.ts`), which
 * the sidebar shows as a row from the click on, since the new chat can take
 * a while to exist.
 */
import { useCallback } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from '@/components/ui/use-toast';
import { apiErrorDetailMessage } from '@/api/errors';
import { queryKeys } from '@/lib/queryKeys';
import { authSessionCheck } from '@/lib/authResets';
import { cachedThreadTitle, refreshThreadLists } from '@/lib/threadListCache';
import type { Thread } from '@/types/api';
import { forkThread } from '../../utils/api';
import { showNewThread } from '../../session/threadCreation';
import type { ForkRequest } from '../../hooks/forkMutation';

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

export function useForkThread({ threadId, workspaceId, open }: {
  /** The thread in view; null or `__default__` before it exists. */
  threadId: string | null | undefined;
  /** The thread's workspace, where the fork is made. */
  workspaceId: string;
  /** Leaves the view for the new chat, as the view's other exits do. */
  open: (threadId: string, state: { workspaceId: string }) => void;
}): (turnIndex: number) => void {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { mutate } = useMutation({
    mutationKey: queryKeys.threads.fork(),
    // The lists take the fork before the mutation settles, so the sidebar's
    // pending row gives way to the real one with nothing missing between.
    // Resolves null when the user signed out or switched accounts meanwhile,
    // which shows nothing.
    mutationFn: async ({ threadId: sourceId, turnIndex, title }: ForkRequest): Promise<Thread | null> => {
      const sameSession = authSessionCheck();
      let made;
      try {
        made = await forkThread(sourceId, { turnIndex, platform: 'web', title });
      } catch (err) {
        if (!sameSession()) return null;
        throw err;
      }
      if (!sameSession()) return null;
      const { thread, turns_borrowed } = made;
      // The new chat's header reads its own row from here rather than asking.
      queryClient.setQueryData<Thread>(queryKeys.threads.detail(thread.thread_id), thread);
      // A fork counts the turns it borrows as its own, as it shows them.
      showNewThread(queryClient, { ...thread, turn_count: turns_borrowed });
      refreshThreadLists(queryClient, thread.workspace_id);
      return thread;
    },
    // Here rather than on the call, so a refusal is shown even once the view
    // that asked is gone.
    onError: (err) => {
      toast({ variant: 'destructive', title: apiErrorDetailMessage(err) ?? t('chat.forkFailed') });
    },
  });

  return useCallback((turnIndex: number) => {
    // One fork at a time: a second running beside the first would open a
    // second chat behind it.
    if (!threadId || threadId === '__default__' || queryClient.isMutating({ mutationKey: queryKeys.threads.fork() }) > 0) return;
    const title = forkTitle(cachedThreadTitle(queryClient, threadId), t);
    // The call's own callback does not run once the view is gone, which
    // leaves the fork waiting in the lists rather than opening it.
    mutate({ threadId, turnIndex, workspaceId, title }, {
      onSuccess: (thread) => {
        if (thread) open(thread.thread_id, { workspaceId: thread.workspace_id });
      },
    });
  }, [threadId, workspaceId, open, queryClient, mutate, t]);
}
