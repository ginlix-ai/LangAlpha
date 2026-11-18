/**
 * Who sees a conversation in a shared workspace, as its surfaces read it
 * (`threadPrivacy.tsx`): its starter alone until they share it with the
 * workspace, and a new thread's switch for what its first send creates. The
 * hook owns the switch and the question asked before sharing, so a host only
 * places what it returns. Nothing for a workspace of one's own, which asks
 * the server nothing more.
 */
import { useState, type ReactElement } from 'react';
import { useTranslation } from 'react-i18next';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from '@/components/ui/use-toast';
import { apiErrorObject } from '@/api/errors';
import { useSharedEntry } from '@/hooks/useSharedWorkspaces';
import { useUser } from '@/hooks/useUser';
import { queryKeys } from '@/lib/queryKeys';
import { threadStanding } from '@/lib/sharedThreads';
import { applyThreadVisibility } from '@/lib/sharedWorkspaceFeed';
import type { Thread } from '@/types/api';
import { setThreadVisibility } from '../../utils/api';
import { threadDetailQuery } from '../../utils/threadQueries';
import { PrivacyChip, ShareThreadDialog } from './threadPrivacy';

export interface ThreadPrivacy {
  workspaceName: string;
  /** Shared with the workspace, or (false) its starter's alone. */
  shared: boolean;
  /** The viewer may change it: their own thread, and to make it private,
   *  one nobody else has written in. A new thread's switch is always theirs. */
  canChange: boolean;
  /** Share it (asks first on a thread that exists) or make it private. */
  request: (shared: boolean) => void;
  /** Making it private just now turned its link off. */
  linkTurnedOff?: boolean;
}

export interface ThreadPrivacyView {
  /** Null outside a shared workspace and while the thread is unread. */
  privacy: ThreadPrivacy | null;
  /** What a send from this composer carries: a new thread's switch. */
  sendOptions: { sharedWithWorkspace: boolean };
  /** The composer's chip, where there is something to choose. */
  chip: ReactElement | null;
  /** The question asked before a thread that exists is shared. */
  dialog: ReactElement | null;
}

const NONE: ThreadPrivacyView = { privacy: null, sendOptions: { sharedWithWorkspace: false }, chip: null, dialog: null };

/** The privacy of the thread in view; a null or `__default__` id is a new
 *  thread, whose switch is held per workspace, so moving to another one
 *  starts it private again. */
export function useThreadPrivacy(threadId: string | null | undefined, workspaceId: string | null | undefined): ThreadPrivacyView {
  const { t } = useTranslation();
  const entry = useSharedEntry(workspaceId);
  const { user } = useUser();
  const queryClient = useQueryClient();
  const [newChoice, setNewChoice] = useState<{ workspaceId: string | null | undefined; shared: boolean }>({ workspaceId, shared: false });
  const [asking, setAsking] = useState(false);
  const isNew = !threadId || threadId === '__default__';
  const { data: thread } = useQuery({ ...threadDetailQuery(threadId ?? ''), enabled: !!entry && !isNew });
  const mutation = useMutation({
    mutationFn: (shared: boolean) => setThreadVisibility(threadId!, shared),
    onSuccess: ({ link_turned_off: linkOff, ...updated }, shared) => {
      queryClient.setQueryData<Thread>(queryKeys.threads.detail(updated.thread_id), updated);
      if (linkOff) void queryClient.invalidateQueries({ queryKey: queryKeys.threads.share(updated.thread_id) });
      applyThreadVisibility(queryClient, { threadId: updated.thread_id, workspaceId: updated.workspace_id, shared, viewerIsStarter: true });
    },
    onError: (err, shared) => {
      const othersSent = apiErrorObject(err)?.type === 'others_sent';
      toast({
        variant: 'destructive',
        title: othersSent
          ? t('sharedWorkspace.privacy.othersSent')
          : t(shared ? 'sharedWorkspace.privacy.shareFailed' : 'sharedWorkspace.privacy.privateFailed'),
      });
      if (threadId) void queryClient.invalidateQueries({ queryKey: queryKeys.threads.detail(threadId) });
    },
  });
  if (!entry) return NONE;
  const workspaceName = entry.workspace.name;
  if (isNew) {
    const shared = newChoice.workspaceId === workspaceId && newChoice.shared;
    const privacy: ThreadPrivacy = {
      workspaceName, shared, canChange: true, request: (next) => setNewChoice({ workspaceId, shared: next }),
    };
    return { privacy, sendOptions: { sharedWithWorkspace: shared }, chip: <PrivacyChip privacy={privacy} />, dialog: null };
  }
  const info = thread?.shared;
  if (!info || thread?.thread_id !== threadId) return NONE;
  const standing = threadStanding(info, user?.user_id, entry.shared.can_manage);
  const shared = standing.sharedWithWorkspace;
  const privacy: ThreadPrivacy = {
    workspaceName,
    shared,
    canChange: standing.canChangePrivacy && !mutation.isPending,
    request: (next) => (next ? setAsking(true) : mutation.mutate(false)),
    linkTurnedOff: !shared && mutation.data?.thread_id === threadId && !!mutation.data.link_turned_off,
  };
  return {
    privacy,
    sendOptions: NONE.sendOptions,
    // A thread already shared has nothing left to choose in the composer.
    chip: shared ? null : <PrivacyChip privacy={privacy} />,
    dialog: (
      <ShareThreadDialog
        open={asking}
        workspaceName={workspaceName}
        onCancel={() => setAsking(false)}
        onConfirm={() => { setAsking(false); mutation.mutate(true); }}
      />
    ),
  };
}
