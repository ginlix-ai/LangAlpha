import type { SharedThreadInfo, ThreadSender } from '@/types/sharedWorkspaces';

export interface BylinePerson {
  name: string | null;
  mine: boolean;
}

type Translate = (key: string, options?: Record<string, unknown>) => string;

/** How a person in a shared workspace reads: "You" for the viewer, else
 *  their name, else "Someone". */
export function personLabel(t: Translate, { name, mine }: BylinePerson): string {
  return mine ? t('sharedWorkspace.youShort') : name || t('sharedWorkspace.someone');
}

/** Who started a shared workspace's thread, and who wrote in it last when
 *  that is someone else; both null on any other thread. */
export function threadByline(
  shared: SharedThreadInfo | null | undefined,
  me: string | null | undefined,
): { starter: BylinePerson | null; last: BylinePerson | null } {
  const person = (s: ThreadSender | null | undefined): BylinePerson | null =>
    s ? { name: s.name, mine: !!me && s.user_id === me } : null;
  const starter = person(shared?.created_by);
  const lastSender = shared?.last_sender;
  const last = lastSender && lastSender.user_id !== shared?.created_by?.user_id ? person(lastSender) : null;
  return { starter, last };
}

/** Where the viewer stands on a thread: who sees it, and what the server
 *  lets them do to it. A thread outside a shared workspace is the viewer's
 *  own and private, with nothing to change about who sees it. */
export interface ThreadStanding {
  /** Every member of its shared workspace reads it. */
  sharedWithWorkspace: boolean;
  /** The viewer started it. */
  mine: boolean;
  /** Its starter or a manager may delete it; the server refuses anyone else. */
  canDelete: boolean;
  /** Its starter may share it, and make it private again while nobody else
   *  has sent a turn there. */
  canChangePrivacy: boolean;
}

export function threadStanding(
  shared: SharedThreadInfo | null | undefined,
  me: string | null | undefined,
  canManage: boolean,
): ThreadStanding {
  if (!shared) return { sharedWithWorkspace: false, mine: true, canDelete: true, canChangePrivacy: false };
  const mine = !!me && shared.created_by?.user_id === me;
  return {
    sharedWithWorkspace: shared.shared_with_workspace,
    mine,
    canDelete: mine || canManage,
    canChangePrivacy: mine && (!shared.shared_with_workspace || !shared.others_sent),
  };
}

/** Who last wrote in a shared thread, when that is not the viewer: their
 *  first name, so a long title gives way before it does. */
export function colleagueName(
  shared: SharedThreadInfo | null | undefined,
  me: string | null,
  someone: string,
): string | null {
  const last = shared?.last_sender;
  if (!last || last.user_id === me) return null;
  return last.name?.trim().split(/\s+/)[0] || someone;
}
