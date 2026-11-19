import { useMemo } from 'react';
import type { ChatMessage } from '@/types/chat';
import { useSharedEntry, useWorkspaceMembers } from '@/hooks/useSharedWorkspaces';
import { useUser } from '@/hooks/useUser';
import type { MessageSender } from '@/types/chat';
import { threadTurns, type SharedThreadView } from '../session/sharedThread';
import type { PendingInterrupt } from '../session/types';

/**
 * The sender view a shared workspace's thread draws with, undefined for any
 * other workspace, so its messages stay as they always were, and the member
 * whose turn holds the thread when it is not the viewer: under way, or
 * `waiting` on a question or approval, which only its sender answers (the
 * server refuses anyone else's send meanwhile; a credit pause waits on no
 * answer and holds nothing). `messages` changes
 * identity with every streamed token, so the turns are read once per change
 * and the view keeps its identity while they say the same, sparing every
 * bubble that reads it a redraw per token; a message's own caption is read
 * off the message, so a new turn's sender shows at once.
 */
export function useSharedThreadView(
  workspaceId: string | null | undefined,
  messages: readonly ChatMessage[],
  streaming: boolean,
  pause: PendingInterrupt | null,
): { view: SharedThreadView | undefined; othersTurn: (MessageSender & { waiting: boolean }) | null } {
  const waitsOnAnswer = !!pause && pause.type !== 'credit_pause';
  const running = streaming || waitsOnAnswer;
  const entry = useSharedEntry(workspaceId);
  const { user } = useUser();
  const { data: members } = useWorkspaceMembers(workspaceId, !!entry);
  const meId = entry && user ? user.user_id : null;
  const turns = useMemo(() => (meId ? threadTurns(messages, meId, running) : null), [messages, meId, running]);
  const lastOthersTurn = turns?.lastOthersTurn ?? -1;
  // A string, so the view keeps its identity while the turns hold the same.
  const ownKey = turns ? [...turns.ownTurns].sort((a, b) => a - b).join(',') : '';
  const view = useMemo(() => {
    if (!entry || !user) return undefined;
    const avatars: Record<string, string | null> = {};
    const names: Record<string, string | null> = {};
    for (const m of members?.members ?? []) {
      avatars[m.user_id] = m.avatar_url;
      names[m.user_id] = m.name;
    }
    if (avatars[user.user_id] === undefined) avatars[user.user_id] = user.avatar_url ?? null;
    if (names[user.user_id] === undefined) names[user.user_id] = user.name ?? null;
    return {
      me: { userId: user.user_id, name: user.name ?? null },
      avatars,
      names,
      lastOthersTurn,
      ownTurns: new Set(ownKey ? ownKey.split(',').map(Number) : []),
    };
  }, [entry, user, members, lastOthersTurn, ownKey]);
  const othersTurn = turns?.othersTurn;
  return { view, othersTurn: othersTurn ? { ...othersTurn, waiting: !streaming && waitsOnAnswer } : null };
}
