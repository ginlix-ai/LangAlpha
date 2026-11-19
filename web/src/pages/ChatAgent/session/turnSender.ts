/**
 * Who sent a turn in a shared workspace. Replay names it on each
 * `user_message` and a live run names it on its start frame (`metadata`),
 * both as `{user_id, name}`; the two meet here so a reload and a live turn
 * draw the same person on the same message.
 */
import type { ChatMessage, MessageSender } from '@/types/chat';

/** The sender a frame or a replay line carries, or null for none (every
 *  message outside a shared workspace). */
export function readSender(value: unknown): MessageSender | null {
  if (!value || typeof value !== 'object') return null;
  const { user_id: userId, name } = value as { user_id?: unknown; name?: unknown };
  if (typeof userId !== 'string' || !userId) return null;
  return { userId, name: typeof name === 'string' && name ? name : null };
}

/** Puts `sender` on the user messages that opened `turnIndex`, as the live
 *  start frame names them. A message already naming its sender keeps it. */
export function withTurnSender(messages: ChatMessage[], turnIndex: number, sender: MessageSender): ChatMessage[] {
  let changed = false;
  const next = messages.map((m) => {
    if (m.role !== 'user' || m.turnIndex !== turnIndex || m.sender || m.provenance) return m;
    changed = true;
    return { ...m, sender };
  });
  return changed ? next : messages;
}
