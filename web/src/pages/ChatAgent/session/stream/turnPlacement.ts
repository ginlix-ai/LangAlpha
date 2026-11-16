/**
 * Where a Home turn ran, when it was the user's cloud computer standing in
 * for the machine Home lives on. The live stream says so with a
 * `turn_placement` frame and a replay with the turn's query metadata; both
 * put it on the turn's opening row, which draws it under the message.
 */
import type { HomeFallback } from '@/types/sse';
import type { MessageRecord } from '../../hooks/utils/types';

/** The server's value, or null for anything that is not one. */
export function readHomeFallback(value: unknown): HomeFallback | null {
  if (!value || typeof value !== 'object') return null;
  const { computer_id, computer_name, cloud_computer_id, cloud_name, at, reason } = value as Record<string, unknown>;
  if (
    typeof computer_id !== 'string'
    || typeof computer_name !== 'string'
    || typeof cloud_computer_id !== 'string'
    || typeof cloud_name !== 'string'
    || !cloud_name
    || typeof at !== 'string'
    || (reason !== 'unreachable' && reason !== 'command')
  ) {
    return null;
  }
  return { computer_id, computer_name, cloud_computer_id, cloud_name, at, reason };
}

/**
 * The rows with `fallback` on the user row the turn answers: the last one
 * above its reply, or above everything while the reply is not drawn yet.
 * Null takes it off. The same rows when there is no such row.
 */
export function stampHomeFallback(
  prev: MessageRecord[],
  assistantMessageId: string,
  fallback: HomeFallback | null,
): MessageRecord[] {
  const at = prev.findIndex((m) => m.id === assistantMessageId);
  const end = at === -1 ? prev.length : at;
  for (let i = end - 1; i >= 0; i -= 1) {
    if (prev[i].role !== 'user') continue;
    const next = prev.slice();
    next[i] = { ...prev[i], homeFallback: fallback ?? undefined };
    return next;
  }
  return prev;
}
