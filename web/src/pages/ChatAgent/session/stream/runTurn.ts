/**
 * The turn the server admitted a run to, stamped onto the bubbles the view
 * opened for it.
 *
 * A live bubble is stamped where it is created, from what the view can count
 * (see projectTurns). Turns added where this view never saw them (another tab,
 * an automation, a channel message, a report-back) leave that count behind, so
 * the run's `metadata` frame names its turn and the run's bubbles take it. The
 * frame is the run's first, so those bubbles are still the transcript's tail:
 * the one the stream writes to and any a dropped stream left it to continue,
 * the message that opened it, and any steering the user sent while the run
 * was being admitted. A steer also names the run it was sent to, and takes
 * that run's turn wherever it sits: a reload places one it kept by the turn
 * its status counts live, and without that count leaves it under the count
 * the view had when it was sent.
 */
import type { ChatMessage } from '@/types/chat';
import { opensTurn } from '../../components/messageList/messagePredicates';

export function stampRunTurn(messages: ChatMessage[], assistantId: string, turnIndex: number, runId?: string): ChatMessage[] {
  const at = messages.findIndex((m) => m.id === assistantId);
  const bubble = messages[at];
  if (!bubble || bubble.role !== 'assistant') return messages;
  const counted = bubble.turnIndex;

  // A send or an edit put its own message before the bubble,
  // under the same count. A reconnect the frame reached first sits after the
  // bubbles it continues, and any steer sent meanwhile, delivered or failed,
  // all under that count too. A resume or an attach has no message, and the
  // bubble before it closed an earlier turn.
  let from = at;
  if (counted !== undefined) {
    for (let i = at - 1; i >= 0; i--) {
      const m = messages[i];
      if (m.role === 'notification') continue;
      if (m.turnIndex !== counted || (m.role === 'user' && m.queued)) break;
      if (m.role === 'user' && !opensTurn(m)) continue;
      from = i;
      if (m.role === 'user') break;
    }
  }

  let changed = false;
  const next = messages.map((m, i) => {
    if (m.role === 'notification') return m;
    if (!(runId && m.role === 'user' && m.steeredRunId === runId)) {
      if (i < from) return m;
      // A parked message is a placeholder: the send it becomes counts afresh.
      if (m.role === 'user' && m.queued) return m;
      if (i !== at && m.turnIndex !== counted) return m;
    }
    if (m.turnIndex === turnIndex) return m;
    changed = true;
    return { ...m, turnIndex };
  });
  return changed ? next : messages;
}
