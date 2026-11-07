// @vitest-environment node
/**
 * A reply whose last request was billed to usage credits replays its notice
 * under it, as the live stream drew it: once, however often the event is
 * replayed, and only for the main agent's reply, never a subagent's.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const CHUNK = { thread_id: 'thread-1', role: 'assistant', id: 'lc_run--x', agent: 'model:abc', turn_index: 0 };
const billed = (eventId: number, extra: Record<string, unknown> = {}) => ({
  event: 'claude_code_plan',
  data: {
    thread_id: 'thread-1', agent: 'model:abc', reason: 'billed_credits', window: 'five_hour', until: '2026-11-06T20:40:00Z',
    _eventId: eventId, ...extra,
  },
});

async function replyOf(items: Array<Record<string, unknown>>) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(rt, makeDeps());
  return read().find((m) => m.id === 'history-assistant-0') as AssistantMessage;
}

const notices = (reply: AssistantMessage) => (reply.contentSegments ?? []).filter((s) => s.type === 'notification');

beforeEach(() => vi.clearAllMocks());

describe('history replay: a reply billed to usage credits', () => {
  it('draws its notice once, after the reply it notes, and none for a subagent', async () => {
    const reply = await replyOf([
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Hi' } },
      { event: 'message_chunk', data: { ...CHUNK, content_type: 'text', content: 'Done.', _eventId: 2 } },
      billed(5, { agent: 'task:k3', window: 'seven_day' }),
      { event: 'message_chunk', data: { ...CHUNK, finish_reason: 'stop', _eventId: 3 } },
      billed(6),
      billed(7),
    ]);

    expect(notices(reply)).toEqual([
      { type: 'notification', order: 6, planBilled: { window: 'five_hour', until: '2026-11-06T20:40:00Z' } },
    ]);
    const last = [...(reply.contentSegments ?? [])].sort((a, b) => a.order - b.order).at(-1);
    expect(last?.type).toBe('notification');
  });
});
