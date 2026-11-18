// @vitest-environment node
/**
 * A turn that waited for a Claude Code CLI replays the wait's notice where the
 * server placed its `admitted` frame, as the live stream drew it: once, and
 * only for the main agent. A wait too short to note replays nothing.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { AssistantMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const chunk = (turn: number, eventId: number, extra: Record<string, unknown> = {}) => ({
  event: 'message_chunk',
  data: {
    thread_id: 'thread-1', role: 'assistant', id: `lc_run--${turn}`, agent: 'model:abc', turn_index: turn,
    content_type: 'text', content: 'Done.', _eventId: eventId, ...extra,
  },
});
const queue = (turn: number, eventId: number, extra: Record<string, unknown> = {}) => ({
  event: 'claude_code_queue',
  data: {
    thread_id: 'thread-1', agent: 'main', turn_index: turn, signal: 'admitted', computer_id: 'c1',
    name: 'MacBook-Pro-7.local', running: 2, cap: 2, settable: true, max: 8, wait_id: 'w1', waited_s: 42,
    _eventId: eventId, ...extra,
  },
});

async function replay(items: Array<Record<string, unknown>>) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(rt, makeDeps());
  return (id: string) => read().find((m) => m.id === id) as AssistantMessage;
}

const notices = (reply: AssistantMessage) => (reply.contentSegments ?? []).filter((s) => s.type === 'notification');
const inOrder = (reply: AssistantMessage) => [...(reply.contentSegments ?? [])].sort((a, b) => a.order - b.order).map((s) => s.type);

beforeEach(() => vi.clearAllMocks());

describe('history replay: a turn that waited for a Claude Code CLI', () => {
  it('draws the notice on its own reply where the wait happened, once, and none for a subagent', async () => {
    const reply = await replay([
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Hi' } },
      chunk(0, 2),
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 1, content: 'Again' } },
      chunk(1, 5),
      queue(1, 6),
      queue(1, 6),
      queue(1, 7, { agent: 'task:k3', wait_id: 'w9', waited_s: 9 }),
      chunk(1, 8, { content: ' More.' }),
    ]);

    expect(notices(reply('history-assistant-0'))).toEqual([]);
    const second = reply('history-assistant-1');
    expect(notices(second)).toEqual([expect.objectContaining({
      order: 6,
      notice: expect.objectContaining({ kind: 'cliWait', waitId: 'w1', computerId: 'c1', waitedS: 42 }),
    })]);
    // Mid-reply, not at its head: a turn can start a second CLI mid-turn.
    expect(inOrder(second).indexOf('notification')).toBeGreaterThan(0);
  });

  it('notes nothing for a wait too short to note, nor for a frame with no wait id', async () => {
    const reply = await replay([
      { event: 'user_message', data: { thread_id: 'thread-1', turn_index: 0, content: 'Hi' } },
      queue(0, 2, { signal: 'cleared', waited_s: 2 }),
      queue(0, 3, { wait_id: undefined }),
      chunk(0, 4),
    ]);

    expect(notices(reply('history-assistant-0'))).toEqual([]);
  });
});
