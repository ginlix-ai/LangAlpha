// @vitest-environment node
/**
 * Wakeups in a replayed thread. A wake turn's query is written for the model,
 * so its metadata heads the turn as a wakeup row rather than a user bubble; a
 * notice delivered mid-turn is a `steering_delivered` entry and splits the
 * turn where a delivered steer does, under the id the live stream gives it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ChatMessage } from '@/types/chat';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

import { loadConversationHistory } from '../replayHistory';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';
import { deliveredNoticeRow } from '../../wakeupEvents';

// Made-up labels only.
const NOTICE = {
  wakeup_id: 'wk-1', short_id: 'w_ab12', label: 'morning check', kind: 'at', notice: 'fire',
  seq: 1, total: 1, last: true, skipped: 0, fired_at: '2030-01-01T15:05:00Z',
};

const chunk = (turn: number, content: string) => ({
  event: 'message_chunk',
  data: { thread_id: 'thread-1', turn_index: turn, role: 'assistant', agent: 'main', content_type: 'text', content },
});
const opener = (turn: number, content: string) => ({
  event: 'user_message', data: { thread_id: 'thread-1', turn_index: turn, content },
});
const delivery = (turn: number, messages: Array<Record<string, unknown>>) => ({
  event: 'steering_delivered',
  data: { thread_id: 'thread-1', turn_index: turn, count: 0, messages },
});

type Row = Record<string, unknown>;

beforeEach(() => vi.clearAllMocks());

describe('history replay: wakeups', () => {
  it('heads a wake turn with the wakeup row, not the query text', async () => {
    const { rt, read } = buildRuntime();
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Check back at ten'),
      chunk(0, 'Set.'),
      {
        event: 'user_message',
        data: {
          thread_id: 'thread-1',
          turn_index: 1,
          query_type: 'wakeup',
          content: '<wakeup>It is 10:00, the time you set.</wakeup>',
          metadata: { wakeup: { v: 1, notices: [NOTICE] } },
        },
      },
      chunk(1, 'Checked.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const messages = read() as unknown as Row[];
    expect(messages.map((m) => m.role)).toEqual(['user', 'assistant', 'notification', 'assistant']);
    expect(JSON.stringify(messages)).not.toContain('<wakeup>');
    expect(messages[2]).toMatchObject({
      id: 'history-wakeup-wake-1',
      isHistory: true,
      wakeup: { source: 'wake', turnIndex: 1, notices: [NOTICE] },
    });
  });

  it('splits a turn at a delivered notice, in place of a copy the live stream left on screen', async () => {
    const { rt, read } = buildRuntime();
    const live = deliveredNoticeRow({ content: '', wakeup: NOTICE } as never, new Date(0));
    rt.setMessages(() => [live as unknown as ChatMessage]);
    api.replayThreadHistory.mockImplementation(replayOf([
      opener(0, 'Run the long job'),
      chunk(0, 'Running.'),
      delivery(0, [{ content: '', timestamp: 1_700_000_000, wakeup: NOTICE }]),
      chunk(0, 'Done.'),
    ]));

    await loadConversationHistory(rt, makeDeps());

    const messages = read() as unknown as Row[];
    expect(messages.map((m) => m.id)).toEqual([
      'history-user-0', 'history-assistant-0', live!.id, 'history-assistant-steering-0-1',
    ]);
    expect(messages[2]).toMatchObject({ role: 'notification', isHistory: true, wakeup: { source: 'delivered', notices: [NOTICE] } });
    expect(messages[3]).toMatchObject({ content: 'Done.', isSteering: true, turnIndex: 0 });
  });
});
