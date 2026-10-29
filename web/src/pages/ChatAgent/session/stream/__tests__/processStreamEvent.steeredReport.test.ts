// @vitest-environment node
/**
 * A report a live turn took in mid-turn arrives as a `steering_delivered`
 * entry with a `report_source` and no words. It is not the user's own steer:
 * it must not spend the boundary that steer's acceptance set. The rows are
 * the fold's (`steeringDelivery.test.ts`); this pins the stream around it.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ReportSource } from '@/types/api';
import type { ChatMessage, UserMessage } from '@/types/chat';
import type { SteeringDeliveredEntry } from '@/types/sse';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

const TASK: ReportSource = { kind: 'task', task_id: 'k3', display_id: 'Task-k3', description: 'Pull filings', failed: false };

function setup(initial: ChatMessage[]) {
  let messages = initial;
  const setMessages = (updater: (prev: ChatMessage[]) => ChatMessage[]) => {
    messages = updater(messages);
  };
  const steeringAtOrderRef = { current: 7 as number | null };
  const rt = {
    setMessages,
    queueMessages: setMessages,
    flushMessages: vi.fn(),
    lastEventIdRef: { current: null },
    threadIdRef: { current: 'thread-1' },
    currentMessageRef: { current: 'a1' },
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    steeringAtOrderRef,
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { clearModelStatus: () => {} } as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const deliver = (entries: SteeringDeliveredEntry[]) =>
    process({ event: 'steering_delivered', messages: entries } as unknown as SSEEvent);
  return { deliver, process, steeringAtOrderRef, messages: () => messages };
}

const user = (id: string, extra: Partial<UserMessage> = {}): UserMessage => ({
  id, role: 'user', content: '', contentType: 'text', timestamp: new Date(0), isStreaming: false, ...extra,
});

const transcript = (): ChatMessage[] => [
  user('u0', { content: 'Compare the gaps', turnIndex: 0 }),
  { id: 'a1', role: 'assistant', content: 'working', contentType: 'text', isStreaming: true, turnIndex: 0 } as unknown as ChatMessage,
];

describe('live: a report the turn took in as a steer', () => {
  it('leaves the user\'s pending steer and its boundary for the delivery that takes it', () => {
    const { deliver, steeringAtOrderRef, messages } = setup([
      ...transcript(),
      user('s1', { content: 'also check margins', steering: true, turnIndex: 0 }),
    ]);

    deliver([{ content: '', report_source: TASK, timestamp: 1_700_000_000 }]);

    expect(messages().find((m) => m.id === 's1')).toMatchObject({ steering: true });
    expect(steeringAtOrderRef.current).toBe(7);
  });

  it('opens the reply to the reports above the steer still waiting, where replay draws it', () => {
    const { deliver, messages } = setup([
      ...transcript(),
      user('s1', { content: 'also check margins', steering: true, turnIndex: 0 }),
    ]);

    deliver([{ content: '', report_source: TASK }]);
    expect(messages().map((m) => m.id)).toEqual(['u0', 'a1', expect.stringMatching(/^steering-report-/),
      expect.stringMatching(/^assistant-/), 's1']);

    deliver([{ content: 'also check margins' }]);
    const rows = messages();
    expect(rows[4]).toMatchObject({ id: 's1', steeringDelivered: true });
    expect(rows[5]).toMatchObject({ role: 'assistant', isSteering: true });
  });

  it('spends the boundary when the user\'s steer comes with the report', () => {
    const { deliver, steeringAtOrderRef, messages } = setup([
      ...transcript(),
      user('s1', { content: 'also check margins', steering: true, turnIndex: 0 }),
    ]);

    deliver([
      { content: '', report_source: TASK },
      { content: 'also check margins' },
    ]);

    expect(messages().find((m) => m.id === 's1')).toMatchObject({ steering: false, steeringDelivered: true });
    expect(steeringAtOrderRef.current).toBeNull();
  });

  it('draws the reports ahead of the user\'s rows, then the continuation (reconnect)', () => {
    const { deliver, messages } = setup(transcript());

    deliver([
      { content: '', report_source: { kind: 'analyst', thread_id: 'th-a', workspace_name: 'Semis' } },
      { content: 'and margins', timestamp: 1_700_000_000 },
    ]);

    const rows = messages();
    expect(rows[2].id).toMatch(/^steering-report-/);
    expect(rows[2]).toMatchObject({ content: '', steeringDelivered: true, turnIndex: 0 });
    expect(rows[3].id).toMatch(/^steering-user-/);
    expect(rows[3]).toMatchObject({ content: 'and margins', steeringDelivered: true, turnIndex: 0 });
    expect(rows[4]).toMatchObject({ role: 'assistant', isSteering: true });
  });
});

describe('live: a main-thread steering_returned', () => {
  // It hands a chat app's missed steers back to the gateway. A steer this
  // view sent stays on the server, whose next turn reads it, so the frame
  // leaves the view's steers waiting and the composer alone.
  it('leaves the view\'s waiting steer where it is', () => {
    const initial = [...transcript(), user('s1', { content: 'also check margins', steering: true, turnIndex: 0, requestKey: 'rk-1' })];
    const { process, messages, steeringAtOrderRef } = setup(initial);

    process({ event: 'steering_returned', messages: [{ content: 'also check margins' }] } as unknown as SSEEvent);

    expect(messages()).toBe(initial);
    expect(steeringAtOrderRef.current).toBe(7);
  });
});
