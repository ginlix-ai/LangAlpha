// @vitest-environment node
/**
 * A Claude Code turn waiting for one of its computer's CLIs shows the wait
 * live, and once it gets one, a wait long enough to note leaves its notice in
 * stream order: once, however often the frame is delivered, and only for the
 * main agent.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent } from '../../types';
import { createStreamEventProcessor } from '../processStreamEvent';

type Processor = Parameters<typeof createStreamEventProcessor>;

function setup() {
  const setTurnStatus = vi.fn();
  let messages = [{ id: 'a1', role: 'assistant', content: '', contentSegments: [] }] as unknown as ChatMessage[];
  const rt = {
    setMessages: (updater: (prev: ChatMessage[]) => ChatMessage[]) => { messages = updater(messages); },
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: vi.fn(),
    setTurnStatus,
    lastEventIdRef: { current: null },
    currentRunIdRef: { current: null },
    latestRunIdRef: { current: null },
    latestRunEpochRef: { current: 0 },
    currentMessageRef: { current: 'a1' },
    threadId: 'thread-1',
    threadIdRef: { current: 'thread-1' },
    pendingPTCBackfillRef: { current: new Map() },
  };
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {},
    steeringAtOrderRef: { current: null },
    isReconnect: false,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { clearModelStatus: () => {}, refreshClaudeCode: vi.fn() } as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    () => null,
  );
  const notices = () => (messages[0] as unknown as { contentSegments: Array<Record<string, unknown>> })
    .contentSegments.filter((s) => s.type === 'notification');
  return { process, setTurnStatus, notices };
}

const queue = (signal: 'waiting' | 'admitted' | 'cleared', eventId: number, extra: Record<string, unknown> = {}) => ({
  event: 'claude_code_queue', thread_id: 'thread-1', agent: 'main', signal, wait_id: 'w1',
  ...(signal === 'cleared'
    ? { waited_s: 2 }
    : { computer_id: 'c1', name: 'MacBook-Pro-7.local', running: 2, cap: 2, settable: true, max: 8 }),
  ...(signal === 'admitted' ? { waited_s: 186 } : {}),
  _eventId: eventId, ...extra,
}) as unknown as SSEEvent;

describe('live: a turn waiting for a Claude Code CLI', () => {
  it('shows the wait while it lasts, and adds no notice yet', () => {
    const { process, setTurnStatus, notices } = setup();
    process(queue('waiting', 3));
    expect(setTurnStatus).toHaveBeenLastCalledWith({
      cliQueue: expect.objectContaining({ waitId: 'w1', computerId: 'c1', name: 'MacBook-Pro-7.local', running: 2 }),
    });
    expect(notices()).toEqual([]);
  });

  it('clears the line once admitted and notes the wait once, however often it is delivered', () => {
    const { process, setTurnStatus, notices } = setup();
    process(queue('waiting', 3));
    process(queue('admitted', 4));
    expect(setTurnStatus).toHaveBeenLastCalledWith({ cliQueue: null });
    process(queue('admitted', 4));
    process(queue('admitted', 5));
    expect(notices()).toEqual([expect.objectContaining({
      order: 4,
      notice: expect.objectContaining({ kind: 'cliWait', waitId: 'w1', computerId: 'c1', waitedS: 186 }),
    })]);
  });

  it('clears the line of a wait too short to note, and notes nothing', () => {
    const { process, setTurnStatus, notices } = setup();
    process(queue('waiting', 3));
    process(queue('cleared', 4));
    expect(setTurnStatus).toHaveBeenLastCalledWith({ cliQueue: null });
    expect(notices()).toEqual([]);
  });

  it('notes each wait of a turn that started a second CLI and waited again', () => {
    const { process, notices } = setup();
    process(queue('admitted', 4));
    process(queue('admitted', 9, { wait_id: 'w2', waited_s: 12 }));
    expect(notices().map((s) => (s.notice as { waitId: string }).waitId)).toEqual(['w1', 'w2']);
  });

  it('ignores a subagent\'s frame, one with no wait id, and one it cannot read', () => {
    const { process, setTurnStatus, notices } = setup();
    process(queue('waiting', 3, { agent: 'task:k3' }));
    process(queue('admitted', 4, { agent: 'task:k3' }));
    process(queue('waiting', 5, { wait_id: undefined }));
    process(queue('admitted', 6, { wait_id: undefined }));
    process(queue('cleared', 7, { wait_id: undefined }));
    process(queue('waiting', 8, { cap: 'two' }));
    expect(setTurnStatus).not.toHaveBeenCalled();
    expect(notices()).toEqual([]);
  });
});
