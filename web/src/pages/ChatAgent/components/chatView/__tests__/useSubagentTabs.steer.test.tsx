/**
 * Steering a subagent while it streams. The instruction shows at once as a
 * pending bubble, and the server delivers it only before the subagent's next
 * model call, so the message the subagent is writing keeps streaming with the
 * bubble on screen. Every stream write carries the task's whole transcript,
 * and the card keeps a longer list over a shorter one, so a bubble the
 * transcript does not hold froze the message until the delivery landed.
 *
 * A send that fails never reaches the queue, so no stream event will settle
 * its bubble; the send's own failure takes it back.
 *
 * Each instruction carries an id the client names before the send goes out,
 * since the delivery can land before the POST answers. The queue has a second
 * producer, the main agent's Task update, and one step drains whatever both
 * queued, so a delivery is settled entry by entry rather than by its text.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import React from 'react';
import { act, renderHook } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { mockFrames, runFrame, settleFrames } from '@/test/frames';
import { useCardState } from '../../../hooks/useCardState';
import { createStreamEventProcessor } from '../../../session/stream/processStreamEvent';
import { sendTaskInstruction } from '../../../session/subagents/sendTaskInstruction';
import type { TaskRefs } from '../../../session/streamRefs';
import type { SSEEvent } from '../../../session/types';
import { sendSubagentMessage } from '../../../utils/api';
import { useSubagentTabs } from '../useSubagentTabs';

vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../../utils/api')>()),
  sendSubagentMessage: vi.fn(),
}));

type Processor = Parameters<typeof createStreamEventProcessor>;

const TASK = 'task:k7Xm2p';

beforeEach(() => {
  mockFrames();
  vi.mocked(sendSubagentMessage).mockReset().mockResolvedValue({});
});

afterEach(() => {
  settleFrames();
  vi.restoreAllMocks();
});

function harness() {
  const refs = {
    contentOrderCounterRef: { current: 0 },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
    subagentStateRefs: {} as Record<string, TaskRefs>,
  };
  const subagentStateRefsRef = { current: refs.subagentStateRefs };
  const wrapper = ({ children }: { children: React.ReactNode }) => <MemoryRouter>{children}</MemoryRouter>;
  const rendered = renderHook(
    () => {
      const cardState = useCardState();
      const [activeAgentId, setActiveAgentId] = React.useState(TASK);
      const activeAgentIdRef = React.useRef(TASK);
      const tabs = useSubagentTabs({
        threadId: 'thread-1',
        workspaceId: 'ws-1',
        // The task's own URL, with the deep-link refresh held off: the card
        // here is the one the stream builds.
        initialTaskId: TASK.slice('task:'.length),
        isLoadingHistory: true,
        activeAgentId,
        setActiveAgentId,
        cards: cardState.cards,
        updateSubagentCard: cardState.updateSubagentCard,
        // How useChatMessages binds it: to the transcripts the stream writes.
        sendSubagentInstruction: (tid: string, agentId: string, content: string) =>
          sendTaskInstruction(
            { t: (key: string) => key, updateSubagentCard: cardState.updateSubagentCard, subagentStateRefsRef },
            tid,
            agentId,
            content,
          ),
        getSubagentHistory: (() => null) as never,
        getTaskRuns: () => [],
        putTaskRun: () => {},
        resolveSubagentIdToAgentId: ((id: string) => id) as never,
        hydrateTaskTranscript: (async () => null) as never,
        saveScrollPosition: () => {},
        activeAgentIdRef,
        resolvedThreadIdRef: { current: 'thread-1' },
      });
      return { ...tabs, cards: cardState.cards, updateSubagentCard: cardState.updateSubagentCard };
    },
    { wrapper },
  );
  const rt = {
    setMessages: vi.fn(),
    queueMessages: vi.fn(),
    flushMessages: vi.fn(),
    updateSubagentCard: (...args: Parameters<typeof rendered.result.current.updateSubagentCard>) =>
      rendered.result.current.updateSubagentCard(...args),
    lastEventIdRef: { current: null },
    threadIdRef: { current: null },
    t: (key: string) => key,
  };
  const process = createStreamEventProcessor(
    rt as unknown as Processor[0],
    { clearModelStatus: () => {} } as unknown as Processor[1],
    'a1',
    refs as unknown as Processor[3],
    (event) => (event.agent as string) ?? null,
  );
  const feed = (event: Record<string, unknown>) => act(() => process({ agent: TASK, ...event } as SSEEvent));
  const text = (content: string) => feed({ event: 'message_chunk', content_type: 'text', content });
  const steer = (content: string) => act(() => rendered.result.current.handleSubagentInstruction(content));
  // Starts a send and leaves it in flight; the returned promise settles with it.
  const startSteer = (content: string) => {
    let sent!: Promise<void>;
    act(() => {
      sent = rendered.result.current.handleSubagentInstruction(content);
    });
    return sent;
  };
  const frame = () => act(() => runFrame());
  const card = () => rendered.result.current.cards[`subagent-${TASK}`]?.subagentData?.messages ?? [];
  const messages = () =>
    card().map((m) => {
      const segments = (m.contentSegments ?? []) as { type: string; content?: string }[];
      const notice = segments.find((seg) => seg.type === 'notification')?.content;
      return {
        role: m.role,
        ...(notice ? { notice } : { content: m.content }),
        ...(m.isPending ? { isPending: true } : {}),
        ...(m.provenance ? { from: (m.provenance as { kind: string }).kind } : {}),
      };
    });
  const ids = () => card().map((m) => m.id);
  // The ids the sends carried, in send order.
  const inputIds = () => vi.mocked(sendSubagentMessage).mock.calls.map((call) => call[3]);
  // A delivery as the drain frames it: the joined text the subagent reads,
  // and the queue entries it took.
  const deliver = (...entries: { input_id: string; content: string; sender?: 'user' | 'agent' }[]) =>
    feed({
      event: 'steering_delivered',
      content: entries.map((entry) => entry.content).join('\n'),
      count: entries.length,
      entries,
    });
  return { feed, deliver, text, steer, startSteer, frame, messages, ids, inputIds };
}

describe('useSubagentTabs — steering a streaming subagent', () => {
  it('keeps the running message streaming under a pending instruction', async () => {
    const { text, steer, frame, messages } = harness();
    text('Revenue');
    frame();

    await steer('Focus on margins');
    text(' grew 12%');
    frame();

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue grew 12%' },
      { role: 'user', content: 'Focus on margins', isPending: true },
    ]);
  });

  it('confirms the pending bubble itself when the instruction is delivered', async () => {
    const { deliver, text, steer, frame, messages, ids, inputIds } = harness();
    text('Revenue');
    frame();
    await steer('Focus on margins');
    const bubbleId = ids()[1];

    deliver({ input_id: inputIds()[0], content: 'Focus on margins' });
    text('Margins widened');
    frame();

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
      { role: 'assistant', content: 'Margins widened' },
    ]);
    expect(ids()[1]).toBe(bubbleId);
  });

  it('shows a delivered instruction after the answer that streamed while it waited', async () => {
    const { deliver, text, steer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();
    await steer('Focus on margins');
    deliver({ input_id: inputIds()[0], content: 'Focus on margins' });
    // Sent before the subagent answered the first; it reads this one after.
    await steer('Skip 2019');
    text('Margins widened');
    frame();

    deliver({ input_id: inputIds()[1], content: 'Skip 2019' });
    text('2019 dropped');
    frame();

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
      { role: 'assistant', content: 'Margins widened' },
      { role: 'user', content: 'Skip 2019' },
      { role: 'assistant', content: '2019 dropped' },
    ]);
  });

  it('confirms every instruction one delivery drained, without a second bubble', async () => {
    const { deliver, text, steer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();
    await steer('Focus on margins');
    await steer('Skip 2019');

    // The middleware drains the whole queue before the next model call.
    const [first, second] = inputIds();
    deliver({ input_id: first, content: 'Focus on margins' }, { input_id: second, content: 'Skip 2019' });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
      { role: 'user', content: 'Skip 2019' },
    ]);
  });

  it('shows a main agent follow-up drained with the instruction as a bubble of its own', async () => {
    const { deliver, text, steer, frame, messages, ids, inputIds } = harness();
    text('Revenue');
    frame();
    await steer('Focus on margins');
    const bubbleId = ids()[1];

    // The main agent's Task update queued into the same run, and one step
    // took both.
    deliver(
      { input_id: inputIds()[0], content: 'Focus on margins' },
      { input_id: '5d0c2e8a', content: 'Also cover 2024 guidance' },
    );

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
      { role: 'user', content: 'Also cover 2024 guidance' },
    ]);
    expect(ids()[1]).toBe(bubbleId);
  });

  it('labels what the main agent wrote, never the instruction the user sent', async () => {
    const { feed, deliver, text, steer, frame, messages, inputIds } = harness();
    feed({ event: 'user_message', content: 'Pull the 10-K' });
    text('Revenue');
    frame();
    await steer('Focus on margins');

    deliver(
      { input_id: inputIds()[0], content: 'Focus on margins', sender: 'user' },
      { input_id: '5d0c2e8a', content: 'Also cover 2024 guidance', sender: 'agent' },
    );

    expect(messages()).toEqual([
      { role: 'user', content: 'Pull the 10-K', from: 'main_agent' },
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
      { role: 'user', content: 'Also cover 2024 guidance', from: 'main_agent' },
    ]);
  });

  it('confirms an instruction delivered before its send answered', async () => {
    let resolve!: (value: unknown) => void;
    vi.mocked(sendSubagentMessage).mockReturnValueOnce(new Promise((r) => { resolve = r; }));
    const { deliver, text, startSteer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();
    const sent = startSteer('Focus on margins');

    deliver({ input_id: inputIds()[0], content: 'Focus on margins' });
    await act(async () => {
      resolve({});
      await sent;
    });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
    ]);
  });

  it('confirms by text a delivery framed without its entries', async () => {
    const { feed, text, steer, frame, messages } = harness();
    text('Revenue');
    frame();
    await steer('Focus on margins');
    await steer('Skip 2019');

    // An older server sends only the newline-joined instruction.
    feed({ event: 'steering_delivered', content: 'Focus on margins\nSkip 2019' });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
      { role: 'user', content: 'Skip 2019' },
    ]);
  });

  it('takes the bubble back when the run ends before delivering it', async () => {
    const { feed, text, steer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();
    await steer('Focus on margins');

    feed({ event: 'steering_returned', content: 'Focus on margins', input_id: inputIds()[0] });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'assistant', notice: 'chat.taskSteeringReturnedNotification' },
    ]);
  });

  it('takes back the instruction the run returned, not another with the same text', async () => {
    const { feed, text, steer, frame, messages, ids, inputIds } = harness();
    text('Revenue');
    frame();
    await steer('Skip 2019');
    await steer('Skip 2019');
    const second = ids()[2];

    feed({ event: 'steering_returned', content: 'Skip 2019', input_id: inputIds()[0] });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Skip 2019', isPending: true },
      { role: 'assistant', notice: 'chat.taskSteeringReturnedNotification' },
    ]);
    expect(ids()[1]).toBe(second);
  });

  it('takes the bubble back when the send is rejected', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    // The task finished between the user opening the input and sending.
    vi.mocked(sendSubagentMessage).mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 409'), { response: { status: 409 } }),
    );
    const { text, steer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();

    await steer('Focus on margins');

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'assistant', notice: 'chat.taskSteeringReturnedNotification' },
    ]);
    expect(sendSubagentMessage).toHaveBeenCalledWith('thread-1', 'k7Xm2p', 'Focus on margins', inputIds()[0]);
  });

  it('says only that it was not sent when the send fails for another reason', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(sendSubagentMessage).mockRejectedValueOnce(new Error('Network Error'));
    const { text, steer, frame, messages } = harness();
    text('Revenue');
    frame();

    await steer('Focus on margins');

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'assistant', notice: 'chat.taskSteeringNotSentNotification' },
    ]);
  });

  it('adds no second notice when the run returned the instruction before its send failed', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    let reject!: (err: unknown) => void;
    vi.mocked(sendSubagentMessage).mockReturnValueOnce(new Promise((_, r) => { reject = r; }));
    const { feed, text, startSteer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();
    const sent = startSteer('Focus on margins');

    // The run's terminal sweep took the queued entry back, then the send's
    // own liveness recheck answered 409.
    feed({ event: 'steering_returned', content: 'Focus on margins', input_id: inputIds()[0] });
    await act(async () => {
      reject(new Error('Request failed with status code 409'));
      await sent;
    });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'assistant', notice: 'chat.taskSteeringReturnedNotification' },
    ]);
  });

  it('adds no second notice when the send was refused before the run returned it', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(sendSubagentMessage).mockRejectedValueOnce(
      Object.assign(new Error('Request failed with status code 409'), { response: { status: 409 } }),
    );
    const { feed, text, steer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();

    await steer('Focus on margins');
    // The sweep read the entry before the send's recheck took it back.
    feed({ event: 'steering_returned', content: 'Focus on margins', input_id: inputIds()[0] });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'assistant', notice: 'chat.taskSteeringReturnedNotification' },
    ]);
  });

  it('shows an instruction delivered after its send failed in place of the notice', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    // The server queued it, and the answer was lost on the way back.
    vi.mocked(sendSubagentMessage).mockRejectedValueOnce(new Error('Network Error'));
    const { deliver, text, steer, frame, messages, inputIds } = harness();
    text('Revenue');
    frame();

    await steer('Focus on margins');
    deliver({ input_id: inputIds()[0], content: 'Focus on margins' });

    expect(messages()).toEqual([
      { role: 'assistant', content: 'Revenue' },
      { role: 'user', content: 'Focus on margins' },
    ]);
  });
});
