// @vitest-environment node
import { describe, it, expect, vi } from 'vitest';
import { prependSubagentRuns, projectSubagentHistory } from '../projectHistory';
import {
  addPendingTaskInstruction, handleSubagentMessageChunk, handleTaskSteeringAccepted, returnTaskInstruction,
} from '../liveEventHandlers';
import { createSubagentHistoryStore, type SubagentHistorySnapshot } from '../historyStore';
import type { SubagentRuntime } from '../../runtime';
import type { SSEEvent } from '../../types';

const makeRuntime = (onChange?: (s: SubagentHistorySnapshot) => void) => {
  const rt = {
    t: (key: string) => key,
    subagentHistory: createSubagentHistoryStore(onChange),
    subagentStateRefsRef: { current: {} },
  };
  return rt as unknown as SubagentRuntime;
};

const lifecycle = (fields: Record<string, unknown>) =>
  ({ event: 'workflow_lifecycle', ...fields }) as unknown as SSEEvent;

/** Replay ghost lane: table-sourced metadata events only, no transcript. */
const ghostEvents = [
  { event: 'provenance', tool_call_id: 'tc-1', sources: [] },
  { event: 'context_window', action: 'token_usage', input_tokens: 10, output_tokens: 5 },
] as unknown as SSEEvent[];

describe('projectSubagentHistory workflow-child backfill', () => {
  it('settles ghost-lane children from the owning run lifecycle', () => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([
        [
          'task:wf1',
          {
            messages: [],
            type: 'workflow',
            events: [
              lifecycle({ phase: 'run_started', name: 'briefs', description: 'Fan out' }),
              lifecycle({ phase: 'child_started', seq: 0, label: 'NVDA', subagent_type: 'research', child_task_id: 'ch1' }),
              lifecycle({ phase: 'child_started', seq: 1, label: 'AMD', subagent_type: 'research', child_task_id: 'ch2' }),
              lifecycle({ phase: 'child_done', seq: 0, status: 'ok', child_task_id: 'ch1' }),
              lifecycle({ phase: 'run_completed', status: 'cancelled' }),
            ],
          },
        ],
        ['task:ch1', { messages: [], events: ghostEvents }],
        ['task:ch2', { messages: [], events: ghostEvents }],
      ]),
    );

    const entries = rt.subagentHistory.get().entries;
    expect(entries['task:ch1']).toMatchObject({
      status: 'completed',
      description: 'NVDA',
      type: 'research',
      ownerTaskId: 'task:wf1',
    });
    // Never marked done before the run settled → torn down with the run.
    expect(entries['task:ch2']).toMatchObject({ status: 'cancelled', description: 'AMD' });
  });

  it('leaves children of a still-running run as running', () => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([
        [
          'task:wf1',
          {
            messages: [],
            type: 'workflow',
            events: [
              lifecycle({ phase: 'run_started', name: 'briefs' }),
              lifecycle({ phase: 'child_started', seq: 0, label: 'NVDA', subagent_type: 'research', child_task_id: 'ch1' }),
            ],
          },
        ],
        ['task:ch1', { messages: [], events: ghostEvents }],
      ]),
    );

    expect(rt.subagentHistory.get().entries['task:ch1']!.status).toBe('running');
  });

  it('does not override a backend-stamped child status', () => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([
        [
          'task:wf1',
          {
            messages: [],
            type: 'workflow',
            events: [
              lifecycle({ phase: 'child_started', seq: 0, label: 'NVDA', child_task_id: 'ch1' }),
              lifecycle({ phase: 'run_completed', status: 'completed' }),
            ],
          },
        ],
        ['task:ch1', { messages: [], events: ghostEvents, status: 'error', error: 'boom' }],
      ]),
    );

    expect(rt.subagentHistory.get().entries['task:ch1']!.status).toBe('error');
  });

  it('backfills a child projected earlier by copy, in one publish', () => {
    const published: SubagentHistorySnapshot[] = [];
    const rt = makeRuntime((s) => published.push(s));
    projectSubagentHistory(rt, new Map([['task:ch1', { messages: [], events: ghostEvents }]]));
    const before = rt.subagentHistory.get();
    const childBefore = before.entries['task:ch1']!;

    projectSubagentHistory(
      rt,
      new Map([
        [
          'task:wf1',
          {
            messages: [],
            type: 'workflow',
            events: [
              lifecycle({ phase: 'child_started', seq: 0, label: 'NVDA', subagent_type: 'research', child_task_id: 'ch1' }),
              lifecycle({ phase: 'child_done', seq: 0, status: 'ok', child_task_id: 'ch1' }),
            ],
          },
        ],
      ]),
    );

    // The run and its child's backfill land together, as one new snapshot.
    expect(published).toHaveLength(2);
    const after = rt.subagentHistory.get();
    expect(after).not.toBe(before);
    expect(after.entries['task:ch1']).toMatchObject({
      description: 'NVDA', type: 'research', ownerTaskId: 'task:wf1', status: 'completed',
    });
    // A reader still holding the earlier snapshot sees it exactly as it was.
    expect(before.entries['task:ch1']).toBe(childBefore);
    expect(childBefore.ownerTaskId).toBeUndefined();
    expect(childBefore.status).toBe('running');
  });
});

describe('projectSubagentHistory steering', () => {
  const replayDelivery = (delivery: Record<string, unknown>) => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([
        [
          'task:k7Xm2p',
          {
            messages: [],
            events: [
              { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Revenue' },
              { event: 'steering_delivered', ...delivery },
            ] as unknown as SSEEvent[],
          },
        ],
      ]),
    );
    return (rt.subagentHistory.get().entries['task:k7Xm2p']!.messages as Record<string, unknown>[])
      .filter((m) => m.role === 'user')
      .map((m) => m.content);
  };

  it('replays each entry one delivery took as its own instruction', () => {
    // The user's instruction and the main agent's follow-up, drained in one step.
    expect(replayDelivery({
      content: 'Focus on margins\nAlso cover 2024 guidance',
      entries: [
        { input_id: 'a1', content: 'Focus on margins' },
        { input_id: 'b2', content: 'Also cover 2024 guidance' },
      ],
    })).toEqual(['Focus on margins', 'Also cover 2024 guidance']);
  });

  it('replays a delivery captured before entries as its joined text', () => {
    expect(replayDelivery({ content: 'Focus on margins\nSkip 2019' }))
      .toEqual(['Focus on margins\nSkip 2019']);
  });

  const replayLabels = (events: Record<string, unknown>[]) => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([['task:k7Xm2p', { messages: [], events: events as unknown as SSEEvent[] }]]),
    );
    return (rt.subagentHistory.get().entries['task:k7Xm2p']!.messages as Record<string, unknown>[])
      .filter((m) => m.role === 'user')
      .map((m) => [m.content, (m.provenance as { kind: string } | undefined)?.kind ?? null]);
  };

  it('labels what the main agent wrote and leaves what the user sent unlabelled', () => {
    expect(replayLabels([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Revenue' },
      {
        event: 'steering_delivered',
        content: 'Focus on margins\nAlso cover 2024 guidance',
        entries: [
          { input_id: 'a1', content: 'Focus on margins', sender: 'user' },
          { input_id: 'b2', content: 'Also cover 2024 guidance', sender: 'agent' },
        ],
      },
      // Captured before the drain named its sender.
      { event: 'steering_delivered', content: 'Skip 2019', entries: [{ input_id: 'c3', content: 'Skip 2019' }] },
    ])).toEqual([
      ['Pull the 10-K', 'main_agent'],
      ['Focus on margins', null],
      ['Also cover 2024 guidance', 'main_agent'],
      ['Skip 2019', null],
    ]);
  });

  const replayMarks = (events: Record<string, unknown>[]) => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([['task:k7Xm2p', { messages: [], events: events as unknown as SSEEvent[] }]]),
    );
    return (rt.subagentHistory.get().entries['task:k7Xm2p']!.messages as Record<string, unknown>[])
      .filter((m) => m.role === 'user')
      .map((m) => [m.content, m.steeringDelivered === true]);
  };

  it('marks what a running run took in as steered, and the instruction a run opens with as not', () => {
    expect(replayMarks([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Revenue' },
      { event: 'steering_delivered', content: 'Focus on margins', entries: [{ input_id: 'a1', content: 'Focus on margins', sender: 'user' }] },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Margins' },
      // The run's resume opens the next, as the first instruction opened the first.
      { event: 'user_message', role: 'user', content: 'Now cover guidance' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Guidance' },
    ])).toEqual([
      ['Pull the 10-K', false],
      ['Focus on margins', true],
      ['Now cover guidance', false],
    ]);
  });

  it('marks a legacy follow-up as steered, and a legacy turn boundary as not', () => {
    expect(replayMarks([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Revenue' },
      { event: 'subagent_followup_injected', content: 'Skip 2019' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Margins' },
      { event: 'turn_start', content: 'Cover guidance' },
    ])).toEqual([
      ['Pull the 10-K', false],
      ['Skip 2019', true],
      ['Cover guidance', false],
    ]);
  });
});

describe('handleTaskSteeringAccepted: the steered flag', () => {
  const accept = ({ steered, entries, pending }: {
    steered: boolean;
    entries?: Parameters<typeof handleTaskSteeringAccepted>[0]['entries'];
    pending?: { inputId: string; content: string };
  }) => {
    const rt = makeRuntime();
    const updateSubagentCard = vi.fn();
    const refs = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: rt.subagentStateRefsRef.current,
    };
    if (pending) addPendingTaskInstruction({ taskId: 'task:t', ...pending, subagentStateRefs: refs.subagentStateRefs, updateSubagentCard });
    handleTaskSteeringAccepted({ taskId: 'task:t', content: 'Focus on margins\nAlso cover 2024', entries, steered, refs, updateSubagentCard });
    return refs.subagentStateRefs['task:t'].messages.filter((m) => m.role === 'user');
  };
  const TWO = [
    { input_id: 'a1', content: 'Focus on margins', sender: 'user' as const },
    { input_id: 'b2', content: 'Also cover 2024', sender: 'agent' as const },
  ];

  it('marks every instruction a steered delivery took, whoever sent it', () => {
    const delivered = accept({ steered: true, entries: TWO });
    expect(delivered.map((m) => [m.content, m.steeringDelivered])).toEqual([
      ['Focus on margins', true],
      ['Also cover 2024', true],
    ]);
  });

  it('marks none when what it took is the instruction a run opens with', () => {
    const delivered = accept({ steered: false, entries: TWO });
    expect(delivered).toHaveLength(2);
    expect(delivered.map((m) => m.steeringDelivered)).toEqual([undefined, undefined]);
  });

  it('marks a delivery that names no entries by its text', () => {
    expect(accept({ steered: true })[0].steeringDelivered).toBe(true);
    expect(accept({ steered: false })[0].steeringDelivered).toBeUndefined();
  });

  it('marks the bubble it confirms as it confirms it', () => {
    const pending = { inputId: 'a1', content: 'Focus on margins' };
    const [bubble] = accept({ steered: true, entries: [TWO[0]], pending });
    expect(bubble).toMatchObject({ content: 'Focus on margins', isPending: false, steeringDelivered: true });
    const [opening] = accept({ steered: false, entries: [TWO[0]], pending });
    expect(opening.isPending).toBe(false);
    expect(opening.steeringDelivered).toBeUndefined();
  });
});

describe('projectSubagentHistory returned steering', () => {
  const replay = (events: Record<string, unknown>[]) => {
    const rt = makeRuntime();
    projectSubagentHistory(
      rt,
      new Map([['task:k7Xm2p', { messages: [], events: events as unknown as SSEEvent[], status: 'completed' }]]),
    );
    return rt;
  };
  const transcript = (messages: Record<string, unknown>[]) => messages.map((m) => {
    const notice = (m.contentSegments as Array<{ type: string; content: string; detail?: string }> | undefined)
      ?.find((s) => s.type === 'notification');
    return notice ? ['notice', notice.content, notice.detail, m.inputId] : [m.role, m.content];
  });

  it('puts up the notice the live stream did, where the run returned it', () => {
    const rt = replay([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Revenue' },
      { event: 'steering_returned', content: 'Focus on margins', input_id: 'a1', reason: 'run_ended' },
    ]);

    const entry = rt.subagentHistory.get().entries['task:k7Xm2p']!;
    expect(transcript(entry.messages as Record<string, unknown>[])).toEqual([
      ['user', 'Pull the 10-K'],
      ['assistant', 'Revenue'],
      ['notice', 'chat.taskSteeringReturnedNotification', 'Focus on margins', 'a1'],
    ]);

    // The same frame re-sent by the live stream on reconnect adds nothing.
    const held = rt.subagentStateRefsRef.current['task:k7Xm2p'];
    const before = held.messages;
    returnTaskInstruction(held, { inputId: 'a1', content: 'Focus on margins' }, 'chat.taskSteeringReturnedNotification');
    expect(held.messages).toBe(before);
  });

  it('reads the text from messages[] when a frame carries no top-level content', () => {
    const rt = replay([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'steering_returned', messages: [{ content: 'Skip 2019' }] },
    ]);
    expect(transcript(rt.subagentHistory.get().entries['task:k7Xm2p']!.messages as Record<string, unknown>[]))
      .toEqual([
        ['user', 'Pull the 10-K'],
        ['notice', 'chat.taskSteeringReturnedNotification', 'Skip 2019', undefined],
      ]);
  });

  it('adds nothing when a reconnect re-sends a return that carries no id', () => {
    const rt = replay([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'steering_returned', content: 'Skip 2019', reason: 'run_ended' },
    ]);
    const held = rt.subagentStateRefsRef.current['task:k7Xm2p'];
    const before = held.messages;
    returnTaskInstruction(held, { content: 'Skip 2019' }, 'chat.taskSteeringReturnedNotification');
    expect(held.messages).toBe(before);
  });

  it('drops the notice of steering a later run took after all', () => {
    const rt = replay([
      { event: 'user_message', role: 'user', content: 'Pull the 10-K' },
      { event: 'steering_returned', content: 'Focus on margins', input_id: 'a1', reason: 'run_mismatch' },
      { event: 'message_chunk', role: 'assistant', content_type: 'text', content: 'Revenue' },
      {
        event: 'steering_delivered',
        content: 'Focus on margins',
        entries: [{ input_id: 'a1', content: 'Focus on margins', sender: 'user' }],
      },
    ]);
    expect(transcript(rt.subagentHistory.get().entries['task:k7Xm2p']!.messages as Record<string, unknown>[]))
      .toEqual([
        ['user', 'Pull the 10-K'],
        ['assistant', 'Revenue'],
        ['user', 'Focus on margins'],
      ]);
  });
});

describe('prependSubagentRuns', () => {
  const run = (instruction: string, says: string, input: number, output: number) => [
    { event: 'user_message', role: 'user', content: instruction },
    { event: 'message_chunk', role: 'assistant', content_type: 'text', content: says },
    { event: 'context_window', action: 'token_usage', input_tokens: input, output_tokens: output },
  ] as unknown as SSEEvent[];

  it('puts the older runs ahead of a live run and keeps the stream writing to its own message', () => {
    const updateSubagentCard = vi.fn();
    const rt = {
      t: (key: string) => key,
      subagentHistory: createSubagentHistoryStore(),
      subagentStateRefsRef: { current: {} },
      subagentTokenUsageRef: { current: {} },
      updateSubagentCard,
    } as unknown as SubagentRuntime;
    // The newest page holds the task's second run.
    projectSubagentHistory(rt, new Map([['task:t', { messages: [], events: run('second', 'B', 10, 5), status: 'running' }]]));
    rt.subagentTokenUsageRef.current['task:t'] = { input: 10, output: 5, total: 15 };

    // Its third run streams live, as processStreamEvent writes it.
    const refs = {
      contentOrderCounterRef: { current: 0 },
      currentReasoningIdRef: { current: null },
      currentToolCallIdRef: { current: null },
      subagentStateRefs: rt.subagentStateRefsRef.current,
    };
    const stream = (says: string) => handleSubagentMessageChunk({
      taskId: 'task:t',
      assistantMessageId: `subagent-task:t-assistant-${rt.subagentStateRefsRef.current['task:t'].runIndex}`,
      contentType: 'text',
      content: says,
      finishReason: undefined,
      refs,
      updateSubagentCard,
    });
    handleTaskSteeringAccepted({ taskId: 'task:t', content: 'third', steered: false, refs, updateSubagentCard });
    stream('C');
    rt.subagentTokenUsageRef.current['task:t'] = { input: 13, output: 7, total: 20 };

    // An older page holds its first run.
    prependSubagentRuns(rt, 'task:t', { messages: [], events: run('first', 'A', 100, 50), status: 'completed' });
    stream(' and more');

    const messages = rt.subagentStateRefsRef.current['task:t'].messages;
    expect(messages.map((m) => [m.role, m.content])).toEqual([
      ['user', 'first'], ['assistant', 'A'],
      ['user', 'second'], ['assistant', 'B'],
      ['user', 'third'], ['assistant', 'C and more'],
    ]);
    expect(new Set(messages.map((m) => m.id)).size).toBe(messages.length);

    const entry = rt.subagentHistory.get().entries['task:t'];
    expect(entry.messages).toHaveLength(6);
    // The task's status stays what the live side knew, not the page's.
    expect(entry.status).toBe('running');
    expect(entry.tokenUsage).toEqual({ input: 110, output: 55, total: 165 });
    expect(rt.subagentTokenUsageRef.current['task:t']).toEqual({ input: 113, output: 57, total: 170 });
    expect(updateSubagentCard).toHaveBeenCalledWith('task:t', {
      messages: expect.any(Array),
      tokenUsage: { input: 113, output: 57, total: 170 },
    });
  });

  it('projects the runs as replay would when nothing is held for the task yet', () => {
    const rt = {
      t: (key: string) => key,
      subagentHistory: createSubagentHistoryStore(),
      subagentStateRefsRef: { current: {} },
      subagentTokenUsageRef: { current: {} },
      updateSubagentCard: vi.fn(),
    } as unknown as SubagentRuntime;
    prependSubagentRuns(rt, 'task:t', { messages: [], events: run('first', 'A', 100, 50), status: 'running' });

    expect(rt.subagentStateRefsRef.current['task:t'].runIndex).toBe(1);
    expect(rt.subagentHistory.get().entries['task:t']).toMatchObject({
      status: 'running',
      tokenUsage: { input: 100, output: 50, total: 150 },
    });
  });
});
