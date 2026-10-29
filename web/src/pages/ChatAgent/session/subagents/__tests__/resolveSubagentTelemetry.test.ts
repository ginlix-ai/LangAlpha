// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  resolveSubagentTelemetry,
  sameTelemetry,
  type SubagentDataLike,
  type SubagentHistoryLike,
  type SubagentTelemetry,
} from '../resolveSubagentTelemetry';
import { parseSubagentModel, type TaskRun } from '../taskRuns';
import { ZERO_USAGE } from '../../../utils/tokenUsage';

const msgWith = (tools: number) => ({
  toolCallProcesses: Object.fromEntries(Array.from({ length: tools }, (_, i) => [`tc-${i}`, {}])),
});

describe('resolveSubagentTelemetry', () => {
  it('returns undefined when neither card nor history is present', () => {
    expect(resolveSubagentTelemetry(undefined, undefined)).toBeUndefined();
  });

  it('uses card state when populated with messages', () => {
    const card: SubagentDataLike = {
      messages: [msgWith(2), msgWith(3)],
      tokenUsage: { input: 100, output: 50, total: 150 },
    };
    expect(resolveSubagentTelemetry(card, undefined)).toEqual({
      toolCalls: 5,
      tokenUsage: { input: 100, output: 50, total: 150 },
    });
  });

  it('uses card state when messages are empty but tokenUsage.total > 0', () => {
    // Live token_usage event landed before any tool call — card has tokens
    // but no messages yet.
    const card: SubagentDataLike = {
      messages: [],
      tokenUsage: { input: 10, output: 5, total: 15 },
    };
    expect(resolveSubagentTelemetry(card, undefined)).toEqual({
      toolCalls: 0,
      tokenUsage: { input: 10, output: 5, total: 15 },
    });
  });

  it('falls back to history when card is present but empty (namespace-race fix)', () => {
    // The exact scenario the click-replay path used to hit: a freshly
    // created card with no live state should NOT shadow the history total.
    const emptyCard: SubagentDataLike = { messages: [], tokenUsage: ZERO_USAGE };
    const history: SubagentHistoryLike = {
      messages: [msgWith(4)],
      tokenUsage: { input: 200, output: 100, total: 300 },
    };
    expect(resolveSubagentTelemetry(emptyCard, history)).toEqual({
      toolCalls: 4,
      tokenUsage: { input: 200, output: 100, total: 300 },
    });
  });

  it('falls back to history when no card exists at all (post-refresh path)', () => {
    const history: SubagentHistoryLike = {
      messages: [msgWith(7)],
      tokenUsage: { input: 500, output: 250, total: 750 },
    };
    expect(resolveSubagentTelemetry(undefined, history)).toEqual({
      toolCalls: 7,
      tokenUsage: { input: 500, output: 250, total: 750 },
    });
  });

  it('prefers explicit history.toolCalls over derived count', () => {
    // History writers may stamp toolCalls explicitly; the resolver should
    // trust that over re-deriving (avoids double-counting reconnect events).
    const history: SubagentHistoryLike = {
      messages: [msgWith(3)],
      toolCalls: 99,
      tokenUsage: { input: 0, output: 0, total: 1000 },
    };
    expect(resolveSubagentTelemetry(undefined, history)).toEqual({
      toolCalls: 99,
      tokenUsage: { input: 0, output: 0, total: 1000 },
    });
  });

  it('history with missing tokenUsage falls through to ZERO_USAGE', () => {
    const history: SubagentHistoryLike = { messages: [msgWith(2)] };
    expect(resolveSubagentTelemetry(undefined, history)).toEqual({
      toolCalls: 2,
      tokenUsage: ZERO_USAGE,
    });
  });

  it('card present but unpopulated AND no history → returns ZERO defaults from card path', () => {
    // Edge case: the card was created (e.g. user clicked) but neither live
    // events nor history exist yet. We still return a value so callers can
    // render "0 tools · 0 tokens" without flickering through undefined.
    const emptyCard: SubagentDataLike = { messages: [], tokenUsage: ZERO_USAGE };
    expect(resolveSubagentTelemetry(emptyCard, undefined)).toEqual({
      toolCalls: 0,
      tokenUsage: ZERO_USAGE,
    });
  });

  it('does not mutate inputs', () => {
    const card: SubagentDataLike = { messages: [msgWith(1)], tokenUsage: { input: 1, output: 1, total: 2 } };
    const history: SubagentHistoryLike = { messages: [msgWith(9)], tokenUsage: { input: 9, output: 9, total: 18 } };
    const cardSnap = JSON.stringify(card);
    const historySnap = JSON.stringify(history);
    resolveSubagentTelemetry(card, history);
    expect(JSON.stringify(card)).toBe(cardSnap);
    expect(JSON.stringify(history)).toBe(historySnap);
  });
});

/**
 * The stop reason has its own precedence, deliberately unlike the numbers.
 * Only one of the two writers ever holds a reason for a given task — the live
 * error frame stamps the card, replay stamps history from the ledger — so
 * reading it from whichever source won the count would blank it on exactly
 * the permutation the other one covers.
 */
describe('resolveSubagentTelemetry: stop reason', () => {
  const REASON = 'Monthly credit limit reached (50/50 credits)';

  it('takes the reason from history while the numbers come from the card', () => {
    const card: SubagentDataLike = { messages: [msgWith(2)], tokenUsage: ZERO_USAGE };
    const history: SubagentHistoryLike = { error: REASON, errorType: 'credit_stop' };
    expect(resolveSubagentTelemetry(card, history)).toEqual({
      toolCalls: 2,
      tokenUsage: ZERO_USAGE,
      stopReason: REASON,
      stopReasonType: 'credit_stop',
    });
  });

  it('takes the reason from the card when history is the source of the numbers', () => {
    const card: SubagentDataLike = { error: REASON, errorType: 'credit_stop' };
    const history: SubagentHistoryLike = { toolCalls: 13, tokenUsage: ZERO_USAGE };
    expect(resolveSubagentTelemetry(card, history)).toEqual({
      toolCalls: 13,
      tokenUsage: ZERO_USAGE,
      stopReason: REASON,
      stopReasonType: 'credit_stop',
    });
  });

  it('omits both keys entirely when neither source has a reason', () => {
    const result = resolveSubagentTelemetry({ messages: [msgWith(1)] }, undefined);
    expect(result).not.toHaveProperty('stopReason');
    expect(result).not.toHaveProperty('stopReasonType');
  });

  it('keeps a reason that arrived without a type, as it is still worth stating', () => {
    const result = resolveSubagentTelemetry(undefined, { error: 'run ended: cancelled' });
    expect(result?.stopReason).toBe('run ended: cancelled');
    expect(result).not.toHaveProperty('stopReasonType');
  });
});

/**
 * A run's own fields. The bounds and the model come from the run ledger,
 * filed by run (`taskRuns.ts`), and each row draws the run its own launch
 * started: a resumed task has one row per launch over one task.
 */
describe('resolveSubagentTelemetry: the run a row draws', () => {
  const GLM = { id: 'glm-5.3-cn', provider: 'z-ai-cn' };
  const TASK = 'task:T1';
  const run = (fields: Omit<TaskRun, 'taskId'>): TaskRun => ({ taskId: TASK, ...fields });
  const call = (
    order: number,
    extra: Record<string, unknown> = {},
  ): [string, Record<string, unknown>] => [
    `call-${order}`,
    { toolName: 'bash', toolCall: { args: { command: `step ${order}` } }, isInProgress: false, isComplete: true, order, ...extra },
  ];
  const message = (
    calls: Array<[string, Record<string, unknown>]>,
    segments: unknown[] = [],
    extra: Record<string, unknown> = {},
  ) => ({
    role: 'assistant',
    toolCallProcesses: Object.fromEntries(calls),
    contentSegments: segments,
    ...extra,
  });

  it('draws a live run: its start, its model, the call it is in', () => {
    const card: SubagentDataLike = {
      status: 'active',
      messages: [message([call(0), call(1), call(2, { isInProgress: true, isComplete: false })])],
    };
    const runs = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, model: GLM })];
    const result = resolveSubagentTelemetry(card, undefined, runs, 'launch-1');
    expect(result).toMatchObject({
      toolCalls: 3,
      startedAtMs: 1_000,
      model: GLM,
      currentToolCall: { toolName: 'bash', args: { command: 'step 2' } },
      running: true,
    });
    expect(result).not.toHaveProperty('finishedAtMs');
  });

  it('draws a settled launch from its run, with the prose it closed on', () => {
    const history: SubagentHistoryLike = {
      messages: [
        message([call(0)], [
          { type: 'tool_call', toolCallId: 'call-0', order: 0 },
          { type: 'text', content: 'Revenue grew 12%.', order: 1 },
        ]),
      ],
    };
    const runs = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 9_000, model: GLM })];
    expect(resolveSubagentTelemetry(undefined, history, runs, 'launch-1')).toMatchObject({
      startedAtMs: 1_000,
      finishedAtMs: 9_000,
      model: GLM,
      lastText: 'Revenue grew 12%.',
    });
  });

  it('names no model for a run on its launcher\'s', () => {
    const history: SubagentHistoryLike = { messages: [message([call(0)])] };
    const runs = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 9_000 })];
    expect(resolveSubagentTelemetry(undefined, history, runs, 'launch-1')).not.toHaveProperty('model');
  });

  it('keeps an earlier launch on its own run after the task resumed', () => {
    const card: SubagentDataLike = {
      status: 'active',
      messages: [
        message([call(0)], [
          { type: 'tool_call', toolCallId: 'call-0', order: 0 },
          { type: 'text', content: 'Revenue grew 12%.', order: 1 },
        ], { openedAtMs: 1_050 }),
        message([call(1, { isInProgress: true, isComplete: false })], [
          { type: 'text', content: 'Checking the margins.', order: 0 },
          { type: 'tool_call', toolCallId: 'call-1', order: 1 },
        ], { openedAtMs: 5_050 }),
      ],
    };
    const runs = [
      run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 3_000, model: GLM }),
      // The resume's run, on the launcher's model.
      run({ launchToolCallId: 'launch-2', startedAtMs: 5_000 }),
    ];
    const first = resolveSubagentTelemetry(card, undefined, runs, 'launch-1');
    expect(first).toMatchObject({
      startedAtMs: 1_000,
      finishedAtMs: 3_000,
      model: GLM,
      // Its own closing prose, not the resume's.
      lastText: 'Revenue grew 12%.',
    });
    expect(first).not.toHaveProperty('currentToolCall');
    expect(first).not.toHaveProperty('running');

    const resume = resolveSubagentTelemetry(card, undefined, runs, 'launch-2');
    expect(resume).toMatchObject({ startedAtMs: 5_000, running: true });
    expect(resume).not.toHaveProperty('model');
    expect(resume?.currentToolCall?.toolName).toBe('bash');
    expect(resume).not.toHaveProperty('lastText');
  });

  it("finds an earlier launch's prose by its run's window, finish or not", () => {
    // Its close never reached this page, so it has no finish to stop at.
    const card: SubagentDataLike = {
      status: 'completed',
      messages: [
        message([call(0)], [{ type: 'text', content: 'First pass done.', order: 0 }], { openedAtMs: 1_100 }),
        message([call(1)], [{ type: 'text', content: 'Second pass done.', order: 0 }], { openedAtMs: 5_100 }),
      ],
    };
    const runs = [
      run({ launchToolCallId: 'launch-1', startedAtMs: 1_000 }),
      run({ launchToolCallId: 'launch-2', startedAtMs: 5_000, finishedAtMs: 8_000 }),
    ];
    expect(resolveSubagentTelemetry(card, undefined, runs, 'launch-1')?.lastText).toBe('First pass done.');
    expect(resolveSubagentTelemetry(card, undefined, runs, 'launch-2')?.lastText).toBe('Second pass done.');
  });

  it('gives the earliest run what was written before its start', () => {
    // The writer's clock ran a little behind the database's.
    const card: SubagentDataLike = {
      status: 'completed',
      messages: [
        message([call(0)], [{ type: 'text', content: 'First pass done.', order: 0 }], { openedAtMs: 990 }),
        message([call(1)], [{ type: 'text', content: 'Second pass done.', order: 0 }], { openedAtMs: 5_100 }),
      ],
    };
    const runs = [
      run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 3_000 }),
      run({ launchToolCallId: 'launch-2', startedAtMs: 5_000, finishedAtMs: 8_000 }),
    ];
    expect(resolveSubagentTelemetry(card, undefined, runs, 'launch-1')?.lastText).toBe('First pass done.');
  });

  it('draws the task\'s newest run for a launch no run names', () => {
    // A launch from before the ledger, or one whose run has not opened its lane.
    const history: SubagentHistoryLike = { status: 'completed', messages: [message([call(0)])] };
    const runs = [
      run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 3_000 }),
      run({ startedAtMs: 5_000, finishedAtMs: 8_000, model: GLM }),
    ];
    expect(resolveSubagentTelemetry(undefined, history, runs, 'launch-0')).toMatchObject({
      startedAtMs: 5_000,
      finishedAtMs: 8_000,
      model: GLM,
    });
  });

  it('names no current call once the task settled', () => {
    // A call left unanswered by a stop is not one the task is still in.
    const card: SubagentDataLike = {
      status: 'cancelled',
      messages: [message([call(0, { isInProgress: true, isComplete: false })])],
    };
    const runs = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000 })];
    expect(resolveSubagentTelemetry(card, undefined, runs, 'launch-1')).not.toHaveProperty('currentToolCall');
  });

  it('has no closing prose while the run is in a call', () => {
    const card: SubagentDataLike = {
      messages: [
        message([call(0)], [
          { type: 'text', content: 'Pulling the filing.', order: 0 },
          { type: 'tool_call', toolCallId: 'call-0', order: 1 },
        ]),
      ],
    };
    expect(resolveSubagentTelemetry(card, undefined)).not.toHaveProperty('lastText');
  });

  it('says a run is the live one only while it is, and once the ledger says it started', () => {
    const card: SubagentDataLike = { status: 'active', messages: [message([call(0)])] };
    const started = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000 })];
    expect(resolveSubagentTelemetry(card, undefined, started, 'launch-1')).toMatchObject({ running: true });
    // Its start not yet heard of.
    expect(resolveSubagentTelemetry(card, undefined, [], 'launch-1')).not.toHaveProperty('running');
    // The ledger settled it, the card's status not yet.
    const finished = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 9_000 })];
    expect(resolveSubagentTelemetry(card, undefined, finished, 'launch-1')).not.toHaveProperty('running');
    // The card settled it, the ledger's finish not yet here.
    expect(resolveSubagentTelemetry({ ...card, status: 'completed' }, undefined, started, 'launch-1'))
      .not.toHaveProperty('running');
  });

  it('settles a run from the status history replayed, with no card and no finish kept', () => {
    const history: SubagentHistoryLike = {
      status: 'completed',
      messages: [message([call(0)], [{ type: 'tool_call', toolCallId: 'call-0', order: 0 }, { type: 'text', content: 'Done.', order: 1 }])],
    };
    const runs = [run({ launchToolCallId: 'launch-1', startedAtMs: 1_000 })];
    const result = resolveSubagentTelemetry(undefined, history, runs, 'launch-1');
    expect(result).not.toHaveProperty('running');
    expect(result).toMatchObject({ lastText: 'Done.' });
  });

  it('closes on the run\'s own prose, not a notice the view added after it', () => {
    const card: SubagentDataLike = {
      status: 'completed',
      messages: [
        message([call(0)], [{ type: 'tool_call', toolCallId: 'call-0', order: 0 }, { type: 'text', content: 'Margins held.', order: 1 }]),
        { id: 'task-notice-1', role: 'assistant', contentSegments: [{ type: 'text', content: 'Your instruction came back.' }] },
      ],
    };
    expect(resolveSubagentTelemetry(card, undefined)).toMatchObject({ lastText: 'Margins held.' });
  });
});

describe('parseSubagentModel', () => {
  it('reads a model the wire names in full, and nothing else', () => {
    expect(parseSubagentModel({ id: 'glm-5.3-cn', provider: 'z-ai-cn', extra: 1 })).toEqual({
      id: 'glm-5.3-cn',
      provider: 'z-ai-cn',
    });
    expect(parseSubagentModel({ id: 'glm-5.3-cn' })).toBeUndefined();
    expect(parseSubagentModel({ id: '', provider: 'z-ai-cn' })).toBeUndefined();
    expect(parseSubagentModel(null)).toBeUndefined();
    expect(parseSubagentModel('glm-5.3-cn')).toBeUndefined();
  });
});

describe('sameTelemetry', () => {
  const base: SubagentTelemetry = {
    toolCalls: 1,
    tokenUsage: { input: 1, output: 1, total: 2 },
    startedAtMs: 1_000,
    model: { id: 'glm-5.3-cn', provider: 'z-ai-cn' },
    currentToolCall: { toolName: 'bash', args: { command: 'ls' } },
  };
  const copy = (): SubagentTelemetry => ({
    ...base,
    tokenUsage: { ...base.tokenUsage },
    model: { ...base.model! },
    currentToolCall: { ...base.currentToolCall! },
  });

  it('matches two answers that draw the same, whatever their identity', () => {
    expect(sameTelemetry(base, copy())).toBe(true);
    expect(sameTelemetry(undefined, undefined)).toBe(true);
  });

  it('tells a moved part, an added field and a missing answer apart', () => {
    expect(sameTelemetry(base, { ...copy(), tokenUsage: { input: 5, output: 5, total: 10 } })).toBe(false);
    expect(sameTelemetry(base, { ...copy(), model: { id: 'qwen3.8-flash', provider: 'dashscope' } })).toBe(false);
    expect(sameTelemetry(base, { ...copy(), finishedAtMs: 9_000 })).toBe(false);
    expect(sameTelemetry(base, { ...copy(), currentToolCall: { toolName: 'bash' } })).toBe(false);
    expect(sameTelemetry(base, undefined)).toBe(false);
  });
});
