// @vitest-environment node
/**
 * A task's close tells the tray when it went quiet only when the close is
 * news here, which may be the first the page hears of the task. A drain that
 * re-closes a task this page already knew had settled must not restart its
 * five-turn clock.
 */
import { afterEach, describe, it, expect, vi } from 'vitest';
import type { ThreadMuxSink } from '../../stream/threadStreamMux';
import type { SubagentRuntime } from '../../runtime';
import { createSubagentMuxController } from '../muxSink';
import { createSubagentHistoryStore } from '../historyStore';
import type { SubagentHistoryEntry } from '../../types';

const attached = vi.hoisted(() => ({ sink: null as ThreadMuxSink | null }));
vi.mock('../../stream/threadStreamMux', () => ({
  getThreadMux: () => ({ attach: (sink: ThreadMuxSink) => { attached.sink = sink; } }),
}));

const NOW = 1_700_000_500_000;

function setup(historyStatus?: string) {
  vi.useFakeTimers({ now: NOW, toFake: ['Date'] });
  const updateSubagentCard = vi.fn();
  const subagentHistory = createSubagentHistoryStore();
  if (historyStatus) {
    subagentHistory.putEntries({ 'task:Qm3xLp': { status: historyStatus } as SubagentHistoryEntry });
  }
  const rt = {
    subagentHistory,
    terminalTaskOutcomesRef: { current: new Map<string, string>() },
    updateSubagentCard,
    subagentStateRefsRef: { current: {} },
    subagentProcessEventRef: { current: null },
    setHasActiveSubagents: vi.fn(),
    setMessages: vi.fn(),
    // Mid-turn, so no report-back watch is armed.
    isStreamingRef: { current: true },
    threadIdRef: { current: 'thread-1' },
  } as unknown as SubagentRuntime;
  createSubagentMuxController(rt, { muxOpenTaskIds: () => new Set(), armReportBackWatch: vi.fn(), hydrateTaskTranscript: vi.fn(async () => null) })
    .attachSubagentMux('thread-1', vi.fn());
  const close = () => attached.sink!.onTaskRunClosed('Qm3xLp', 'completed');
  const written = () => updateSubagentCard.mock.calls.map(([, data]) => data as Record<string, unknown>);
  const runs = () => Object.fromEntries(subagentHistory.get().runs);
  return { close, written, runs };
}

afterEach(() => {
  vi.useRealTimers();
  attached.sink = null;
});

describe('muxSink: when a closed task went quiet', () => {
  it('stamps a close that is news here, and not the same close again', () => {
    const { close, written } = setup();
    close();
    close();
    expect(written()).toEqual([
      { status: 'completed', isActive: false, lastActivityMs: NOW },
      { status: 'completed', isActive: false },
    ]);
  });

  it('dates a close that is news by the ledger\'s finish of the task\'s latest run', () => {
    const { close, written } = setup();
    const sink = attached.sink!;
    sink.onTaskRun!('Qm3xLp', 'run-1', { startedAtMs: 1_700_000_100_000 });
    sink.onTaskRun!('Qm3xLp', 'run-1', { finishedAtMs: 1_700_000_200_000 });
    sink.onTaskRun!('Qm3xLp', 'run-2', { startedAtMs: 1_700_000_300_000 });
    sink.onTaskRun!('Qm3xLp', 'run-2', { finishedAtMs: 1_700_000_400_000 });
    close();
    expect(written()).toEqual([{ status: 'completed', isActive: false, lastActivityMs: 1_700_000_400_000 }]);
  });

  it('files each run\'s facts under the run, and writes no card for them', () => {
    const { written, runs } = setup();
    const sink = attached.sink!;
    const glm = { id: 'glm-5.3-cn', provider: 'z-ai-cn' };
    // The channel opens before anything has made the task a card.
    sink.onTaskRun!('Qm3xLp', 'run-1', { startedAtMs: 1_700_000_100_000 });
    sink.onTaskRun!('Qm3xLp', 'run-1', { launchToolCallId: 'launch-1', model: glm });
    sink.onTaskRun!('Qm3xLp', 'run-1', { finishedAtMs: 1_700_000_200_000 });
    expect(written()).toEqual([]);
    expect(runs()).toEqual({
      'run-1': {
        taskId: 'task:Qm3xLp',
        launchToolCallId: 'launch-1',
        startedAtMs: 1_700_000_100_000,
        finishedAtMs: 1_700_000_200_000,
        model: glm,
      },
    });
  });

  it('leaves the clock of a task the page loaded as settled', () => {
    const { close, written } = setup('completed');
    close();
    expect(written()).toEqual([{ status: 'completed', isActive: false }]);
  });
});

describe('muxSink: drain of a task the history loaded as settled', () => {
  const PROJECTED = 1_700_000_000_000;

  function drainSetup() {
    const processed: unknown[] = [];
    const rt = {
      subagentHistory: {
        get: () => ({ entries: { 'task:Qm3xLp': { status: 'completed', projectedRunStartedMs: PROJECTED } } }),
      },
      terminalTaskOutcomesRef: { current: new Map<string, string>() },
      updateSubagentCard: vi.fn(),
      subagentStateRefsRef: { current: {} },
      subagentProcessEventRef: { current: null },
      setHasActiveSubagents: vi.fn(),
      setMessages: vi.fn(),
      isStreamingRef: { current: true },
      threadIdRef: { current: 'thread-1' },
    } as unknown as SubagentRuntime;
    createSubagentMuxController(rt, { muxOpenTaskIds: () => new Set(), armReportBackWatch: vi.fn(), hydrateTaskTranscript: vi.fn(async () => null) })
      .attachSubagentMux('thread-1', (ev) => { processed.push(ev); });
    const drain = (runStartedMs: number) =>
      attached.sink!.onTaskEvent({ agent: 'task:Qm3xLp', _drain: true, _runStartedMs: runStartedMs });
    return { rt, drain, processed };
  }

  it('drops the run the history projected', () => {
    const { drain, processed } = drainSetup();
    drain(PROJECTED);
    expect(processed).toEqual([]);
  });

  it('delivers a later run the open turn resumed it under', () => {
    const { drain, processed } = drainSetup();
    drain(PROJECTED + 60_000);
    expect(processed).toHaveLength(1);
  });

  it('drops that later run once this page saw it close', () => {
    const { rt, drain, processed } = drainSetup();
    rt.terminalTaskOutcomesRef.current.set('Qm3xLp', 'completed');
    drain(PROJECTED + 60_000);
    expect(processed).toEqual([]);
  });
});
