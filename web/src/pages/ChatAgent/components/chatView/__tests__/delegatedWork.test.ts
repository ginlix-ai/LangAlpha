// @vitest-environment node
/**
 * The roster's rules: each task and Analyst stays while it runs, then until
 * five turns that started new work here have begun since it last did
 * anything, each on its own clock. Report-backs, steering and refused sends
 * never age it.
 */
import { describe, it, expect } from 'vitest';
import type { ChatMessage, MessageProvenance, PTCAgentProposalState } from '@/types/chat';
import {
  ROSTER_IDLE_TURNS,
  analystCandidates,
  countedTurnStarts,
  staysOnRoster,
  taskRoster,
} from '../delegatedWork';

function user(at: number, extra: { provenance?: MessageProvenance; steering?: boolean; steeringDelivered?: boolean; queued?: boolean; queueError?: string } = {}): ChatMessage {
  return {
    id: `u${at}`, role: 'user', content: 'x', contentType: 'text', timestamp: new Date(at), isStreaming: false, ...extra,
  };
}

function assistant(at: number, ptcAgentProposals?: Record<string, PTCAgentProposalState>): ChatMessage {
  return {
    id: `a${at}`, role: 'assistant', content: '', contentType: 'text', timestamp: new Date(at), isStreaming: false,
    contentSegments: [], reasoningProcesses: {}, toolCallProcesses: {}, ptcAgentProposals,
  };
}

function taskReport(at: number, taskId: string, failed = false): ChatMessage {
  return user(at, { provenance: { kind: 'task_report', taskId, displayId: `Task-${taskId}`, description: `Report of ${taskId}`, failed } });
}

/** A turn the agent answered, so it counts. */
function turn(at: number, extra: Parameters<typeof user>[1] = {}): ChatMessage[] {
  return [user(at, extra), assistant(at + 1)];
}

/** Counted turns starting at 100, 200, … */
function turns(count: number): ChatMessage[] {
  return Array.from({ length: count }, (_, i) => turn((i + 1) * 100)).flat();
}

describe('countedTurnStarts', () => {
  it('counts every turn that starts new work, whoever sent it', () => {
    const messages = [
      ...turn(10),
      ...turn(20, { provenance: { kind: 'automation', automationId: 'a', name: 'Daily' } }),
      ...turn(30, { provenance: { kind: 'channel', platform: 'telegram' } }),
      ...turn(40, { provenance: { kind: 'agent', agent: null, threadId: 'home' } }),
    ];
    expect(countedTurnStarts(messages)).toEqual([10, 20, 30, 40]);
  });

  it('skips report-backs, steering and queued messages, and the agent', () => {
    const messages = [
      ...turn(10, { provenance: { kind: 'task_report', taskId: 'a', displayId: 'Task-a', description: null, failed: false } }),
      ...turn(20, { provenance: { kind: 'analyst_report', threadId: 't', workspaceId: null, workspaceName: null } }),
      user(30, { steering: true }),
      user(40, { steeringDelivered: true }),
      user(50, { queued: true }),
      assistant(60),
    ];
    expect(countedTurnStarts(messages)).toEqual([]);
  });

  it('skips a send the server refused, and a steer it refused', () => {
    const messages = [
      user(10), // refused: its reply was removed
      ...turn(20),
      user(30, { queueError: 'Failed to send steering' }),
      user(40, { steeringDelivered: true }),
      assistant(41),
      user(50), // refused, last
    ];
    expect(countedTurnStarts(messages)).toEqual([20]);
  });

  it('skips a send that failed before any reply arrived', () => {
    const failed = { ...assistant(11), content: 'Failed to send message. Please try again.', error: true };
    const messages = [user(10), failed, user(20), failed, ...turn(30)];
    expect(countedTurnStarts(messages)).toEqual([30]);
  });
});

describe('staysOnRoster', () => {
  const starts = [100, 200, 300, 400, 500, 600];

  it('keeps an item while it runs, however long ago it last did anything', () => {
    expect(staysOnRoster(true, 0, starts)).toBe(true);
    expect(staysOnRoster(true, null, starts)).toBe(true);
  });

  it(`drops it once ${ROSTER_IDLE_TURNS} turns have started since its last activity`, () => {
    expect(staysOnRoster(false, 250, starts)).toBe(true); // 4 since
    expect(staysOnRoster(false, 50, starts)).toBe(false); // 6 since
    expect(staysOnRoster(false, 100, starts)).toBe(false); // 5 since: the turn at 100 did not start after it
  });

  it('shows an item with no known activity only while it runs', () => {
    expect(staysOnRoster(false, null, [])).toBe(false);
  });
});

describe('taskRoster', () => {
  const entry = (description: string, lastActivityMs: number, status = 'completed') => ({
    description, status, messages: [], lastActivityMs,
  });

  it('ages each task on its own clock', () => {
    const messages = turns(6);
    const rows = taskRoster({
      cards: {},
      history: { 'task:old': entry('Old', 50), 'task:recent': entry('Recent', 450) },
      messages,
      turnStarts: countedTurnStarts(messages),
      workRunning: false,
    });
    expect(rows.map((r) => r.id)).toEqual(['task:recent']);
  });

  it('takes the latest activity a live card saw, and its status over the page load', () => {
    const messages = turns(6);
    const rows = taskRoster({
      cards: { 'subagent-task:a': { subagentData: { status: 'error', lastActivityMs: 550, messages: [] } } },
      history: { 'task:a': entry('Pull filings', 50, 'running') },
      messages,
      turnStarts: countedTurnStarts(messages),
      workRunning: false,
    });
    expect(rows).toEqual([expect.objectContaining({ id: 'task:a', description: 'Pull filings', status: 'error', running: false })]);
  });

  it('labels a task its close created from its report', () => {
    const messages = [...turns(2), taskReport(250, 'a'), assistant(251)];
    const rows = taskRoster({
      cards: { 'subagent-task:a': { subagentData: { status: 'completed', lastActivityMs: 240, messages: [] } } },
      history: {},
      messages,
      turnStarts: countedTurnStarts(messages),
      workRunning: false,
    });
    expect(rows).toEqual([expect.objectContaining({ id: 'task:a', description: 'Report of a' })]);
  });

  it('keeps a task its report-back names, and adds one the page never loaded', () => {
    const messages = [...turns(6), taskReport(650, 'old'), assistant(651), taskReport(660, 'gone', true), assistant(661)];
    const rows = taskRoster({
      cards: {},
      history: { 'task:old': entry('Old', 50) },
      messages,
      turnStarts: countedTurnStarts(messages),
      workRunning: false,
    });
    expect(rows.map((r) => [r.id, r.status])).toEqual([['task:gone', 'error'], ['task:old', 'completed']]);
  });

  it('settles a task left open once nothing runs by its latest report', () => {
    const messages = [taskReport(300, 'a'), assistant(301), taskReport(400, 'a', true), assistant(401)];
    const args = {
      // Its close never reached this view, so the card still reads live.
      cards: { 'subagent-task:a': { subagentData: { status: 'active', description: 'Pull filings', messages: [] } } },
      history: {},
      messages,
      turnStarts: countedTurnStarts(messages),
    };
    expect(taskRoster({ ...args, workRunning: false }).map((r) => [r.id, r.status, r.running])).toEqual([['task:a', 'error', false]]);
    // While work can run, the card may be right: a newer run the reports predate.
    expect(taskRoster({ ...args, workRunning: true }).map((r) => [r.status, r.running])).toEqual([['active', true]]);
  });

  it('keeps the outcome of the latest report for a task known only from reports', () => {
    const messages = [taskReport(300, 'a'), assistant(301), taskReport(400, 'a', true), assistant(401)];
    const rows = taskRoster({ cards: {}, history: {}, messages, turnStarts: countedTurnStarts(messages), workRunning: false });
    expect(rows.map((r) => [r.id, r.status])).toEqual([['task:a', 'error']]);
  });

  it("leaves a workflow's own steps and the main agent off", () => {
    const rows = taskRoster({
      cards: {
        'subagent-main': { subagentData: { status: 'active', lastActivityMs: 10 } },
        'subagent-task:step': { subagentData: { status: 'completed', ownerTaskId: 'wf', lastActivityMs: 10 } },
        // Seeded on reload before the workflow replayed, so it has no owner yet.
        'subagent-task:step2': { subagentData: { status: 'active', lastActivityMs: 10 } },
      },
      history: { 'task:step2': { ...entry('Step', 10), ownerTaskId: 'wf' } },
      messages: [taskReport(20, 'step2')],
      turnStarts: [],
      workRunning: true,
    });
    expect(rows).toEqual([]);
  });

  it('calls a task running only while work can run, and lists running ones first', () => {
    const args = {
      cards: { 'subagent-task:live': { subagentData: { status: 'active', description: 'Live', messages: [] } } },
      history: { 'task:done': entry('Done', 90) },
      messages: [],
      turnStarts: [],
    };
    expect(taskRoster({ ...args, workRunning: true }).map((r) => [r.id, r.running])).toEqual([
      ['task:live', true],
      ['task:done', false],
    ]);
    // Its status reads live but nothing runs: it ended unseen, and with no
    // known activity it has no clock to stay by.
    expect(taskRoster({ ...args, workRunning: false }).map((r) => r.id)).toEqual(['task:done']);
  });
});

describe('analystCandidates', () => {
  const owed = { thread_id: 't1', workspace_id: 'w1', workspace_name: 'Semis', title: 'Chip demand' };

  it('merges a hand-off, its report and the owed list into one candidate per thread', () => {
    const messages = [
      assistant(100, { p: { status: 'approved', thread_id: 't1', workspace_id: 'w1', workspace_name: 'Semis', question: 'Size the market' } }),
      user(300, { provenance: { kind: 'analyst_report', threadId: 't1', workspaceId: 'w1', workspaceName: 'Semis' } }),
    ];
    expect(analystCandidates(messages, [owed])).toEqual([
      { threadId: 't1', workspaceId: 'w1', workspaceName: 'Semis', title: 'Chip demand', seenMs: 300, owed: true, dispatchFailed: false },
    ]);
  });

  it('skips a hand-off that never reached a thread', () => {
    const messages = [
      assistant(100, {
        failed: { status: 'approved', question: '', dispatch_failed: true },
        pending: { status: 'pending', question: '' },
      }),
    ];
    expect(analystCandidates(messages, null)).toEqual([]);
  });

  it('keeps a failed hand-off that names its thread, where a run may yet turn up', () => {
    const unknown = { status: 'approved', question: '', thread_id: 't2', dispatch_failed: true } as const;
    expect(analystCandidates([assistant(100, { p: unknown })], null)).toEqual([
      expect.objectContaining({ threadId: 't2', seenMs: 100, dispatchFailed: true }),
    ]);
    // A later hand-off to the same thread that went through clears it.
    const retried = [assistant(100, { p: unknown }), assistant(200, { q: { status: 'approved', question: '', thread_id: 't2' } })];
    expect(analystCandidates(retried, null)).toEqual([expect.objectContaining({ threadId: 't2', dispatchFailed: false })]);
  });
});
