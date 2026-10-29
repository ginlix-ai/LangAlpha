import { describe, expect, it } from 'vitest';
import type { SubagentTaskRecord } from '@/types/chat';
import type { SubagentTelemetry } from '../../../../session/subagents/resolveSubagentTelemetry';
import {
  describeMember,
  hasOwnRun,
  runElapsed,
  subagentRowKind,
  type SubagentRowView,
} from '../model';
import { refusalReason, resultLine, summarize } from '../text';

const task = (over: Partial<SubagentTaskRecord> = {}): SubagentTaskRecord => ({
  subagentId: 'a', description: 'd', prompt: '', type: 'research', action: 'init', status: 'running', ...over,
});
const tel = (over: Partial<SubagentTelemetry> = {}): SubagentTelemetry => ({
  toolCalls: 0, tokenUsage: { input: 0, output: 0, total: 0 }, ...over,
});
const LIVE_KINDS = new Set(['starting', 'working', 'pausing']);
const view = (over: Partial<SubagentRowView>): SubagentRowView => ({
  id: 'a', type: 'research', kind: 'done', rawStatus: 'completed', title: '', fullTitle: '', detail: '',
  live: LIVE_KINDS.has(over.kind ?? 'done'), ...over,
});
const t = (key: string) => key;

describe('describeMember', () => {
  it('reads a running task as working where telemetry does not follow runs', () => {
    expect(subagentRowKind(task(), undefined, false, false)).toBe('working');
    expect(describeMember({ subagentId: 'a', task: task() }, undefined, { pausing: false, followed: false, t })?.kind).toBe('working');
  });

  it('times no run for an update, which only speaks to a task', () => {
    const v = describeMember({ subagentId: 'u', task: task({ action: 'update' }) }, tel({ startedAtMs: 1, finishedAtMs: 9 }), { pausing: false, t });
    expect(v).toMatchObject({ kind: 'updated', live: false });
    expect(v?.startMs).toBeUndefined();
    expect(v && hasOwnRun([v])).toBe(false);
  });

  it('keeps a resume live while the run it started goes on', () => {
    const running = describeMember({ subagentId: 'r', task: task({ action: 'resume' }) }, tel({ startedAtMs: 5, running: true }), { pausing: false, t });
    expect(running).toMatchObject({ kind: 'resumed', live: true, startMs: 5 });
    const settled = describeMember({ subagentId: 'r', task: task({ action: 'resume' }) }, tel({ startedAtMs: 5, finishedAtMs: 9 }), { pausing: false, t });
    expect(settled).toMatchObject({ kind: 'resumed', live: false, endMs: 9 });
    expect(settled && hasOwnRun([settled])).toBe(true);
  });
});

describe('subagentRowKind', () => {
  it('starts a running task as starting, until the run shows a sign of life', () => {
    expect(subagentRowKind(task(), undefined, false)).toBe('starting');
    expect(subagentRowKind(task(), tel(), false)).toBe('starting');
    expect(subagentRowKind(task(), tel({ startedAtMs: 1 }), false)).toBe('working');
    expect(subagentRowKind(task(), tel({ tokenUsage: { input: 5, output: 1, total: 6 } }), false)).toBe('working');
    expect(subagentRowKind(task(), tel({ currentToolCall: { toolName: 'x' } }), false)).toBe('working');
  });

  it('reads a running task as pausing under a credit pause, begun or not', () => {
    expect(subagentRowKind(task(), undefined, true)).toBe('pausing');
  });

  it('maps settled states, and a refused launch before any verb', () => {
    expect(subagentRowKind(task({ status: 'completed' }), undefined, false)).toBe('done');
    expect(subagentRowKind(task({ status: 'cancelled' }), undefined, false)).toBe('stopped');
    expect(subagentRowKind(task({ status: 'error' }), undefined, false)).toBe('failed');
    expect(subagentRowKind(task({ status: 'error', launchFailed: true, action: 'update' }), undefined, false)).toBe('refused');
  });

  it('reads follow-ups by their verb, and an unknown verb or status as unknown', () => {
    expect(subagentRowKind(task({ action: 'update' }), undefined, false)).toBe('updated');
    expect(subagentRowKind(task({ action: 'resume' }), undefined, false)).toBe('resumed');
    expect(subagentRowKind(task({ action: 'steer' }), undefined, false)).toBe('unknown');
    expect(subagentRowKind(task({ status: 'queued' }), undefined, false)).toBe('unknown');
  });
});

describe('runElapsed', () => {
  it('spans first start to last finish once settled', () => {
    const views = [
      view({ startMs: 1_000, endMs: 61_000 }),
      view({ id: 'b', startMs: 11_000, endMs: 121_000 }),
    ];
    expect(runElapsed(views, 999_999)).toBe(120_000);
  });

  it('runs to now while live', () => {
    expect(runElapsed([view({ kind: 'working', startMs: 0 })], 100_000)).toBe(100_000);
  });

  it('has no elapsed without a start, nor once settled without every finish', () => {
    expect(runElapsed([view({})], 0)).toBeUndefined();
    expect(runElapsed([view({ startMs: 0 }), view({ id: 'b', startMs: 5, endMs: 10 })], 0)).toBeUndefined();
  });
});

describe('row text', () => {
  it("takes a result's first line that says something, as plain text", () => {
    expect(resultLine('# Report\n\n---\n\n> **Revenue** rose [8%](https://x.test).')).toBe('Revenue rose 8%.');
    expect(resultLine('| a | b |\n|---|---|\n- `AAPL` leads')).toBe('AAPL leads');
    // Only a matched pair is formatting.
    expect(resultLine('**Revenue rose 8%')).toBe('**Revenue rose 8%');
    expect(resultLine(undefined)).toBe('');
  });

  it('takes a title from the first line of a task, as plain text', () => {
    expect(summarize('## **Pull** the `10-Q`:\nThen compare.')).toBe('Pull the 10-Q');
  });

  it("drops the model's envelope from a refusal, and keeps one line", () => {
    expect(refusalReason('Refused: subagents are off.\nDo it yourself.')).toBe('subagents are off.');
    expect(refusalReason('Error:   boom')).toBe('boom');
    expect(refusalReason(undefined)).toBe('');
  });

  it('keeps only the first sentence, since the rest tells the model what to do next', () => {
    expect(refusalReason('Error: Task-3 is still running. Use action=\'update\' to send instructions.'))
      .toBe('Task-3 is still running.');
    expect(refusalReason('Error: could not start Task-3 \u2014 at the cap of 5 running subagents.'))
      .toBe('at the cap of 5 running subagents.');
  });
});
