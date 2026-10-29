/**
 * Each row of an activity block names the tool calls it draws, which is how a
 * card landing for one of them finds the row whose place it takes.
 */
import { describe, expect, it } from 'vitest';
import { layoutRows, layoutStatusLine, type LineStep } from '../activityLayout';
import type { ActivityItem, PreparingCall, ToolActivityItem } from '../activityTypes';

const keys = { forItem: (item: ActivityItem) => item.id, forCall: (call: PreparingCall) => call.pendingKey };

const tool = (id: string, running: boolean): ToolActivityItem => ({
  type: 'tool_call', id, toolCallId: id, toolName: 'bash',
  _liveState: running ? 'active' : 'completed', isComplete: !running,
});
const thought = (id: string, running: boolean): ActivityItem => ({
  type: 'reasoning', id, content: 'Thinking', _liveState: running ? 'active' : 'completed',
});
const preparing: PreparingCall = { pendingKey: 'p0', argsLength: 4 };
const lineOn = (item: ActivityItem): LineStep => ({ key: item.id, step: { kind: 'item', item }, running: true });

describe('the calls each row draws', () => {
  it('names a run by its calls, and a thought or a call still being written by none', () => {
    const { slot, rows } = layoutRows({
      timeline: [thought('t1', false), tool('b1', true)], calls: [preparing], keys, expanded: true, inline: false,
    });
    expect(rows.map((row) => [row.key, row.calls])).toEqual([['t1', []], ['b1', ['b1']], ['p0', []]]);
    expect(slot).toMatchObject({ kind: 'summary', calls: [] });
  });

  it('names every call of its block on the status line', () => {
    const timeline = [tool('b1', false), thought('t1', false), tool('b2', true)];
    const { slot, rows } = layoutStatusLine({
      timeline, calls: [], keys, expanded: false, liveReasoningOpen: false, line: lineOn(timeline[2]), up: true,
    });
    expect(rows.map((row) => [row.kind, row.calls])).toEqual([['line', ['b1', 'b2']]]);
    expect(slot).toMatchObject({ kind: 'summary', calls: [] });
  });

  it('names the call the block opened on while it runs in the summary place', () => {
    const call = tool('b1', true);
    const { slot, rows } = layoutStatusLine({
      timeline: [call], calls: [], keys, expanded: false, liveReasoningOpen: false, line: lineOn(call), up: true,
    });
    expect(rows).toEqual([]);
    expect(slot).toMatchObject({ kind: 'lead', isLine: true, calls: ['b1'] });

    const opener = thought('t1', true);
    const opened = layoutStatusLine({
      timeline: [opener], calls: [], keys, expanded: false, liveReasoningOpen: false, line: lineOn(opener), up: true,
    });
    expect(opened.slot).toMatchObject({ kind: 'lead', calls: [] });
  });
});
