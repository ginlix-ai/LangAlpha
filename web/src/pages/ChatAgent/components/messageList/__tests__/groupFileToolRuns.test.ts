import { describe, expect, it } from 'vitest';
import { groupFileToolRuns } from '../groupFileToolRuns';
import type { ToolActivityItem } from '../activityTypes';

const read = (id: string, path: string, state: ToolActivityItem['_liveState'] = 'completed'): ToolActivityItem => ({
  type: 'tool_call', id, toolCallId: id, toolName: 'Read', _liveState: state,
  toolCall: { args: { file_path: path } },
});

/** What a settled failure looks like: `buildRenderBlocks` gives it the same
 *  `completed` state as a success and reports the failure on `isFailed`. */
const failedRead = (id: string, path: string): ToolActivityItem => ({ ...read(id, path), isFailed: true });

describe('file tool grouping', () => {
  it('groups adjacent ordinary settled reads', () => {
    const items = [read('a', 'data/a.csv'), read('b', 'data/b.csv')];
    expect(groupFileToolRuns(items)).toEqual([items]);
  });

  it.each(['.agents/user/memory/note.md', '.agents/user/memo/note.md', '.agents/user/profile/portfolio.json', '.agents/user/automations/morning-brief.json', '.agents/skills/research/SKILL.md'])('keeps %s distinct from other reads', (path) => {
    const items = [read('a', path), read('b', path), read('c', 'data/a.csv')];
    expect(groupFileToolRuns(items)).toEqual(items.map((item) => [item]));
  });

  it('leaves a running read visible until it settles', () => {
    const items = [read('a', 'data/a.csv'), read('b', 'data/b.csv', 'active')];
    expect(groupFileToolRuns(items)).toEqual(items.map((item) => [item]));
  });

  it('keeps a failed read out of the group of reads that worked', () => {
    // A settled failure carries `_liveState: 'completed'` like any other, and
    // `FileToolGroupRow` badges a whole group from `items.some(isFailed)`. So
    // grouping them put a failure badge and a count of three on two files that
    // were read fine.
    const items = [read('a', 'data/a.csv'), failedRead('b', 'data/missing.csv'), read('c', 'data/c.csv')];
    expect(groupFileToolRuns(items)).toEqual(items.map((item) => [item]));
  });

  it('keeps reads on another computer out of a run of reads here', () => {
    // A pill opens its file on this computer, and a call naming a computer
    // read that one's: a shared run would give both kinds one behavior.
    const there = (id: string, path: string): ToolActivityItem => ({
      ...read(id, path), toolCall: { args: { file_path: path, computer: 'Studio Mac' } },
    });
    const items = [read('a', 'data/a.csv'), there('b', 'data/b.csv'), there('c', 'data/c.csv')];
    expect(groupFileToolRuns(items)).toEqual([[items[0]], [items[1], items[2]]]);
  });

  it('keeps reads on two other computers in two runs', () => {
    // One path on two machines is two files, which one run would show as one
    // pill opening the first call.
    const on = (id: string, computer: string): ToolActivityItem => ({
      ...read(id, 'data/q3.csv'), toolCall: { args: { file_path: 'data/q3.csv', computer } },
    });
    const items = [on('a', 'Studio Mac'), on('b', 'studio  mac'), on('c', 'Lab box')];
    expect(groupFileToolRuns(items)).toEqual([[items[0], items[1]], [items[2]]]);
  });

  it('groups consecutive failures with each other, not with the successes', () => {
    const ok = read('a', 'data/a.csv');
    const bad1 = failedRead('b', 'data/x.csv');
    const bad2 = failedRead('c', 'data/y.csv');
    expect(groupFileToolRuns([ok, bad1, bad2])).toEqual([[ok], [bad1, bad2]]);
  });
});
