// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { DriveRecordEvent, DriveRecordItem, DriveRecordVersion } from '@/types/drive';
import { changeStats, recordRows } from '../recordRows';

const actor = { kind: 'agent' as const, user_id: 'u1', agent: 'main', via: 'tool', thread_id: 't1', thread_title: null, run_id: null, workspace_id: null };

function v(seq: number, changeId: string | null, over: Partial<DriveRecordVersion> = {}): DriveRecordVersion {
  return {
    type: 'version', seq, size: 1, sha256: 'x', is_text: true, created_at: '2026-11-25T00:00:00Z', actor,
    note: changeId ? `why ${changeId}` : null, change_id: changeId, restored_from: null, expires_at: null,
    added: 1, removed: 0, text: 'text', ...over,
  };
}

const event: DriveRecordEvent = { type: 'event', id: 1, kind: 'mark', created_at: '2026-11-25T00:00:00Z', actor, note: null, status_after: null, tickers_after: [], final_seq: null };

const shape = (items: DriveRecordItem[]) => recordRows(items).map((r) => (
  r.kind === 'change' ? `c:${r.versions.map((x) => x.seq).join(',')}` : r.kind === 'version' ? `v:${r.version.seq}` : 'e'
));

describe('recordRows', () => {
  it('joins neighbouring versions of one change, newest first', () => {
    expect(shape([v(9, 'a'), v(8, 'a'), v(7, 'a'), v(6, null), v(5, 'b')])).toEqual(['c:9,8,7', 'v:6', 'v:5']);
  });

  it('splits a change where another version or an event sits between its versions', () => {
    expect(shape([v(9, 'a'), v(8, null), v(7, 'a')])).toEqual(['v:9', 'v:8', 'v:7']);
    expect(shape([v(9, 'a'), v(8, 'a'), event, v(7, 'a'), v(6, 'a')])).toEqual(['c:9,8', 'e', 'c:7,6']);
  });

  it('never joins versions made outside a change', () => {
    expect(shape([v(9, null), v(8, null)])).toEqual(['v:9', 'v:8']);
  });

  it('groups over the pages loaded so far, so a change straddling a boundary is one row', () => {
    const first = [v(9, 'a'), v(8, 'a')];
    const second = [v(7, 'a'), v(6, null)];
    expect(shape(first)).toEqual(['c:9,8']);
    expect(shape([...first, ...second])).toEqual(['c:9,8,7', 'v:6']);
  });
});

describe('changeStats', () => {
  it('sums the lines of every version', () => {
    expect(changeStats([v(2, 'a', { added: 3, removed: 1 }), v(1, 'a', { added: 2, removed: 5 })])).toEqual({ added: 5, removed: 6 });
  });

  it('gives no counts when a version has none, rather than a partial sum', () => {
    expect(changeStats([v(2, 'a'), v(1, 'a', { added: null, removed: null })])).toEqual({ added: null, removed: null });
  });
});
