import { describe, expect, it } from 'vitest';
import { driveHref, readDriveLocation, withFile } from '@/lib/driveLinks';
import type { DriveEntry, DriveMemoMark } from '@/types/drive';
import { authorRuns, memoState, parseTickers, resolveFileView, revisedSince, undoDeadline, type FileAddress } from '../memoMarks';

describe('memo addresses', () => {
  it('round-trips a memo view, its version and what it is compared with', () => {
    const href = driveHref({ file: 'm/aapl.md', tab: 'changes', version: 7, against: 3 });
    expect(href).toBe('/drive?file=m%2Faapl.md&tab=changes&v=7&vs=3');
    expect(readDriveLocation(new URL(href, 'http://x').searchParams)).toMatchObject({
      file: 'm/aapl.md', tab: 'changes', version: 7, against: 3, memos: false,
    });
  });

  it('keeps the memo list beside an open file', () => {
    expect(driveHref({ memos: true, file: 'a.md', tab: 'record' })).toBe('/drive?memos=1&file=a.md&tab=record');
    expect(driveHref({ memos: true, dir: 'x' })).toBe('/drive?memos=1');
  });

  it('reads an old history link as the record, and an unknown tab and a bad version as nothing', () => {
    expect(readDriveLocation(new URLSearchParams('file=a.md&history=1&v=-2&vs=x'))).toMatchObject({ tab: 'record', version: null, against: null });
    expect(readDriveLocation(new URLSearchParams('file=a.md&tab=nope'))).toMatchObject({ tab: null });
    // An old link is read, never written back.
    expect(driveHref({ file: 'a.md' })).toBe('/drive?file=a.md');
  });

  it('carries the view, version and comparison with a file that moved', () => {
    const loc = readDriveLocation(new URLSearchParams('memos=1&file=a.md&tab=changes&v=3&vs=1'));
    expect(withFile(loc, 'b/a.md')).toEqual({ file: 'b/a.md', tab: 'changes', version: 3, against: 1 });
  });
});

const mark = (over: Partial<DriveMemoMark> = {}): DriveMemoMark => ({ marked: true, status: 'draft', tickers: [], final_seq: null, final_at: null, ...over });
const file = (memo: DriveMemoMark | null, version = 5) => ({ kind: 'file', version, memo, undo_until: null } as DriveEntry);
const at = (over: Partial<FileAddress> = {}): FileAddress => ({ tab: null, version: null, against: null, ...over });

describe('memoState', () => {
  it('tells a memo, a former memo and a plain file apart', () => {
    expect(memoState(file(mark())).kind).toBe('memo');
    expect(memoState(file(mark({ marked: false }))).kind).toBe('former');
    expect(memoState(file(null))).toEqual({ kind: 'plain' });
    expect(memoState(undefined)).toEqual({ kind: 'plain' });
  });
});

describe('resolveFileView', () => {
  it('gives a memo its four views, a former memo its record, a plain file its document', () => {
    expect(resolveFileView(file(mark()), at({ tab: 'who' }), false).view).toBe('who');
    expect(resolveFileView(file(mark({ marked: false })), at({ tab: 'record' }), false).view).toBe('record');
    expect(resolveFileView(file(mark({ marked: false })), at({ tab: 'changes' }), false).view).toBe('document');
    expect(resolveFileView(file(null), at({ tab: 'record', version: 2 }), false)).toEqual({ view: 'document', version: null, against: null });
  });

  it('opens an old history link on a former memo at its record', () => {
    const loc = readDriveLocation(new URLSearchParams('file=a.md&history=1'));
    expect(resolveFileView(file(mark({ marked: false })), loc, false).view).toBe('record');
  });

  it('reads the head named as a version as the head, and a comparison with the shown version as none', () => {
    expect(resolveFileView(file(mark()), at({ version: 5 }), false).version).toBeNull();
    expect(resolveFileView(file(mark()), at({ tab: 'changes', against: 5 }), false).against).toBeNull();
    expect(resolveFileView(file(mark()), at({ tab: 'changes', version: 3, against: 3 }), false)).toEqual({ view: 'changes', version: 3, against: null });
    expect(resolveFileView(file(mark()), at({ tab: 'changes', version: 3, against: 1 }), false)).toEqual({ view: 'changes', version: 3, against: 1 });
  });

  it('compares with the version before once the version named is no longer kept', () => {
    expect(resolveFileView(file(mark()), at({ tab: 'changes', version: 3, against: 9 }), false, [5, 4, 3, 2])).toEqual({ view: 'changes', version: 3, against: null });
    expect(resolveFileView(file(mark()), at({ tab: 'changes', version: 3, against: 2 }), false, [5, 4, 3, 2]).against).toBe(2);
    // Before the versions arrive, the address is taken as it is.
    expect(resolveFileView(file(mark()), at({ tab: 'changes', version: 3, against: 9 }), false, null).against).toBe(9);
  });

  it('is the document while an editor is open, and keeps a comparison only in Changes', () => {
    expect(resolveFileView(file(mark()), at({ tab: 'record' }), true).view).toBe('document');
    expect(resolveFileView(file(mark()), at({ tab: 'record', version: 3, against: 1 }), false)).toEqual({ view: 'record', version: null, against: null });
  });
});

describe('revisedSince and undoDeadline', () => {
  it('names the pinned version only once the memo moved past it', () => {
    expect(revisedSince(mark({ status: 'final', final_seq: 3 }), 5)).toBe(3);
    expect(revisedSince(mark({ status: 'final', final_seq: 5 }), 5)).toBeNull();
    expect(revisedSince(mark({ status: 'draft', final_seq: null }), 5)).toBeNull();
  });

  it('offers Undo until its time, as a time', () => {
    const until = '2026-11-25T12:00:00Z';
    const now = Date.parse('2026-11-25T11:00:00Z');
    expect(undoDeadline({ kind: 'file', undo_until: until }, now)).toBe(Date.parse(until));
    expect(undoDeadline({ kind: 'file', undo_until: until }, Date.parse(until) + 1)).toBeNull();
    expect(undoDeadline({ kind: 'folder', undo_until: until }, now)).toBeNull();
  });
});

describe('tickers', () => {
  it('reads tickers as the server keeps them: upper case, each once, in order', () => {
    expect(parseTickers(' aapl, msft  AAPL,brk.b ,')).toEqual({ tickers: ['AAPL', 'MSFT', 'BRK.B'], bad: [], tooMany: false });
  });

  it('names what the server would refuse', () => {
    expect(parseTickers('AAPL $TSLA').bad).toEqual(['$TSLA']);
    expect(parseTickers(Array.from({ length: 21 }, (_, i) => `T${i}`).join(',')).tooMany).toBe(true);
  });
});

describe('authorRuns', () => {
  it('cuts the text into the runs named, in line order, with the version that wrote each', () => {
    const v = (seq: number) => ({ seq } as never);
    const runs = authorRuns('a\nb\nc\nd\n', { runs: [{ start: 3, count: 2, seq: 7 }, { start: 1, count: 2, seq: 2 }], versions: [v(2), v(7)] });
    expect(runs.map((r) => [r.seq, r.lines])).toEqual([[2, ['a', 'b']], [7, ['c', 'd']]]);
    expect(runs[1].version).toEqual({ seq: 7 });
  });
});
