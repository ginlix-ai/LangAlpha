import { act, waitFor } from '@testing-library/react';
import type { UseQueryResult } from '@tanstack/react-query';
import type { editor } from 'monaco-editor';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/api/client';
import { getEntry, saveFile } from '@/api/drive';
import { runAuthResets } from '@/lib/authResets';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { renderHookWithProviders } from '@/test/utils';
import type { DriveEntry, DriveMemoMark } from '@/types/drive';
import { useDriveEditor } from '../useDriveEditor';

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  saveFile: vi.fn(),
  getEntry: vi.fn(),
}));

const SHA = { base: 'a'.repeat(64), theirs: 'b'.repeat(64), saved: 'c'.repeat(64) };

function entryAt(sha: string, path = 'notes/q3.md'): DriveEntry {
  return {
    id: path === 'notes/q3.md' ? 'e1' : 'e2', path, name: path.split('/').pop()!, kind: 'file', size: 4, sha256: sha,
    content_type: 'text/markdown', is_text: true, version: 1,
    created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null, memo: null, undo_until: null,
  };
}

function headAt(sha: string, content: string): UseQueryResult<FileBody> {
  return { data: { content, from: 'live', version: sha } } as unknown as UseQueryResult<FileBody>;
}

function refused412() {
  return new ApiError('Request failed with status code 412', 'ERR_BAD_REQUEST', {
    data: { detail: { type: 'changed', message: 'The file changed.' } }, status: 412, statusText: '', headers: {},
  });
}

/**
 * The editor on one file, with the page's entry and head swapped in between
 * renders. The editor's model stands in as Monaco's does: a write to it
 * reports a change, as typing does.
 */
const MARK: DriveMemoMark = { marked: true, status: 'draft', tickers: [], final_seq: null, final_at: null };

function setup({ atHeadDocument = true, memo = false }: { atHeadDocument?: boolean; memo?: boolean } = {}) {
  const page = { path: 'notes/q3.md', entry: { ...entryAt(SHA.base), memo: memo ? MARK : null }, head: headAt(SHA.base, 'base') };
  const view = renderHookWithProviders(() => useDriveEditor('space-1', page.path, page.entry, page.head, atHeadDocument));
  let value = '';
  const model = {
    getValue: () => value,
    setValue: (next: string) => {
      value = next;
      view.result.current.change(next);
    },
  };
  const start = () => {
    act(() => view.result.current.start());
    value = view.result.current.session?.draft ?? '';
    view.result.current.editorRef.current = { getModel: () => model } as unknown as editor.IStandaloneCodeEditor;
  };
  const type = (text: string) => act(() => model.setValue(text));
  const moveHead = (sha: string, content: string) => {
    page.entry = entryAt(sha);
    page.head = headAt(sha, content);
    view.rerender();
  };
  const openFile = (path: string) => {
    page.path = path;
    page.entry = entryAt(SHA.base, path);
    view.rerender();
  };
  /** The mark taken off while the editor stays open. */
  const unmark = () => {
    page.entry = { ...page.entry, memo: { ...MARK, marked: false } };
    view.rerender();
  };
  return { ...view, start, type, moveHead, openFile, unmark };
}

beforeEach(() => {
  runAuthResets();
  vi.mocked(saveFile).mockReset();
  vi.mocked(getEntry).mockReset();
});

describe('useDriveEditor: a save that lands', () => {
  it('keeps the saved text while the page still reads the hash it wrote over', async () => {
    const { result, start, type } = setup();
    start();
    type('mine');
    vi.mocked(saveFile).mockResolvedValueOnce(entryAt(SHA.saved));
    await act(() => result.current.save());

    // The entry and head given to the hook still name the old hash: that is
    // the save catching up, not the file moving on, so nothing reverts.
    expect(result.current.behind).toBe(false);
    expect(result.current.theirs).toBeNull();
    expect(result.current.session).toMatchObject({ draft: 'mine', base: { sha: SHA.saved, content: 'mine' } });
  });
});

describe('useDriveEditor: what changed, on a memo', () => {
  it('sends the note with the save that carries it, then starts the next one empty', async () => {
    const { result, start, type } = setup({ memo: true });
    start();
    type('mine');
    act(() => result.current.setNote('  cut the risks section '));
    vi.mocked(saveFile).mockResolvedValueOnce(entryAt(SHA.saved));
    await act(() => result.current.save());
    expect(saveFile).toHaveBeenCalledWith('space-1', 'notes/q3.md', expect.any(Blob), expect.objectContaining({ note: 'cut the risks section' }));
    expect(result.current.session?.note).toBe('');

    type('mine, again');
    vi.mocked(saveFile).mockResolvedValueOnce(entryAt('d'.repeat(64)));
    await act(() => result.current.save());
    expect(vi.mocked(saveFile).mock.calls[1][3]).toMatchObject({ note: null });
  });

  it('never sends a note on a file that is no longer a memo', async () => {
    const { result, start, type, unmark } = setup({ memo: true });
    start();
    expect(result.current.asksWhy).toBe(true);
    type('mine');
    act(() => result.current.setNote('typed while it was a memo'));
    unmark();
    expect(result.current.asksWhy).toBe(false);
    vi.mocked(saveFile).mockResolvedValueOnce(entryAt(SHA.saved));
    await act(() => result.current.save());
    expect(vi.mocked(saveFile).mock.calls[0][3]).toMatchObject({ note: null });
  });

  it('belongs to its session: a new one starts without it', () => {
    const { result, start } = setup();
    start();
    act(() => result.current.setNote('draft note'));
    act(() => result.current.close());
    start();
    expect(result.current.session?.note).toBe('');
  });
});

describe('useDriveEditor: the file moving on under an open editor', () => {
  it('follows it while nothing is unsaved, and stays clean', () => {
    const { result, start, moveHead } = setup();
    start();
    moveHead(SHA.theirs, 'theirs');
    expect(result.current.session).toMatchObject({ draft: 'theirs', base: { sha: SHA.theirs, content: 'theirs' } });
    expect(result.current.dirty).toBe(false);
    expect(result.current.behind).toBe(false);
  });

  it('holds unsaved text and says the file moved on', () => {
    const { result, start, type, moveHead } = setup();
    start();
    type('mine');
    moveHead(SHA.theirs, 'theirs');
    expect(result.current.session).toMatchObject({ draft: 'mine', base: { sha: SHA.base } });
    expect(result.current.behind).toBe(true);
  });
});

describe('useDriveEditor: a save refused because the file moved on', () => {
  it('holds the draft in conflict until theirs arrives, then keeps mine over theirs', async () => {
    const { result, start, type, moveHead } = setup();
    start();
    type('mine');
    expect(result.current.dirty).toBe(true);

    vi.mocked(saveFile).mockRejectedValueOnce(refused412());
    vi.mocked(getEntry).mockResolvedValueOnce(entryAt(SHA.theirs));
    await act(() => result.current.save());

    // Refused: the draft stays, the save is off, and nothing is offered until
    // the head that refused it has loaded.
    expect(result.current.session).toMatchObject({ conflict: true, save: 'idle', draft: 'mine' });
    expect(result.current.theirs).toBeNull();

    moveHead(SHA.theirs, 'theirs');
    expect(result.current.theirs).toEqual({ sha: SHA.theirs, content: 'theirs' });

    vi.mocked(saveFile).mockResolvedValueOnce(entryAt(SHA.saved));
    act(() => result.current.keepMine());
    await waitFor(() => expect(result.current.session?.save).toBe('idle'));

    // Mine went over theirs, matched on their hash, and the session sits on what was saved.
    expect(vi.mocked(saveFile).mock.calls.at(-1)?.[3]).toMatchObject({ ifMatch: SHA.theirs });
    expect(result.current.session).toMatchObject({ conflict: false, draft: 'mine', base: { sha: SHA.saved, content: 'mine' } });
    expect(result.current.dirty).toBe(false);
  });

  it('takes theirs in place of the draft', async () => {
    const { result, start, type, moveHead } = setup();
    start();
    type('mine');
    vi.mocked(saveFile).mockRejectedValueOnce(refused412());
    vi.mocked(getEntry).mockResolvedValueOnce(entryAt(SHA.theirs));
    await act(() => result.current.save());
    moveHead(SHA.theirs, 'theirs');

    act(() => result.current.takeTheirs());
    expect(result.current.session).toMatchObject({ conflict: false, draft: 'theirs', base: { sha: SHA.theirs } });
    expect(result.current.dirty).toBe(false);
    expect(saveFile).toHaveBeenCalledTimes(1);
  });

  it('drops the note written for the edits theirs replaced', async () => {
    const { result, start, type, moveHead } = setup();
    start();
    type('mine');
    act(() => result.current.setNote('cut the risks section'));
    vi.mocked(saveFile).mockRejectedValueOnce(refused412());
    vi.mocked(getEntry).mockResolvedValueOnce(entryAt(SHA.theirs));
    await act(() => result.current.save());
    moveHead(SHA.theirs, 'theirs');
    act(() => result.current.takeTheirs());
    expect(result.current.session?.note).toBe('');
  });

  it('says the file is gone when the refusal finds no text file left', async () => {
    const { result, start, type } = setup();
    start();
    type('mine');
    vi.mocked(saveFile).mockRejectedValueOnce(refused412());
    vi.mocked(getEntry).mockRejectedValueOnce(new ApiError('Request failed with status code 404', 'ERR_BAD_REQUEST', {
      data: { detail: { type: 'not_found', message: 'gone' } }, status: 404, statusText: '', headers: {},
    }));
    await act(() => result.current.save());
    expect(result.current.session).toMatchObject({ conflict: false, save: 'idle', draft: 'mine' });
  });
});

/** A save held open until the test lets it land. */
function deferredSave() {
  let land!: (entry: DriveEntry) => void;
  vi.mocked(saveFile).mockReturnValueOnce(new Promise((resolve) => { land = resolve; }));
  return (sha: string, path?: string) => land(entryAt(sha, path));
}

describe('useDriveEditor: a save that lands after its session ended', () => {
  it('leaves the editor reopened on the same file alone', async () => {
    const { result, start, type } = setup();
    start();
    type('first draft');
    const land = deferredSave();
    let pending!: Promise<void>;
    act(() => { pending = result.current.save(); });
    expect(result.current.session?.save).toBe('saving');

    act(() => result.current.close());
    start();
    const reopened = result.current.session;
    expect(reopened).toMatchObject({ save: 'idle', base: { sha: SHA.base, content: 'base' } });

    await act(async () => {
      land(SHA.saved);
      await pending;
    });
    // The new sitting neither took the old text as its base nor marked itself saved.
    expect(result.current.session).toEqual(reopened);
  });

  it('leaves the editor on another file alone', async () => {
    const { result, start, type, openFile } = setup();
    start();
    type('first draft');
    const land = deferredSave();
    let pending!: Promise<void>;
    act(() => { pending = result.current.save(); });

    openFile('notes/other.md');
    start();
    const other = result.current.session;
    expect(other).toMatchObject({ path: 'notes/other.md', save: 'idle' });

    await act(async () => {
      land(SHA.saved);
      await pending;
    });
    expect(result.current.session).toEqual(other);
  });

  it('leaves the reopened editor alone when the late save is refused', async () => {
    const { result, start, type } = setup();
    start();
    type('first draft');
    let refuse!: (err: unknown) => void;
    vi.mocked(saveFile).mockReturnValueOnce(new Promise((_resolve, reject) => { refuse = reject; }));
    vi.mocked(getEntry).mockResolvedValueOnce(entryAt(SHA.theirs));
    let pending!: Promise<void>;
    act(() => { pending = result.current.save(); });
    act(() => result.current.close());
    start();
    const reopened = result.current.session;

    await act(async () => {
      refuse(refused412());
      await pending;
    });
    expect(result.current.session).toEqual(reopened);
    expect(result.current.session?.conflict).toBe(false);
  });
});

describe('useDriveEditor: unsaved text kept while the person is away', () => {
  /** Leaves the route with the editor dirty: the page unmounts, nothing is discarded. */
  function leaveDirty() {
    const view = setup();
    view.start();
    view.type('mine');
    view.unmount();
  }

  it('opens the file in the editor with the draft, dirty, on the way back', () => {
    leaveDirty();
    const { result } = setup();
    expect(result.current.session).toMatchObject({ draft: 'mine', base: { sha: SHA.base, content: 'base' } });
    expect(result.current.dirty).toBe(true);
  });

  it('keeps the note typed beside the draft, and sends it with the save', async () => {
    const away = setup({ memo: true });
    away.start();
    away.type('mine');
    act(() => away.result.current.setNote('trimmed the valuation'));
    away.unmount();
    const { result } = setup({ memo: true });
    expect(result.current.session?.note).toBe('trimmed the valuation');
    vi.mocked(saveFile).mockResolvedValueOnce(entryAt(SHA.saved));
    await act(() => result.current.save());
    expect(vi.mocked(saveFile).mock.calls.at(-1)?.[3]).toMatchObject({ note: 'trimmed the valuation' });
  });

  it('saves the draft over the content it was started from, even after the file moved on', async () => {
    leaveDirty();
    const { result, moveHead } = setup();
    moveHead(SHA.theirs, 'theirs');
    // Not rebased: the person still sees their text, and the pane says the file moved on.
    expect(result.current.session).toMatchObject({ draft: 'mine', base: { sha: SHA.base } });
    expect(result.current.behind).toBe(true);
    vi.mocked(saveFile).mockRejectedValueOnce(refused412());
    vi.mocked(getEntry).mockResolvedValueOnce(entryAt(SHA.theirs));
    await act(() => result.current.save());
    expect(vi.mocked(saveFile).mock.calls.at(-1)?.[3]).toMatchObject({ ifMatch: SHA.base });
    expect(result.current.session?.conflict).toBe(true);
  });

  it('is gone once saved', async () => {
    leaveDirty();
    const view = setup();
    expect(view.result.current.dirty).toBe(true);
    vi.mocked(saveFile).mockResolvedValueOnce(entryAt(SHA.saved));
    await act(() => view.result.current.save());
    expect(view.result.current.dirty).toBe(false);
    view.unmount();
    expect(setup().result.current.session).toBeNull();
  });

  it('is gone once discarded', () => {
    leaveDirty();
    const view = setup();
    expect(view.result.current.dirty).toBe(true);
    act(() => view.result.current.close());
    view.unmount();
    expect(setup().result.current.session).toBeNull();
  });

  it('is gone after a sign-out', () => {
    leaveDirty();
    const kept = setup();
    expect(kept.result.current.dirty).toBe(true);
    kept.unmount();
    runAuthResets();
    expect(setup().result.current.session).toBeNull();
  });

  it('opens only over the head\'s document: never a past version, a record or a comparison', () => {
    leaveDirty();
    expect(setup({ atHeadDocument: false }).result.current.session).toBeNull();
  });
});
