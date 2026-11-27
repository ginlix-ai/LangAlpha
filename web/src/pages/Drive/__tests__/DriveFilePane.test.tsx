import type React from 'react';
import { fireEvent, screen } from '@testing-library/react';
import { QueryClient, type UseQueryResult } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import { useDriveVersions } from '@/hooks/useDrive';
import { queryKeys } from '@/lib/queryKeys';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { LIVE } from '@/pages/ChatAgent/utils/api/readFrom';
import { renderWithProviders } from '@/test/utils';
import type { MemoTab } from '@/lib/driveLinks';
import type { DriveEntry, DriveVersion } from '@/types/drive';
import { getRecord } from '@/api/drive';
import { DriveFilePane } from '../components/DriveFilePane';
import { memoState, resolveFileView, type FileAddress } from '../memoMarks';
import { driveBodyKey } from '../driveBody';
import { DriveContext, type DriveContextValue } from '../driveContext';
import type { DriveActions } from '../useDriveActions';
import type { DriveEditor } from '../useDriveEditor';

// Monaco and the viewers are their own concern; the pane decides which one
// shows and with what, so each stands in as what it was handed.
vi.mock('@/pages/ChatAgent/components/viewers/CodeEditor', () => ({
  default: (p: { value: string; diffMode?: boolean; originalValue?: string }) => (
    <pre data-testid={p.diffMode ? 'diff' : 'editor'}>{p.diffMode ? `${p.originalValue} | ${p.value}` : p.value}</pre>
  ),
}));
vi.mock('@/pages/ChatAgent/components/filePanel/FileViewer', () => ({
  FileViewer: (p: { body: FileBody | null; error?: { category: string } | null }) => (
    <div data-testid="viewer">{p.body?.content ?? p.error?.category}</div>
  ),
}));

vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  listVersions: vi.fn(() => Promise.reject(new Error('Network Error'))),
  getRecord: vi.fn(() => Promise.resolve({ items: [], next: null })),
}));

const SPACE = 'space-1';
const SHA = { v1: '1'.repeat(64), v2: '2'.repeat(64), theirs: '3'.repeat(64) };
const agent = { kind: 'agent' as const, user_id: 'u', agent: 'main', via: 'tool', thread_id: null, thread_title: null };

const plain: DriveEntry = {
  id: 'e1', path: 'notes/q3.md', name: 'q3.md', kind: 'file', size: 8, sha256: SHA.v2,
  content_type: 'text/markdown', is_text: true, version: 2,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-20T00:00:00Z', modified_by: agent, deleted_at: null, memo: null, undo_until: null,
};
const memo: DriveEntry = { ...plain, memo: { marked: true, status: 'draft', tickers: ['AAPL'], final_seq: null, final_at: null } };
const entry = plain;

const versions: DriveVersion[] = [2, 1].map((seq) => ({
  seq, size: 8, sha256: seq === 2 ? SHA.v2 : SHA.v1, created_at: '2026-11-20T00:00:00Z',
  actor: { ...agent, run_id: null, workspace_id: null }, note: null, change_id: null, restored_from: null, is_text: true, expires_at: null, text: 'text',
}));

function body(content: string, version: string): FileBody {
  return { mode: 'text', mime: 'text/markdown', content, buffer: null, truncated: false, from: LIVE, version };
}

function headOf(content: string, version: string): UseQueryResult<FileBody> {
  return { data: body(content, version), isPending: false, error: null } as unknown as UseQueryResult<FileBody>;
}

const viewing = {
  session: null, dirty: false, theirs: null, behind: false, editorRef: { current: null },
  start: vi.fn(), change: vi.fn(), save: vi.fn(), keepMine: vi.fn(), takeTheirs: vi.fn(), close: vi.fn(), review: vi.fn(),
} as unknown as DriveEditor;

function inConflict(theirs: { sha: string; content: string } | null): DriveEditor {
  return {
    ...viewing,
    session: {
      mode: 'editing', path: entry.path, sitting: 'web-1', modelPath: 'm-1', base: { sha: SHA.v2, content: 'v2 text' },
      replaced: null, draft: 'mine', note: '', save: 'idle', savedAt: null, conflict: true,
    },
    dirty: true,
    theirs,
    keepMine: vi.fn(),
    takeTheirs: vi.fn(),
  } as unknown as DriveEditor;
}

/** The pane as the page hands it the file: its versions read and the address resolved against them. */
function Pane({ file, at, editor, ...rest }: Omit<React.ComponentProps<typeof DriveFilePane>, 'path' | 'entry' | 'entryError' | 'onRetry' | 'fileView' | 'versions'> & {
  file: DriveEntry;
  at: FileAddress;
}) {
  const versions = useDriveVersions(SPACE, file.id, memoState(file).kind !== 'plain');
  const fileView = resolveFileView(file, at, !!editor.session, versions.data?.versions.map((v) => v.seq));
  return <DriveFilePane path={file.path} entry={file} entryError={null} onRetry={vi.fn()} fileView={fileView} versions={versions} editor={editor} {...rest} />;
}

function renderPane(
  editor: DriveEditor,
  version: number | null = null,
  { list = versions, mobile = false, tab = null, file = memo, head = headOf('v2 text', SHA.v2) }: {
    /** null: no listing cached, so it is read, and the read fails. */
    list?: DriveVersion[] | null; mobile?: boolean; tab?: MemoTab | null; file?: DriveEntry; head?: UseQueryResult<FileBody>;
  } = {},
) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  if (list) queryClient.setQueryData(queryKeys.drive.versions(SPACE, entry.id), { versions: list, history_bytes: 0, too_large_for_history: false, clearable: 0 });
  queryClient.setQueryData(driveBodyKey(SPACE, entry, SHA.v1), body('v1 text', SHA.v1));
  const actions = {
    pending: { restore: null, purge: null, restoreVersion: false, setMemo: false, undoSave: false, explain: false },
    restoreVersion: vi.fn(), setMemo: vi.fn(), undoSave: vi.fn(),
  } as unknown as DriveActions;
  const go = vi.fn();
  const ctx: DriveContextValue = {
    space: SPACE, actions, go, open: vi.fn(), trash: vi.fn(), leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const,
    ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
  };
  renderWithProviders(
    <DriveContext value={ctx}>
      <Pane
        file={file}
        at={{ version, tab, against: null }}
        mobile={mobile}
        editor={editor}
        head={head}
        imageLoader={() => Promise.resolve('')}
        onOpenLink={vi.fn()}
      />
    </DriveContext>,
    { queryClient },
  );
  return { go, actions, ctx };
}

describe('DriveFilePane', () => {
  it('says a version the URL names is no longer kept, rather than showing the current file under it', () => {
    const { go } = renderPane(viewing, 5);
    expect(screen.queryByText('v2 text')).toBeNull();
    expect(screen.getByText('It is no longer there. It may have been moved or deleted.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Back to current' }));
    expect(go).toHaveBeenCalledWith(expect.objectContaining({ file: entry.path, version: null }));
  });

  it('says the versions could not be read, with a retry, rather than showing the current file', async () => {
    renderPane(viewing, 1, { list: null });
    expect(await screen.findByText('Could not load this. Check your connection and try again.')).toBeInTheDocument();
    expect(screen.queryByText('v2 text')).toBeNull();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
  });

  it('offers the download card for a body read as none (a large PDF that could not be read by range)', () => {
    const none = { data: { ...body('', SHA.v2), mode: 'none', content: null }, isPending: false, error: null } as unknown as UseQueryResult<FileBody>;
    renderPane(viewing, null, { head: none });
    expect(screen.getByTestId('viewer')).toHaveTextContent('too_large');
  });

  it('shows an ordinary file at its head with Edit offered, and no history or memo views', () => {
    renderPane(viewing, null, { file: plain });
    expect(screen.getByTestId('viewer')).toHaveTextContent('v2 text');
    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'History' })).toBeNull();
    expect(screen.queryByRole('tablist')).toBeNull();
    expect(screen.queryByText('Memo')).toBeNull();
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('opens the record tab on an ordinary file (an old history link) as the file itself', () => {
    renderPane(viewing, null, { file: plain, tab: 'record' });
    expect(screen.getByTestId('viewer')).toHaveTextContent('v2 text');
  });

  it("opens a memo's record tab", async () => {
    renderPane(viewing, null, { tab: 'record' });
    expect(await screen.findByRole('list', { name: 'Record' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Record' })).toHaveAttribute('aria-selected', 'true');
  });

  it("shows a memo's past version in place of the head, with restore and the way back", () => {
    const { go, actions } = renderPane(viewing, 1);
    expect(screen.getByText(/^Version 1,/)).toBeInTheDocument();
    expect(screen.getByTestId('viewer')).toHaveTextContent('v1 text');
    expect(screen.queryByRole('button', { name: 'Edit' })).toBeNull();
    expect(screen.queryByRole('button', { name: /Compare with/ })).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Restore' }));
    expect(actions.restoreVersion).toHaveBeenCalledWith(memo, 1);
    fireEvent.click(screen.getByRole('button', { name: 'Back to current' }));
    expect(go).toHaveBeenCalledWith({ file: entry.path, tab: null, version: null, against: null });
  });

  it("carries a memo's pill, status, tickers and views, and goes to a view by the URL", () => {
    const { go, ctx } = renderPane(viewing);
    expect(screen.getByText('Memo')).toBeInTheDocument();
    expect(screen.getByText('Draft')).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Document' })).toHaveAttribute('aria-selected', 'true');
    fireEvent.click(screen.getByRole('tab', { name: 'Record' }));
    expect(go).toHaveBeenCalledWith({ file: entry.path, tab: 'record', version: null, against: null });
    fireEvent.click(screen.getByRole('button', { name: 'Status and tickers' }));
    expect(ctx.ask.memo).toHaveBeenCalledWith(memo, 'edit');
  });

  it('offers who wrote what only where the server reads the head as its own text', () => {
    renderPane(viewing);
    expect(screen.getByRole('tab', { name: 'Who wrote what' })).toBeEnabled();
  });

  it('offers no who wrote what on a memo read by extraction, whatever its bytes look like', () => {
    renderPane(viewing, null, { list: versions.map((v) => ({ ...v, text: 'extracted' as const })) });
    expect(screen.getByRole('tab', { name: 'Who wrote what' })).toBeDisabled();
  });

  it('says a final memo was revised since, with the way to the final and to make the newest final', () => {
    const final = { ...memo, memo: { marked: true, status: 'final' as const, tickers: [], final_seq: 1, final_at: '2026-11-24T12:00:00Z' } };
    const { go, actions } = renderPane(viewing, null, { file: final });
    expect(screen.getByText('Final v1 · Nov 24')).toBeInTheDocument();
    expect(screen.getByText('revised since')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'View the final' }));
    expect(go).toHaveBeenCalledWith({ file: entry.path, tab: null, version: 1, against: null });
    fireEvent.click(screen.getByRole('button', { name: 'Make v2 final' }));
    expect(actions.setMemo).toHaveBeenCalledWith(final, { status: 'final', ifMatch: final.sha256 });
  });

  it("reads an address naming the head's own version as the head", () => {
    renderPane(viewing, memo.version);
    expect(screen.queryByRole('button', { name: /restore/i })).toBeNull();
    expect(screen.getByTestId('viewer')).toHaveTextContent('v2 text');
  });

  it('never names the head as a version, nor a version compared with itself, when it moves', () => {
    const { go } = renderPane(viewing, null, { tab: 'record' });
    fireEvent.click(screen.getByRole('tab', { name: 'Changes' }));
    expect(go).toHaveBeenLastCalledWith({ file: memo.path, tab: 'changes', version: null, against: null });
  });

  it('says nothing more of a final memo whose head is the final', () => {
    const final = { ...memo, memo: { marked: true, status: 'final' as const, tickers: [], final_seq: 2, final_at: null } };
    renderPane(viewing, null, { file: final });
    expect(screen.getByText('Final v2')).toBeInTheDocument();
    expect(screen.queryByText('revised since')).toBeNull();
  });

  it("offers to undo an ordinary file's last save while its copy is kept, naming who saved it and where", async () => {
    const saved = {
      ...plain,
      modified_at: new Date(Date.now() - 120_000).toISOString(),
      modified_by: { ...agent, thread_id: 't1', thread_title: 'Q3 research' },
      undo_until: new Date(Date.now() + 6.5 * 86_400_000).toISOString(),
    };
    const { actions } = renderPane(viewing, null, { file: saved });
    const line = screen.getByRole('status');
    expect(line).toHaveTextContent('Saved by Agent 2m ago in');
    expect(screen.getByRole('link', { name: 'Q3 research' })).toHaveAttribute('href', '/chat/t/t1');
    expect(line).toHaveTextContent('The copy before is kept 7 more days');
    fireEvent.click(screen.getByRole('button', { name: 'Undo this save' }));
    expect(actions.undoSave).toHaveBeenCalledWith(saved);
  });

  it('offers no undo on a memo', () => {
    const later = new Date(Date.now() + 86_400_000).toISOString();
    renderPane(viewing, null, { file: { ...memo, undo_until: later } });
    expect(screen.queryByRole('button', { name: 'Undo this save' })).toBeNull();
  });

  it('offers no undo once the kept copy has gone', () => {
    renderPane(viewing, null, { file: { ...plain, undo_until: new Date(Date.now() - 1000).toISOString() } });
    expect(screen.queryByRole('button', { name: 'Undo this save' })).toBeNull();
  });

  it('offers the way out of a conflict once theirs has arrived, and compares the two', async () => {
    const editor = inConflict({ sha: SHA.theirs, content: 'theirs' });
    renderPane(editor);
    const bar = screen.getByRole('alert');
    expect(bar).toHaveTextContent('Not saved.');
    expect(bar).toHaveTextContent('Agent changed this file');
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
    expect(await screen.findByTestId('editor')).toHaveTextContent('mine');

    fireEvent.click(screen.getByRole('button', { name: 'Compare' }));
    expect(await screen.findByTestId('diff')).toHaveTextContent('theirs | mine');

    fireEvent.click(screen.getByRole('button', { name: 'Keep mine' }));
    expect(editor.keepMine).toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Take theirs' }));
    expect(editor.takeTheirs).toHaveBeenCalled();
  });

  it('asks what changed beside Save where the editor says a save asks', () => {
    const base = inConflict(null);
    const editor = { ...base, asksWhy: true, session: { ...base.session!, note: 'trimmed the risks' }, setNote: vi.fn() } as unknown as DriveEditor;
    renderPane(editor);
    const field = screen.getByRole('textbox', { name: 'What changed?' });
    expect(field).toHaveValue('trimmed the risks');
    fireEvent.change(field, { target: { value: 'cut Risks' } });
    expect(editor.setNote).toHaveBeenCalledWith('cut Risks');
  });

  it('saves on Enter in the note, but not on the Enter that ends an input method composition', () => {
    const base = inConflict(null);
    const editor = { ...base, asksWhy: true, canSave: true, save: vi.fn(), setNote: vi.fn() } as unknown as DriveEditor;
    renderPane(editor);
    const field = screen.getByRole('textbox', { name: 'What changed?' });
    fireEvent.keyDown(field, { key: 'Enter', isComposing: true });
    fireEvent.keyDown(field, { key: 'Enter', keyCode: 229 });
    expect(editor.save).not.toHaveBeenCalled();
    fireEvent.keyDown(field, { key: 'Enter' });
    expect(editor.save).toHaveBeenCalledTimes(1);
  });

  it('asks nothing beside Save where the editor says a save does not ask', () => {
    renderPane({ ...inConflict(null), asksWhy: false, setNote: vi.fn() } as unknown as DriveEditor, null, { file: plain });
    expect(screen.queryByRole('textbox', { name: 'What changed?' })).toBeNull();
  });

  it('keeps the draft on screen and offers nothing while theirs is still loading', async () => {
    renderPane(inConflict(null));
    expect(screen.queryByRole('alert')).toBeNull();
    expect(await screen.findByTestId('editor')).toHaveTextContent('mine');
  });

  it('reads a past version by its own size, not the current one', () => {
    // Version 1 was too large to preview; the current one is small.
    const big = versions.map((v) => (v.seq === 1 ? { ...v, size: 40 * 1024 * 1024 } : v));
    renderPane(viewing, 1, { list: big });
    expect(screen.getByTestId('viewer')).toHaveTextContent('too_large');
  });

  it('reads a past version by its own kind of bytes, where the server says it', () => {
    const binary = versions.map((v) => (v.seq === 1 ? { ...v, is_text: false } : v));
    renderPane(viewing, 1, { list: binary });
    expect(screen.getByTestId('viewer')).toHaveTextContent('binary_file');
  });

  it('opens a memo version picked from the record in Changes, and one with no text to compare as it was', async () => {
    vi.mocked(getRecord).mockResolvedValue({
      items: [{ ...versions[0], type: 'version', added: 1, removed: 0 }, { ...versions[1], type: 'version', added: null, removed: null, text: 'none' }],
      next: null,
    });
    const { go } = renderPane(viewing, null, { tab: 'record' });
    fireEvent.click(await screen.findByRole('button', { name: /^v2 · / }));
    expect(go).toHaveBeenLastCalledWith({ file: memo.path, tab: 'changes', version: null, against: null });
    fireEvent.click(screen.getByRole('button', { name: /^v1 · / }));
    expect(go).toHaveBeenLastCalledWith({ file: memo.path, tab: null, version: 1, against: null });
  });
});
