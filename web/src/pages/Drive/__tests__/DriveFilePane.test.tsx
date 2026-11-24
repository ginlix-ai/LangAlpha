import { fireEvent, screen } from '@testing-library/react';
import { QueryClient, type UseQueryResult } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import { queryKeys } from '@/lib/queryKeys';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { LIVE } from '@/pages/ChatAgent/utils/api/readFrom';
import { renderWithProviders } from '@/test/utils';
import type { DriveEntry, DriveVersion } from '@/types/drive';
import { DriveFilePane } from '../components/DriveFilePane';
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
}));

const SPACE = 'space-1';
const SHA = { v1: '1'.repeat(64), v2: '2'.repeat(64), theirs: '3'.repeat(64) };
const agent = { kind: 'agent' as const, user_id: 'u', agent: 'main', via: 'tool', thread_id: null };

const entry: DriveEntry = {
  id: 'e1', path: 'notes/q3.md', name: 'q3.md', kind: 'file', size: 8, sha256: SHA.v2,
  content_type: 'text/markdown', is_text: true, version: 2,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-20T00:00:00Z', modified_by: agent, deleted_at: null,
};

const versions: DriveVersion[] = [2, 1].map((seq) => ({
  seq, size: 8, sha256: seq === 2 ? SHA.v2 : SHA.v1, created_at: '2026-11-20T00:00:00Z',
  actor: { ...agent, run_id: null, workspace_id: null }, note: null, restored_from: null,
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
      replaced: null, draft: 'mine', save: 'idle', savedAt: null, conflict: true,
    },
    dirty: true,
    theirs,
    keepMine: vi.fn(),
    takeTheirs: vi.fn(),
  } as unknown as DriveEditor;
}

function renderPane(
  editor: DriveEditor,
  version: number | null = null,
  { list = versions, mobile = false, showHistory = false, head = headOf('v2 text', SHA.v2) }: {
    /** null: no listing cached, so it is read, and the read fails. */
    list?: DriveVersion[] | null; mobile?: boolean; showHistory?: boolean; head?: UseQueryResult<FileBody>;
  } = {},
) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  if (list) queryClient.setQueryData(queryKeys.drive.versions(SPACE, entry.id), { versions: list, history_bytes: 0, too_large_for_history: false });
  queryClient.setQueryData(driveBodyKey(SPACE, entry, SHA.v1), body('v1 text', SHA.v1));
  const actions = { pending: { restore: null, purge: null, restoreVersion: false }, restoreVersion: vi.fn() } as unknown as DriveActions;
  const go = vi.fn();
  const ctx: DriveContextValue = {
    space: SPACE, actions, go, open: vi.fn(), trash: vi.fn(), leaveEditor: vi.fn(),
    ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn() },
  };
  renderWithProviders(
    <DriveContext value={ctx}>
      <DriveFilePane
        path={entry.path}
        entry={entry}
        entryError={null}
        onRetry={vi.fn()}
        version={version}
        showHistory={showHistory}
        mobile={mobile}
        editor={editor}
        head={head}
        imageLoader={() => Promise.resolve('')}
        onOpenLink={vi.fn()}
      />
    </DriveContext>,
    { queryClient },
  );
  return { go, actions };
}

describe('DriveFilePane', () => {
  it('says a comparison could not load, with a retry, rather than loading for ever', async () => {
    const refetch = vi.fn();
    const failed = { data: undefined, isPending: false, error: new Error('Network Error'), refetch } as unknown as UseQueryResult<FileBody>;
    renderPane(viewing, 1, { head: failed });
    fireEvent.click(screen.getByRole('button', { name: 'Compare with current' }));
    expect(await screen.findByText('Could not load this. Check your connection and try again.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(refetch).toHaveBeenCalled();
  });

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

  it('shows the head in the viewer, with Edit offered', () => {
    renderPane(viewing);
    expect(screen.getByTestId('viewer')).toHaveTextContent('v2 text');
    expect(screen.getByRole('button', { name: 'Edit' })).toBeInTheDocument();
    expect(screen.queryByRole('alert')).toBeNull();
  });

  it('shows a past version in place of the head, compared with the current one on request', async () => {
    const { go } = renderPane(viewing, 1);
    expect(screen.getByText(/^Version 1,/)).toBeInTheDocument();
    expect(screen.getByTestId('viewer')).toHaveTextContent('v1 text');
    expect(screen.queryByRole('button', { name: 'Edit' })).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Compare with current' }));
    expect(await screen.findByTestId('diff')).toHaveTextContent('v1 text | v2 text');
    fireEvent.click(screen.getByRole('button', { name: 'Compare with current' }));
    expect(screen.queryByTestId('diff')).toBeNull();

    fireEvent.click(screen.getByRole('button', { name: 'Back to current' }));
    expect(go).toHaveBeenCalledWith({ file: entry.path, history: false, version: null });
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

  it('on a phone, picking a past version opens it and closes the history in one step', () => {
    const { go } = renderPane(viewing, null, { mobile: true, showHistory: true });
    fireEvent.click(screen.getByText('v1').closest('button')!);
    expect(go.mock.calls).toEqual([[{ file: entry.path, history: false, version: 1 }]]);
  });
});
