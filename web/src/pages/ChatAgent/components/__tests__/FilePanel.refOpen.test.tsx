import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor, fireEvent, act, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { QueryClient } from '@tanstack/react-query';

const wsStatus = { value: 'running' };

// `renderWithProviders` mounts no Toaster, so a toast has nowhere to appear in
// the DOM. The spy stands in for that viewport, and the assertion below pins
// the sentence the reader would have read, not merely that a toast happened.
const { toastSpy } = vi.hoisted(() => ({ toastSpy: vi.fn() }));
vi.mock('@/components/ui/use-toast', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return { ...orig, toast: toastSpy };
});

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    readWorkspaceFile: vi.fn(),
    readWorkspaceFileFull: vi.fn(),
    writeWorkspaceFile: vi.fn(async () => ({})),
    downloadWorkspaceFile: vi.fn(),
    downloadWorkspaceFileAsArrayBuffer: vi.fn(),
    triggerFileDownload: vi.fn(),
    resolveWorkspaceFile: vi.fn(),
    deleteWorkspaceFiles: vi.fn(async (_ws: string, paths: string[]) => ({ deleted: paths, errors: [] })),
  };
});
vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => ({ data: { status: wsStatus.value, name: 'ws' } }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'dark' }) }));
vi.mock('@/pages/ChatAgent/components/FilePanelMemo', () => ({
  memoMimeForName: () => null,
  useAddToMemo: () => vi.fn(),
  useWorkspaceMemoIndex: () => new Map(),
  useMemoStaleCheck: () => ({ status: null, sandboxText: null, refresh: () => {} }),
  MemoStaleBanner: () => null,
  MemoDiffModal: () => null,
}));
vi.mock('@/pages/ChatAgent/components/SandboxSettingsPanel', () => ({ SandboxSettingsContent: () => null }));
vi.mock('@/pages/ChatAgent/components/viewers/CodeEditor', () => ({
  default: ({ value, fileName, onChange }: { value?: string; fileName: string; onChange?: (v: string) => void }) => (
    <textarea
      data-testid="editor"
      data-file={fileName}
      value={value ?? ''}
      onChange={(e) => onChange?.(e.target.value)}
    />
  ),
}));

import * as api from '@/pages/ChatAgent/utils/api';
import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import { WorkspaceProvider } from '@/pages/ChatAgent/contexts/WorkspaceContext';
import { createTranscriptStore } from '@/pages/ChatAgent/components/filePanel/transcriptStore';

const resolveMock = () => api.resolveWorkspaceFile as ReturnType<typeof vi.fn>;

const CONTENT: Record<string, string> = {
  'notes.md': '# Notes\n\nMy private notes body.',
  'report.py': 'print("report")\n',
  'results/report2.md': '# Report',
  'results/tools/x.py': 'x = 1\n',
  // Two namesakes at two depths, which is what makes the reading matter.
  'docs/index.md': [
    '# Index',
    '',
    '[sandbox](/home/workspace/results/report.md)',
    '[qualified](__wsref__/ws/results/report.md)',
    '[sibling](results/report.md)',
    '[up-folder](../data/)',
    '[sub-folder](assets/)',
    '[rooted-folder](__wsref__/ws/results/)',
    '[sandbox-folder](/home/workspace/results/)',
    '[own-folder](../Home/results/report.md)',
    '[own-folder-joined](../../Home/results/report.md)',
    '[sibling-climbed](../NVDA/dcf/report.md)',
    '[sibling-rooted](/home/workspace/NVDA/dcf/report.md)',
  ].join('\n'),
  'docs/results/report.md': '# The nested one',
  'results/report.md': '# The rooted one',
};

/** The three namesake files, plus the document whose links point at them. */
const NAMESAKES = ['docs/index.md', 'docs/results/report.md', 'results/report.md'];

beforeEach(() => {
  vi.clearAllMocks();
  // Tabs persist per workspace and every test here mounts `ws`, so without
  // this each test would inherit the strip the previous one left behind.
  localStorage.clear();
  wsStatus.value = 'running';
  (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockImplementation(async (_ws: string, p: string) => {
    if (p in CONTENT) return { content: CONTENT[p], mime: p.endsWith('.md') ? 'text/markdown' : 'text/x-python', truncated: false };
    throw { response: { status: 404, data: { detail: 'File not found' } } };
  });
  (api.readWorkspaceFileFull as ReturnType<typeof vi.fn>).mockImplementation(async (_ws: string, p: string) => ({ content: CONTENT[p] }));
});

describe('FilePanel reference opens', () => {
  it('leaves edit mode when a reference opens another file', async () => {
    const files = ['notes.md', 'report.py'];
    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} target={{ kind: 'file', path: 'notes.md' }} />,
    );
    await screen.findByText('My private notes body.');
    fireEvent.click(screen.getByTitle('Edit file'));
    await screen.findByTestId('editor');

    rerender(<FilePanel workspaceId="ws" onClose={() => {}} files={files} target={{ kind: 'file', path: 'report.py' }} />);
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'report.py'));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });

    expect(screen.queryByTestId('editor')).toBeNull();
    expect(screen.queryByTitle('Save (Cmd+S)')).toBeNull();
  });

  it('asks the server for a known path that is no longer there', async () => {
    resolveMock().mockResolvedValue({ status: 'resolved', path: 'results/report2.md', matches: ['results/report2.md'] });
    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={[]} getRecentWritePaths={() => ['report2.md']} target={{ kind: 'file', path: 'report2.md' }} />,
    );
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'results/report2.md'));
    expect(api.resolveWorkspaceFile).toHaveBeenCalledWith('ws', ['report2.md'], ['report2.md']);
  });

  // The stale path was only a guess the panel tried on the way to the file.
  it('leaves no Back step onto a path that missed before the lookup found the file', async () => {
    resolveMock().mockResolvedValue({ status: 'resolved', path: 'results/report2.md', matches: ['results/report2.md'] });
    const panel = (target: { kind: 'file'; path: string; seq: number }) => (
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} getRecentWritePaths={() => ['report2.md']} target={target} />
    );
    const { rerender } = renderWithProviders(panel({ kind: 'file', path: 'notes.md', seq: 1 }));
    await screen.findByText('My private notes body.');
    rerender(panel({ kind: 'file', path: 'report2.md', seq: 2 }));
    await screen.findByText('report2.md', { selector: '.file-panel-tab-name' });
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'results/report2.md'));

    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await screen.findByText('My private notes body.')).toBeTruthy();
  });

  it('lets a file opened from the tree win over a reference still being looked up', async () => {
    let answer!: (r: { status: string; path?: string; matches: string[] }) => void;
    resolveMock().mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const files = ['notes.md', 'report.py'];
    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} target={{ kind: 'file', path: 'missing.md' }} />,
    );
    await waitFor(() => expect(api.resolveWorkspaceFile).toHaveBeenCalled());

    fireEvent.click(await screen.findByText('report.py', { selector: '[data-row-path] *' }));
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'report.py'));

    answer({ status: 'resolved', path: 'notes.md', matches: ['notes.md'] });
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    expect(api.readWorkspaceFile).not.toHaveBeenCalledWith('ws', 'notes.md');
  });

  it('lets a tab opened by hand win over a reference still being looked up', async () => {
    let answer!: (r: { status: string; path?: string; matches: string[] }) => void;
    resolveMock().mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} target={{ kind: 'file', path: 'missing.md' }} />,
    );
    await waitFor(() => expect(api.resolveWorkspaceFile).toHaveBeenCalled());

    fireEvent.click(screen.getByLabelText('New tab'));

    answer({ status: 'resolved', path: 'notes.md', matches: ['notes.md'] });
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    expect(api.readWorkspaceFile).not.toHaveBeenCalledWith('ws', 'notes.md');
  });

  it('closes the tab of a file the tree just deleted and reads it afresh if reopened', async () => {
    // A panel-side delete never reaches the write log, so the change marker
    // cannot force the re-read; the delete has to drop the tab and its bytes.
    const first = { kind: 'file', path: 'notes.md', seq: 1 } as const;
    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md', 'report.py']} target={first} />,
    );
    await screen.findByText('My private notes body.');

    fireEvent.click(screen.getByTitle('Select files'));
    fireEvent.click(within(screen.getByRole('tree')).getByText('notes.md'));
    fireEvent.click(screen.getByTitle('Delete selected'));
    fireEvent.click(await screen.findByText('Delete 1?'));

    await waitFor(() => expect(screen.queryByLabelText('Close notes.md')).toBeNull());
    expect(screen.queryByText('My private notes body.')).toBeNull();

    rerender(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['report.py']} target={{ kind: 'file', path: 'notes.md', seq: 2 }} />,
    );
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledTimes(2));
  });

  it('keeps the change marker on a returning tab until the fresh bytes land', async () => {
    // The body query keeps the old bytes through the refetch a changed tab
    // triggers, so the marker must wait for the read to settle, not for a body
    // to be present. The default test client evicts a left tab at once, which
    // would hide the cached-body case this is about.
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 5 * 60_000 } } });
    const log = { current: [] as { id: string; path: string }[] };
    const collectWrites = () => log.current;
    const writes = createTranscriptStore({ messages: [], collectWrites });
    const files = ['notes.md', 'report.py'];
    const first = { kind: 'file', path: 'notes.md', seq: 1 } as const;
    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} transcript={writes.reader} target={first} />,
      { queryClient },
    );
    await screen.findByText('My private notes body.');
    // Pinned, so the next single open sits beside it instead of taking its tab.
    fireEvent.doubleClick(within(screen.getByRole('tablist')).getByText('notes.md'));

    rerender(<FilePanel workspaceId="ws" onClose={() => {}} files={files} transcript={writes.reader} target={{ kind: 'file', path: 'report.py', seq: 2 }} />);
    await screen.findByLabelText('Close report.py');
    log.current = [{ id: 'w1', path: 'notes.md' }];
    act(() => writes.publish({ messages: [], collectWrites }));
    await screen.findByTitle('Changed since you opened it');

    let land: (v: { content: string; mime: string; truncated: boolean }) => void = () => {};
    (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockImplementationOnce(() => new Promise((r) => { land = r; }));
    fireEvent.click(within(screen.getByRole('tablist')).getByText('notes.md'));
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenLastCalledWith('ws', 'notes.md'));
    // Still the old bytes on screen, so still marked as changed.
    expect(screen.getByText('My private notes body.')).toBeInTheDocument();
    expect(screen.getByTitle('Changed since you opened it')).toBeInTheDocument();

    await act(async () => { land({ content: 'Rewritten notes body.', mime: 'text/markdown', truncated: false }); });
    await screen.findByText('Rewritten notes body.');
    expect(screen.queryByTitle('Changed since you opened it')).toBeNull();
  });

  it('re-reads a restored tab whose file was rewritten while the panel was closed', async () => {
    // The body outlives the panel mount in the shared query cache, the read
    // marks do not. A remount inside the fresh window has to read again
    // rather than adopt the old bytes and stamp the newer write as read.
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 5 * 60_000 } } });
    const log = { current: [] as { id: string; path: string }[] };
    const collectWrites = () => log.current;
    const writes = createTranscriptStore({ messages: [], collectWrites });
    const files = ['notes.md'];
    const first = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} transcript={writes.reader} target={{ kind: 'file', path: 'notes.md', seq: 1 }} />,
      { queryClient },
    );
    await screen.findByText('My private notes body.');
    first.unmount();

    log.current = [{ id: 'w1', path: 'notes.md' }];
    writes.publish({ messages: [], collectWrites });
    (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      { content: 'Rewritten notes body.', mime: 'text/markdown', truncated: false },
    );
    // No target: the strip comes back from storage with notes.md active.
    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} transcript={writes.reader} />,
      { queryClient },
    );
    await screen.findByText('Rewritten notes body.');
    expect(screen.queryByTitle('Changed since you opened it')).toBeNull();
  });

  it('reads a file again when it was rewritten while its tab was closed', async () => {
    // The body query stays fresh for a minute after a read. Closing the tab
    // forgets the change marker, so the marker cannot force the re-read; the
    // close has to drop the cached bytes instead.
    const log = { current: [] as { id: string; path: string }[] };
    const collectWrites = () => log.current;
    const writes = createTranscriptStore({ messages: [], collectWrites });
    const first = { kind: 'file', path: 'notes.md', seq: 1 } as const;
    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} transcript={writes.reader} target={first} />,
    );
    // The marker is stamped once the bytes are in hand, so the write has to
    // come after the body is on screen to count as a change.
    await screen.findByText('My private notes body.');

    log.current = [{ id: 'w1', path: 'notes.md' }];
    act(() => writes.publish({ messages: [], collectWrites }));
    await screen.findByTitle('Changed since you opened it');
    fireEvent.click(screen.getByLabelText('Close notes.md'));

    rerender(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} transcript={writes.reader} target={{ kind: 'file', path: 'notes.md', seq: 2 }} />,
    );
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledTimes(2));
  });

  it('asks the server when a stopped workspace reports the path as not backed up', async () => {
    wsStatus.value = 'stopped';
    resolveMock().mockResolvedValue({ status: 'resolved', path: 'results/tools/x.py', matches: ['results/tools/x.py'] });
    renderWithProviders(<FilePanel workspaceId="ws" onClose={() => {}} files={[]} target={{ kind: 'file', path: 'tools/x.py' }} />);
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'results/tools/x.py'));
  });

  it('asks the server for a download-only reference the listing still remembers', async () => {
    // A .docx opens with no read at all, so a listing hit is not evidence the
    // path is live: the card would offer to save a name nothing occupies.
    resolveMock().mockResolvedValue({ status: 'resolved', path: 'results/deck.docx', matches: ['results/deck.docx'] });
    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['deck.docx']} target={{ kind: 'file', path: 'deck.docx' }} />,
    );
    await waitFor(() => expect(api.resolveWorkspaceFile).toHaveBeenCalledWith('ws', ['deck.docx'], []));
    expect(api.readWorkspaceFile).not.toHaveBeenCalled();
  });

  it('lands on the matches when the server cannot pick one', async () => {
    resolveMock().mockResolvedValue({ status: 'ambiguous', matches: ['a/model.py', 'b/model.py'] });
    renderWithProviders(<FilePanel workspaceId="ws" onClose={() => {}} files={['a/model.py', 'b/model.py']} target={{ kind: 'file', path: 'model.py' }} />);
    await screen.findByText(/More than one file matches model\.py/);
    expect(api.readWorkspaceFile).not.toHaveBeenCalled();
  });

  it('reads the path as written when the server cannot look yet', async () => {
    resolveMock().mockResolvedValue({ status: 'unavailable', reason: 'sandbox_starting', matches: [] });
    renderWithProviders(<FilePanel workspaceId="ws" onClose={() => {}} files={[]} target={{ kind: 'file', path: 'results/new.csv' }} />);
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'results/new.csv'));
  });

  // A reference that named where it starts is not written relative to the file
  // quoting it, so joining it against that file's directory opens a namesake.
  const openDocAndClick = async (linkText: string) => {
    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={NAMESAKES} target={{ kind: 'file', path: 'docs/index.md' }} />,
    );
    await screen.findByText('Index');
    (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockClear();
    fireEvent.click(await screen.findByText(linkText));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    return (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mock.calls.map((c) => c[1]);
  };

  it('opens the rooted file a sandbox-absolute link named, not the nearer namesake', async () => {
    const read = await openDocAndClick('sandbox');
    expect(read).toContain('results/report.md');
    expect(read).not.toContain('docs/results/report.md');
  });

  it('opens the rooted file a same-workspace qualifier named', async () => {
    const read = await openDocAndClick('qualified');
    expect(read).toContain('results/report.md');
    expect(read).not.toContain('docs/results/report.md');
  });

  it('still reads a bare link against the directory of the file quoting it', async () => {
    // The control: a reference that named no starting point is the one case
    // where the open file's own directory is the better guess.
    const read = await openDocAndClick('sibling');
    expect(read).toContain('docs/results/report.md');
  });

  // `../Home/x` from Home's working directory is Home's `x`; joined onto the
  // file's directory it would be a nested `docs/Home/`, and as written the
  // server refuses it for climbing out.
  it.each([['own-folder'], ['own-folder-joined']])('opens the file a link through the workspace\'s own folder names (%s)', async (linkText) => {
    renderWithProviders(
      <WorkspaceProvider workspaceId="ws" downloadFile={null} folders={{ dirName: 'Home', siblings: [] }}>
        <FilePanel workspaceId="ws" onClose={() => {}} files={NAMESAKES} target={{ kind: 'file', path: 'docs/index.md' }} />
      </WorkspaceProvider>,
    );
    await screen.findByText('Index');
    (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockClear();
    fireEvent.click(await screen.findByText(linkText));
    await screen.findByText('The rooted one');
    const read = (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mock.calls.map((c) => c[1]);
    expect(read).toEqual(['results/report.md']);
    expect(api.resolveWorkspaceFile).not.toHaveBeenCalled();
  });

  // A note in Home links a sibling workspace's file; it opens there, not in Home.
  it.each([['sibling-climbed'], ['sibling-rooted']])('opens a sibling workspace\'s file in that workspace (%s)', async (linkText) => {
    const onOpenFile = vi.fn();
    renderWithProviders(
      <WorkspaceProvider
        workspaceId="ws"
        downloadFile={null}
        folders={{ dirName: 'Home', siblings: [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }] }}
      >
        <FilePanel workspaceId="ws" onClose={() => {}} files={NAMESAKES} target={{ kind: 'file', path: 'docs/index.md' }} onOpenFile={onOpenFile} />
      </WorkspaceProvider>,
    );
    await screen.findByText('Index');
    fireEvent.click(await screen.findByText(linkText));
    await waitFor(() => expect(onOpenFile).toHaveBeenCalledWith('dcf/report.md', 'ws-nvda', undefined));
  });

  // A folder is delegated to the router rather than resolved, because the
  // lookup globs files and a directory never matches one. The router reads the
  // path as given, so the reading a relative folder invited has to be applied
  // before it is handed over, exactly as it is for a file.
  const clickFolder = async (linkText: string) => {
    const onOpenFile = vi.fn();
    renderWithProviders(
      <FilePanel
        workspaceId="ws"
        onClose={() => {}}
        files={NAMESAKES}
        target={{ kind: 'file', path: 'docs/index.md' }}
        onOpenFile={onOpenFile}
      />,
    );
    await screen.findByText('Index');
    fireEvent.click(await screen.findByText(linkText));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    return onOpenFile;
  };

  it('opens a climbing folder at the directory it names from the viewed file', async () => {
    expect(await clickFolder('up-folder'))
      .toHaveBeenCalledWith('data/', undefined, undefined);
  });

  it('opens a sibling folder under the directory of the file quoting it', async () => {
    expect(await clickFolder('sub-folder'))
      .toHaveBeenCalledWith('docs/assets/', undefined, undefined);
  });

  it('leaves a rooted folder where it points, not one level down', async () => {
    // The control for the join above: `docs/results/` exists, so joining a
    // reference that named its own starting point would land on it. The
    // qualifier also carries the workspace, which is the third argument here.
    expect(await clickFolder('rooted-folder'))
      .toHaveBeenCalledWith('results/', 'ws', undefined);
  });

  it('opens a folder a sandbox-rooted link names, rather than leaving the app', async () => {
    // `/home/workspace/data/` is the form the prompts teach, and it names no
    // workspace of its own. The click used to fall through to a plain app link,
    // because a folder has no extension to offer the rooted test.
    expect(await clickFolder('sandbox-folder'))
      .toHaveBeenCalledWith('results/', undefined, undefined);
  });

  it('drops a save that fails after the reader has opened another file', async () => {
    // A save reads the whole body before the anchor click and the client sets
    // no timeout, so the window is open for as long as the request hangs.
    // Opening another file clears `fileError` on the way in and nothing clears
    // it again, so an unguarded rejection replaced the file on screen with the
    // previous one's error, and `not_found` renders without a Retry button.
    const files = ['data.parquet', 'notes.md'];
    let failDownload: (err: unknown) => void = () => {};
    (api.triggerFileDownload as ReturnType<typeof vi.fn>).mockImplementation(
      () => new Promise((_resolve, reject) => { failDownload = reject; }),
    );

    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} target={{ kind: 'file', path: 'data.parquet' }} />,
    );
    fireEvent.click(await screen.findByText('Download instead'));

    rerender(<FilePanel workspaceId="ws" onClose={() => {}} files={files} target={{ kind: 'file', path: 'notes.md' }} />);
    await screen.findByText('My private notes body.');

    await act(async () => {
      failDownload({ response: { status: 404, data: { detail: 'File not found' } } });
      await new Promise((r) => setTimeout(r, 20));
    });

    expect(screen.getByText('My private notes body.')).toBeTruthy();
    expect(screen.queryByText('File not found')).toBeNull();
    expect(screen.queryByText('Cannot preview this file')).toBeNull();
  });

  it('still reports a save that fails while its own file is on screen', async () => {
    // The control for the guard above: the reader has not moved, so the failure
    // belongs to the file being looked at and is the one thing that tells them
    // the download they asked for did not happen.
    const files = ['data.parquet', 'notes.md'];
    let failDownload: (err: unknown) => void = () => {};
    (api.triggerFileDownload as ReturnType<typeof vi.fn>).mockImplementation(
      () => new Promise((_resolve, reject) => { failDownload = reject; }),
    );

    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={files} target={{ kind: 'file', path: 'data.parquet' }} />,
    );
    fireEvent.click(await screen.findByText('Download instead'));

    await act(async () => {
      failDownload({ response: { status: 404, data: { detail: 'File not found' } } });
      await new Promise((r) => setTimeout(r, 20));
    });

    expect(await screen.findByText('File not found')).toBeTruthy();
  });

  // A write holds the whole body and the client sets no timeout, so the reader
  // reaches the next file long before a hung save answers. This leaves one
  // write in flight on `notes.md` with the panel already showing `report.py`.
  const EDIT_FILES = ['notes.md', 'report.py'];

  const hangSaveThenOpenNextFile = async () => {
    let rejectWrite: (err: unknown) => void = () => {};
    (api.writeWorkspaceFile as ReturnType<typeof vi.fn>).mockImplementation(
      () => new Promise((_resolve, reject) => { rejectWrite = reject; }),
    );

    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={EDIT_FILES} target={{ kind: 'file', path: 'notes.md' }} />,
    );
    await screen.findByText('My private notes body.');
    fireEvent.click(screen.getByTitle('Edit file'));
    fireEvent.change(await screen.findByTestId('editor'), { target: { value: '# Notes\n\nEdited body.' } });
    fireEvent.click(screen.getByTitle('Save (Cmd+S)'));
    fireEvent.click(within(await screen.findByRole('dialog')).getByRole('button', { name: 'Save' }));
    await waitFor(() => expect(api.writeWorkspaceFile).toHaveBeenCalledWith('ws', 'notes.md', '# Notes\n\nEdited body.'));

    rerender(<FilePanel workspaceId="ws" onClose={() => {}} files={EDIT_FILES} target={{ kind: 'file', path: 'report.py' }} />);
    await waitFor(() => expect(api.readWorkspaceFile).toHaveBeenCalledWith('ws', 'report.py'));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    return { rejectWrite };
  };

  it('a hung save on one file does not disable Save on the next', async () => {
    // `isSaving` was one hook-wide flag, and only the write that set it could
    // clear it. A save that never answered therefore outlived its own file and
    // left Save greyed out on every file opened after it, with no way back
    // short of reloading the page.
    await hangSaveThenOpenNextFile();

    fireEvent.click(screen.getByTitle('Edit file'));
    const editor = await screen.findByTestId('editor');
    expect(editor.getAttribute('data-file')).toBe('report.py');
    fireEvent.change(editor, { target: { value: 'print("edited")\n' } });

    expect(screen.getByTitle('Save (Cmd+S)')).not.toBeDisabled();
  });

  it('a save that fails after the reader has left the file says so', async () => {
    // The panel has moved on, so there is no header left to carry the inline
    // error and the failure used to return in silence. A reader who then
    // answered "discard unsaved changes" lost the edit believing it had landed.
    const { rejectWrite } = await hangSaveThenOpenNextFile();

    await act(async () => {
      rejectWrite({ response: { status: 500, data: { detail: 'Sandbox is gone' } } });
      await new Promise((r) => setTimeout(r, 20));
    });

    expect(toastSpy).toHaveBeenCalledWith(
      expect.objectContaining({ description: "Couldn't save notes.md", variant: 'destructive' }),
    );
  });

  // A folder accepted from chat points the tree, which is now a column beside
  // the viewer rather than the thing the viewer was replaced by — so honouring
  // one never costs the reader the file they were reading. the target's `dir`
  // is stored with its trailing slash stripped (`computeAgentArtifactRouting`)
  // and the scope chip adds the slash back.
  const scopeChip = () => document.querySelector('.file-panel-tree-scope');
  const treeList = () => document.querySelector('.file-panel-tree-list');

  const openDocThenFolder = async ({ edit = false }: { edit?: boolean } = {}) => {
    const onTargetHandled = vi.fn();
    const panel = (extra: Record<string, unknown>) => (
      <FilePanel
        workspaceId="ws"
        onClose={() => {}}
        files={NAMESAKES}
        onTargetHandled={onTargetHandled}
        {...extra}
      />
    );
    const { rerender } = renderWithProviders(panel({ target: { kind: 'file', path: 'docs/index.md' } }));
    await screen.findByText('Index');
    if (edit) {
      fireEvent.click(screen.getByTitle('Edit file'));
      fireEvent.change(await screen.findByTestId('editor'), { target: { value: '# Index\n\nEdited.' } });
    }
    onTargetHandled.mockClear();

    rerender(panel({ target: { kind: 'file', dir: 'docs' } }));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });
    return { onTargetHandled };
  };

  it('a folder target scopes the tree without closing the open file', async () => {
    await openDocThenFolder();

    expect(scopeChip()?.textContent).toContain('docs/');
    expect(screen.getByText('Index')).toBeTruthy();
  });

  it('a folder target leaves an editor and its unsaved edits alone', async () => {
    // The folder is answered by the column beside the editor, so there is
    // nothing to discard and nothing to ask about.
    const { onTargetHandled } = await openDocThenFolder({ edit: true });

    expect(screen.getByTestId('editor').getAttribute('data-file')).toBe('docs/index.md');
    expect((screen.getByTestId('editor') as HTMLTextAreaElement).value).toContain('Edited.');
    expect(scopeChip()?.textContent).toContain('docs/');
    expect(screen.queryByRole('dialog')).toBeNull();
    // The folder is handed back once applied, like any other target, so a
    // remount does not replay it; the scope itself stays on as the panel's own.
    expect(onTargetHandled).toHaveBeenCalled();
  });

  const folderPanel = (extra: Record<string, unknown>) => (
    <FilePanel workspaceId="ws" onClose={() => {}} files={NAMESAKES} {...extra} />
  );

  const openFileInsideFolder = async () => {
    const { rerender } = renderWithProviders(folderPanel({ target: { kind: 'file', dir: 'docs', seq: 1 } }));
    rerender(folderPanel({ target: { kind: 'file', dir: 'docs', seq: 1, path: 'docs/index.md' } }));
    await screen.findByText('Index');
    return rerender;
  };

  it('shows the tree again when the same folder is asked for twice', async () => {
    // The reader opened a file from `docs/`, then hid the tree, then clicked
    // `docs/` again. Keyed on the folder, the prop was byte-identical and
    // nothing ran, so the second click read as dead.
    const rerender = await openFileInsideFolder();
    fireEvent.click(screen.getByTitle('Toggle file tree'));
    await waitFor(() => expect(treeList()).toBeNull());

    rerender(folderPanel({ target: { kind: 'file', dir: 'docs', seq: 2 } }));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });

    expect(treeList()).toBeTruthy();
    expect(scopeChip()?.textContent).toContain('docs/');
  });

  it('leaves a hidden tree hidden while the same request is still in effect', async () => {
    // The control for the count: a re-render carrying the request already
    // handled must not push the tree back over the file the reader opened.
    const rerender = await openFileInsideFolder();
    fireEvent.click(screen.getByTitle('Toggle file tree'));

    rerender(folderPanel({ target: { kind: 'file', dir: 'docs', seq: 1 } }));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });

    await waitFor(() => expect(treeList()).toBeNull());
    expect(screen.getByText('Index')).toBeTruthy();
  });

  it('scopes the tree to the workspace root a `/home/workspace/` link names', async () => {
    // The router returns `''` for the root, which is a folder like any other.
    // Read as "no folder was asked for", it left the tree scoped to wherever
    // it already was and the link did nothing at all.
    const { rerender } = renderWithProviders(folderPanel({ target: { kind: 'file', path: 'docs/index.md' } }));
    await screen.findByText('Index');

    rerender(folderPanel({ target: { kind: 'file', dir: '', seq: 1 } }));
    await act(async () => { await new Promise((r) => setTimeout(r, 20)); });

    // The root filters nothing, so the chip names the whole workspace.
    expect(scopeChip()?.textContent).toContain('/');
    expect(screen.getByText('Index')).toBeTruthy();
  });

  it('re-reads the file a retry was pressed on, not the reference another tab still owes', async () => {
    resolveMock().mockResolvedValueOnce({ status: 'unavailable', reason: 'sandbox_starting', matches: [] });
    (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockImplementation(async (_ws: string, p: string) => {
      if (!(p in CONTENT)) throw { response: { status: 503, data: { detail: 'Sandbox is starting' } } };
      return { content: CONTENT[p], mime: 'text/markdown', truncated: false };
    });
    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} target={{ kind: 'file', path: 'report.md', seq: 1 }} />,
    );
    await screen.findByText('Try again');

    // A second file opens in its own tab and fails on its own account.
    delete CONTENT['notes.md'];
    rerender(<FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} target={{ kind: 'file', path: 'notes.md', seq: 2 }} />);
    await waitFor(() => expect(screen.getAllByText('Try again').length).toBeGreaterThan(0));
    CONTENT['notes.md'] = '# Notes\n\nMy private notes body.';

    fireEvent.click(screen.getByText('Try again'));

    await screen.findByText('My private notes body.');
    expect(resolveMock()).toHaveBeenCalledTimes(1);
  });

  it('asks the lookup again when a retry follows a lookup that could not answer', async () => {
    // The lookup is the only thing that knows `report.md` is `results/report.md`,
    // and the lookup is what was unavailable. Retrying the fallback path instead
    // re-asked the question that had already failed, so a file that existed the
    // whole time stayed a not-found once the sandbox was up.
    resolveMock().mockResolvedValueOnce({ status: 'unavailable', reason: 'sandbox_starting', matches: [] });
    // The sandbox still coming up is what left the lookup unable to answer, so
    // the fallback read of the path as written cannot land either.
    (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockImplementation(async (_ws: string, p: string) => {
      if (!(p in CONTENT)) throw { response: { status: 503, data: { detail: 'Sandbox is starting' } } };
      return { content: CONTENT[p], mime: 'text/markdown', truncated: false };
    });
    renderWithProviders(<FilePanel workspaceId="ws" onClose={() => {}} files={[]} target={{ kind: 'file', path: 'report.md' }} />);
    await screen.findByText('Try again');

    resolveMock().mockResolvedValueOnce({ status: 'resolved', path: 'results/report.md', matches: ['results/report.md'] });
    fireEvent.click(screen.getByText('Try again'));

    await screen.findByText('The rooted one');
    expect(resolveMock()).toHaveBeenCalledTimes(2);
  });
});
