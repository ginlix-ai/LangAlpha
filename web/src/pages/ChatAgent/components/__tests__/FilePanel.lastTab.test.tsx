/**
 * Closing the last tab closes the panel. The host unmounts the panel in the
 * same update, so the strip the close leaves has to be saved before then, or
 * the panel reopens on the tab the reader just closed.
 */
import { useState } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, act, within, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    readWorkspaceFile: vi.fn(async () => ({ content: '# Notes', mime: 'text/markdown', truncated: false })),
    readWorkspaceFileFull: vi.fn(async () => ({ content: '# Notes' })),
    writeWorkspaceFile: vi.fn(async () => ({})),
    downloadWorkspaceFileAsArrayBuffer: vi.fn(),
    triggerFileDownload: vi.fn(),
    resolveWorkspaceFile: vi.fn(),
    getPreviewUrl: vi.fn(),
  };
});
vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => ({ data: { status: 'running', name: 'ws' } }) }));
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
  default: ({ value, onChange }: { value?: string; onChange?: (v: string) => void }) => (
    <textarea data-testid="editor" value={value ?? ''} onChange={(e) => onChange?.(e.target.value)} />
  ),
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import { resolveWorkspaceFile } from '@/pages/ChatAgent/utils/api';
import type { FileRefResolution, PanelTarget } from '@/pages/ChatAgent/components/filePanel/types';

/** Unmounts the panel on its close, as the chat does on mobile. */
function Host({ target, onClose }: { target: PanelTarget | null; onClose: () => void }) {
  const [open, setOpen] = useState(true);
  if (!open) return <div data-testid="panel-closed" />;
  return (
    <FilePanel
      workspaceId="ws"
      threadId="thread-1"
      onClose={() => { onClose(); setOpen(false); }}
      files={['notes.md', 'todo.md']}
      target={target}
    />
  );
}

/**
 * Keeps the panel mounted after its close, as the desktop chat does while the
 * panel animates out; a reopen inside that window gets the same mount back.
 */
function LingeringHost({ target, onClose }: { target: PanelTarget | null; onClose: () => void }) {
  return <FilePanel workspaceId="ws" threadId="thread-1" onClose={onClose} files={['notes.md', 'todo.md']} target={target} />;
}

const fileTarget = (path: string, seq: number): PanelTarget => ({ kind: 'file', path, seq });

/** Opens the active file for editing and changes a line. */
async function dirtyTheDraft(): Promise<void> {
  fireEvent.click(await screen.findByTitle('Edit file'));
  const editor = await screen.findByTestId('editor');
  await act(async () => { fireEvent.change(editor, { target: { value: '# Notes, revised' } }); });
}

const tabNamed = (name: string) => screen.queryByText(name, { selector: '.file-panel-tab-name' });

beforeEach(() => { localStorage.clear(); });
afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

describe('FilePanel closing its last tab', () => {
  it('closes the panel, and the panel reopens without the closed tab', async () => {
    const onClose = vi.fn();
    const first = renderWithProviders(<Host target={fileTarget('notes.md', 1)} onClose={onClose} />);
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));

    expect(onClose).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('panel-closed')).toBeTruthy();

    first.unmount();
    renderWithProviders(<Host target={null} onClose={vi.fn()} />);
    expect(await screen.findAllByRole('tab')).toHaveLength(1);
    expect(tabNamed('notes.md')).toBeNull();
  });

  it('keeps the panel while another tab is open', async () => {
    const onClose = vi.fn();
    const { rerender } = renderWithProviders(<Host target={fileTarget('notes.md', 1)} onClose={onClose} />);
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    fireEvent.doubleClick(screen.getByRole('tab', { name: /notes\.md/ }));
    rerender(<Host target={fileTarget('todo.md', 2)} onClose={onClose} />);
    await screen.findByText('todo.md', { selector: '.file-panel-tab-name' });

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));

    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getAllByRole('tab')).toHaveLength(1);
    expect(tabNamed('todo.md')).toBeTruthy();
  });

  it('leaves nothing of the closed tab in a panel still mounted on its way out', async () => {
    const onClose = vi.fn();
    const { rerender } = renderWithProviders(<LingeringHost target={fileTarget('notes.md', 1)} onClose={onClose} />);
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    fireEvent.doubleClick(screen.getByRole('tab', { name: /notes\.md/ }));

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(tabNamed('notes.md')).toBeNull();

    // Reopened before the exit finished, onto another file: the closed tab
    // must not come back, on screen or in what the strip saves.
    rerender(<LingeringHost target={fileTarget('todo.md', 2)} onClose={onClose} />);
    await screen.findByText('todo.md', { selector: '.file-panel-tab-name' });
    expect(screen.getAllByRole('tab')).toHaveLength(1);
    expect(tabNamed('notes.md')).toBeNull();
  });

  it('keeps the panel when a tab joins while the discard question is open', async () => {
    const onClose = vi.fn();
    const { rerender } = renderWithProviders(<Host target={fileTarget('notes.md', 1)} onClose={onClose} />);
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    await dirtyTheDraft();

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));
    const dialog = await screen.findByRole('dialog');
    rerender(<Host target={fileTarget('todo.md', 2)} onClose={onClose} />);
    await screen.findByText('todo.md', { selector: '.file-panel-tab-name' });
    fireEvent.click(within(dialog).getByRole('button', { name: 'Discard' }));

    expect(onClose).not.toHaveBeenCalled();
    expect(tabNamed('notes.md')).toBeNull();
    expect(tabNamed('todo.md')).toBeTruthy();
  });

  it('keeps the panel for a file still being looked up when the discard question is answered', async () => {
    let answer: (r: FileRefResolution) => void = () => {};
    vi.mocked(resolveWorkspaceFile).mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const onClose = vi.fn();
    const { rerender } = renderWithProviders(<Host target={fileTarget('notes.md', 1)} onClose={onClose} />);
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    await dirtyTheDraft();

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));
    const dialog = await screen.findByRole('dialog');
    // Not in the listing, so it waits on the lookup and has no tab yet.
    rerender(<Host target={fileTarget('q3.md', 2)} onClose={onClose} />);
    await waitFor(() => expect(resolveWorkspaceFile).toHaveBeenCalled());
    fireEvent.click(within(dialog).getByRole('button', { name: 'Discard' }));

    expect(onClose).not.toHaveBeenCalled();
    await act(async () => { answer({ status: 'resolved', path: 'reports/q3.md', matches: [] }); });
    expect(await screen.findByText('q3.md', { selector: '.file-panel-tab-name' })).toBeTruthy();
    expect(tabNamed('notes.md')).toBeNull();
  });

  it('asks about an unsaved edit before the panel goes', async () => {
    const onClose = vi.fn();
    renderWithProviders(<Host target={fileTarget('notes.md', 1)} onClose={onClose} />);
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    await dirtyTheDraft();

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));
    const cancel = within(await screen.findByRole('dialog')).getByRole('button', { name: 'Cancel' });
    fireEvent.click(cancel);
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Close notes.md' }));
    fireEvent.click(within(await screen.findByRole('dialog')).getByRole('button', { name: 'Discard' }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
