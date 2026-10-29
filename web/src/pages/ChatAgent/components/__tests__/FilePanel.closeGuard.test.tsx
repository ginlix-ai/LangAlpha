/**
 * The panel's own close under an unsaved edit. The drafts live in the mount,
 * so the close asks the question closing a dirty tab asks, rather than going
 * away: on mobile the panel covers the chat, and a hidden close was a dead end.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, act, within } from '@testing-library/react';
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
// Monaco stands in as a textarea: the guard reads what the editor reports, not how.
vi.mock('@/pages/ChatAgent/components/viewers/CodeEditor', () => ({
  default: ({ value, onChange }: { value?: string; onChange?: (v: string) => void }) => (
    <textarea data-testid="editor" value={value ?? ''} onChange={(e) => onChange?.(e.target.value)} />
  ),
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';

const panel = (onClose: () => void) => (
  <FilePanel workspaceId="ws" onClose={onClose} files={['notes.md']} target={{ kind: 'file', path: 'notes.md', seq: 1 }} />
);

const closeButton = () => screen.getByRole('button', { name: 'Close' });

/** Answers the discard question the close asked. */
async function answer(choice: 'Discard' | 'Cancel'): Promise<void> {
  const dialog = await screen.findByRole('dialog');
  expect(within(dialog).getByText('You have unsaved changes. Discard them?')).toBeTruthy();
  fireEvent.click(within(dialog).getByRole('button', { name: choice }));
}

/** Opens the file, enters edit mode and changes a line. */
async function dirtyTheDraft(): Promise<void> {
  await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
  fireEvent.click(await screen.findByTitle('Edit file'));
  const editor = await screen.findByTestId('editor');
  await act(async () => { fireEvent.change(editor, { target: { value: '# Notes, revised' } }); });
}

beforeEach(() => { localStorage.clear(); });
afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

describe('FilePanel close under an unsaved edit', () => {
  it('still offers the close, and keeps the panel when the edit is not discarded', async () => {
    const onClose = vi.fn();
    renderWithProviders(panel(onClose));
    await dirtyTheDraft();

    fireEvent.click(closeButton());
    await answer('Cancel');

    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByTestId('editor')).toBeTruthy();
  });

  it('closes once the edit is discarded', async () => {
    const onClose = vi.fn();
    renderWithProviders(panel(onClose));
    await dirtyTheDraft();

    fireEvent.click(closeButton());
    expect(onClose).not.toHaveBeenCalled();
    await answer('Discard');

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('closes without asking when nothing is unsaved', async () => {
    const onClose = vi.fn();
    renderWithProviders(panel(onClose));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });

    fireEvent.click(closeButton());

    expect(screen.queryByRole('dialog')).toBeNull();
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
