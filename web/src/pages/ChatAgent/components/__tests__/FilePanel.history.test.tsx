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
// Monaco stands in as a textarea: the guard reads what the editor reports, not how.
vi.mock('@/pages/ChatAgent/components/viewers/CodeEditor', () => ({
  default: ({ value, onChange }: { value?: string; onChange?: (v: string) => void }) => (
    <textarea data-testid="editor" value={value ?? ''} onChange={(e) => onChange?.(e.target.value)} />
  ),
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';

const panel = () => (
  <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} target={null} />
);

const back = () => screen.getByRole('button', { name: 'Back' });
const forward = () => screen.getByRole('button', { name: 'Forward' });

/** Answers the discard question the step asked. */
async function answer(choice: 'Discard' | 'Cancel'): Promise<void> {
  const dialog = await screen.findByRole('dialog');
  expect(within(dialog).getByText('You have unsaved changes. Discard them?')).toBeTruthy();
  fireEvent.click(within(dialog).getByRole('button', { name: choice }));
}

/** Opens the file from the empty tab's tree, enters edit mode and changes a line. */
async function editFromTheTree(): Promise<void> {
  fireEvent.click(document.querySelector('[data-row-path="notes.md"]')!);
  await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
  fireEvent.click(await screen.findByTitle('Edit file'));
  const editor = await screen.findByTestId('editor');
  await act(async () => { fireEvent.change(editor, { target: { value: '# Notes, revised' } }); });
}

beforeEach(() => { localStorage.clear(); });
afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

/**
 * Back and forward step the tab in front through what it showed. A step
 * replaces the tab's body, so it asks what closing the tab would ask, and the
 * draft it discards does not follow the tab to what it shows next.
 */
describe('FilePanel back and forward', () => {
  it('has nowhere to go on a tab nothing has replaced yet', () => {
    renderWithProviders(panel());
    expect(back()).toBeDisabled();
    expect(forward()).toBeDisabled();
  });

  it('keeps an unsaved edit when the step is not confirmed', async () => {
    renderWithProviders(panel());
    await editFromTheTree();

    fireEvent.click(back());
    await answer('Cancel');

    expect(screen.getByTestId('editor')).toBeTruthy();
    expect(screen.getByText('notes.md', { selector: '.file-panel-tab-name' })).toBeTruthy();
  });

  it('goes back once the edit is discarded, and comes forward to the file without it', async () => {
    renderWithProviders(panel());
    await editFromTheTree();

    fireEvent.click(back());
    await answer('Discard');

    expect(document.querySelector('.file-panel-tree.is-full')).toBeTruthy();
    await waitFor(() => expect(back()).toBeDisabled());

    fireEvent.click(forward());
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    expect(screen.queryByTestId('editor')).toBeNull();
  });

  it('steps back in a panel locked to one file', async () => {
    const locked = (path: string, seq: number) => (
      <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md', 'plan.md']} singleFileMode readOnly target={{ kind: 'file', path, seq }} />
    );
    const { rerender } = renderWithProviders(locked('notes.md', 1));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    rerender(locked('plan.md', 2));
    await screen.findByText('plan.md', { selector: '.file-panel-tab-name' });

    fireEvent.click(back());
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    expect(forward()).toBeEnabled();
  });
});
