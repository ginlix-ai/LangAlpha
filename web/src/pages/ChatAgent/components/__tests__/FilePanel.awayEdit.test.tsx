/**
 * A file read from the backup while its computer is away is shown, never
 * edited: the backup may be older than the file on the machine, and a save
 * made from it would write that older text over the newer one once the
 * machine is back. A save also names the version its edit began from, so a
 * file changed since is refused rather than overwritten.
 */
import { createHash } from 'node:crypto';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, act, within, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { readWorkspaceFile, readWorkspaceFileFull, writeWorkspaceFile } from '@/pages/ChatAgent/utils/api';
import { toast } from '@/components/ui/use-toast';

vi.mock('@/components/ui/use-toast', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  toast: vi.fn(),
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    readWorkspaceFile: vi.fn(),
    readWorkspaceFileFull: vi.fn(),
    writeWorkspaceFile: vi.fn(),
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

const AWAY = { kind: 'backup', unreachable: 'not_connected' } as const;
const STOPPED = { kind: 'backup', unreachable: null } as const;
const LIVE = { content: '# Notes', mime: 'text/markdown', truncated: false, from: { kind: 'live' } } as const;
const LIVE_FULL = { content: '# Notes', version: 'v1', rewriting: false, from: { kind: 'live' } } as const;
const FROM_BACKUP = { ...LIVE, from: AWAY };
const FULL_FROM_BACKUP = { ...LIVE_FULL, version: null, from: AWAY };
const AWAY_RETRY = "Can't reach this workspace's computer right now. Try again once it's on and online.";
const AWAY_BACKUP = "Can't reach this workspace's computer, so these are its backed-up files. They go live again once it's back.";

const panel = () => (
  <FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md']} target={{ kind: 'file', path: 'notes.md', seq: 1 }} />
);

beforeEach(() => { localStorage.clear(); });
afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

describe('FilePanel editing while the computer is away', () => {
  it('marks a body read from the backup and offers no edit, though the listing was live', async () => {
    vi.mocked(readWorkspaceFile).mockResolvedValue(FROM_BACKUP);
    renderWithProviders(panel());

    expect(await screen.findByText(AWAY_BACKUP)).toBeTruthy();
    expect(screen.queryByTitle('Edit file')).toBeNull();
  });

  it('words a file the gateway could not serve as the listing does', async () => {
    vi.mocked(readWorkspaceFile).mockRejectedValue({
      response: { status: 503, headers: { 'x-computer-unreachable': 'gateway_unreachable' }, data: { detail: 'x' } },
    });
    renderWithProviders(panel());

    expect(await screen.findByText("Can't reach the computer right now. Try again in a moment.")).toBeTruthy();
  });

  it('refuses to open the editor on a full read the backup answered', async () => {
    vi.mocked(readWorkspaceFile).mockResolvedValue(LIVE);
    vi.mocked(readWorkspaceFileFull).mockResolvedValue(FULL_FROM_BACKUP);
    renderWithProviders(panel());
    fireEvent.click(await screen.findByTitle('Edit file'));
    // The machine went away before the edit's read: the body is read again,
    // and that read finds the backup too.
    vi.mocked(readWorkspaceFile).mockResolvedValue(FROM_BACKUP);

    await waitFor(() => expect(toast).toHaveBeenCalledWith({ description: AWAY_RETRY, variant: 'destructive' }));
    expect(screen.queryByTestId('editor')).toBeNull();
    expect(await screen.findByText(AWAY_BACKUP)).toBeTruthy();
    expect(screen.queryByTitle('Edit file')).toBeNull();
  });

  it('refuses to open the editor on a stopped workspace\'s backup, which gives no version to save against', async () => {
    vi.mocked(readWorkspaceFile).mockResolvedValue({ ...LIVE, from: STOPPED });
    vi.mocked(readWorkspaceFileFull).mockResolvedValue({ ...LIVE_FULL, version: null, from: STOPPED });
    renderWithProviders(panel());
    fireEvent.click(await screen.findByTitle('Edit file'));

    await waitFor(() => expect(toast).toHaveBeenCalledWith({
      description: 'The sandbox is not available. Start the workspace to access files.',
      variant: 'destructive',
    }));
    expect(screen.queryByTestId('editor')).toBeNull();
    expect(writeWorkspaceFile).not.toHaveBeenCalled();
  });

  it('saves against the version it read, and keeps the draft when the file changed since', async () => {
    vi.mocked(readWorkspaceFile).mockResolvedValue(LIVE);
    vi.mocked(readWorkspaceFileFull).mockResolvedValue(LIVE_FULL);
    vi.mocked(writeWorkspaceFile).mockRejectedValue({
      response: { status: 409, data: { detail: { code: 'file_changed', message: 'changed' } } },
    });
    renderWithProviders(panel());

    fireEvent.click(await screen.findByTitle('Edit file'));
    const editor = await screen.findByTestId('editor');
    await act(async () => { fireEvent.change(editor, { target: { value: '# Notes, revised' } }); });
    fireEvent.click(screen.getByTitle('Save (Cmd+S)'));
    const dialog = await screen.findByRole('dialog');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save' }));

    // The text it began from goes by digest too, so a file only touched since still takes the save.
    const loaded = createHash('sha256').update('# Notes').digest('hex');
    await waitFor(() => expect(writeWorkspaceFile).toHaveBeenCalledWith('ws', 'notes.md', '# Notes, revised', 'v1', loaded));
    expect(await screen.findByText(/This file changed after you started editing/)).toBeTruthy();
    expect((screen.getByTestId('editor') as HTMLTextAreaElement).value).toBe('# Notes, revised');
  });

  it('waits for a read with a version before opening the editor', async () => {
    // Read while the agent rewrote it, the file has no version, and a save
    // naming none would land unchecked over whatever comes next.
    vi.mocked(readWorkspaceFile).mockResolvedValue(LIVE);
    vi.mocked(readWorkspaceFileFull).mockResolvedValue({ ...LIVE_FULL, version: null, rewriting: true });
    renderWithProviders(panel());
    fireEvent.click(await screen.findByTitle('Edit file'));

    await waitFor(() => expect(toast).toHaveBeenCalledWith({
      description: 'This file was changing as it opened for editing. Try Edit again in a moment.',
      variant: 'destructive',
    }));
    expect(screen.queryByTestId('editor')).toBeNull();
    expect(screen.getByTitle('Edit file')).toBeTruthy();
  });
});
