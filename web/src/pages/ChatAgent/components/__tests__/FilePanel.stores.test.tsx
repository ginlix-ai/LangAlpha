/**
 * Memory, Memo and Status as tabs of the file panel: one tab each, reached
 * from the tree's pinned rows, a transcript target, or (for Status) the
 * market watch itself, which adds the tab without taking focus.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, within } from '@testing-library/react';
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
// The bodies are the panels that used to sit behind the segments; here each
// reports the entry it was pointed at, which is all the panel has to hand it.
vi.mock('@/pages/ChatAgent/components/MemoryPanel', () => ({
  default: ({ targetKey, targetTier }: { targetKey?: string | null; targetTier?: string | null }) => (
    <div data-testid="memory-panel" data-key={targetKey ?? ''} data-tier={targetTier ?? ''}>memory</div>
  ),
}));
vi.mock('@/pages/ChatAgent/components/MemoPanel', () => ({
  default: ({ targetKey }: { targetKey?: string | null }) => (
    <div data-testid="memo-panel" data-key={targetKey ?? 'none'}>memo</div>
  ),
}));
vi.mock('@/pages/ChatAgent/components/StatusPanel', () => ({
  default: ({ marketWatch }: { marketWatch?: { symbols: string[] } | null }) => (
    <div data-testid="status-panel">{(marketWatch?.symbols ?? []).join(',')}</div>
  ),
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import { tabsStorageKey } from '@/pages/ChatAgent/components/filePanel/useFileTabs';
import { userLocalStorage } from '@/lib/userStorage';

const FILES = ['notes.md'];

const panel = (props: Record<string, unknown> = {}) => (
  <FilePanel workspaceId="ws" onClose={() => {}} files={FILES} {...props} />
);

const tabNames = () => within(screen.getByRole('tablist')).getAllByRole('tab').map((el) => el.textContent);
const storeRow = (name: string) => within(document.querySelector<HTMLElement>('.file-panel-store-group')!).getByRole('button', { name });

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe('FilePanel store tabs', () => {
  it('opens the store from its pinned row in the tree', async () => {
    renderWithProviders(panel({ target: { kind: 'file', path: 'notes.md', seq: 1 } }));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });

    fireEvent.click(storeRow('Memo'));

    expect(await screen.findByTestId('memo-panel')).toBeTruthy();
    // A file being read keeps its tab; the store opens beside it.
    expect(tabNames()).toEqual(['notes.md', 'Memo']);
  });

  it('opens each store once: a second ask focuses the tab it already has', async () => {
    const { rerender } = renderWithProviders(panel());
    fireEvent.click(storeRow('Memory'));
    await screen.findByTestId('memory-panel');

    // Look at a file, then ask for memory again from the transcript.
    rerender(panel({ target: { kind: 'file', path: 'notes.md', seq: 1, pin: true } }));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    rerender(panel({ target: { kind: 'memory', key: 'notes.md', tier: 'user' } }));

    expect(await screen.findByTestId('memory-panel')).toBeTruthy();
    expect(tabNames()).toEqual(['Memory', 'notes.md']);
  });

  it('opens the Memory tab on the entry a memory target names', async () => {
    renderWithProviders(panel({ target: { kind: 'memory', key: 'risk-preferences.md', tier: 'workspace' } }));

    const body = await screen.findByTestId('memory-panel');
    expect(body.dataset.key).toBe('risk-preferences.md');
    expect(body.dataset.tier).toBe('workspace');
    expect(tabNames()).toEqual(['Memory']);
  });

  it('opens the Memo tab on its list for the index target', async () => {
    renderWithProviders(panel({ target: { kind: 'memo', key: '' } }));

    expect((await screen.findByTestId('memo-panel')).dataset.key).toBe('');
    expect(tabNames()).toEqual(['Memo']);
  });

  it('offers no store where the reader has none', () => {
    // A share reads through an adapter and has no workspace of its own.
    renderWithProviders(panel({ workspaceId: '', apiAdapter: {}, target: { kind: 'file', path: 'notes.md', seq: 1 } }));

    expect(screen.queryByRole('button', { name: 'Memory' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Memo' })).toBeNull();
  });

  it('offers the stores from the strip where the panel is locked to one file', async () => {
    // A link into a sibling workspace shows only that file: no tree, so no pinned rows.
    renderWithProviders(panel({ singleFileMode: true, readOnly: true, target: { kind: 'file', path: 'notes.md', seq: 1 } }));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    expect(document.querySelector('.file-panel-store-group')).toBeNull();

    const strip = document.querySelector<HTMLElement>('.file-panel-strip-right')!;
    fireEvent.click(within(strip).getByRole('button', { name: 'Memory' }));
    expect(await screen.findByTestId('memory-panel')).toBeTruthy();

    fireEvent.click(within(strip).getByRole('button', { name: 'Memo' }));
    expect(await screen.findByTestId('memo-panel')).toBeTruthy();
    expect(tabNames()).toEqual(['notes.md', 'Memory', 'Memo']);
  });

  it('keeps the strip clear of store buttons where the tree pins them', async () => {
    renderWithProviders(panel({ target: { kind: 'file', path: 'notes.md', seq: 1 } }));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });

    const strip = document.querySelector<HTMLElement>('.file-panel-strip-right')!;
    expect(within(strip).queryByRole('button', { name: 'Memory' })).toBeNull();
    expect(storeRow('Memory')).toBeTruthy();
  });

  it('keeps the store tabs in the strip it saves, and not the watch', async () => {
    renderWithProviders(panel({ marketWatch: { symbols: [] } }));
    fireEvent.click(storeRow('Memory'));
    await screen.findByTestId('memory-panel');

    expect(JSON.parse(userLocalStorage.getItem(tabsStorageKey('ws'))!).tabs).toEqual([{ kind: 'memory' }]);
  });
});

describe('FilePanel status tab', () => {
  it('adds the Status tab as a watch starts, and leaves it showing nothing watched as the watch ends', async () => {
    const { rerender } = renderWithProviders(panel({ marketWatch: { symbols: [] } }));
    expect(screen.queryByTestId('status-panel')).toBeNull();

    rerender(panel({ marketWatch: { symbols: ['NVDA', 'TSLA'] } }));
    expect(tabNames()).toEqual(['Open a file', 'Status']);
    fireEvent.click(within(screen.getByRole('tablist')).getByRole('tab', { name: /Status/ }));
    expect((await screen.findByTestId('status-panel')).textContent).toBe('NVDA,TSLA');

    // The tab is the reader's to close: taken away it would go from under
    // them, and a watch resumed would bring it back unfocused as a new tab.
    rerender(panel({ marketWatch: { symbols: [] } }));
    expect(tabNames()).toEqual(['Open a file', 'Status']);
    expect(screen.getByTestId('status-panel').textContent).toBe('');

    rerender(panel({ marketWatch: { symbols: ['NVDA'] } }));
    expect(tabNames()).toEqual(['Open a file', 'Status']);
    expect(screen.getByTestId('status-panel').textContent).toBe('NVDA');
  });

  it('leaves the reader where they are when the panel mounts under a running watch', () => {
    renderWithProviders(panel({ marketWatch: { symbols: ['NVDA'] } }));
    expect(screen.queryByTestId('status-panel')).toBeNull();
  });

  it('opens on a status target, and consumes it', async () => {
    const onTargetHandled = vi.fn();
    renderWithProviders(panel({ target: { kind: 'status', seq: 1 }, marketWatch: { symbols: ['NVDA'] }, onTargetHandled }));

    expect(await screen.findByTestId('status-panel')).toBeTruthy();
    expect(onTargetHandled).toHaveBeenCalledTimes(1);
  });

  it('keeps the reader on the file they are reading as a watch starts', async () => {
    const { rerender } = renderWithProviders(panel({ target: { kind: 'file', path: 'notes.md', seq: 1, pin: true }, marketWatch: { symbols: [] } }));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });

    rerender(panel({ target: { kind: 'file', path: 'notes.md', seq: 1, pin: true }, marketWatch: { symbols: ['NVDA'] } }));
    expect(tabNames()).toEqual(['notes.md', 'Status']);
    expect(screen.queryByTestId('status-panel')).toBeNull();
  });

  it('does not pull the strip back to a Status tab the reader closed', async () => {
    const { rerender } = renderWithProviders(panel({ marketWatch: { symbols: [] } }));
    rerender(panel({ marketWatch: { symbols: ['NVDA'] } }));
    fireEvent.click(screen.getByLabelText('Close Status'));
    expect(tabNames()).toEqual(['Open a file']);

    // A later stamp on the same watch is not a new watch.
    rerender(panel({ marketWatch: { symbols: ['NVDA'], content: 'As of now\nNVDA 100' } }));
    expect(tabNames()).toEqual(['Open a file']);
  });
});
