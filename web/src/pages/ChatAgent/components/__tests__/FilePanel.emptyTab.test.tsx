/**
 * The empty tab is the file listing: the tree lies across the whole body
 * rather than beside a placeholder, and a file opened from it takes the body,
 * with the tree back to being a column the reader can fold. A right click on
 * the new tab button picks what the new tab opens on instead.
 */
import type { ComponentProps } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    readWorkspaceFile: vi.fn(async () => ({ content: '# Notes', mime: 'text/markdown', truncated: false, from: { kind: 'live' } })),
    readWorkspaceFileFull: vi.fn(async () => ({ content: '# Notes', version: null, rewriting: false, from: { kind: 'live' } })),
    writeWorkspaceFile: vi.fn(async () => ({ version: null })),
    downloadWorkspaceFileAsArrayBuffer: vi.fn(),
    triggerFileDownload: vi.fn(),
    resolveWorkspaceFile: vi.fn(),
    getPreviewUrl: vi.fn(async () => ({ url: 'https://8050-sandbox.example.com/' })),
  };
});
// The reader has turned the browser on.
vi.mock('@/pages/ChatAgent/hooks/useBrowserHere', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/hooks/useBrowserHere')>()),
  useBrowserHere: (computer: object | null | undefined, shared = false) => !browserOff && (computer != null || shared),
  useBrowserGone: () => browserOff,
}));
vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => ({ data: { status: 'running', name: 'ws', computer_id: 'c1' } }) }));
let browserOff = false;
vi.mock('@/pages/ChatAgent/hooks/useComputers', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useComputer: (id: string | null) => ({ data: id ? { computer_id: id, kind: 'docker', name: 'box', status: 'running' } : null }),
}));
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
vi.mock('@/pages/MarketView/components/MarketChartSurface', () => ({
  MarketChartSurface: (props: { symbol: string }) => <div data-testid="chart-surface" data-symbol={props.symbol} />,
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import { BrowserViewSession } from '@/pages/ChatAgent/components/filePanel/browserStream';

const DASHBOARD = { kind: 'preview', port: 8050, title: 'Dashboard', command: 'python app.py', seq: 1 } as const;

const panel = (target: ComponentProps<typeof FilePanel>['target'] = null, browserView = new BrowserViewSession()) => (
  <FilePanel
    workspaceId="ws"
    threadId="thread-1"
    onClose={() => {}}
    files={['notes.md', 'todo.md']}
    target={target}
    browserView={browserView}
  />
);

const tabNames = () => within(screen.getByRole('tablist')).getAllByRole('tab').map((tab) => tab.textContent);

async function openFromTheTree(path: string): Promise<void> {
  fireEvent.click(document.querySelector(`[data-row-path="${path}"]`)!);
  await screen.findByText(path, { selector: '.file-panel-tab-name' });
}

beforeEach(() => {
  localStorage.clear();
  browserOff = false;
});
afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

describe('FilePanel empty tab', () => {
  it('lays the tree across the body, with no column to fold and nothing behind it', () => {
    renderWithProviders(panel());

    expect(document.querySelector('.file-panel-tree.is-full')).toBeTruthy();
    expect(document.querySelector('.file-panel-empty-tab')).toBeNull();
    expect(screen.queryByTitle('Toggle file tree')).toBeNull();
  });

  it('gives the body to a file opened from it and makes the tree a column again', async () => {
    renderWithProviders(panel());

    fireEvent.click(document.querySelector('[data-row-path="notes.md"]')!);

    await waitFor(() => expect(screen.getByText('notes.md', { selector: '.file-panel-tab-name' })).toBeTruthy());
    expect(screen.getByTitle('Toggle file tree')).toBeTruthy();
    expect(document.querySelector('.file-panel-empty-tab')).toBeNull();
  });
});

describe('FilePanel new tab menu', () => {
  it('opens a new tab on the files or straight on a chart', async () => {
    renderWithProviders(panel());
    await openFromTheTree('notes.md');

    fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
    const menu = await screen.findByRole('menu');
    expect(within(menu).getAllByRole('menuitem').map((item) => item.textContent)).toEqual(['Files', 'Chart', 'Browser', 'Status']);
    fireEvent.click(within(menu).getByRole('menuitem', { name: 'Chart' }));

    await screen.findByTestId('chart-surface');
    expect(tabNames()).toHaveLength(2);
    expect(document.querySelector('.file-panel-tree')).toBeNull();
  });

  it("opens the thread's Status once, however often it is picked", async () => {
    renderWithProviders(panel());
    await openFromTheTree('notes.md');

    for (let i = 0; i < 2; i += 1) {
      fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
      fireEvent.click(within(await screen.findByRole('menu')).getByRole('menuitem', { name: 'Status' }));
    }

    expect(tabNames().filter((name) => name === 'Status')).toHaveLength(1);
  });

  it('offers no Status where there is no thread', async () => {
    renderWithProviders(<FilePanel workspaceId="ws" onClose={() => {}} files={['notes.md', 'todo.md']} target={null} />);
    await openFromTheTree('notes.md');

    fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
    const menu = await screen.findByRole('menu');

    expect(within(menu).getAllByRole('menuitem').map((item) => item.textContent)).toEqual(['Files', 'Chart']);
  });

  it('offers no Browser tab where the reader turned the browser off', async () => {
    browserOff = true;
    renderWithProviders(panel());
    expect(screen.queryByRole('button', { name: 'Browser' })).toBeNull();
    await openFromTheTree('notes.md');

    fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
    const menu = await screen.findByRole('menu');
    expect(within(menu).getAllByRole('menuitem').map((item) => item.textContent)).toEqual(['Files', 'Chart', 'Status']);
  });

  it('pins the Browser beside the listing on a computer with one', () => {
    renderWithProviders(panel());
    expect(screen.getByRole('button', { name: 'Browser' })).toBeTruthy();
  });

  it('closes a Browser tab kept from before once the reader turns the browser off', async () => {
    const view = new BrowserViewSession();
    const { rerender } = renderWithProviders(panel(null, view));
    await openFromTheTree('notes.md');
    fireEvent.click(screen.getByRole('button', { name: 'Browser' }));
    await waitFor(() => expect(tabNames()).toContain('Browser'));

    browserOff = true;
    rerender(panel(null, view));
    await waitFor(() => expect(tabNames()).toEqual(['notes.md']));
  });

  it('opens the files as a plain click does, on the whole body', async () => {
    renderWithProviders(panel());
    await openFromTheTree('notes.md');

    fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
    fireEvent.click(within(await screen.findByRole('menu')).getByRole('menuitem', { name: 'Files' }));

    expect(tabNames()).toHaveLength(2);
    expect(document.querySelector('.file-panel-tree.is-full')).toBeTruthy();
  });

  it('closes on Escape without leaving the tree\'s selection', async () => {
    renderWithProviders(panel());
    await openFromTheTree('notes.md');
    fireEvent.click(screen.getByTitle('Select files'));

    fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
    fireEvent.keyDown(await screen.findByRole('menu'), { key: 'Escape' });

    await waitFor(() => expect(screen.queryByRole('menu')).toBeNull());
    expect(screen.getByTitle('Cancel selection')).toBeTruthy();
  });

  it('opens on a long press without the release opening a tab too', async () => {
    renderWithProviders(panel());
    await openFromTheTree('notes.md');
    const button = screen.getByRole('button', { name: 'New tab' });

    fireEvent.pointerDown(button, { pointerType: 'touch' });
    await screen.findByRole('menu', {}, { timeout: 1500 });
    fireEvent.pointerUp(button, { pointerType: 'touch' });
    fireEvent.click(button);

    // The open menu hides the strip from the accessibility tree.
    expect(document.querySelectorAll('[role="tab"]')).toHaveLength(1);
  });

  it('lists the running apps after the rest', async () => {
    renderWithProviders(panel(DASHBOARD));
    await screen.findByTitle('App on port 8050');

    fireEvent.contextMenu(screen.getByRole('button', { name: 'New tab' }));
    const items = within(await screen.findByRole('menu')).getAllByRole('menuitem');

    expect(items.at(-1)!.textContent).toBe('Dashboard');
  });
});
