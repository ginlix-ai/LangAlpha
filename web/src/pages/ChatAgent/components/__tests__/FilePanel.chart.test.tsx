import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, within, waitFor } from '@testing-library/react';
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
// The real surface opens a websocket and pulls the chart stack; the panel only
// has to mount it with the right symbol and interval. `mounts` counts how many
// times a surface instance came up, so a remount is visible to a test.
const { surface, mounts } = vi.hoisted(() => ({ surface: vi.fn(), mounts: vi.fn() }));
vi.mock('@/pages/MarketView/components/MarketChartSurface', () => ({
  MarketChartSurface: (props: Record<string, unknown>) => {
    surface(props);
    React.useEffect(() => { mounts(); }, []);
    return (
      <div data-testid="chart-surface" data-symbol={props.symbol as string} data-timeframe={props.timeframe as string}>
        <button type="button" onClick={() => (props.onIntervalChange as (i: string) => void)('1hour')}>switch</button>
        <button type="button" onClick={() => (props.onSwitchSymbol as (s: string) => void)('nvda')}>retarget</button>
        {props.headerActions as React.ReactNode}
      </div>
    );
  },
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import { createTranscriptStore } from '@/pages/ChatAgent/components/filePanel/transcriptStore';
import { lastChartStorageKey, tabsStorageKey } from '@/pages/ChatAgent/components/filePanel/useFileTabs';
import { userLocalStorage } from '@/lib/userStorage';

const FILES = ['notes.md'];
const GOOGL = { kind: 'chart', symbol: 'GOOGL', timeframe: '1day', seq: 1 } as const;

const panel = (props: Record<string, unknown> = {}) => (
  <FilePanel workspaceId="ws" onClose={() => {}} files={FILES} {...props} />
);

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe('FilePanel chart tabs', () => {
  it('tells its host what kind of tab is in front, from the first paint to the last', async () => {
    // The host sizes the panel, and a chart has a floor the other kinds do
    // not, so it hears every change and a null once the panel is gone.
    const onActiveTabKindChange = vi.fn();
    const { rerender, unmount } = renderWithProviders(panel({ onActiveTabKindChange }));
    expect(onActiveTabKindChange).toHaveBeenLastCalledWith('empty');

    rerender(panel({ onActiveTabKindChange, target: GOOGL }));
    await screen.findByTestId('chart-surface');
    expect(onActiveTabKindChange).toHaveBeenLastCalledWith('chart');

    unmount();
    expect(onActiveTabKindChange).toHaveBeenLastCalledWith(null);
  });

  it('opens a chart target as a tab and mounts the chart on it', async () => {
    renderWithProviders(panel({ target: GOOGL }));

    const chart = await screen.findByTestId('chart-surface');
    expect(chart.dataset.symbol).toBe('GOOGL');
    expect(chart.dataset.timeframe).toBe('1day');
    expect(within(screen.getByRole('tablist')).getByText('GOOGL')).toBeTruthy();
    expect(surface).toHaveBeenCalledWith(expect.objectContaining({ workspaceId: 'ws' }));
  });

  it('draws another workspace\'s chart when the ask names one, and the panel\'s otherwise', async () => {
    const { rerender } = renderWithProviders(panel({ target: { ...GOOGL, workspaceId: 'ws-art' } }));
    await screen.findByTestId('chart-surface');
    expect(surface).toHaveBeenLastCalledWith(expect.objectContaining({ symbol: 'GOOGL', workspaceId: 'ws-art' }));

    // The same symbol asked for from this workspace comes back to the one tab, retargeted.
    rerender(panel({ target: { ...GOOGL, seq: 2 } }));
    await waitFor(() => expect(surface).toHaveBeenLastCalledWith(expect.objectContaining({ symbol: 'GOOGL', workspaceId: 'ws' })));
    expect(within(screen.getByRole('tablist')).getAllByRole('tab')).toHaveLength(1);
  });

  it('hands the chart to the composer as a one-line pointer, not its data', async () => {
    const onAddContext = vi.fn();
    renderWithProviders(panel({ target: GOOGL, onAddContext }));
    await screen.findByTestId('chart-surface');

    fireEvent.click(screen.getByTitle('Add chart to context'));
    expect(onAddContext).toHaveBeenCalledTimes(1);
    const ctx = onAddContext.mock.calls[0][0];
    expect(ctx).toMatchObject({ source: 'chart', label: 'GOOGL · 1day' });
    expect(ctx.snippet).toContain('GOOGL');
    expect(ctx.path).toBeUndefined();
  });

  it('has no context action where there is no composer to hand it to', async () => {
    renderWithProviders(panel({ target: GOOGL }));
    await screen.findByTestId('chart-surface');
    expect(screen.queryByTitle('Add chart to context')).toBeNull();
  });

  it('asks before leaving for MarketView with an unsaved edit parked on another tab', async () => {
    const onOpenInMarketView = vi.fn();
    const { rerender } = renderWithProviders(panel({ target: { kind: 'file', path: 'notes.md', seq: 1 }, onOpenInMarketView }));
    fireEvent.click(await screen.findByTitle('Edit file'));
    fireEvent.change(await screen.findByTestId('editor'), { target: { value: '# Notes, edited' } });

    rerender(panel({ target: GOOGL, onOpenInMarketView }));
    await screen.findByTestId('chart-surface');

    fireEvent.click(screen.getByTitle('Open in MarketView'));
    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByText('You have unsaved changes. Discard them?')).toBeTruthy();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
    expect(onOpenInMarketView).not.toHaveBeenCalled();
  });

  it('forgets a parked draft once its tab took the editor back and cancelled it', async () => {
    const { rerender } = renderWithProviders(panel({ target: { kind: 'file', path: 'notes.md', seq: 1 } }));
    fireEvent.click(await screen.findByTitle('Edit file'));
    fireEvent.change(await screen.findByTestId('editor'), { target: { value: '# Notes, edited' } });

    rerender(panel({ target: GOOGL }));
    await screen.findByTestId('chart-surface');
    fireEvent.click(within(screen.getByRole('tablist')).getByText('notes.md'));
    fireEvent.click(await screen.findByTitle('Cancel editing'));
    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByText('Discard unsaved changes?')).toBeTruthy();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Discard' }));

    // Still on the tab that cancelled: a copy left parked under its id would
    // keep the page guarding an edit that no longer exists.
    const leave = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(leave);
    expect(leave.defaultPrevented).toBe(false);
  });

  it('leaves for MarketView on the symbol and interval it is showing', async () => {
    const onOpenInMarketView = vi.fn();
    renderWithProviders(panel({ target: GOOGL, onOpenInMarketView }));
    await screen.findByTestId('chart-surface');

    fireEvent.click(screen.getByTitle('Open in MarketView'));
    expect(onOpenInMarketView).toHaveBeenCalledWith({ symbol: 'GOOGL', timeframe: '1day' });
  });

  it('leaves for MarketView on the drawings it is showing, another workspace\'s included', async () => {
    const onOpenInMarketView = vi.fn();
    renderWithProviders(panel({ target: { ...GOOGL, workspaceId: 'ws-art' }, onOpenInMarketView }));
    await screen.findByTestId('chart-surface');

    fireEvent.click(screen.getByTitle('Open in MarketView'));
    expect(onOpenInMarketView).toHaveBeenCalledWith({ symbol: 'GOOGL', timeframe: '1day', workspaceId: 'ws-art' });
  });

  it('turns an empty tab into a chart at once, on the last symbol looked at', async () => {
    userLocalStorage.setItem(lastChartStorageKey('ws'), 'MSFT');
    renderWithProviders(panel());

    fireEvent.click(screen.getByRole('button', { name: 'Chart' }));

    const chart = await screen.findByTestId('chart-surface');
    expect(chart.dataset.symbol).toBe('MSFT');
    // In place: the strip has the chart and nothing else, not an empty tab beside it.
    expect(within(screen.getByRole('tablist')).getAllByRole('tab')).toHaveLength(1);
  });

  it('opens on a broad index before any symbol has been looked at', async () => {
    renderWithProviders(panel());
    fireEvent.click(screen.getByRole('button', { name: 'Chart' }));
    expect((await screen.findByTestId('chart-surface')).dataset.symbol).toBe('SPY');
  });

  it('remembers the last symbol per workspace, not across them', async () => {
    userLocalStorage.setItem(lastChartStorageKey('other'), 'MSFT');
    renderWithProviders(panel({ target: GOOGL }));
    await screen.findByTestId('chart-surface');

    expect(userLocalStorage.getItem(lastChartStorageKey('ws'))).toBe('GOOGL');
    expect(userLocalStorage.getItem(lastChartStorageKey('other'))).toBe('MSFT');
    expect(userLocalStorage.getItem('filePanel.lastChart')).toBeNull();
  });

  it('changes symbol from the chart header, in the same tab', async () => {
    renderWithProviders(panel({ target: GOOGL }));
    await screen.findByTestId('chart-surface');

    fireEvent.click(screen.getByText('retarget'));

    expect((await screen.findByTestId('chart-surface')).dataset.symbol).toBe('NVDA');
    expect(within(screen.getByRole('tablist')).getAllByRole('tab')).toHaveLength(1);
    expect(within(screen.getByRole('tablist')).getByText('NVDA')).toBeTruthy();
  });

  it('follows a symbol switch in place rather than remounting the surface', async () => {
    renderWithProviders(panel({ target: GOOGL }));
    await screen.findByTestId('chart-surface');
    expect(mounts).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('retarget'));
    expect((await screen.findByTestId('chart-surface')).dataset.symbol).toBe('NVDA');

    // The websocket and drawn state live on the mounted surface; a remount
    // would reopen the one and drop the other for a change it follows itself.
    expect(mounts).toHaveBeenCalledTimes(1);
  });

  it('keeps a non-persisting strip out of the workspace seed', async () => {
    userLocalStorage.setItem(tabsStorageKey('ws'), JSON.stringify({ tabs: [{ kind: 'file', path: 'notes.md', preview: false }], active: 0 }));
    renderWithProviders(panel({ target: GOOGL, persistTabs: false }));
    await screen.findByTestId('chart-surface');

    // A gallery browsing beside the threads opened a chart; the seed the
    // threads start from still names the file, not the chart.
    const seed = JSON.parse(userLocalStorage.getItem(tabsStorageKey('ws'))!);
    expect(seed.tabs).toEqual([{ kind: 'file', path: 'notes.md', preview: false }]);
    expect(userLocalStorage.getItem(lastChartStorageKey('ws'))).toBeNull();
  });

  it('puts its actions in the chart header rather than a crumb row of its own', async () => {
    renderWithProviders(panel({ target: GOOGL, onAddContext: vi.fn(), onOpenInMarketView: vi.fn() }));
    const chart = await screen.findByTestId('chart-surface');
    expect(within(chart).getByTitle('Add chart to context')).toBeTruthy();
    expect(within(chart).getByTitle('Open in MarketView')).toBeTruthy();
    expect(document.querySelector('.file-panel-crumbs')).toBeNull();
  });

  it('brings a listing tab to the front when a folder is opened over a chart', async () => {
    // The tree sits beside a file or the empty tab only, so a folder link
    // clicked with a chart in front has to move off the chart first, or the
    // click lands nowhere.
    const { rerender } = renderWithProviders(panel({ target: GOOGL, files: ['notes.md', 'docs/index.md'] }));
    await screen.findByTestId('chart-surface');

    rerender(panel({ target: { kind: 'file', dir: 'docs', seq: 2 }, files: ['notes.md', 'docs/index.md'] }));

    await waitFor(() => expect(document.querySelector('.file-panel-tree-list')).toBeTruthy());
    expect(document.querySelector('.file-panel-tree-scope')?.textContent).toContain('docs/');
    // The chart tab is still there; the listing opened beside it, not over it.
    expect(within(screen.getByRole('tablist')).getByText('GOOGL')).toBeTruthy();
    expect(screen.queryByTestId('chart-surface')).toBeNull();
  });

  it('offers no chart on a read-only panel', () => {
    renderWithProviders(panel({ readOnly: true }));
    expect(document.querySelector('.file-panel-tree')).toBeTruthy();
    expect(screen.queryByRole('button', { name: 'Chart' })).toBeNull();
  });

  it('remembers the interval the toolbar moved to, so the tab comes back there', async () => {
    const { rerender } = renderWithProviders(panel({ target: GOOGL }));
    await screen.findByTestId('chart-surface');
    fireEvent.click(screen.getByText('switch'));

    // Look at a file, then come back to the chart through its tab.
    rerender(panel({ target: { kind: 'file', path: 'notes.md' } }));
    await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
    fireEvent.click(within(screen.getByRole('tablist')).getByText('GOOGL'));

    const chart = await screen.findByTestId('chart-surface');
    expect(chart.dataset.timeframe).toBe('1hour');
  });
});

describe('FilePanel tool tabs', () => {
  it('says the call is gone when its record has left the chat', async () => {
    renderWithProviders(panel({ target: { kind: 'tool', toolCallId: 'call-1', seq: 1 }, transcript: createTranscriptStore({ messages: [] }).reader }));
    expect(await screen.findByText(/This tool call is no longer in the chat/)).toBeTruthy();
  });
});
