import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, waitFor, fireEvent, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { tabsStorageKey } from '@/pages/ChatAgent/components/filePanel/useFileTabs';
import { URL_FRESH_MS } from '@/pages/ChatAgent/components/filePanel/usePreviews';

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
    getPreviewUrl: vi.fn(async () => ({ url: 'https://8050-sandbox.example.com/' })),
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

import * as api from '@/pages/ChatAgent/utils/api';
import FilePanel from '@/pages/ChatAgent/components/FilePanel';

const previewUrl = () => api.getPreviewUrl as ReturnType<typeof vi.fn>;
const FILES = ['notes.md', 'app.py'];
const DASHBOARD = { kind: 'preview', port: 8050, title: 'Dashboard', command: 'python app.py', seq: 1 } as const;

const panel = (props: Record<string, unknown> = {}) => (
  <FilePanel workspaceId="ws" onClose={() => {}} files={FILES} {...props} />
);

/** Whether the running app's pane is behind another tab rather than unmounted. */
const hiddenPane = () =>
  screen.getByTitle('App on port 8050').closest('.file-panel-preview-pane')!.hasAttribute('hidden');

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  previewUrl().mockResolvedValue({ url: 'https://8050-sandbox.example.com/' });
});

afterEach(() => {
  vi.useRealTimers();
});

describe('FilePanel running apps', () => {
  it('opens a preview target as a tab and mints a URL for it', async () => {
    renderWithProviders(panel({ target: DASHBOARD }));

    await waitFor(() => expect(previewUrl()).toHaveBeenCalledWith('ws', 8050, 'python app.py', false));
    const frame = await screen.findByTitle('App on port 8050');
    expect(frame.getAttribute('src')).toBe('https://8050-sandbox.example.com/');
    expect(within(screen.getByRole('tablist')).getByText('Dashboard')).toBeTruthy();
    // The app's own actions live in the crumb row, not in a second header.
    expect(screen.getByTitle('Open in browser')).toBeTruthy();
  });

  it('hangs a path suffix off the signed URL', async () => {
    renderWithProviders(panel({ target: { ...DASHBOARD, path: '/timeline.html' } }));

    const frame = await screen.findByTitle('App on port 8050');
    expect(frame.getAttribute('src')).toBe('https://8050-sandbox.example.com/timeline.html');
  });

  it('lists every running app in the tree, and a click brings its tab forward', async () => {
    const { container, rerender } = renderWithProviders(panel({ target: DASHBOARD }));
    await screen.findByTitle('App on port 8050');

    // Open a file over it, then come back through the tree.
    rerender(panel({ target: { kind: 'file', path: 'notes.md' } }));
    await waitFor(() => expect(hiddenPane()).toBe(true));

    const group = container.querySelector('.file-panel-preview-group')!;
    fireEvent.click(within(group as HTMLElement).getByTitle('Dashboard :8050'));
    await waitFor(() => expect(hiddenPane()).toBe(false));
    // Coming back to an app already resolved must not mint a second URL.
    expect(previewUrl()).toHaveBeenCalledTimes(1);
  });

  it('mints again for a tab left behind long enough for its URL to have expired', async () => {
    // Only the clock is faked: the panel's own async work still runs for real.
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-09-18T10:00:00Z'));
    const { rerender } = renderWithProviders(panel({ target: DASHBOARD }));
    await screen.findByTitle('App on port 8050');
    expect(previewUrl()).toHaveBeenCalledTimes(1);

    rerender(panel({ target: { kind: 'file', path: 'notes.md' } }));
    await waitFor(() => expect(hiddenPane()).toBe(true));

    // Back within the window: the URL in hand is trusted.
    vi.setSystemTime(new Date('2026-09-18T10:05:00Z'));
    fireEvent.click(within(screen.getByRole('tablist')).getByText('Dashboard'));
    await waitFor(() => expect(hiddenPane()).toBe(false));
    expect(previewUrl()).toHaveBeenCalledTimes(1);

    rerender(panel({ target: { kind: 'file', path: 'notes.md', seq: 2 } }));
    await waitFor(() => expect(hiddenPane()).toBe(true));

    // Back past it: a fresh URL, minted the ordinary way rather than as a restart.
    vi.setSystemTime(new Date(Date.parse('2026-09-18T10:00:00Z') + URL_FRESH_MS + 1000));
    fireEvent.click(within(screen.getByRole('tablist')).getByText('Dashboard'));
    await waitFor(() => expect(previewUrl()).toHaveBeenCalledTimes(2));
    expect(previewUrl()).toHaveBeenLastCalledWith('ws', 8050, 'python app.py', false);
  });

  it('keeps a running app loaded while another tab is in front', async () => {
    const { rerender } = renderWithProviders(panel({ target: DASHBOARD }));
    const frame = await screen.findByTitle('App on port 8050');

    rerender(panel({ target: { kind: 'file', path: 'notes.md' } }));
    await waitFor(() => expect(hiddenPane()).toBe(true));

    // Same element, still in the tree — a tab switch must not reload the server.
    expect(screen.getByTitle('App on port 8050')).toBe(frame);
  });

  it('re-mints the URL for a tab restored from storage, which stored none', async () => {
    // The entry is in the shape strips were written in before tabs shed the
    // file-only fields, so a strip saved then still comes back.
    localStorage.setItem(tabsStorageKey('ws'), JSON.stringify({
      tabs: [{ path: null, kind: 'preview', preview: false, port: 8050, title: 'Dashboard' }],
      active: 0,
    }));
    renderWithProviders(panel());

    await waitFor(() => expect(previewUrl()).toHaveBeenCalledWith('ws', 8050, undefined, false));
    expect(await screen.findByTitle('App on port 8050')).toBeTruthy();
  });

  it('restarts the server behind the port on Refresh', async () => {
    renderWithProviders(panel({ target: DASHBOARD }));
    await screen.findByTitle('App on port 8050');

    fireEvent.click(screen.getByTitle('Reload app'));
    await waitFor(() => expect(previewUrl()).toHaveBeenCalledWith('ws', 8050, 'python app.py', true));
  });

  it('offers a way back rather than a blank frame when the server is gone', async () => {
    previewUrl().mockRejectedValue(new Error('no listener'));
    const { rerender } = renderWithProviders(panel({ target: DASHBOARD }));

    expect(await screen.findByText('App not running')).toBeTruthy();
    expect(previewUrl()).toHaveBeenCalledTimes(1);

    // A dead port is not retried on every visit: leave for a file and come back.
    const pane = () => screen.getByText('App not running').closest('.file-panel-preview-pane')!;
    rerender(panel({ target: { kind: 'file', path: 'notes.md' } }));
    await waitFor(() => expect(pane().hasAttribute('hidden')).toBe(true));
    fireEvent.click(within(screen.getByRole('tablist')).getByText('Dashboard'));
    await waitFor(() => expect(pane().hasAttribute('hidden')).toBe(false));
    expect(previewUrl()).toHaveBeenCalledTimes(1);

    // Reload app is the retry, and it restarts the process behind the port.
    fireEvent.click(screen.getByTitle('Reload app'));
    await waitFor(() => expect(previewUrl()).toHaveBeenCalledTimes(2));
    expect(previewUrl()).toHaveBeenLastCalledWith('ws', 8050, 'python app.py', true);
  });
});
