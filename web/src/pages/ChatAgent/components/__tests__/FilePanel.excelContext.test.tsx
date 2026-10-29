import React from 'react';
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
    downloadWorkspaceFileAsArrayBuffer: vi.fn(async () => new ArrayBuffer(8)),
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

// The real viewer parses a workbook; the panel only has to hand it a callback
// and answer for what that callback does to the tab.
vi.mock('@/pages/ChatAgent/components/viewers/ExcelViewer', () => ({
  default: ({ onAddContext }: { onAddContext?: (c: unknown) => void }) => (
    <button type="button" onClick={() => onAddContext?.({ path: 'model.xlsx', locator: 'Model!A1:B2' })}>add-range</button>
  ),
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';

const FILES = ['model.xlsx', 'notes.md'];

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe('FilePanel Excel range context', () => {
  it('pins the workbook’s tab once a range from it is added to the context', async () => {
    const onAddContext = vi.fn();
    const { rerender } = renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={FILES} onAddContext={onAddContext} target={{ kind: 'file', path: 'model.xlsx', seq: 1 }} />,
    );
    const tab = await screen.findByRole('tab', { name: /model\.xlsx/ });
    expect(tab.className).toContain('is-preview');

    fireEvent.click(await screen.findByText('add-range'));
    expect(onAddContext).toHaveBeenCalledWith(expect.objectContaining({ path: 'model.xlsx', locator: 'Model!A1:B2' }));
    expect(screen.getByRole('tab', { name: /model\.xlsx/ }).className).not.toContain('is-preview');

    // The next single open no longer takes the workbook's tab.
    rerender(
      <FilePanel workspaceId="ws" onClose={() => {}} files={FILES} onAddContext={onAddContext} target={{ kind: 'file', path: 'notes.md', seq: 2 }} />,
    );
    await screen.findByRole('tab', { name: /notes\.md/ });
    expect(within(screen.getByRole('tablist')).getAllByRole('tab')).toHaveLength(2);
  });
});
