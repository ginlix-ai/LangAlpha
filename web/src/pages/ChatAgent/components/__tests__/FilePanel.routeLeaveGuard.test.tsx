/**
 * A tool tab's result under an unsaved edit. The links in a result view change
 * the route directly, and a route change fires no beforeunload, so the panel
 * and its drafts would go with no question asked. The link asks the panel
 * first, through the same guard its own close and chart action use.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent, act, within } from '@testing-library/react';
import { Route, Routes } from 'react-router';
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

import FilePanel, { type PanelTarget } from '@/pages/ChatAgent/components/FilePanel';
import type { ToolCallProcessRecord } from '@/pages/ChatAgent/components/ToolCallDetailView';
import { createTranscriptStore } from '@/pages/ChatAgent/components/filePanel/transcriptStore';

// An automations listing, whose result view ends in a link to the Automations page.
const automationsCall: ToolCallProcessRecord = {
  toolName: 'list_automations',
  toolCall: { id: 'call-1', name: 'list_automations', args: {} },
  toolCallResult: {
    content: '1 automation',
    artifact: {
      type: 'automations',
      mode: 'list',
      total: 1,
      automations: [{ automation_id: 'auto-1', name: 'Daily brief', status: 'active', trigger_type: 'cron', schedule: '0 9 * * *' }],
    },
  },
  isComplete: true,
};

const transcript = createTranscriptStore({ messages: [{ toolCallProcesses: { 'call-1': automationsCall } }] }).reader;

const FILE: PanelTarget = { kind: 'file', path: 'notes.md', seq: 1 };
const TOOL: PanelTarget = { kind: 'tool', toolCallId: 'call-1', seq: 2 };

const app = (target: PanelTarget) => (
  <Routes>
    <Route
      path="/"
      element={(
        <FilePanel
          workspaceId="ws"
          onClose={() => {}}
          files={['notes.md']}
          target={target}
          transcript={transcript}
        />
      )}
    />
    <Route path="/automations" element={<div data-testid="automations-page" />} />
  </Routes>
);

/** Opens the file, enters edit mode and changes a line. */
async function dirtyTheDraft(): Promise<void> {
  await screen.findByText('notes.md', { selector: '.file-panel-tab-name' });
  fireEvent.click(await screen.findByTitle('Edit file'));
  const editor = await screen.findByTestId('editor');
  await act(async () => { fireEvent.change(editor, { target: { value: '# Notes, revised' } }); });
}

// The tool tab's body is a lazy chunk, slower to land under a loaded suite than the default wait.
const findAutomationsLink = () => screen.findByRole('button', { name: 'View in Automations' }, { timeout: 5000 });

/** Dirties the file tab, then brings a tool tab in front of it and finds its link off the route. */
async function dirtyThenOpenToolTab() {
  const view = renderWithProviders(app(FILE));
  await dirtyTheDraft();
  view.rerender(app(TOOL));
  return findAutomationsLink();
}

/** Answers the discard question the link asked. */
async function answer(choice: 'Discard' | 'Cancel') {
  const dialog = await screen.findByRole('dialog');
  expect(within(dialog).getByText('You have unsaved changes. Discard them?')).toBeTruthy();
  fireEvent.click(within(dialog).getByRole('button', { name: choice }));
}

beforeEach(() => { localStorage.clear(); });
afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

describe('FilePanel tool-tab links under an unsaved edit', () => {
  it('stays on the route when the edit is not discarded', async () => {
    const link = await dirtyThenOpenToolTab();

    fireEvent.click(link);
    await answer('Cancel');

    expect(screen.queryByTestId('automations-page')).toBeNull();
    expect(screen.getByRole('button', { name: 'View in Automations' })).toBeTruthy();
  });

  it('leaves once the edit is discarded', async () => {
    const link = await dirtyThenOpenToolTab();

    fireEvent.click(link);
    await answer('Discard');

    expect(await screen.findByTestId('automations-page')).toBeTruthy();
  });

  it('leaves without asking when nothing is unsaved', async () => {
    renderWithProviders(app(TOOL));
    const link = await findAutomationsLink();

    fireEvent.click(link);

    expect(screen.queryByRole('dialog')).toBeNull();
    expect(await screen.findByTestId('automations-page')).toBeTruthy();
  });
});
