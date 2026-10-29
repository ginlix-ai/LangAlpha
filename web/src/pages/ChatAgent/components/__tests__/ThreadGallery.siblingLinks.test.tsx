/**
 * The gallery's file panel reads a link into another workspace the way the
 * chat's panel does. Home's documents are written by the Chief of Staff, which
 * names every file by its absolute path, so a link through a sibling's folder
 * has to open that workspace's file rather than a lookup inside Home.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';

// Home, bound to the same computer as the NVDA workspace.
const { HOME, NVDA, ROWS, FILES, CONTENT } = vi.hoisted(() => {
  const HOME = '11111111-1111-4111-8111-111111111111';
  const NVDA = '22222222-2222-4222-8222-222222222222';
  return {
    HOME,
    NVDA,
    ROWS: {
      [HOME]: { workspace_id: HOME, name: 'Home', status: 'running', is_home: true, computer_id: 'c-1', dir_name: 'Home' },
      [NVDA]: { workspace_id: NVDA, name: 'NVDA', status: 'running', computer_id: 'c-1', dir_name: 'NVDA' },
    } as Record<string, Record<string, unknown>>,
    FILES: { [HOME]: ['notes/brief.md'], [NVDA]: ['dcf/report.md'] } as Record<string, string[]>,
    CONTENT: {
      [`${HOME}:notes/brief.md`]: '# Brief\n\n[the DCF](/home/workspace/NVDA/dcf/report.md)',
      [`${NVDA}:dcf/report.md`]: '# The NVDA model',
    } as Record<string, string>,
  };
});

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    getWorkspaceThreads: vi.fn(async () => ({ threads: [], total: 0 })),
    listWorkspaceFiles: vi.fn(async (ws: string) => ({ files: FILES[ws] ?? [] })),
    readWorkspaceFile: vi.fn(async (ws: string, p: string) => {
      const content = CONTENT[`${ws}:${p}`];
      if (content == null) throw { response: { status: 404, data: { detail: 'File not found' } } };
      return { content, mime: 'text/markdown', truncated: false };
    }),
    readWorkspaceFileFull: vi.fn(),
    resolveWorkspaceFile: vi.fn(async (_ws: string, candidates: string[]) => ({
      status: 'resolved', path: candidates[0], matches: [candidates[0]],
    })),
  };
});
vi.mock('@/hooks/useWorkspace', () => ({
  useWorkspace: (id: string | null | undefined) => ({ data: id ? ROWS[id] : undefined, error: null }),
}));
vi.mock('@/hooks/useAllWorkspaces', () => ({
  useAllWorkspaces: () => ({ data: { workspaces: Object.values(ROWS) } }),
}));
vi.mock('@/hooks/useWorkspaceLabel', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useWorkspaceLabel: () => (ws: { name?: string } | null | undefined) => ws?.name ?? '',
}));
vi.mock('@/components/ui/chat-input', () => ({ default: () => null }));
vi.mock('@/pages/ChatAgent/components/SandboxSettingsPanel', () => ({
  default: () => null,
  SandboxSettingsContent: () => null,
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

import * as api from '@/pages/ChatAgent/utils/api';
import ThreadGallery from '../ThreadGallery';

const reads = () => vi.mocked(api.readWorkspaceFile).mock.calls.map(([ws, p]) => [ws, p]);

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
});

describe("ThreadGallery's file panel", () => {
  it("opens a Home document's absolute link into a sibling's folder in that workspace", async () => {
    renderWithProviders(
      <ThreadGallery workspaceId={HOME} onBack={vi.fn()} onThreadSelect={vi.fn()} />,
      { route: `/chat/${HOME}` },
    );
    fireEvent.click(await screen.findByText('brief.md'));
    fireEvent.click(await screen.findByText('the DCF'));

    await screen.findByText('The NVDA model');
    expect(reads()).toContainEqual([NVDA, 'dcf/report.md']);
    expect(reads()).not.toContainEqual([HOME, 'NVDA/dcf/report.md']);
    // Read-only, with the way back to Home's own files.
    expect(screen.getByText('In NVDA')).toBeTruthy();
    expect(screen.getByRole('button', { name: 'Home' })).toBeTruthy();
  });
});
