/**
 * The gallery's Files card toggles the panel, and closing the panel unmounts
 * the drafts it holds. The toggle leaves through the guard the panel hands up,
 * as the panel's own close does, and a declined ask keeps the panel open.
 */
import React, { useEffect } from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import type { RouteLeaveGuard } from '@/pages/ChatAgent/contexts/RouteLeaveGuardContext';

const { WS, guard } = vi.hoisted(() => ({
  WS: '33333333-3333-4333-8333-333333333333',
  guard: vi.fn<RouteLeaveGuard>(),
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getWorkspaceThreads: vi.fn(async () => ({ threads: [], total: 0 })),
  listWorkspaceFiles: vi.fn(async () => ({ files: ['notes/brief.md'] })),
}));
vi.mock('@/hooks/useWorkspace', () => ({
  useWorkspace: () => ({
    data: { workspace_id: WS, name: 'NVDA', status: 'running', computer_id: 'c-1', dir_name: 'NVDA' },
    error: null,
  }),
}));
vi.mock('@/hooks/useAllWorkspaces', () => ({ useAllWorkspaces: () => ({ data: { workspaces: [] } }) }));
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
// The panel stands in as what the gallery sees of it: a mount that hands up
// its leave guard, answered here the way the reader answers its dialog.
vi.mock('@/pages/ChatAgent/components/FilePanel', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  default: function FilePanelStub({ onLeaveGuardChange }: { onLeaveGuardChange?: (g: RouteLeaveGuard | null) => void }) {
    useEffect(() => {
      onLeaveGuardChange?.(guard);
      return () => onLeaveGuardChange?.(null);
    }, [onLeaveGuardChange]);
    return <div data-testid="file-panel" />;
  },
}));

import ThreadGallery from '../ThreadGallery';

const answer = (leave: boolean) => guard.mockImplementation((go) => { if (leave) go(); });

async function openPanel() {
  renderWithProviders(
    <ThreadGallery workspaceId={WS} onBack={vi.fn()} onThreadSelect={vi.fn()} />,
    { route: `/chat/${WS}` },
  );
  fireEvent.click(await screen.findByText('View all'));
  await screen.findByTestId('file-panel');
}

beforeEach(() => {
  vi.clearAllMocks();
  guard.mockReset();
  localStorage.clear();
});

describe("ThreadGallery's Files toggle with a draft open", () => {
  it('holds the panel on a no', async () => {
    answer(false);
    await openPanel();
    fireEvent.click(screen.getByText('Close'));
    expect(guard).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('file-panel')).toBeTruthy();
  });

  it('closes once the ask is accepted', async () => {
    answer(true);
    await openPanel();
    fireEvent.click(screen.getByText('Close'));
    expect(guard).toHaveBeenCalledTimes(1);
    expect(screen.getByText('View all')).toBeTruthy();
  });
});
