/**
 * The workspace page draws its header before the workspace row arrives, so
 * the header's icon decides nothing until the row says what it is: Home has no
 * machine settings to open. A row that never arrives draws the plain icon,
 * inert, rather than loading for good.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const WORKSPACE_ID = '7f3c1e2a-9b4d-4c6e-8a1f-2d3b4c5e6f70';
const workspace = vi.hoisted(() => ({ current: { data: undefined as unknown, error: null as unknown } }));

vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => workspace.current }));
vi.mock('../../hooks/useWorkspaceFiles', () => ({
  useWorkspaceFiles: () => ({ files: [], loading: false, error: null, refresh: vi.fn() }),
}));
vi.mock('../FilePanel', () => ({ default: () => null, SYSTEM_DIR_PREFIXES: [] }));
vi.mock('../SandboxSettingsPanel', () => ({ default: () => <div data-testid="machine-settings" /> }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getWorkspaceThreads: vi.fn().mockResolvedValue({ threads: [], total: 0 }),
}));

import ThreadGallery from '../ThreadGallery';

function renderGallery() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[`/chat/${WORKSPACE_ID}`]}>
        <Routes>
          <Route
            path="/chat/:workspaceId"
            element={<ThreadGallery workspaceId={WORKSPACE_ID} onBack={vi.fn()} onThreadSelect={vi.fn()} />}
          />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const machineIcon = () => screen.queryByRole('img', { name: 'Workspace' });

describe('ThreadGallery: the header before and without the workspace row', () => {
  beforeEach(() => {
    Element.prototype.scrollIntoView = vi.fn();
  });

  it('holds the icon as a placeholder while the row loads', async () => {
    workspace.current = { data: undefined, error: null };
    renderGallery();
    await screen.findByRole('textbox');
    expect(machineIcon()).toBeNull();
  });

  it('draws the plain icon, inert, once the row has failed', async () => {
    workspace.current = { data: undefined, error: Object.assign(new Error('Request failed with status code 500'), { response: { status: 500 } }) };
    renderGallery();
    await screen.findByRole('textbox');
    fireEvent.click(machineIcon()!);
    expect(screen.queryByTestId('machine-settings')).toBeNull();
  });

  it('opens machine settings from the icon of a loaded workspace', async () => {
    workspace.current = { data: { workspace_id: WORKSPACE_ID, name: 'Research', status: 'running', is_home: false }, error: null };
    renderGallery();
    await screen.findByRole('textbox');
    fireEvent.click(machineIcon()!);
    expect(screen.getByTestId('machine-settings')).toBeInTheDocument();
  });
});
