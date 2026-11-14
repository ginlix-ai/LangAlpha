import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { queryKeys } from '@/lib/queryKeys';
import { allWorkspacesKey } from '@/hooks/useAllWorkspaces';
import { WorkspaceCreatedSlip } from '../WorkspaceCreatedSlip';

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  getWorkspaces: vi.fn(async () => ({ workspaces: [], total: 0 })),
}));

function Where() {
  return <span data-testid="where">{useLocation().pathname}</span>;
}

function renderSlip(qc: QueryClient, workspaceId?: string) {
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={['/chat/home']}>
        <Routes>
          <Route path="*" element={<><WorkspaceCreatedSlip name="Lithium" workspaceId={workspaceId} description="Lithium miners" /><Where /></>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('WorkspaceCreatedSlip', () => {
  it('names the workspace and what it is for, and opens it once the app holds it', () => {
    const qc = new QueryClient();
    qc.setQueryData(queryKeys.workspaces.list({ limit: 20 }), {
      workspaces: [{ workspace_id: 'ws-1', name: 'Lithium', status: 'running' }],
    });
    renderSlip(qc, 'ws-1');

    expect(screen.getByText('Workspace created')).toBeTruthy();
    expect(screen.getByText('Lithium miners')).toBeTruthy();
    fireEvent.click(screen.getByRole('button', { name: 'Lithium' }));
    expect(screen.getByTestId('where').textContent).toBe('/chat/ws-1');
  });

  it('follows the id the create printed through a rename, never a later namesake', () => {
    const qc = new QueryClient();
    qc.setQueryData(queryKeys.workspaces.list({ limit: 20 }), {
      workspaces: [
        { workspace_id: 'ws-new', name: 'Lithium', status: 'running' },
        { workspace_id: 'ws-1', name: 'Battery metals', status: 'running' },
      ],
    });
    renderSlip(qc, 'ws-1');

    fireEvent.click(screen.getByRole('button', { name: 'Lithium' }));
    expect(screen.getByTestId('where').textContent).toBe('/chat/ws-1');
  });

  it('reads every workspace once as it shows when no list holds it, and is words until then', async () => {
    const qc = new QueryClient();
    // A list written after the create, by a read already in flight or a patch
    // to another row, is no proof it was read again.
    qc.setQueryData(allWorkspacesKey('custom', false), { workspaces: [], total: 0 }, { updatedAt: Date.now() + 60_000 });
    const cancel = vi.spyOn(qc, 'cancelQueries');
    const prefetch = vi.spyOn(qc, 'prefetchQuery');
    renderSlip(qc, 'ws-1');

    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.getByText('Lithium')).toBeTruthy();
    // A read already in flight may have left before the create: it is not the one.
    expect(cancel).toHaveBeenCalledWith(expect.objectContaining({ queryKey: allWorkspacesKey('custom', false), exact: true }));
    await waitFor(() => expect(prefetch).toHaveBeenCalledTimes(1));
    expect(prefetch).toHaveBeenCalledWith(expect.objectContaining({ queryKey: allWorkspacesKey('custom', false), staleTime: 0 }));
  });

  it('opens nothing for a create that printed no id, even beside a workspace of its name', () => {
    const qc = new QueryClient();
    // The one it made may be gone, and this a later one under its name.
    qc.setQueryData(queryKeys.workspaces.list({ limit: 20 }), {
      workspaces: [{ workspace_id: 'ws-new', name: 'Lithium', status: 'running' }],
    });
    const prefetch = vi.spyOn(qc, 'prefetchQuery');
    renderSlip(qc);

    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.getByText('Lithium')).toBeTruthy();
    expect(prefetch).not.toHaveBeenCalled();
  });
});
