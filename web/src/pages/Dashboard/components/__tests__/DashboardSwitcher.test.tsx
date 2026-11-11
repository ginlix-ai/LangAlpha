import { describe, it, expect, vi } from 'vitest';
import type { ReactElement } from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import { createTestQueryClient } from '@/test/utils';
import { OPEN_SWITCHER, type SavedDashboard, type SavedDashboardsApi } from '@/lib/dashboards';
import { DashboardSwitcher } from '../DashboardSwitcher';

vi.mock('../../hooks/useDashboardAgent', () => ({
  useDashboardAgent: () => ({ build: vi.fn(), edit: vi.fn() }),
}));

const entry = (code: string, title: string): SavedDashboard => ({
  kind: 'app', code, title, path: null, workspaceId: null, threadId: null,
});

const api = (current: SavedDashboard | null = null): SavedDashboardsApi => ({
  list: [entry('a', 'Alpha'), entry('b', 'Beta')],
  current,
  select: vi.fn(),
  add: vi.fn(),
  remove: vi.fn(),
  canAdd: vi.fn(),
});

// Sent here to make room for another, so the menu is open on arrival.
function renderAsked(ui: ReactElement) {
  return render(
    <QueryClientProvider client={createTestQueryClient()}>
      <MemoryRouter initialEntries={[{ pathname: '/dashboard', state: { [OPEN_SWITCHER]: true } }]}>{ui}</MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('DashboardSwitcher', () => {
  it('removes a row in one press without choosing it', () => {
    const dashboards = api();
    renderAsked(<DashboardSwitcher dashboards={dashboards} />);
    fireEvent.click(screen.getByRole('menuitem', { name: 'Remove Beta' }));
    expect(dashboards.remove).toHaveBeenCalledWith('b');
    expect(dashboards.select).not.toHaveBeenCalled();
  });

  it('asks to see the one already chosen rather than saving it again', () => {
    const dashboards = api(entry('a', 'Alpha'));
    const onReselect = vi.fn();
    renderAsked(<DashboardSwitcher dashboards={dashboards} onReselect={onReselect} />);
    fireEvent.click(screen.getByRole('menuitemradio', { name: 'Alpha' }));
    expect(onReselect).toHaveBeenCalled();
    expect(dashboards.select).not.toHaveBeenCalled();
  });
});
