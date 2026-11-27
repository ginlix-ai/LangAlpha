/**
 * Load all sits outside every workspace section, so the swap from the static
 * kit to the interactive one finds its twin by nav key alone. Its own file so
 * the kit's module state starts empty and the first render is static.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import NavigationPanel from '../NavigationPanel';
import type { NavWorkspace } from '../../hooks/useNavigationData';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

function Providers({ children }: { children: React.ReactNode }) {
  const [client] = React.useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }));
  return (
    <MemoryRouter>
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    </MemoryRouter>
  );
}

const WORKSPACES: NavWorkspace[] = [
  { workspace_id: 'ws-a', name: 'A', is_home: false },
  { workspace_id: 'ws-b', name: 'B', is_home: false },
];

describe('nav tree kit swap', () => {
  it('carries focus on Load all to the new Load all', async () => {
    const { container } = render(
      <NavigationPanel
        workspaces={WORKSPACES}
        workspaceThreads={{}}
        currentWorkspaceId="ws-a"
        currentThreadId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onNavigateThread={vi.fn()}
        hasMore
        onLoadMore={vi.fn()}
        onReorderWorkspace={vi.fn()}
      />,
      { wrapper: Providers },
    );
    const handles = () => Array.from(container.querySelectorAll('[data-ws-id] [data-drag-handle]'));
    expect(handles().filter((handle) => handle.hasAttribute('aria-describedby'))).toEqual([]);

    // One synchronous click: the focus it brings starts the kit's download,
    // which must not land before the click does.
    const loadAll = screen.getByRole('button', { name: 'nav.loadAll' });
    fireEvent.pointerDown(loadAll);
    loadAll.focus();
    fireEvent.pointerUp(loadAll);
    fireEvent.click(loadAll);
    expect(document.activeElement).toBe(loadAll);
    await waitFor(() => {
      expect(handles().filter((handle) => !handle.hasAttribute('aria-describedby'))).toEqual([]);
    });

    expect(loadAll.isConnected).toBe(false);
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'nav.loadAll' }));
  });
});
