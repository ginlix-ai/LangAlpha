/**
 * A "show more" pressed from the keyboard lands its focus on the first thread
 * the page adds, and the swap to the interactive kit, which remounts every
 * row, waits for it. Its own file so the kit's module state starts empty and
 * the first render is static.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import NavigationPanel from '../NavigationPanel';
import { expandedWorkspaces, notifyNavExpansion } from '../navExpansionStore';
import type { NavWorkspace } from '../../hooks/useNavigationData';
import type { ThreadsData } from '../NavigationRows';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const WORKSPACES: NavWorkspace[] = [{ workspace_id: 'ws-a', name: 'A', is_home: false }];
const FIRST: ThreadsData['threads'] = [{ thread_id: 't-1', title: 'One' }];

let page: (threads: ThreadsData) => void = () => {};

function Panel() {
  const [client] = React.useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }));
  const [threads, setThreads] = React.useState<ThreadsData>({ threads: FIRST, total: 2, loading: false });
  React.useLayoutEffect(() => { page = setThreads; }, []);
  return (
    <MemoryRouter>
      <QueryClientProvider client={client}>
        <NavigationPanel
          workspaces={WORKSPACES}
          workspaceThreads={{ 'ws-a': threads }}
          currentWorkspaceId="ws-a"
          currentThreadId={null}
          expandWorkspace={vi.fn()}
          onSelectAgent={vi.fn()}
          onNavigateThread={vi.fn()}
          onLoadMoreThreads={() => setThreads((now) => ({ ...now, loading: true }))}
          onReorderWorkspace={vi.fn()}
        />
      </QueryClientProvider>
    </MemoryRouter>
  );
}

describe('nav tree kit swap', () => {
  it('waits for a keyboard "show more" to land its focus', async () => {
    expandedWorkspaces.add('ws-a');
    notifyNavExpansion();
    const { container } = render(<Panel />);
    const interactive = () => container.querySelector('[data-ws-id] [data-drag-handle][aria-describedby]');
    expect(interactive()).toBeNull();

    // From the keyboard, in one step: the focus starts the kit's download.
    fireEvent.keyDown(window, { key: 'Enter' });
    const more = container.querySelector<HTMLElement>('[data-nav-key="show-more"]')!;
    more.focus();
    fireEvent.click(more);
    expect(container.querySelector('[data-nav-key="show-more"]')).toBeNull();

    // Long past the kit's arrival and its retry: the swap holds meanwhile.
    await act(async () => { await new Promise((r) => setTimeout(r, 600)); });
    expect(interactive()).toBeNull();

    act(() => page({ threads: [...FIRST, { thread_id: 't-2', title: 'Two' }], total: 2, loading: false }));
    expect(document.activeElement).toBe(container.querySelector('[data-nav-key="thread:t-2:title"]'));
    // Landed, the swap goes ahead.
    (document.activeElement as HTMLElement).blur();
    await waitFor(() => expect(interactive()).not.toBeNull());
    expect(screen.getByText('Two')).toBeInTheDocument();
  });
});
