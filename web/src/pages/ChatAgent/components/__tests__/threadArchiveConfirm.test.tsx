/**
 * Archiving a thread stays allowed while its run is live (the run survives —
 * only the row leaves the lists), so the gate is a confirm dialog, not a
 * refusal. Both trigger surfaces route through useArchiveThreadConfirm: the
 * sidebar tree is exercised through NavigationPanel here, and the gallery's
 * card-shaped call through a harness on the same hook.
 *
 * Liveness comes from the real lifecycle store rather than a stubbed selector
 * — publishLocalRunning is the exact path this tab's chat stream takes, so the
 * derivation stays covered; resetThreadLifecycle keeps the cases isolated.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render as rtlRender, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import NavigationPanel from '../NavigationPanel';
import { useArchiveThreadConfirm } from '../threadArchiveAction';
import { resetNavPanelExpansion } from '../navExpansionStore';
import { publishLocalRunning, resetThreadLifecycle } from '@/lib/threadLifecycle/store';

// `t()` identity mock (same as NavigationPanel.test.tsx) — assertions match on
// keys, not on bundled copy.
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

function Providers({ children }: { children: React.ReactNode }) {
  const [client] = React.useState(
    () => new QueryClient({ defaultOptions: { queries: { retry: false } } }),
  );
  return (
    <MemoryRouter>
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    </MemoryRouter>
  );
}

function render(ui: React.ReactElement) {
  return rtlRender(ui, { wrapper: Providers });
}

const WS_ID = 'ws-1';
const THREAD_ID = 'thread-1';

// Reset before, not after: the store notifies its subscribers, and rows are
// still mounted until Testing Library's own cleanup runs.
beforeEach(() => {
  resetNavPanelExpansion();
  resetThreadLifecycle();
});

function clickArchive() {
  return userEvent.setup().click(screen.getByRole('button', { name: 'nav.archiveThread' }));
}

describe('NavigationPanel — archive confirm', () => {
  function renderPanel() {
    const onArchiveThread = vi.fn();
    render(
      <NavigationPanel
        workspaces={[{ workspace_id: WS_ID, name: 'Test workspace' }]}
        workspaceThreads={{
          [WS_ID]: { threads: [{ thread_id: THREAD_ID, title: 'Test thread' }], loading: false },
        }}
        currentWorkspaceId={WS_ID}
        currentThreadId={THREAD_ID}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
        onArchiveThread={onArchiveThread}
      />,
    );
    return onArchiveThread;
  }

  it('archives an idle thread immediately, with no dialog', async () => {
    const onArchiveThread = renderPanel();

    await clickArchive();

    expect(onArchiveThread).toHaveBeenCalledWith(WS_ID, THREAD_ID);
    expect(screen.queryByText('chat.archiveConfirm.title')).toBeNull();
  });

  it('asks first when the thread has a live run, and archives on confirm', async () => {
    publishLocalRunning(THREAD_ID);
    const onArchiveThread = renderPanel();

    await clickArchive();
    expect(onArchiveThread).not.toHaveBeenCalled();
    expect(await screen.findByText('chat.archiveConfirm.title')).toBeInTheDocument();

    await userEvent.setup().click(screen.getByRole('button', { name: 'chat.archiveConfirm.confirm' }));

    expect(onArchiveThread).toHaveBeenCalledTimes(1);
    expect(onArchiveThread).toHaveBeenCalledWith(WS_ID, THREAD_ID);
    await waitFor(() => expect(screen.queryByText('chat.archiveConfirm.title')).toBeNull());
  });

  it('leaves the thread alone when the confirm is cancelled', async () => {
    publishLocalRunning(THREAD_ID);
    const onArchiveThread = renderPanel();

    await clickArchive();
    expect(await screen.findByText('chat.archiveConfirm.title')).toBeInTheDocument();

    await userEvent.setup().click(screen.getByRole('button', { name: 'common.cancel' }));

    await waitFor(() => expect(screen.queryByText('chat.archiveConfirm.title')).toBeNull());
    expect(onArchiveThread).not.toHaveBeenCalled();
  });
});

describe('useArchiveThreadConfirm — shared choke point', () => {
  // Stands in for the gallery card trigger: same hook, a closure carrying the
  // host's own archive path instead of the tree's (wsId, threadId) pair.
  function Harness({ onArchive }: { onArchive: () => void }) {
    const { requestArchive, dialog } = useArchiveThreadConfirm();
    return (
      <>
        <button type="button" onClick={() => requestArchive(THREAD_ID, onArchive)}>
          trigger
        </button>
        {dialog}
      </>
    );
  }

  function renderHarness() {
    const onArchive = vi.fn();
    render(<Harness onArchive={onArchive} />);
    return onArchive;
  }

  it('calls the archive action straight through for an idle thread', async () => {
    const onArchive = renderHarness();

    await userEvent.setup().click(screen.getByRole('button', { name: 'trigger' }));

    expect(onArchive).toHaveBeenCalledTimes(1);
    expect(screen.queryByText('chat.archiveConfirm.title')).toBeNull();
  });

  it('defers the archive action to the confirm button for a live thread', async () => {
    publishLocalRunning(THREAD_ID);
    const onArchive = renderHarness();
    const user = userEvent.setup();

    await user.click(screen.getByRole('button', { name: 'trigger' }));
    expect(onArchive).not.toHaveBeenCalled();

    await user.click(screen.getByRole('button', { name: 'chat.archiveConfirm.confirm' }));
    expect(onArchive).toHaveBeenCalledTimes(1);
  });

  it('drops the pending archive when the dialog is dismissed', async () => {
    publishLocalRunning(THREAD_ID);
    const onArchive = renderHarness();
    const user = userEvent.setup();

    await user.click(screen.getByRole('button', { name: 'trigger' }));
    await user.click(screen.getByRole('button', { name: 'common.cancel' }));

    await waitFor(() => expect(screen.queryByText('chat.archiveConfirm.title')).toBeNull());
    expect(onArchive).not.toHaveBeenCalled();
  });
});
