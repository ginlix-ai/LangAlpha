/**
 * Coverage for the agent-row rendering changes:
 *  - subagents prefer their description over the displayId / "Worker" name.
 *  - main agent renders without a leading icon; subagents render `└─`.
 *  - long descriptions clip with truncate + reveal full text via title.
 *
 * The panel renders agent rows only when (workspace expanded) AND (thread is
 * the current thread, expanded). Each test wires currentWorkspaceId +
 * currentThreadId so the rows mount on first render.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, within, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import NavigationPanel from '../NavigationPanel';
import { resetNavPanelExpansion, forgetNavPanelExpansion } from '../navExpansionStore';

// `t()` identity mock — we don't depend on bundled English copy here, but
// the component reads i18n keys for some labels and we want the fallback
// strings ("Worker") to come from the agents array, not from t().
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const WS_ID = 'ws-1';
const THREAD_ID = 'thread-1';

interface RenderOpts {
  agents: React.ComponentProps<typeof NavigationPanel>['agents'];
}

function renderPanel({ agents }: RenderOpts) {
  return render(
    <NavigationPanel
      workspaces={[{ workspace_id: WS_ID, name: 'Test workspace' }]}
      workspaceThreads={{
        [WS_ID]: {
          threads: [{ thread_id: THREAD_ID, title: 'Test thread' }],
          loading: false,
        },
      }}
      currentWorkspaceId={WS_ID}
      currentThreadId={THREAD_ID}
      agents={agents}
      activeAgentId={null}
      expandWorkspace={vi.fn()}
      onSelectAgent={vi.fn()}
      onRemoveAgent={vi.fn()}
      onNavigateThread={vi.fn()}
    />,
  );
}

describe('NavigationPanel — subagent description fallback', () => {
  it('renders the trimmed description in place of the displayId / Worker name', () => {
    renderPanel({
      agents: [
        { id: 'main', name: 'Lead Agent', isMainAgent: true },
        {
          id: 'sub-1',
          name: 'Task-k7Xm2p',
          description: 'Research AAPL Q3 revenue drivers',
          isMainAgent: false,
        },
      ],
    });

    expect(screen.getByText('Research AAPL Q3 revenue drivers')).toBeInTheDocument();
    expect(screen.queryByText('Task-k7Xm2p')).toBeNull();
  });

  it('falls back to agent.name when description is empty / whitespace-only / null', () => {
    renderPanel({
      agents: [
        { id: 'main', name: 'Lead Agent', isMainAgent: true },
        { id: 'sub-1', name: 'Worker', description: '   ', isMainAgent: false },
        { id: 'sub-2', name: 'Worker', description: undefined, isMainAgent: false },
        // JSON wire shape: backend may emit `null` rather than omit the field.
        // The runtime guard is `typeof agent.description === 'string'`, which
        // correctly rejects null — pinning that contract here so a future
        // refactor to a truthy check (`agent.description?.trim()`) doesn't
        // silently break for `description: 0` or other falsy non-strings.
        { id: 'sub-3', name: 'Worker', description: null as unknown as undefined, isMainAgent: false },
      ],
    });

    expect(screen.getAllByText('Worker').length).toBe(3);
  });

  it('ignores description on the main agent row and always renders agent.name', () => {
    renderPanel({
      agents: [
        // A main agent that carries a description should still render as
        // 'Lead Agent' — the description fallback is gated on !isMainAgent.
        { id: 'main', name: 'Lead Agent', description: 'should be ignored', isMainAgent: true },
        { id: 'sub-1', name: 'Worker', description: 'visible sub label', isMainAgent: false },
      ],
    });

    expect(screen.getByText('Lead Agent')).toBeInTheDocument();
    expect(screen.queryByText('should be ignored')).toBeNull();
    expect(screen.getByText('visible sub label')).toBeInTheDocument();
  });

  it('exposes the full description via the title attribute for hover-reveal', () => {
    const long = 'a'.repeat(300);
    renderPanel({
      agents: [
        { id: 'main', name: 'Lead Agent', isMainAgent: true },
        { id: 'sub-1', name: 'Worker', description: long, isMainAgent: false },
      ],
    });

    const label = screen.getByTitle(long);
    expect(label).toBeInTheDocument();
    expect(label.textContent).toBe(long);
  });
});

describe('NavigationPanel — hierarchy markers', () => {
  // Rows are queried via `data-testid="agent-row"` + `data-agent-role` rather
  // than the styling-hook class `.nav-panel-agent-row` so that a CSS refactor
  // can rename the class without silently breaking these tests.
  function findRows() {
    const rows = screen.getAllByTestId('agent-row');
    const mainRow = rows.find((r) => r.dataset.agentRole === 'main') as HTMLElement;
    const subRow = rows.find((r) => r.dataset.agentRole === 'sub') as HTMLElement;
    return { rows, mainRow, subRow };
  }

  it('renders the └─ glyph for subagent rows but not for the main agent row', () => {
    renderPanel({
      agents: [
        { id: 'main', name: 'Lead Agent', isMainAgent: true },
        { id: 'sub-1', name: 'Worker', description: 'Build DCF', isMainAgent: false },
      ],
    });

    const { rows, mainRow, subRow } = findRows();
    expect(rows.length).toBe(2);

    // Main-agent row: no glyph in its DOM subtree.
    expect(mainRow.textContent).not.toContain('└─');
    expect(within(mainRow).queryByText('Lead Agent')).toBeInTheDocument();

    // Subagent row: glyph appears as its own aria-hidden inline span.
    expect(subRow.textContent).toContain('└─');
    expect(within(subRow).queryByText('Build DCF')).toBeInTheDocument();
  });

  it('marks the hierarchy glyph aria-hidden so screen readers ignore it', () => {
    renderPanel({
      agents: [
        { id: 'main', name: 'Lead Agent', isMainAgent: true },
        { id: 'sub-1', name: 'Worker', description: 'Build DCF', isMainAgent: false },
      ],
    });

    const { subRow } = findRows();
    const glyph = Array.from(subRow.children).find((c) => c.textContent === '└─');
    expect(glyph).toBeTruthy();
    expect(glyph!.getAttribute('aria-hidden')).toBe('true');
  });
});

describe('NavigationPanel — workspace render order', () => {
  it('renders workspaces in prop order without hoisting the current one', () => {
    render(
      <NavigationPanel
        workspaces={[
          { workspace_id: 'ws-a', name: 'Workspace A' },
          { workspace_id: 'ws-b', name: 'Workspace B' },
          { workspace_id: 'ws-c', name: 'Workspace C' },
        ]}
        workspaceThreads={{}}
        currentWorkspaceId="ws-c"
        currentThreadId={null}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
      />,
    );

    const names = screen.getAllByText(/^Workspace [ABC]$/).map((el) => el.textContent);
    expect(names).toEqual(['Workspace A', 'Workspace B', 'Workspace C']);
  });
});

describe('NavigationPanel — show more threads', () => {
  function renderWithThreads(threadsData: { threads: { thread_id: string; title: string }[]; loading: boolean; total?: number }, onLoadMoreThreads = vi.fn()) {
    resetNavPanelExpansion();
    render(
      <NavigationPanel
        workspaces={[{ workspace_id: WS_ID, name: 'Test workspace' }]}
        workspaceThreads={{ [WS_ID]: threadsData }}
        currentWorkspaceId={WS_ID}
        currentThreadId={null}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
        onLoadMoreThreads={onLoadMoreThreads}
      />,
    );
    return onLoadMoreThreads;
  }

  it('renders a show-more row when more threads exist server-side and pages on click', async () => {
    const user = userEvent.setup();
    const onLoadMoreThreads = renderWithThreads({
      threads: [
        { thread_id: 't-1', title: 'Thread one' },
        { thread_id: 't-2', title: 'Thread two' },
      ],
      loading: false,
      total: 5,
    });

    const row = screen.getByText('nav.showMore');
    await user.click(row);
    expect(onLoadMoreThreads).toHaveBeenCalledWith(WS_ID);
  });

  it('hides the show-more row when every thread is already shown or total is unknown', () => {
    renderWithThreads({
      threads: [{ thread_id: 't-1', title: 'Thread one' }],
      loading: false,
      total: 1,
    });
    expect(screen.queryByText('nav.showMore')).toBeNull();

    renderWithThreads({
      threads: [{ thread_id: 't-1', title: 'Thread one' }],
      loading: false,
    });
    expect(screen.queryByText('nav.showMore')).toBeNull();
  });
});

describe('NavigationPanel — active-thread auto-reveal', () => {
  function renderWithActiveThread(opts: {
    threadsData: { threads: { thread_id: string; title: string }[]; loading: boolean; total?: number };
    currentThreadId: string | null;
    status?: string;
    isActive?: boolean;
    onLoadMoreThreads?: ReturnType<typeof vi.fn>;
  }) {
    resetNavPanelExpansion();
    const onLoadMoreThreads = opts.onLoadMoreThreads ?? vi.fn();
    render(
      <NavigationPanel
        isActive={opts.isActive ?? true}
        workspaces={[{ workspace_id: WS_ID, name: 'Test workspace', status: opts.status }]}
        workspaceThreads={{ [WS_ID]: opts.threadsData }}
        currentWorkspaceId={WS_ID}
        currentThreadId={opts.currentThreadId}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
        onLoadMoreThreads={onLoadMoreThreads}
      />,
    );
    return onLoadMoreThreads;
  }

  it('pages in more threads when the active thread is hidden in the collapsed tail', async () => {
    const onLoadMoreThreads = renderWithActiveThread({
      threadsData: {
        threads: [
          { thread_id: 't-1', title: 'Thread one' },
          { thread_id: 't-2', title: 'Thread two' },
        ],
        loading: false,
        total: 14,
      },
      currentThreadId: 't-9', // beyond the loaded page
    });

    await waitFor(() => expect(onLoadMoreThreads).toHaveBeenCalledWith(WS_ID));
  });

  it('does not auto-reveal when the panel is inactive (hidden cached instance)', async () => {
    const onLoadMoreThreads = renderWithActiveThread({
      threadsData: {
        threads: [
          { thread_id: 't-1', title: 'Thread one' },
          { thread_id: 't-2', title: 'Thread two' },
        ],
        loading: false,
        total: 14,
      },
      currentThreadId: 't-9', // beyond the loaded page — would page in if active
      isActive: false,
    });

    // Give any effect a chance to (wrongly) fire before asserting it did not.
    await new Promise((r) => setTimeout(r, 0));
    expect(onLoadMoreThreads).not.toHaveBeenCalled();
  });

  it('does not page when the active thread is already visible', () => {
    const onLoadMoreThreads = renderWithActiveThread({
      threadsData: {
        threads: [
          { thread_id: 't-1', title: 'Thread one' },
          { thread_id: 't-2', title: 'Thread two' },
        ],
        loading: false,
        total: 14,
      },
      currentThreadId: 't-2', // present in the loaded set
    });

    expect(onLoadMoreThreads).not.toHaveBeenCalled();
  });

  it('does not page when every thread is already loaded', () => {
    const onLoadMoreThreads = renderWithActiveThread({
      threadsData: {
        threads: [
          { thread_id: 't-1', title: 'Thread one' },
          { thread_id: 't-2', title: 'Thread two' },
        ],
        loading: false,
        total: 2, // loaded.length >= total, nothing more to fetch
      },
      currentThreadId: 't-9',
    });

    expect(onLoadMoreThreads).not.toHaveBeenCalled();
  });

  it('does not auto-reveal in the flash workspace (capped at 3, no show-more)', () => {
    const onLoadMoreThreads = renderWithActiveThread({
      threadsData: {
        threads: [{ thread_id: 't-1', title: 'Thread one' }],
        loading: false,
        total: 14,
      },
      currentThreadId: 't-9',
      status: 'flash',
    });

    expect(onLoadMoreThreads).not.toHaveBeenCalled();
  });

  it('does not page while a fetch is already in flight', () => {
    const onLoadMoreThreads = renderWithActiveThread({
      threadsData: {
        threads: [{ thread_id: 't-1', title: 'Thread one' }],
        loading: true, // a page is loading; wait for it before paging again
        total: 14,
      },
      currentThreadId: 't-9',
    });

    expect(onLoadMoreThreads).not.toHaveBeenCalled();
  });
});

describe('NavigationPanel — workspace drag-reorder affordances', () => {
  function renderReorderPanel(onReorderWorkspace?: (a: string, b: string) => void) {
    return render(
      <NavigationPanel
        workspaces={[
          { workspace_id: 'ws-flash', name: 'Flash workspace', status: 'flash' },
          { workspace_id: 'ws-a', name: 'Workspace A' },
        ]}
        workspaceThreads={{}}
        currentWorkspaceId="ws-a"
        currentThreadId={null}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
        onReorderWorkspace={onReorderWorkspace}
      />,
    );
  }

  it('marks workspace header rows sortable, but never the flash workspace', () => {
    renderReorderPanel(vi.fn());

    const sortable = screen.getByText('Workspace A').closest('[aria-roledescription="sortable"]');
    expect(sortable).not.toBeNull();
    expect(screen.getByText('Flash workspace').closest('[aria-roledescription="sortable"]')).toBeNull();
  });

  it('renders plain rows when no reorder handler is provided', () => {
    renderReorderPanel(undefined);

    expect(screen.getByText('Workspace A').closest('[aria-roledescription="sortable"]')).toBeNull();
  });
});

describe('NavigationPanel — expansion survives remounts', () => {
  function renderOrderPanel(currentWorkspaceId: string, expandWorkspace: (wsId: string) => void = vi.fn()) {
    return render(
      <NavigationPanel
        workspaces={[
          { workspace_id: 'ws-a', name: 'Workspace A' },
          { workspace_id: 'ws-b', name: 'Workspace B' },
        ]}
        workspaceThreads={{
          'ws-a': { threads: [{ thread_id: 'ta-1', title: 'Thread A1' }], loading: false },
          'ws-b': { threads: [{ thread_id: 'tb-1', title: 'Thread B1' }], loading: false },
        }}
        currentWorkspaceId={currentWorkspaceId}
        currentThreadId={null}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={expandWorkspace}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
      />,
    );
  }

  it('keeps a manually opened folder expanded after the panel remounts', async () => {
    resetNavPanelExpansion();
    const user = userEvent.setup();
    const first = renderOrderPanel('ws-a');

    // ws-b starts collapsed; open it manually.
    expect(screen.queryByText('Thread B1')).toBeNull();
    await user.click(screen.getByText('Workspace B'));
    expect(screen.getByText('Thread B1')).toBeInTheDocument();

    // Thread switch remounts the panel (fresh ChatView instance) — the folder
    // the user opened must not auto-collapse.
    first.unmount();
    renderOrderPanel('ws-a');
    expect(screen.getByText('Thread B1')).toBeInTheDocument();
  });

  it('does not re-expand a forgotten (deleted) workspace on remount', async () => {
    resetNavPanelExpansion();
    const user = userEvent.setup();
    const first = renderOrderPanel('ws-a');

    // Open ws-b, then forget it as the delete path does.
    await user.click(screen.getByText('Workspace B'));
    expect(screen.getByText('Thread B1')).toBeInTheDocument();
    forgetNavPanelExpansion('ws-b');

    // On remount the mount-effect must not re-expand ws-b (no spurious 404) and
    // its folder stays collapsed.
    first.unmount();
    const expandSpy = vi.fn();
    renderOrderPanel('ws-a', expandSpy);
    expect(screen.queryByText('Thread B1')).toBeNull();
    expect(expandSpy).not.toHaveBeenCalledWith('ws-b');
  });
});

describe('NavigationPanel — shared expansion across instances', () => {
  // One panel mounts per cached ChatView, all alive at once. Folder expansion
  // must be consistent across them: opening a folder in the active panel must
  // be reflected by every other mounted panel, not just the one toggled. (The
  // bug: a per-instance snapshot left a panel cached before a folder was opened
  // showing it collapsed, so the same folder appeared open or closed depending
  // on which thread was active.)
  function renderInstance(currentWorkspaceId: string) {
    return render(
      <NavigationPanel
        workspaces={[
          { workspace_id: 'ws-a', name: 'Workspace A' },
          { workspace_id: 'ws-b', name: 'Workspace B' },
        ]}
        workspaceThreads={{
          'ws-a': { threads: [{ thread_id: 'ta-1', title: 'Thread A1' }], loading: false },
          'ws-b': { threads: [{ thread_id: 'tb-1', title: 'Thread B1' }], loading: false },
        }}
        currentWorkspaceId={currentWorkspaceId}
        currentThreadId={null}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
      />,
    );
  }

  it('reflects a folder toggle in one panel across all mounted panels', async () => {
    resetNavPanelExpansion();
    const user = userEvent.setup();
    const a = renderInstance('ws-a');
    const b = renderInstance('ws-a');

    // ws-b starts collapsed in both panels.
    expect(within(a.container).queryByText('Thread B1')).toBeNull();
    expect(within(b.container).queryByText('Thread B1')).toBeNull();

    // Open ws-b in panel A only.
    await user.click(within(a.container).getByText('Workspace B'));

    // Both panels show it open — the store is shared and live, not snapshotted.
    expect(within(a.container).getByText('Thread B1')).toBeInTheDocument();
    expect(within(b.container).getByText('Thread B1')).toBeInTheDocument();

    // Collapsing in panel B also propagates back to panel A. Use waitFor: the
    // in-place collapse runs an AnimatePresence exit, so the row lingers in the
    // DOM for a tick before it's removed.
    await user.click(within(b.container).getByText('Workspace B'));
    await waitFor(() => expect(within(a.container).queryByText('Thread B1')).toBeNull());
    await waitFor(() => expect(within(b.container).queryByText('Thread B1')).toBeNull());
  });

  // Models the reported repro: expand a NON-current folder, then navigate to a
  // thread in a DIFFERENT workspace (fresh panel, different currentWorkspaceId).
  // The manual expansion must persist — it should not auto-collapse.
  function renderInstanceWithThreeWorkspaces(currentWorkspaceId: string) {
    return render(
      <NavigationPanel
        workspaces={[
          { workspace_id: 'ws-a', name: 'Workspace A' },
          { workspace_id: 'ws-b', name: 'Workspace B' },
          { workspace_id: 'ws-c', name: 'Workspace C' },
        ]}
        workspaceThreads={{
          'ws-a': { threads: [{ thread_id: 'ta-1', title: 'Thread A1' }], loading: false },
          'ws-b': { threads: [{ thread_id: 'tb-1', title: 'Thread B1' }], loading: false },
          'ws-c': { threads: [{ thread_id: 'tc-1', title: 'Thread C1' }], loading: false },
        }}
        currentWorkspaceId={currentWorkspaceId}
        currentThreadId={null}
        agents={[]}
        activeAgentId={null}
        expandWorkspace={vi.fn()}
        onSelectAgent={vi.fn()}
        onRemoveAgent={vi.fn()}
        onNavigateThread={vi.fn()}
      />,
    );
  }

  it('keeps a manually expanded non-current folder open after navigating to another workspace', async () => {
    resetNavPanelExpansion();
    const user = userEvent.setup();

    // On a thread in ws-a; manually expand ws-b (not the current workspace).
    const first = renderInstanceWithThreeWorkspaces('ws-a');
    expect(within(first.container).queryByText('Thread B1')).toBeNull();
    await user.click(within(first.container).getByText('Workspace B'));
    expect(within(first.container).getByText('Thread B1')).toBeInTheDocument();

    // Navigate to a thread in ws-c: old ChatView's panel unmounts, a fresh panel
    // mounts with a different currentWorkspaceId.
    first.unmount();
    const second = renderInstanceWithThreeWorkspaces('ws-c');

    // ws-b must still be expanded — manual expansion persists across navigation.
    expect(within(second.container).getByText('Thread B1')).toBeInTheDocument();
  });
});
