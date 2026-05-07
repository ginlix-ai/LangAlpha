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
import { render, screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import NavigationPanel from '../NavigationPanel';

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
