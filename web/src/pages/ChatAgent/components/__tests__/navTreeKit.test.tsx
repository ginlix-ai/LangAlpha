/**
 * The nav tree paints with the static kit and swaps in the interactive one
 * (../navTreeKit). The swap remounts every row and has to change nothing a
 * person sees: both kits must lay a section out alike, and focus and animation
 * clocks find their twin in the new tree by `data-nav-key`.
 *
 * Its own file so the kit's module state starts empty: the first render is the
 * static kit, which no other test renders.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render as rtlRender, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import NavigationPanel from '../NavigationPanel';
import { expandedWorkspaces, notifyNavExpansion, resetNavPanelExpansion } from '../navExpansionStore';
import { publishLocalRunning, resetThreadLifecycle } from '@/lib/threadLifecycle/store';
import type { NavWorkspace } from '../../hooks/useNavigationData';
import type { SidebarAgentRow } from '../../session/subagents/subagentStatus';
import type { ThreadsData } from '../NavigationRows';

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

// One of each row shape: the current workspace with a running thread, a pinned
// one, the current thread's agents and a page left to show; Home (no options
// menu) with no threads; a pinned one, collapsed, still loading.
const WORKSPACES: NavWorkspace[] = [
  { workspace_id: 'ws-home', name: 'Home', status: 'running', is_home: true },
  { workspace_id: 'ws-pinned', name: 'Pinned', is_pinned: true, is_home: false },
  { workspace_id: 'ws-main', name: 'Main', is_home: false },
];

const THREADS: Record<string, ThreadsData> = {
  'ws-home': { threads: [], loading: false },
  'ws-pinned': { threads: [], loading: true },
  'ws-main': {
    threads: [
      { thread_id: 't-current', title: 'Current' },
      { thread_id: 't-pinned', title: 'Pinned thread', is_pinned: true },
      { thread_id: 't-other', title: 'Other' },
    ],
    total: 5,
    loading: false,
  },
};

const AGENTS: SidebarAgentRow[] = [
  { id: 'main', name: 'Lead', description: '', isMainAgent: true, status: 'active' },
  { id: 'sub-running', name: 'Worker', description: 'Pulls filings', isMainAgent: false, status: 'active' },
  { id: 'sub-done', name: 'Worker', description: 'Drafts memo', isMainAgent: false, status: 'completed' },
];

function renderPanel() {
  return rtlRender(
    <NavigationPanel
      workspaces={WORKSPACES}
      workspaceThreads={THREADS}
      currentWorkspaceId="ws-main"
      currentThreadId="t-current"
      agents={AGENTS}
      activeAgentId="sub-running"
      expandWorkspace={vi.fn()}
      onSelectAgent={vi.fn()}
      onRemoveAgent={vi.fn()}
      onNavigateThread={vi.fn()}
      onLoadMoreThreads={vi.fn()}
      onReorderWorkspace={vi.fn()}
      onPinWorkspace={vi.fn()}
      onRenameWorkspace={vi.fn()}
      onNewThread={vi.fn()}
      onPinThread={vi.fn()}
      onArchiveThread={vi.fn()}
    />,
    { wrapper: Providers },
  );
}

// What a section's markup decides: element order and nesting, classes, roles,
// tab order and the rest of the attributes. Generated ids differ with the
// tree's depth and are left out, and so are the resting values motion writes
// inline, which read the same as none.
const GENERATED_ID_ATTRS = new Set(['id', 'aria-describedby', 'aria-controls', 'aria-labelledby']);
const RESTING_STYLES = new Set(['height: auto', 'opacity: 1', 'transform: none']);

function skeleton(el: Element, depth = 0): string {
  const { style } = el as HTMLElement | SVGElement;
  const declarations = Array.from(style, (name) => `${name}: ${style.getPropertyValue(name)}`)
    .filter((declaration) => !RESTING_STYLES.has(declaration))
    .sort();
  const attrs = Array.from(el.attributes)
    .filter((attr) => attr.name !== 'style' && !GENERATED_ID_ATTRS.has(attr.name))
    .map((attr) => `${attr.name}="${attr.value}"`)
    .concat(declarations.length ? [`style="${declarations.join('; ')}"`] : [])
    .sort();
  const line = `${'  '.repeat(depth)}<${el.localName}${attrs.length ? ` ${attrs.join(' ')}` : ''}>`;
  return [line, ...Array.from(el.children, (child) => skeleton(child, depth + 1))].join('\n');
}

const sections = (container: HTMLElement) => Array.from(container.querySelectorAll<HTMLElement>('[data-ws-id]'));

const navKeys = (section: Element) =>
  Array.from(section.querySelectorAll('[data-nav-key]'), (el) => el.getAttribute('data-nav-key'));

function snapshot(container: HTMLElement) {
  return sections(container).map((section) => ({
    wsId: section.dataset.wsId,
    keys: navKeys(section),
    skeleton: skeleton(section),
  }));
}

const navElement = (container: HTMLElement, wsId: string, key: string) =>
  container.querySelector<HTMLElement>(`[data-ws-id="${wsId}"] [data-nav-key="${key}"]`);

const FOCUSABLE = 'button, input, select, textarea, a[href], [tabindex]';

beforeEach(() => {
  resetNavPanelExpansion();
  resetThreadLifecycle();
  // Home's folder open too, so its empty state renders.
  expandedWorkspaces.add('ws-home');
  notifyNavExpansion();
  publishLocalRunning('t-other');
});

describe('nav tree kits', () => {
  // One case, as only the first render in this file is static: the kit stays loaded.
  it('swap without changing a section, carrying click focus to its twin by nav key', async () => {
    const { container } = renderPanel();
    const handles = () => Array.from(container.querySelectorAll('[data-ws-id] [data-drag-handle]'));
    expect(handles()).toHaveLength(WORKSPACES.length);
    // Only dnd-kit's handle points into its DndContext: none yet, so this is the static kit.
    expect(handles().filter((handle) => handle.hasAttribute('aria-describedby'))).toEqual([]);
    const staticTree = snapshot(container);

    // The pointer arriving starts the kit's download, and the swap waits while
    // it rests on a row. A click's focus does not match :focus-visible, so it
    // lets the swap through once the pointer has left.
    const pin = navElement(container, 'ws-main', 'thread:t-pinned:pin')!;
    const user = userEvent.setup();
    await user.click(pin);
    expect(document.activeElement).toBe(pin);
    await user.unhover(pin);
    await waitFor(() => {
      expect(handles().filter((handle) => !handle.hasAttribute('aria-describedby'))).toEqual([]);
    });

    expect(snapshot(container)).toEqual(staticTree);
    expect(pin.isConnected).toBe(false);
    expect(document.activeElement).toBe(navElement(container, 'ws-main', 'thread:t-pinned:pin'));
    // The fixture reaches every keyed element a section can show, bar the rename field.
    expect(staticTree.find((section) => section.wsId === 'ws-main')?.keys).toEqual([
      'header', 'new-thread', 'options',
      'thread:t-current:title', 'thread:t-current:agents', 'thread:t-current:pin', 'thread:t-current:archive',
      'agent:main', 'agent:sub-running', 'agent:sub-running:remove', 'agent:sub-done', 'agent:sub-done:remove',
      'thread:t-pinned:title', 'thread:t-pinned:pin', 'thread:t-pinned:archive',
      'thread:t-other:title', 'thread:t-other:pin', 'thread:t-other:archive',
    ]);
  });

  it('give every focusable element in a section a nav key of its own', () => {
    const { container } = renderPanel();

    for (const section of sections(container)) {
      const keys = navKeys(section);
      expect(new Set(keys).size).toBe(keys.length);
      for (const el of section.querySelectorAll(FOCUSABLE)) {
        expect(el, el.outerHTML.slice(0, 120)).toHaveAttribute('data-nav-key');
      }
    }
  });
});
