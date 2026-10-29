/**
 * The sidebar's workspace folders say which kind of machine each lives on: ⌘
 * in the folder for a Mac, >_ for another SSH machine, the plain folder for a
 * hosted sandbox. A pin still outranks the machine.
 */
import React from 'react';
import { beforeAll, describe, it, expect, vi } from 'vitest';
import { render as rtlRender, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { Computer } from '@/types/api';
import NavigationPanel from '../NavigationPanel';
import { loadNavTreeKit } from '../navTreeKit';

beforeAll(() => loadNavTreeKit());

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: unknown) =>
      (opts && typeof opts === 'object' ? `${key} ${(opts as { name: string }).name} ${(opts as { kind: string }).kind}` : key),
  }),
}));

const computer = (o: Partial<Computer>): Computer => ({
  computer_id: 'c',
  user_id: 'u',
  kind: 'daytona',
  name: 'Box',
  status: 'running',
  resource_tier: 'standard',
  is_always_on: false,
  is_primary: false,
  root_dir: '/home/user',
  ssh: null,
  ...o,
});

const sshTarget = (helper: boolean) => ({
  host: 'h', port: 22, username: 'u', fingerprint: 'SHA256:x', tunnel: false, tunnel_connected: false, helper,
});

const COMPUTERS = [
  computer({ computer_id: 'cloud', kind: 'daytona', name: 'Research box', is_primary: true }),
  computer({ computer_id: 'mac', kind: 'ssh', name: 'Studio', ssh: sshTarget(true) }),
  computer({ computer_id: 'gpu', kind: 'ssh', name: 'gpu-box', ssh: sshTarget(false) }),
];

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  client.setQueryData(queryKeys.computers.lists(), { computers: COMPUTERS, total: COMPUTERS.length });
  return rtlRender(
    <MemoryRouter>
      <QueryClientProvider client={client}>
        <NavigationPanel
          workspaces={[
            { workspace_id: 'w-cloud', name: 'Cloud work', is_home: false, computer_id: 'cloud' },
            { workspace_id: 'w-mac', name: 'Mac work', is_home: false, computer_id: 'mac' },
            { workspace_id: 'w-gpu', name: 'GPU work', is_home: false, computer_id: 'gpu' },
            { workspace_id: 'w-pin', name: 'Pinned Mac work', is_home: false, is_pinned: true, computer_id: 'mac' },
          ]}
          workspaceThreads={{}}
          currentWorkspaceId={null}
          currentThreadId={null}
          agents={[]}
          activeAgentId={null}
          expandWorkspace={vi.fn()}
          onSelectAgent={vi.fn()}
          onRemoveAgent={vi.fn()}
          onNavigateThread={vi.fn()}
        />
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

/** The leading glyph's Lucide name on the row titled `name`. */
function glyphOf(name: string): string | undefined {
  const row = screen.getByText(name).closest('.nav-panel-row');
  const svg = row?.querySelector('svg');
  return [...(svg?.classList ?? [])].find((c) => c.startsWith('lucide-'));
}

describe('NavigationPanel — the machine in the folder', () => {
  it('marks a Mac with ⌘ and an SSH machine with >_, and leaves a hosted sandbox a plain folder', () => {
    renderPanel();

    expect(glyphOf('Cloud work')).toBe('lucide-folder');
    expect(glyphOf('Mac work')).toBe('lucide-folder-command');
    expect(glyphOf('GPU work')).toBe('lucide-folder-terminal');
  });

  it('names the machine on hover, and keeps the pin on a pinned row', () => {
    renderPanel();

    expect(screen.getAllByTitle('nav.onComputer Studio computer.badge.mac')).toHaveLength(2);
    expect(screen.getByTitle('nav.onComputer gpu-box computer.badge.ssh')).toBeInTheDocument();
    // Said to a screen reader as well, not only on hover.
    expect(screen.getByText('nav.onComputer gpu-box computer.badge.ssh')).toHaveClass('sr-only');
    expect(glyphOf('Pinned Mac work')).toBe('lucide-pin');
  });
});
