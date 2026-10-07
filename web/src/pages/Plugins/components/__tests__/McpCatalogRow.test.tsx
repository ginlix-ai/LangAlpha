import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import { McpCatalogRow } from '../McpCatalogRow';
import type { CatalogServer, McpProbeResult, ProbeVerdict } from '@/pages/ChatAgent/utils/api';
import type { BulkSelection } from '../useBulkSelection';

// The row ensures the Flash workspace through React Query; nothing here is
// about that, and an unmocked call would dial the dev server.
vi.mock('@/pages/ChatAgent/utils/api/workspaces', () => ({
  getFlashWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'flash' }),
}));

const selection: BulkSelection = {
  selecting: false,
  selected: new Set<string>(),
  start: vi.fn(),
  exit: vi.fn(),
  toggle: vi.fn(),
  setMany: vi.fn(),
};

function probe(verdict: ProbeVerdict, error = ''): McpProbeResult {
  return {
    verdict,
    tools: [],
    server_info: null,
    error,
    http_status: null,
    missing_secrets: [],
    probed_at: '2026-01-01T00:00:00Z',
  };
}

function makeServer(overrides: Partial<CatalogServer> = {}): CatalogServer {
  return {
    name: 'srv',
    transport: 'http',
    command: null,
    args: [],
    url: 'https://mcp.example.com/mcp',
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    enabled: true,
    created_at: null,
    updated_at: null,
    trading_permission: 'approve_each',
    ...overrides,
  };
}

function renderRow(
  server: CatalogServer,
  workspaces: { id: string; name: string }[] = [],
  onSetNewWorkspacesOn = vi.fn(),
  workspacesLoading = false,
) {
  return renderWithProviders(
    <McpCatalogRow
      server={server}
      vendor={null}
      workspaces={workspaces}
      workspacesLoading={workspacesLoading}
      selection={selection}
      connecting={false}
      refreshing={false}
      toggling={false}
      scopeBusy={false}
      onOpen={vi.fn()}
      onConnect={vi.fn()}
      onDisconnect={vi.fn()}
      onRefreshSchemas={vi.fn()}
      onEdit={vi.fn()}
      onRequestDelete={vi.fn()}
      onToggle={vi.fn()}
      onSetWorkspaceDisabled={vi.fn()}
      onSetNewWorkspacesOn={onSetNewWorkspacesOn}
    />,
  );
}

const connect = () => screen.queryByTestId('catalog-connect-srv');

/**
 * Connect is offered on `http` only, so the verdict table cannot be the whole
 * answer for a row: whatever it says has to reach an `sse` row some other way,
 * and a check that never landed must not be read as an answer at all.
 */
describe('McpCatalogRow: what a stored verdict does to the row', () => {
  it('offers Connect and no note for an http server that answered 401', () => {
    renderRow(makeServer({ probe: probe('oauth') }));
    expect(connect()).toBeInTheDocument();
    // Connect already says it; a note beside it would say it twice.
    expect(screen.getAllByText(/connect to list tools/i)).toHaveLength(1);
  });

  it('says so in a note on an sse server that answered 401', () => {
    // No Connect button here at all, so the row used to render nothing about a
    // server that plainly wants a connection.
    renderRow(makeServer({ transport: 'sse', probe: probe('oauth') }));
    expect(connect()).not.toBeInTheDocument();
    expect(screen.getByText(/connect to list tools/i)).toBeInTheDocument();
  });

  it('keeps Connect on a server the check could not reach', () => {
    // A dropped packet learned nothing about auth, and taking the button away
    // left a never-connected server with no way to connect.
    renderRow(makeServer({ probe: probe('unreachable', 'connection refused') }));
    expect(connect()).toBeInTheDocument();
    // The server's own line still reads, and it is not claimed as an OAuth row.
    expect(screen.getByText(/connection refused/i)).toBeInTheDocument();
    expect(screen.queryByText(/connect to list tools/i)).not.toBeInTheDocument();
  });

  it('takes Connect away only once a server has answered without one', () => {
    renderRow(makeServer({ probe: probe('ok') }));
    expect(connect()).not.toBeInTheDocument();
  });

  it('keeps Connect on a server the check never dialled', () => {
    // `missing_secrets` stopped at the vault, so nothing about this server's
    // auth was learned and the button has to stay where an unprobed row's is.
    renderRow(makeServer({ probe: probe('missing_secrets') }));
    expect(connect()).toBeInTheDocument();
    expect(screen.getByText(/a vault value is missing/i)).toBeInTheDocument();
  });

  it('leaves an unprobed row where it always was', () => {
    renderRow(makeServer());
    expect(connect()).toBeInTheDocument();
  });

  it('stops asking a revoked row to reconnect once its headers answer', () => {
    // Discovery reads a revoked claim as no claim and falls back to the header
    // grant, so the server is usable: the pill and the button would both be
    // asking for a connection the row no longer needs.
    renderRow(makeServer({ oauth_status: 'revoked', probe: probe('ok_authed'), tool_count: 4 }));
    expect(connect()).not.toBeInTheDocument();
    expect(screen.queryByTestId('oauth-status-revoked')).not.toBeInTheDocument();
    expect(screen.getByText('4 tools')).toBeInTheDocument();
  });
});

describe('McpCatalogRow: the state line agrees with the scope badge', () => {
  const workspaces = [
    { id: 'ws-a', name: 'Alpha' },
    { id: 'ws-b', name: 'Beta' },
  ];

  it('does not claim every workspace when one has switched the server off', () => {
    // A disable left behind by a deleted workspace does not count, the same
    // way the badge counts it.
    renderRow(makeServer({ disabled_workspace_ids: ['ws-b', 'ws-gone'] }), workspaces);
    expect(screen.getByText('On, switched off in 1 workspace')).toBeInTheDocument();
    expect(screen.queryByText('On in all workspaces')).not.toBeInTheDocument();
  });

  it('says every workspace when none has', () => {
    renderRow(makeServer({ disabled_workspace_ids: ['ws-gone'] }), workspaces);
    expect(screen.getByText('On in all workspaces')).toBeInTheDocument();
  });

  it('says off everywhere when its plugin is off, whatever its own switch says', () => {
    renderRow(
      makeServer({ plugin_name: 'acme', plugin_enabled: false, disabled_workspace_ids: [] }),
      workspaces,
    );
    expect(screen.getByText('Off in every workspace')).toBeInTheDocument();
    expect(screen.queryByText('On in all workspaces')).not.toBeInTheDocument();
  });
});

/**
 * A server added from a workspace starts off in the ones created later, so
 * "On in all workspaces" on its row would promise the next workspace a server
 * it will not have. The row names where it is on instead, in the same words
 * on the state line and the badge.
 */
describe('McpCatalogRow: a server new workspaces start without', () => {
  const workspaces = [
    { id: 'ws-a', name: 'Alpha' },
    { id: 'ws-b', name: 'Beta' },
  ];
  const offForNew = (disabled: string[]) =>
    makeServer({ enabled_in_new_workspaces: false, disabled_workspace_ids: disabled });

  it('names the one workspace it is on in', () => {
    renderRow(offForNew(['ws-b']), workspaces);
    expect(screen.getByText('On only in Alpha')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Scope: Only in Alpha' })).toBeInTheDocument();
  });

  it('counts the workspaces it is on in, even when that is all of them', () => {
    renderRow(offForNew([]), workspaces);
    expect(screen.getByText('On in 2 workspaces')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Scope: 2 workspaces' })).toBeInTheDocument();
    expect(screen.queryByText('On in all workspaces')).not.toBeInTheDocument();
  });

  it('says so when it is on in none', () => {
    // A disable left by a deleted workspace is not a workspace it is off in.
    renderRow(offForNew(['ws-a', 'ws-b', 'ws-gone']), workspaces);
    expect(screen.getByText('On, but active in no workspace')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Scope: No workspaces' })).toBeInTheDocument();
  });

  it('names no reach while the workspace list is still loading', () => {
    // Counted against the empty list a loading query hands back, the row read
    // "active in no workspace" until the list arrived.
    renderRow(offForNew([]), [], vi.fn(), true);
    expect(screen.queryByText('On, but active in no workspace')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^Scope:/ })).not.toBeInTheDocument();
  });

  it('hands the new-workspaces switch to its caller', async () => {
    const onSet = vi.fn();
    const user = userEvent.setup();
    renderRow(offForNew(['ws-b']), workspaces, onSet);
    await user.click(screen.getByRole('button', { name: 'Scope: Only in Alpha' }));
    const item = await screen.findByRole('menuitemcheckbox', { name: 'On in new workspaces' });
    expect(item).toHaveAttribute('aria-checked', 'false');
    await user.click(item);
    expect(onSet).toHaveBeenCalledWith(true);
  });
});
