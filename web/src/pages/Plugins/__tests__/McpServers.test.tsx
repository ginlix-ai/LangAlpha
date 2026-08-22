import { describe, it, expect, vi, beforeEach, afterAll } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import type { CatalogServer, CatalogServerList } from '@/pages/ChatAgent/utils/api';

/**
 * The Plugins → MCP tab, `Your servers` list. Every mutation here is fire-and-report: the
 * page has no inline error region for them, so a rejected promise that isn't
 * turned into a toast is a silent failure the user reads as success. These
 * tests pin one path per handler, both directions.
 */

// ---------------------------------------------------------------------------
// Mocks — drive the catalog + every mutation outcome from the hook boundary.
// ---------------------------------------------------------------------------

const mutateAsync = {
  create: vi.fn(),
  update: vi.fn(),
  del: vi.fn(),
  toggle: vi.fn(),
  import: vi.fn(),
  disconnect: vi.fn(),
  refresh: vi.fn(),
  createSecret: vi.fn(),
  wsEnable: vi.fn(),
  adopt: vi.fn(),
  moveUp: vi.fn(),
};

let catalogData: CatalogServerList | undefined;
let catalogError: Error | null = null;
let catalogLoading = false;
let deletePending = false;

vi.mock('@/hooks/useMcpServers', () => ({
  useMcpCatalog: () => ({ data: catalogData, isLoading: catalogLoading, error: catalogError }),
  useCreateMcpCatalogServer: () => ({ mutateAsync: mutateAsync.create, isPending: false }),
  useUpdateMcpCatalogServer: () => ({ mutateAsync: mutateAsync.update, isPending: false }),
  useDeleteMcpCatalogServer: () => ({ mutateAsync: mutateAsync.del, isPending: deletePending }),
  useToggleMcpCatalogServer: () => ({ mutateAsync: mutateAsync.toggle, isPending: false }),
  useImportMcpCatalogServers: () => ({ mutateAsync: mutateAsync.import, isPending: false }),
  useDisconnectMcpOauth: () => ({ mutateAsync: mutateAsync.disconnect, isPending: false }),
  useRefreshMcpOauthSchemas: () => ({ mutateAsync: mutateAsync.refresh, isPending: false }),
  // The nested BuiltinMcpSection renders nothing while its list is empty —
  // these tests exercise the user-tier list only.
  useBuiltinMcpServers: () => ({ data: { servers: [] }, isLoading: false, error: null }),
  useToggleBuiltinMcpServer: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useSetMcpServerEnabledInWorkspace: () => ({ mutateAsync: mutateAsync.wsEnable, isPending: false }),
  useAdoptMcpServerToWorkspace: () => ({ mutateAsync: mutateAsync.adopt, isPending: false }),
  usePromoteMcpServerToTemplate: () => ({ mutateAsync: mutateAsync.moveUp, isPending: false }),
}));

// No workspaces → the scope control renders as a plain badge (or the OAuth
// explainer) and the per-workspace checklist stays out of these tests.
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [] }, isLoading: false, error: null }),
}));

vi.mock('@/hooks/useUserVault', () => ({
  useUserVaultSecrets: () => ({ data: { secrets: [], remaining_slots: 20 }, isLoading: false, error: null }),
  useCreateUserVaultSecret: () => ({ mutateAsync: mutateAsync.createSecret, isPending: false }),
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

// `startMcpOauth` is the only direct API call the page makes; everything else
// arrives through the mocked hooks. Keep `formatApiErrorDetail` real — the
// error copy the toasts render is exactly what's under test.
const mockStartMcpOauth = vi.fn();
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, startMcpOauth: (...args: unknown[]) => mockStartMcpOauth(...args) };
});

// Render the Radix dropdown inline — the real one needs portal/pointer
// machinery jsdom doesn't drive (same treatment as McpServerRow.test).
vi.mock('@/components/ui/dropdown-menu', () => ({
  DropdownMenu: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  DropdownMenuContent: ({ children }: { children: React.ReactNode }) => <div role="menu">{children}</div>,
  DropdownMenuItem: ({
    children,
    onSelect,
    disabled,
  }: {
    children: React.ReactNode;
    onSelect?: (e?: { preventDefault: () => void }) => void;
    disabled?: boolean;
    variant?: string;
  }) => (
    <button
      role="menuitem"
      aria-disabled={disabled ? 'true' : undefined}
      onClick={() => { if (!disabled) onSelect?.({ preventDefault: () => {} }); }}
    >
      {children}
    </button>
  ),
  DropdownMenuLabel: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSeparator: () => <hr />,
  DropdownMenuSub: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSubTrigger: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSubContent: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

import { McpServers } from '../components/McpServers';
import { toast } from '@/components/ui/use-toast';

// ---------------------------------------------------------------------------
// window.location.assign — the connect flow is a full-page navigation.
// ---------------------------------------------------------------------------

const realLocation = window.location;
const assign = vi.fn();
Object.defineProperty(window, 'location', {
  configurable: true,
  value: { href: 'http://localhost/plugins', origin: 'http://localhost', assign },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeCatalogServer(overrides: Partial<CatalogServer> = {}): CatalogServer {
  return {
    name: 'placeholder_server',
    transport: 'stdio',
    command: 'npx',
    args: [],
    url: null,
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    enabled: true,
    created_at: null,
    updated_at: null,
    ...overrides,
  };
}

function makeCatalog(servers: CatalogServer[], maxServers = 20): CatalogServerList {
  return { servers, max_servers: maxServers };
}

/** A remote server that carries the OAuth lifecycle. */
function makeOauthServer(overrides: Partial<CatalogServer> = {}): CatalogServer {
  return makeCatalogServer({
    name: 'remote_connector',
    transport: 'http',
    command: null,
    url: 'https://mcp.example.test/mcp',
    ...overrides,
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  catalogData = makeCatalog([]);
  catalogError = null;
  catalogLoading = false;
  deletePending = false;
});

// ---------------------------------------------------------------------------
// List rendering
// ---------------------------------------------------------------------------

describe('McpServers — list rendering', () => {
  it('renders a row per catalog server with transport, scope and description', () => {
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'alpha_server', description: 'does a thing' }),
      makeCatalogServer({ name: 'beta_server', enabled: false }),
    ]);
    renderWithProviders(<McpServers />);

    expect(screen.getByTestId('server-row-alpha_server')).toBeInTheDocument();
    expect(screen.getByTestId('server-row-beta_server')).toBeInTheDocument();
    expect(screen.getByText('does a thing')).toBeInTheDocument();
    // Inheritance scope is spelled out per row — it's the whole point of the page.
    expect(screen.getByText('On in all workspaces')).toBeInTheDocument();
    expect(screen.getByText('Off, not inherited')).toBeInTheDocument();
  });

  it('shows the OAuth pill and tool count on a connected remote server', () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected', tool_count: 4 })]);
    renderWithProviders(<McpServers />);

    expect(screen.getByTestId('oauth-status-connected')).toBeInTheDocument();
    expect(screen.getByText('4 tools')).toBeInTheDocument();
  });

  it('renders the empty state when the catalog has no servers', () => {
    renderWithProviders(<McpServers />);
    expect(screen.getByText(/No servers yet/i)).toBeInTheDocument();
  });

  it('surfaces a catalog load failure instead of an empty list', () => {
    catalogError = new Error('catalog unreachable');
    renderWithProviders(<McpServers />);
    expect(screen.getByText('catalog unreachable')).toBeInTheDocument();
    expect(screen.queryByText(/No servers yet/i)).not.toBeInTheDocument();
  });

  it('shows the cap counter (the add path moved to the page-level Add menu)', () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'a_server' })], 1);
    renderWithProviders(<McpServers />);
    expect(screen.getByText('1 / 1')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /import json/i })).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// handleToggle
// ---------------------------------------------------------------------------

describe('McpServers — enable toggle', () => {
  it('toasts the warnings a successful enable came back with', async () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'warn_server', enabled: false })]);
    mutateAsync.toggle.mockResolvedValue({ name: 'warn_server', enabled: true, warnings: ['runs from a shared env'] });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('switch'));

    await waitFor(() =>
      expect(mutateAsync.toggle).toHaveBeenCalledWith({ name: 'warn_server', enabled: true }),
    );
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Server enabled with warnings',
          description: 'runs from a shared env',
        }),
      ),
    );
  });

  it('surfaces a rejected toggle rather than silently reverting', async () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'toggle_server' })]);
    mutateAsync.toggle.mockRejectedValue({ response: { data: { detail: 'server is misconfigured' } } });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('switch'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          title: 'Could not change server state',
          description: 'server is misconfigured',
        }),
      ),
    );
    // The switch is usable again — the in-flight lock is released in `finally`.
    await waitFor(() => expect(screen.getByRole('switch')).not.toBeDisabled());
  });
});

// ---------------------------------------------------------------------------
// handleDelete
// ---------------------------------------------------------------------------

describe('McpServers — delete', () => {
  it('confirms first, then deletes and drops the strip', async () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'doomed_server' })]);
    mutateAsync.del.mockResolvedValue({ ok: true });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Delete'));
    expect(mutateAsync.del).not.toHaveBeenCalled();
    expect(screen.getByText(/Workspaces stop inheriting it immediately/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^delete$/i }));

    await waitFor(() => expect(mutateAsync.del).toHaveBeenCalledWith('doomed_server'));
    await waitFor(() =>
      expect(screen.queryByText(/Workspaces stop inheriting it immediately/i)).not.toBeInTheDocument(),
    );
  });

  it('surfaces a rejected delete and keeps the confirm strip open', async () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'doomed_server' })]);
    mutateAsync.del.mockRejectedValue({ response: { data: { detail: 'still in use' } } });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^delete$/i }));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          title: 'Could not delete server',
          description: 'still in use',
        }),
      ),
    );
    // Not dismissed — the row is still there to retry or cancel.
    expect(screen.getByText(/Workspaces stop inheriting it immediately/i)).toBeInTheDocument();
  });

  it('cancels without deleting', () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'doomed_server' })]);
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Delete'));
    fireEvent.click(screen.getByRole('button', { name: /^cancel$/i }));

    expect(screen.queryByText(/Workspaces stop inheriting it immediately/i)).not.toBeInTheDocument();
    expect(mutateAsync.del).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// OAuth connect / reconnect
// ---------------------------------------------------------------------------

describe('McpServers — OAuth connect affordance', () => {
  it('offers Connect on a never-connected remote server and navigates to the vendor', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: null })]);
    mockStartMcpOauth.mockResolvedValue({ authorize_url: 'https://vendor.example.test/authorize?x=1' });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));

    await waitFor(() =>
      expect(mockStartMcpOauth).toHaveBeenCalledWith('remote_connector', '/plugins?tab=mcp'),
    );
    await waitFor(() =>
      expect(assign).toHaveBeenCalledWith('https://vendor.example.test/authorize?x=1'),
    );
  });

  it('offers Reconnect — not Connect — once a connection exists but is broken', () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'revoked' })]);
    renderWithProviders(<McpServers />);

    expect(screen.getByRole('button', { name: /reconnect/i })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^connect$/i })).not.toBeInTheDocument();
    expect(screen.getByTestId('oauth-status-revoked')).toBeInTheDocument();
  });

  it.each(['needs_reauth', 'refresh_ambiguous'] as const)(
    'offers Reconnect for the %s state too',
    (oauth_status) => {
      catalogData = makeCatalog([makeOauthServer({ oauth_status })]);
      renderWithProviders(<McpServers />);
      expect(screen.getByRole('button', { name: /reconnect/i })).toBeInTheDocument();
    },
  );

  it('offers no connect affordance on a healthy connection or a stdio server', () => {
    catalogData = makeCatalog([
      makeOauthServer({ oauth_status: 'connected' }),
      makeCatalogServer({ name: 'local_server' }),
    ]);
    renderWithProviders(<McpServers />);
    expect(screen.queryByRole('button', { name: /^(re)?connect$/i })).not.toBeInTheDocument();
  });

  it('surfaces a failed authorize-start and re-enables the button', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: null })]);
    mockStartMcpOauth.mockRejectedValue({ response: { data: { detail: 'no client registered' } } });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          title: 'Could not start the connection',
          description: 'no client registered',
        }),
      ),
    );
    expect(assign).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.getByRole('button', { name: /^connect$/i })).not.toBeDisabled());
  });
});

// ---------------------------------------------------------------------------
// handleDisconnect
// ---------------------------------------------------------------------------

describe('McpServers — disconnect', () => {
  it('confirms the disconnect with a toast naming the server', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.disconnect.mockResolvedValue({ ok: true });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Disconnect'));

    await waitFor(() => expect(mutateAsync.disconnect).toHaveBeenCalledWith('remote_connector'));
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Disconnected',
          description: expect.stringContaining('remote_connector'),
        }),
      ),
    );
  });

  it('surfaces a rejected disconnect', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.disconnect.mockRejectedValue({ response: { data: { detail: 'token store unavailable' } } });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Disconnect'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          title: 'Could not disconnect',
          description: 'token store unavailable',
        }),
      ),
    );
  });

  it('hides Disconnect on an already-revoked row', () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'revoked' })]);
    renderWithProviders(<McpServers />);
    expect(screen.queryByText('Disconnect')).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// handleRefreshSchemas — the one handler with THREE outcomes
// ---------------------------------------------------------------------------

describe('McpServers — refresh tool schemas', () => {
  it('reports the new tool count on success', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.refresh.mockResolvedValue({
      server_name: 'remote_connector', status: 'ok', error: '', tool_count: 7, discovered_at: null,
    });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Refresh tools'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({ title: 'Tools refreshed', description: expect.stringContaining('7') }),
      ),
    );
  });

  it('does not claim success when an ok status carries error text (stale snapshot)', async () => {
    // The schema cache keeps the last good `status`/`tools` when a re-discovery
    // fails but always overwrites `error`. So status ok + non-empty error means
    // THIS attempt failed and `tool_count` is the old number — the success
    // toast would be an affirmative lie, and it's the only feedback channel.
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.refresh.mockResolvedValue({
      server_name: 'remote_connector',
      status: 'ok',
      error: 'connect to 10.0.0.5:8080 failed: connection refused',
      tool_count: 7,
      discovered_at: null,
    });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Refresh tools'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          title: 'Tools not refreshed',
          description: expect.stringContaining('7 previously discovered tools'),
        }),
      ),
    );
    // The raw error is an internal-reachability oracle — it must not reach copy.
    const description = vi.mocked(toast).mock.calls[0][0].description as string;
    expect(description).not.toContain('10.0.0.5');
    expect(description).not.toContain('connection refused');
    expect(toast).not.toHaveBeenCalledWith(
      expect.objectContaining({ title: 'Tools refreshed' }),
    );
  });

  it('surfaces a resolved-but-failed refresh (HTTP 200 with a non-ok status)', async () => {
    // The refresh endpoint reports its own failure in the body rather than
    // rejecting — swallowing that would read to the user as a successful noop.
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.refresh.mockResolvedValue({
      server_name: 'remote_connector', status: 'error', error: 'upstream returned 502', tool_count: 0, discovered_at: null,
    });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Refresh tools'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          title: 'Could not refresh tools',
          description: 'upstream returned 502',
        }),
      ),
    );
  });

  it('falls back to the bare status when a failed refresh carries no error text', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.refresh.mockResolvedValue({
      server_name: 'remote_connector', status: 'needs_reauth', error: '', tool_count: 0, discovered_at: null,
    });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Refresh tools'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({ variant: 'destructive', description: 'needs_reauth' }),
      ),
    );
  });

  it('surfaces a rejected refresh', async () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'connected' })]);
    mutateAsync.refresh.mockRejectedValue({ response: { data: { detail: 'rate limited' } } });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Refresh tools'));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive', title: 'Could not refresh tools', description: 'rate limited',
        }),
      ),
    );
  });

  it('hides Refresh tools unless the connection is live', () => {
    catalogData = makeCatalog([makeOauthServer({ oauth_status: 'needs_reauth' })]);
    renderWithProviders(<McpServers />);
    expect(screen.queryByText('Refresh tools')).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// handleSubmit (create / update through the shared modal)
// ---------------------------------------------------------------------------

describe('McpServers — create and edit', () => {
  it('keeps a rejected create inline in the modal instead of dropping it', async () => {
    mutateAsync.create.mockRejectedValue({
      response: { data: { detail: [{ loc: ['body', 'url'], msg: 'field required', type: 'value_error.missing' }] } },
    });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /add server/i }));
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'new_server' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));

    // FastAPI's array-shaped detail must flatten, not stringify.
    await waitFor(() => expect(screen.getByText('body.url: field required')).toBeInTheDocument());
    expect(screen.queryByText('[object Object]')).not.toBeInTheDocument();
    // Modal stays open so the user can fix and retry.
    expect(screen.getByPlaceholderText('my_server')).toBeInTheDocument();
  });

  it('closes the modal and toasts warnings a successful save came back with', async () => {
    mutateAsync.create.mockResolvedValue(makeCatalogServer({ name: 'new_server', warnings: ['shared-env command'] }));
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /add server/i }));
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'new_server' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));

    await waitFor(() => expect(screen.queryByPlaceholderText('my_server')).not.toBeInTheDocument());
    expect(toast).toHaveBeenCalledWith(
      expect.objectContaining({ title: 'Server saved with warnings', description: 'shared-env command' }),
    );
  });

  it('routes the kebab Edit through the update mutation, not create', async () => {
    catalogData = makeCatalog([makeCatalogServer({ name: 'existing_server' })]);
    mutateAsync.update.mockResolvedValue(makeCatalogServer({ name: 'existing_server' }));
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Edit'));
    fireEvent.click(await screen.findByRole('button', { name: /^save$/i }));

    await waitFor(() =>
      expect(mutateAsync.update).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'existing_server' }),
      ),
    );
    expect(mutateAsync.create).not.toHaveBeenCalled();
  });

  it('re-saves the stored env map on an unrelated edit instead of wiping it', async () => {
    // The PUT is a full replacement, so whatever the modal submits IS the new
    // row. The catalog response has to carry `env` for the form to hydrate it —
    // with only `env_refs` the editor comes up blank and a description-only
    // edit erases the server's settings across every workspace it feeds.
    const stored = { API_TOKEN: '${vault:API_TOKEN}', REGION: 'us-east-1' };
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'existing_server', env: stored, env_refs: ['API_TOKEN'] }),
    ]);
    mutateAsync.update.mockResolvedValue(makeCatalogServer({ name: 'existing_server' }));
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByText('Edit'));
    fireEvent.change(await screen.findByPlaceholderText('What this server does'), {
      target: { value: 'edited description' },
    });
    fireEvent.click(await screen.findByRole('button', { name: /^save$/i }));

    await waitFor(() =>
      expect(mutateAsync.update).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'existing_server',
          // The submitted body IS the new row — env must survive verbatim.
          body: expect.objectContaining({ description: 'edited description', env: stored }),
        }),
      ),
    );
  });
});

// ---------------------------------------------------------------------------
// Import flow
// ---------------------------------------------------------------------------

describe('McpServers — import', () => {
  const BLOB = '{"mcpServers":{"imported_server":{"command":"npx","args":["-y","pkg"]}}}';

  it('reports the per-server outcome and nudges that imports land switched off', async () => {
    mutateAsync.import.mockResolvedValue({
      results: [{ name: 'imported_server', original_name: 'imported_server', renamed: false, status: 'created' }],
      created: 1,
      secrets_created: ['PLACEHOLDER_TOKEN'],
      config_version: 2,
    });
    // The import modal opens via the page-level Add menu's signal.
    renderWithProviders(<McpServers addSignal={{ action: 'import-servers', nonce: 1 }} />);

    fireEvent.change(screen.getByRole('textbox'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));

    await waitFor(() => expect(mutateAsync.import).toHaveBeenCalled());
    // The result view names what happened, including the auto-vaulted secret.
    await waitFor(() => expect(screen.getByText(/Imported 1 of 1 server/i)).toBeInTheDocument());
    expect(screen.getByText('PLACEHOLDER_TOKEN')).toBeInTheDocument();
    // …and the page nudges that nothing is live yet.
    expect(toast).toHaveBeenCalledWith(
      expect.objectContaining({ title: 'Imported switched off' }),
    );
  });

  it('does not nudge when the import created nothing', async () => {
    mutateAsync.import.mockResolvedValue({
      results: [{ name: 'imported_server', original_name: 'imported_server', renamed: false, status: 'exists' }],
      created: 0,
      secrets_created: [],
      config_version: 2,
    });
    renderWithProviders(<McpServers addSignal={{ action: 'import-servers', nonce: 1 }} />);

    fireEvent.change(screen.getByRole('textbox'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));

    await waitFor(() => expect(screen.getByText(/Imported 0 of 1 server/i)).toBeInTheDocument());
    expect(toast).not.toHaveBeenCalled();
  });

  it('surfaces a rejected import inside the modal', async () => {
    mutateAsync.import.mockRejectedValue({ response: { data: { detail: 'catalog at cap' } } });
    renderWithProviders(<McpServers addSignal={{ action: 'import-servers', nonce: 1 }} />);

    fireEvent.change(screen.getByRole('textbox'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));

    await waitFor(() => expect(screen.getByText('catalog at cap')).toBeInTheDocument());
  });
});
