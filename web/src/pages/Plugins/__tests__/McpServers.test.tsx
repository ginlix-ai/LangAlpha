import { describe, it, expect, vi, beforeEach, afterAll } from 'vitest';
import { screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { useSearchParams } from 'react-router-dom';
import { renderWithProviders } from '@/test/utils';
import type { CatalogServer, CatalogServerList } from '@/pages/ChatAgent/utils/api';
// Aliased rather than wrapped: the field list is shared, the name this file
// already calls it by is not worth churning 21 call sites over.
import { catalogServer as makeCatalogServer, httpCatalogServer } from '@/test/factories';

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
// `undefined` is the query with no answer at all -- in flight, or asked and
// failed -- which React Query reports alongside an error only until one lands.
// An array with an error beside it is the third state and a real one: a refetch
// that failed over an answer the query still holds.
let brokerages: unknown[] | undefined = [];
let brokeragesError: Error | null = null;
const refetchBrokerages = vi.fn();
const refetchBuiltins = vi.fn();
let builtinError: Error | null = null;
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
  useBuiltinMcpServers: () => ({
    data: builtinError ? undefined : { servers: [] },
    isLoading: false,
    error: builtinError,
    refetch: refetchBuiltins,
  }),
  useToggleBuiltinMcpServer: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useSetMcpServerEnabledInWorkspace: () => ({ mutateAsync: mutateAsync.wsEnable, isPending: false }),
  useAdoptMcpServerToWorkspace: () => ({ mutateAsync: mutateAsync.adopt, isPending: false }),
  usePromoteMcpServerToTemplate: () => ({ mutateAsync: mutateAsync.moveUp, isPending: false }),
  // The registry the list joins every row against before it will let a connect
  // start. Most rows here are ordinary servers, so what matters is that the
  // query has *answered* -- an unanswered one deliberately holds the button,
  // which is its own test elsewhere. It is only stocked by the tests about a
  // row that turns out to be a broker.
  useBrokerages: () => ({
    data: brokerages,
    error: brokeragesError,
    refetch: refetchBrokerages,
  }),
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

// The page reaches the API directly on two paths: the OAuth connect flow, and
// every bulk action (each target calls a raw API function rather than a
// mutation hook, so one fan-out invalidates once instead of N times). Both are
// stubbed here. A `...actual` spread alone leaves everything it doesn't name
// pointing at real axios, so the bulk calls have to be listed explicitly or
// the first bulk test written against this file goes to the network.
// `formatApiErrorDetail` stays real — the error copy the toasts render is
// exactly what's under test.
const mockStartMcpOauth = vi.fn();
const mockBulkApi = {
  setBuiltinMcpServerEnabled: vi.fn(),
  setMcpCatalogServerEnabled: vi.fn(),
  setWorkspaceMcpServerEnabled: vi.fn(),
  promoteWorkspaceMcpServerToTemplate: vi.fn(),
  adoptMcpServerToWorkspace: vi.fn(),
  deleteMcpCatalogServer: vi.fn(),
};
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    startMcpOauth: (...args: unknown[]) => mockStartMcpOauth(...args),
    setBuiltinMcpServerEnabled: (...args: unknown[]) =>
      mockBulkApi.setBuiltinMcpServerEnabled(...args),
    setMcpCatalogServerEnabled: (...args: unknown[]) =>
      mockBulkApi.setMcpCatalogServerEnabled(...args),
    setWorkspaceMcpServerEnabled: (...args: unknown[]) =>
      mockBulkApi.setWorkspaceMcpServerEnabled(...args),
    promoteWorkspaceMcpServerToTemplate: (...args: unknown[]) =>
      mockBulkApi.promoteWorkspaceMcpServerToTemplate(...args),
    adoptMcpServerToWorkspace: (...args: unknown[]) =>
      mockBulkApi.adoptMcpServerToWorkspace(...args),
    deleteMcpCatalogServer: (...args: unknown[]) =>
      mockBulkApi.deleteMcpCatalogServer(...args),
  };
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


/** A vendor that allows one connected AI platform per account and drops the rest. */
const EXCLUSIVE_VENDOR = {
  name: 'ibkr',
  label: 'Interactive Brokers',
  url: 'https://api.broker.test/mcp',
  site: 'broker.test',
  description: 'Portfolio and draft orders.',
  native_callback_only: false,
  exclusive_connection: true,
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'staged_orders', tone: 'caution', rung: true },
  ],
};
/** What this vendor's connect grants unless the user says otherwise. */
const EXCLUSIVE_DEFAULT = ['market_data', 'staged_orders'];

function makeCatalog(servers: CatalogServer[], maxServers = 20): CatalogServerList {
  return { servers, max_servers: maxServers };
}

/** A remote server that carries the OAuth lifecycle, on this suite's host. */
function makeOauthServer(overrides: Partial<CatalogServer> = {}): CatalogServer {
  return httpCatalogServer({ url: 'https://mcp.example.test/mcp', ...overrides });
}

beforeEach(() => {
  vi.clearAllMocks();
  catalogData = makeCatalog([]);
  catalogError = null;
  catalogLoading = false;
  deletePending = false;
  brokerages = [];
  brokeragesError = null;
  builtinError = null;
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

  it('does not say there are no servers while the count above says four', () => {
    // The count is every catalog row, because that is what the cap counts;
    // this list is only the hand-made ones. Install one plugin that ships a
    // server and the section header sat directly over a sentence denying it.
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'from_a_plugin', plugin_name: 'context7' }),
    ]);
    renderWithProviders(<McpServers />);

    expect(screen.queryByText(/No servers yet/i)).not.toBeInTheDocument();
    expect(screen.getByText(/came from a plugin/i)).toBeInTheDocument();
    expect(screen.getByText('1 / 20')).toBeInTheDocument();
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
      // `false`: an ordinary connector keeps the hosted HTTPS callback, in the
      // desktop shell as much as in a browser. Only a vendor whose AS refuses
      // one asks for the loopback listener. The address goes along so the
      // backend can refuse a row that moved while the page sat here.
      expect(mockStartMcpOauth).toHaveBeenCalledWith(
        'remote_connector',
        '/plugins?tab=mcp',
        {
          vendorRefusesHostedCallback: false,
          expectedUrl: 'https://mcp.example.test/mcp',
          stillWanted: expect.any(Function),
        },
      ),
    );
    await waitFor(() =>
      expect(assign).toHaveBeenCalledWith('https://vendor.example.test/authorize?x=1'),
    );
  });

  // The row a brokerage leaves behind lives in this list too, and this list is
  // the other way to reach its Connect button. The question the vendor's terms
  // raise used to be asked only on the Brokerages tab, so the same click here
  // went straight to the consent screen and took the account's one AI
  // connection from wherever it was.
  it('asks before a connect that costs the account its other AI connection', async () => {
    brokerages = [EXCLUSIVE_VENDOR];
    catalogData = makeCatalog([
      makeOauthServer({ name: 'ibkr', url: EXCLUSIVE_VENDOR.url, oauth_status: null }),
    ]);
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));

    expect(screen.getByText(/replaces whichever one is connected now/i)).toBeInTheDocument();
    expect(mockStartMcpOauth).not.toHaveBeenCalled();
  });

  it('goes on to the vendor once that question is answered', async () => {
    brokerages = [EXCLUSIVE_VENDOR];
    catalogData = makeCatalog([
      makeOauthServer({ name: 'ibkr', url: EXCLUSIVE_VENDOR.url, oauth_status: null }),
    ]);
    mockStartMcpOauth.mockResolvedValue({ authorize_url: 'https://vendor.example.test/a' });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));
    fireEvent.click(
      within(screen.getByRole('dialog')).getByRole('button', { name: /^connect$/i }),
    );

    await waitFor(() =>
      expect(mockStartMcpOauth).toHaveBeenCalledWith(
        'ibkr',
        '/plugins?tab=mcp',
        {
          vendorRefusesHostedCallback: false,
          expectedUrl: EXCLUSIVE_VENDOR.url,
          stillWanted: expect.any(Function),
          // Reached from the Connectors list, the consent is the same consent:
          // a brokerage connected from here must not carry more than one
          // connected from the tab that is about brokerages.
          grantedCapabilities: EXCLUSIVE_DEFAULT,
        },
      ),
    );
  });

  // The same row reached from the other tab, and the two seed the dialog
  // independently. A repair opened from here has to start from the user's last
  // answer for the same reason it does over there.
  it('reopens a repair on the groups the user last chose', async () => {
    brokerages = [EXCLUSIVE_VENDOR];
    catalogData = makeCatalog([
      makeOauthServer({
        name: 'ibkr',
        url: EXCLUSIVE_VENDOR.url,
        oauth_status: 'needs_reauth',
        granted_capabilities: null,
        remembered_capabilities: ['market_data'],
      }),
    ]);
    mockStartMcpOauth.mockResolvedValue({ authorize_url: 'https://vendor.example.test/a' });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^reconnect$/i }));
    fireEvent.click(
      within(screen.getByRole('dialog')).getByRole('button', { name: /^connect$/i }),
    );

    await waitFor(() =>
      expect(mockStartMcpOauth).toHaveBeenCalledWith(
        'ibkr',
        '/plugins?tab=mcp',
        expect.objectContaining({ grantedCapabilities: ['market_data'] }),
      ),
    );
  });

  it('starts nothing when the user backs out of it', () => {
    brokerages = [EXCLUSIVE_VENDOR];
    catalogData = makeCatalog([
      makeOauthServer({ name: 'ibkr', url: EXCLUSIVE_VENDOR.url, oauth_status: null }),
    ]);
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));
    fireEvent.click(
      within(screen.getByRole('dialog')).getByRole('button', { name: /cancel/i }),
    );

    expect(screen.queryByText(/replaces whichever one is connected now/i)).toBeNull();
    expect(mockStartMcpOauth).not.toHaveBeenCalled();
  });

  // A registry that failed is not a registry that answered "no brokers". Read
  // as the latter, a broker row loses the terms its address carries -- and the
  // warning is suppressed on exactly the rows that still need it, because it
  // only renders on one believed unconnected.
  it('holds the click when the registry could not be read at all', () => {
    brokerages = undefined;
    brokeragesError = new Error('offline');
    catalogData = makeCatalog([
      makeOauthServer({ name: 'ibkr', url: EXCLUSIVE_VENDOR.url, oauth_status: null }),
    ]);
    renderWithProviders(<McpServers />);

    const connect = screen.getByRole('button', { name: /^connect$/i });
    fireEvent.click(connect);
    expect(connect).toHaveAttribute('aria-disabled', 'true');
    expect(mockStartMcpOauth).not.toHaveBeenCalled();
  });

  // Held is right; silent is not. The button is the same one every OAuth row on
  // this page uses, brokerage or not, so an outage in an optional listing takes
  // the whole list with it -- and a disabled control with nothing beside it is
  // the user's own page appearing to have broken for no reason.
  it('says why, and offers another go, when the registry is the thing that failed', () => {
    brokerages = undefined;
    brokeragesError = new Error('offline');
    catalogData = makeCatalog([makeOauthServer({ oauth_status: null })]);
    renderWithProviders(<McpServers />);

    const note = screen.getByText(/broker requirements could not be checked/i);
    expect(screen.getByRole('button', { name: /^connect$/i })).toHaveAttribute(
      'aria-describedby',
      note.closest('[id]')!.id,
    );

    fireEvent.click(screen.getByRole('button', { name: /retry/i }));
    expect(refetchBrokerages).toHaveBeenCalledTimes(1);
  });

  // A refetch that failed is not the same as never having been answered. The
  // registry is what this build ships, so the answer already in hand is as good
  // as it was -- and throwing it away holds every connect on the page over a
  // background request the user never made.
  it('keeps the terms it already has when a later refetch fails', () => {
    brokerages = [EXCLUSIVE_VENDOR];
    brokeragesError = new Error('offline');
    catalogData = makeCatalog([
      makeOauthServer({ name: 'ibkr', url: EXCLUSIVE_VENDOR.url, oauth_status: null }),
    ]);
    renderWithProviders(<McpServers />);

    expect(screen.queryByText(/broker requirements could not be checked/i)).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));
    // Still the vendor's own terms, asked before anything is spent.
    expect(screen.getByText(/replaces whichever one is connected now/i)).toBeInTheDocument();
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

  it('says what to do when the row moved while the page sat here', async () => {
    // The backend refuses a connect whose address is not the one this page drew
    // the row from, and the row is editable from any tab. Its 409 is about a
    // row; what the user needs is what to do about it, in our own words. The
    // copy is asserted rather than the status, because a key that goes missing
    // renders as its own name and the toast still counts as raised.
    catalogData = makeCatalog([makeOauthServer({ oauth_status: null })]);
    mockStartMcpOauth.mockRejectedValue({
      response: { status: 409, data: { detail: "This server's address changed" } },
    });
    renderWithProviders(<McpServers />);

    fireEvent.click(screen.getByRole('button', { name: /^connect$/i }));

    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          description:
            'This server\'s address changed since this page was loaded. Reload the page, then connect again.',
        }),
      ),
    );
    expect(assign).not.toHaveBeenCalled();
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
    // The import modal opens via the page-level Add menu's URL intent.
    renderWithProviders(<McpServers />, { route: '/plugins?tab=mcp&add=import' });

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
    renderWithProviders(<McpServers />, { route: '/plugins?tab=mcp&add=import' });

    fireEvent.change(screen.getByRole('textbox'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));

    await waitFor(() => expect(screen.getByText(/Imported 0 of 1 server/i)).toBeInTheDocument());
    expect(toast).not.toHaveBeenCalled();
  });

  it('surfaces a rejected import inside the modal', async () => {
    mutateAsync.import.mockRejectedValue({ response: { data: { detail: 'catalog at cap' } } });
    renderWithProviders(<McpServers />, { route: '/plugins?tab=mcp&add=import' });

    fireEvent.change(screen.getByRole('textbox'), { target: { value: BLOB } });
    fireEvent.click(await screen.findByRole('button', { name: /^import 1$/i }));

    await waitFor(() => expect(screen.getByText('catalog at cap')).toBeInTheDocument());
  });
});

// ---------------------------------------------------------------------------
// Narrowing verdicts
// ---------------------------------------------------------------------------

/**
 * The tab renders several independently-filtered sections, so "is anything
 * left?" is a question about the whole visible population, not about one
 * section. Both directions were wrong at once: a filter matching only a
 * plugin's rows printed the notice directly above the deck that had matched,
 * and a user whose servers are all plugin-owned got a bare header over nothing.
 */
describe('McpServers — filtered and empty', () => {
  it('keeps the no-matches notice out of the way of a plugin deck that matched', async () => {
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'owned_one', plugin_name: 'acme-pack', plugin_enabled: true }),
      makeCatalogServer({ name: 'hand_made' }),
    ]);
    renderWithProviders(<McpServers />);

    fireEvent.change(screen.getByRole('searchbox'), { target: { value: 'acme-pack' } });

    await waitFor(() =>
      expect(screen.getByTestId('server-row-owned_one')).toBeInTheDocument(),
    );
    expect(screen.queryByText('No matches')).not.toBeInTheDocument();
  });

  it('shows the notice exactly once when nothing anywhere matches', async () => {
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'owned_one', plugin_name: 'acme-pack', plugin_enabled: true }),
    ]);
    renderWithProviders(<McpServers />);

    fireEvent.change(screen.getByRole('searchbox'), { target: { value: 'zzzz' } });

    await waitFor(() => expect(screen.getAllByText('No matches')).toHaveLength(1));
  });

  it('still invites a first server when every catalog row is plugin-owned', () => {
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'owned_one', plugin_name: 'acme-pack', plugin_enabled: true }),
    ]);
    renderWithProviders(<McpServers />);

    expect(screen.getByTestId('server-row-owned_one')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /add server/i })).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Add intent
// ---------------------------------------------------------------------------

describe('McpServers — add intent', () => {
  it('consumes the intent so a remount does not re-open the modal', async () => {
    // Tab bodies are conditionally rendered: switching away and back remounts
    // this list, and an intent left in the URL would open the modal again.
    const { unmount } = renderWithProviders(<McpServers />, {
      route: '/plugins?tab=mcp&add=import',
    });
    await waitFor(() => expect(screen.getByRole('textbox')).toBeInTheDocument());
    unmount();

    renderWithProviders(<McpServers />, { route: '/plugins?tab=mcp' });
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Deck actions
// ---------------------------------------------------------------------------

describe('McpServers — View plugin', () => {
  /** The deck already knows which package it is. Setting only `tab=plugins`
   *  drops the reader on the bare list to find it again, which reads as a
   *  no-op whenever more than one package is installed. */
  function Location() {
    const [params] = useSearchParams();
    return <div data-testid="loc">{params.toString()}</div>;
  }

  it('carries the deck name into the plugin detail', async () => {
    catalogData = makeCatalog([
      makeCatalogServer({ name: 'owned_one', plugin_name: 'acme-pack', plugin_enabled: true }),
    ]);
    renderWithProviders(
      <>
        <McpServers />
        <Location />
      </>,
      { route: '/plugins?tab=mcp' },
    );

    fireEvent.click(screen.getByRole('button', { name: /view plugin/i }));

    await waitFor(() => {
      const params = new URLSearchParams(screen.getByTestId('loc').textContent ?? '');
      expect(params.get('tab')).toBe('plugins');
      expect(params.get('detail')).toBe('plugin:acme-pack');
    });
  });
});

// ---------------------------------------------------------------------------
// The shipped servers failing to load
// ---------------------------------------------------------------------------

describe('McpServers — the shipped list fails', () => {
  // The shipped decks are the only thing that query feeds, so with no notice
  // an outright failure renders as "this build ships nothing" — and the user's
  // own section loads fine beside it, which makes the page look healthy.
  it('says so and offers a retry instead of showing nothing', () => {
    builtinError = new Error('boom');
    catalogData = makeCatalog([]);

    renderWithProviders(<McpServers />);

    expect(
      screen.getByText(/servers that ship with the app/i),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /retry/i }));
    expect(refetchBuiltins).toHaveBeenCalled();
  });

  it('stays out of the way when the query is fine', () => {
    renderWithProviders(<McpServers />);

    expect(
      screen.queryByText(/servers that ship with the app/i),
    ).not.toBeInTheDocument();
  });
});
