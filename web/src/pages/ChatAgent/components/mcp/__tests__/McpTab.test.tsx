import { describe, it, expect, vi, beforeEach, afterEach, afterAll } from 'vitest';
import { act, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import type { CatalogServerList, EffectiveServer, EffectiveServerList } from '../../../utils/api';

// ---------------------------------------------------------------------------
// Mock the MCP hooks so we drive list data + mutation outcomes directly.
// ---------------------------------------------------------------------------

const mutateAsync = {
  add: vi.fn(),
  update: vi.fn(),
  toggle: vi.fn(),
  discover: vi.fn(),
  import: vi.fn(),
};

let listData: EffectiveServerList | undefined;
let catalogData: CatalogServerList | undefined;

vi.mock('@/hooks/useMcpServers', () => ({
  useMcpCatalog: () => ({ data: catalogData }),
  // Read by the add form for the names already taken.
  useBuiltinMcpServers: () => ({ data: undefined }),
  useWorkspaceMcpServers: () => ({ data: listData, isLoading: false, error: null }),
  useAddWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.add, isPending: false }),
  useUpdateWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.update, isPending: false }),
  useToggleWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.toggle, isPending: false }),
  useDiscoverWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.discover, isPending: false }),
  useImportWorkspaceMcpServers: () => ({ mutateAsync: mutateAsync.import, isPending: false }),
  // Reached through useMcpOauthActions and the brokerage lookup; inert here.
  useDisconnectMcpOauth: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useRefreshMcpOauthSchemas: () => ({ mutateAsync: vi.fn(), isPending: false }),
  // Never answered here, so every connect has to ask the registry itself.
  useBrokerages: () => ({ data: undefined }),
  brokeragesQuery: { queryKey: ['brokerages', 'list'], queryFn: () => registry() },
  // Pass-through (no fake timers in this suite): the anti-flicker is unit-tested
  // separately in useMcpServers.test; here `synced` should reflect the raw value.
  useDelayedFalse: (v: boolean) => v,
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

/** What the brokerage registry answers a connect that asks for it. */
let registry: () => Promise<unknown[]> = async () => [];

// Inside the desktop app the vendor's page opens in the system browser and this
// page stays where it is. Everything else about the shell stays real.
let inShell = false;
vi.mock('@/lib/desktop', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/desktop')>()),
  isDesktopShell: () => inShell,
}));

// The secret picker reads the account vault through React Query; keep it
// empty and benign. (`formatApiErrorDetail` stays real — the inline submit-error
// copy is what the submit tests assert on.)
vi.mock('@/hooks/useUserVault', () => ({
  useUserVaultSecrets: () => ({ data: { secrets: [], remaining_slots: 10 }, isLoading: false, error: null }),
  useCreateUserVaultSecret: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useUpdateUserVaultSecret: () => ({ mutateAsync: vi.fn(), isPending: false }),
}));

const startPendingMcpOauth = vi.fn();
vi.mock('@/pages/ChatAgent/utils/api/mcpOauth', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/mcpOauth')>()),
  startPendingMcpOauth: (...args: unknown[]) => startPendingMcpOauth(...args),
}));

const navigate = vi.fn();
vi.mock('react-router', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router')>()),
  useNavigate: () => navigate,
}));

// Stub the row so its actions are plain buttons: the real Radix kebab needs
// portal/pointer machinery jsdom doesn't drive (the row's own test mocks the
// dropdown for the same reason). McpServerRow's item wiring is covered there.
vi.mock('../McpServerRow', () => ({
  McpServerRow: ({
    server,
    onSetupSecret,
    onEdit,
  }: {
    server: { name: string; missing_secrets: string[] };
    onSetupSecret: (name: string) => void;
    onEdit: (server: unknown) => void;
  }) => (
    <div data-testid={`row-${server.name}`}>
      <span>{server.name}</span>
      <button type="button" onClick={() => onEdit(server)}>{`edit-${server.name}`}</button>
      {server.missing_secrets.map((secret) => (
        <button key={secret} type="button" onClick={() => onSetupSecret(secret)}>
          {`setup-${secret}`}
        </button>
      ))}
    </div>
  ),
}));

import { McpTab } from '../McpTab';
import { ConnectConsent } from '@/pages/Plugins/components/ConnectConsent';

/** The tab as the app shell mounts it, with the one dialog that asks a
 *  connect's question. */
const renderTab = () =>
  renderWithProviders(
    <>
      <McpTab workspaceId="ws-1" />
      <ConnectConsent />
    </>,
  );
import { toast } from '@/components/ui/use-toast';

// A navigation that never unloads: the stub records the vendor's address and
// the page stays, as it does when the desktop app sends the address to the
// system browser, or a "leave this page?" prompt is answered Stay. The page
// leaving is a `pagehide` the test dispatches itself.
const realLocation = window.location;
const assign = vi.fn();
const WORKSPACE_PATH = '/chat/ws-1';
Object.defineProperty(window, 'location', {
  configurable: true,
  value: {
    href: `http://localhost${WORKSPACE_PATH}`,
    origin: 'http://localhost',
    pathname: WORKSPACE_PATH,
    assign,
  },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

function makeServer(name: string, overrides: Partial<EffectiveServer> = {}): EffectiveServer {
  return {
    name,
    origin: 'user',
    transport: 'stdio',
    enabled: true,
    editable: true,
    status: 'connected',
    error: '',
    tool_count: 0,
    tools: [],
    missing_secrets: [],
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    command: 'npx',
    args: [],
    url: null,
    config_version: 1,
    ...overrides,
  };
}

/** The notice before the vendor's page: nothing starts until it is confirmed. */
async function continueToVendor() {
  const notice = await screen.findByRole('dialog', { name: 'Sign in to good_name' });
  fireEvent.click(within(notice).getByRole('button', { name: 'Continue to example.com' }));
}

function makeList(servers: EffectiveServer[], maxServers = 20): EffectiveServerList {
  return { servers, sandbox_running: true, max_servers: maxServers, config_version: 1 };
}

/** Only the count reaches the tab, so the rows can stay bare. */
function makeCatalog(count: number, maxServers: number): CatalogServerList {
  return {
    servers: Array.from(
      { length: count },
      (_, i) => ({ name: `s${i}`, header_refs: [], env_refs: [] }) as unknown as CatalogServerList['servers'][number],
    ),
    max_servers: maxServers,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  listData = makeList([]);
  catalogData = makeCatalog(0, 20);
  registry = async () => [];
  inShell = false;
  sessionStorage.clear();
});

afterEach(() => {
  // The page "leaves" once, between tests, so a sign-in a test started and
  // never saw out marks nothing in the next test's storage.
  window.dispatchEvent(new Event('pagehide'));
  vi.useRealTimers();
});

describe('McpTab — submit error formatting', () => {
  it('renders FastAPI array-shaped validation detail as readable text (not [object Object])', async () => {
    // FastAPI 422 validation list — must be flattened, not stringified.
    mutateAsync.add.mockRejectedValue({
      response: {
        data: {
          detail: [
            { loc: ['body', 'url'], msg: 'field required', type: 'value_error.missing' },
          ],
        },
      },
    });

    renderTab();

    // Open the add-server modal, give it a valid name, and submit.
    fireEvent.click(screen.getByRole('button', { name: /add connector/i }));
    fireEvent.change(screen.getByTestId('mcp-entry'), { target: { value: 'npx -y @scope/thing' } });
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));

    const expected = 'body.url: field required';
    await waitFor(() => expect(screen.getByText(expected)).toBeInTheDocument());
    // The flattened message must not collapse to the object placeholder.
    expect(screen.queryByText('[object Object]')).not.toBeInTheDocument();
  });
});

describe('McpTab: validate first', () => {
  const fillAndSubmit = () => {
    fireEvent.click(screen.getByRole('button', { name: /add connector/i }));
    fireEvent.change(screen.getByTestId('mcp-entry'), { target: { value: 'https://mcp.example.com/mcp' } });
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
  };

  it('shows each failed server and its reason inline when the check turns the save down', async () => {
    mutateAsync.add.mockRejectedValueOnce({
      response: {
        status: 422,
        data: {
          detail: {
            code: 'mcp_check_failed',
            failures: [{ server: 'good_name', verdict: 'unreachable', reason: 'Connection refused.' }],
          },
        },
      },
    });
    renderTab();
    fillAndSubmit();

    const box = await screen.findByTestId('mcp-check-failures');
    expect(box).toHaveTextContent('good_name');
    expect(box).toHaveTextContent('Connection refused.');
  });

  it('starts a pending sign-in for this workspace when the save needs one', async () => {
    mutateAsync.add.mockRejectedValueOnce({
      response: {
        status: 409,
        data: {
          detail: {
            code: 'mcp_sign_in_required',
            servers: [{ server: 'good_name', url: 'https://mcp.example.com/mcp' }],
          },
        },
      },
    });
    startPendingMcpOauth.mockResolvedValueOnce(null);
    renderTab();
    fillAndSubmit();

    await continueToVendor();
    await waitFor(() => expect(startPendingMcpOauth).toHaveBeenCalledTimes(1));
    expect(startPendingMcpOauth.mock.calls[0][0]).toMatchObject({
      kind: 'workspace_server',
      workspaceId: 'ws-1',
      server: expect.objectContaining({ name: 'good_name', url: 'https://mcp.example.com/mcp' }),
    });
  });
});

describe('McpTab: a sign-in that saves the server', () => {
  const URL = 'https://mcp.example.com/mcp';
  const signInNeeded = () =>
    mutateAsync.add.mockRejectedValueOnce({
      response: {
        status: 409,
        data: { detail: { code: 'mcp_sign_in_required', servers: [{ server: 'good_name', url: URL }] } },
      },
    });
  const started = {
    authorize_url: 'https://vendor.test/authorize',
    state: 'st-1',
    server: 'good_name',
    kind: 'workspace_server',
  };
  const fillAndSubmit = () => {
    fireEvent.click(screen.getByRole('button', { name: /add connector/i }));
    fireEvent.change(screen.getByTestId('mcp-entry'), { target: { value: URL } });
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
  };
  // The connects that left this page, as a return without an answer reads them.
  const markers = () => {
    const records: Array<{ server: string; from: string; left: boolean }> = JSON.parse(
      sessionStorage.getItem('mcp:connect-started') ?? '[]',
    );
    return records.filter((r) => r.left).map(({ server, from }) => ({ server, from }));
  };

  // The desktop app opens the vendor's page in the system browser and this
  // page stays, for up to ten minutes if the user walks away from it. The form
  // used to hold the user all that time.
  it('closes the form once the desktop app has taken the sign-in to the browser', async () => {
    inShell = true;
    signInNeeded();
    startPendingMcpOauth.mockResolvedValueOnce(started);
    renderTab();
    fillAndSubmit();

    await continueToVendor();
    await waitFor(() => expect(assign).toHaveBeenCalledWith('https://vendor.test/authorize'));
    await waitFor(() => expect(screen.queryByTestId('mcp-entry')).not.toBeInTheDocument());
    expect(toast).toHaveBeenCalledWith({
      title: 'Sign-in opened in your browser',
      description: 'Finish signing in to "good_name" there. Nothing is saved until you do.',
    });
    // Nothing has left yet, so nothing is waiting to be called abandoned.
    expect(markers()).toEqual([]);
    // The rest of the tab is the user's again.
    expect(screen.getByRole('button', { name: /add connector/i })).toBeEnabled();

    // The page going is a reload here, with the flow still live in the system
    // browser; only the shell's timeout, which brings the window back, ends it.
    window.dispatchEvent(new Event('pagehide'));
    expect(markers()).toEqual([]);
  });

  // In a browser the page leaves, and a navigation that did not (refused, or a
  // "leave this page?" prompt answered Stay) must not leave the form locked.
  it('gives the form back when the page never left for the vendor', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    signInNeeded();
    startPendingMcpOauth.mockResolvedValueOnce(started);
    renderTab();
    fillAndSubmit();

    await continueToVendor();
    await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
    expect(screen.getByTestId('mcp-submit')).toHaveTextContent('Opening sign-in');
    expect(screen.getByTestId('mcp-submit')).toBeDisabled();

    await act(async () => {
      vi.advanceTimersByTime(5_000);
    });
    expect(screen.getByTestId('mcp-submit')).toBeEnabled();
    expect(screen.getByTestId('mcp-submit')).toHaveTextContent(/^Add$/);
    expect(screen.getByTestId('mcp-entry')).toHaveValue(URL);
    expect(toast).not.toHaveBeenCalled();
    // A page that leaves later, for any other reason, is not this sign-in.
    window.dispatchEvent(new Event('pagehide'));
    expect(markers()).toEqual([]);
  });

  it('keeps the form while the page leaves, and marks the sign-in on this page', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    signInNeeded();
    startPendingMcpOauth.mockResolvedValueOnce(started);
    renderTab();
    fillAndSubmit();

    await continueToVendor();
    await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
    window.dispatchEvent(new Event('pagehide'));
    // Back from the vendor's page lands here, so this page explains it.
    expect(markers()).toEqual([{ server: 'good_name', from: WORKSPACE_PATH }]);

    await act(async () => {
      vi.advanceTimersByTime(5_000);
    });
    expect(screen.getByTestId('mcp-submit')).toBeDisabled();
  });

  // A brokerage's consent comes before its vendor's page. With the registry
  // still unanswered the connect read the address as no vendor at all, sent no
  // capability selection, and the start was refused for want of one.
  it("asks a brokerage's consent even when the registry had not answered yet", async () => {
    registry = async () => [
      {
        name: 'ibkr',
        label: 'Interactive Brokers',
        url: URL,
        site: 'example.com',
        description: '',
        native_callback_only: false,
        exclusive_connection: false,
        capabilities: [
          { key: 'market_data', tone: 'neutral' },
          { key: 'staged_orders', tone: 'caution', rung: true },
        ],
      },
    ];
    signInNeeded();
    startPendingMcpOauth.mockResolvedValueOnce(started);
    renderTab();
    fillAndSubmit();

    const consent = await screen.findByRole('dialog', { name: /Connect “/ });
    expect(startPendingMcpOauth).not.toHaveBeenCalled();
    fireEvent.click(within(consent).getByRole('button', { name: 'Connect' }));

    await waitFor(() => expect(startPendingMcpOauth).toHaveBeenCalledTimes(1));
    expect(startPendingMcpOauth.mock.calls[0][2]).toMatchObject({
      grantedCapabilities: ['market_data', 'staged_orders'],
    });
  });

  it('says so in the form when the registry cannot be read', async () => {
    registry = async () => {
      throw new Error('Brokerage registry unavailable');
    };
    signInNeeded();
    renderTab();
    fillAndSubmit();

    expect(await screen.findByText('Brokerage registry unavailable')).toBeInTheDocument();
    expect(startPendingMcpOauth).not.toHaveBeenCalled();
    expect(screen.getByTestId('mcp-submit')).toBeEnabled();
  });
});

describe('McpTab: account servers', () => {
  it('holds Add and Import at the account cap, with the reason on each', () => {
    // The catalog counts servers switched off account-wide, which this
    // workspace's list leaves out, so a list of one can still be at the cap.
    listData = makeList([makeServer('a')], 3);
    catalogData = makeCatalog(3, 3);
    renderTab();

    const reason = 'Your account is at its limit of 3 connectors. Remove one in Plugins first.';
    for (const name of [/add connector/i, /import json/i]) {
      const button = screen.getByRole('button', { name });
      expect(button).toBeDisabled();
      expect(button).toHaveAttribute('title', reason);
    }
  });

  it('does not count this list against the cap, and shows a 409 in the form', async () => {
    // Two rows here against a cap of two is not a full account: the gate reads
    // the catalog, and a refusal it misses lands where the user is typing.
    listData = makeList([makeServer('a'), makeServer('b')], 2);
    catalogData = makeCatalog(2, 3);
    mutateAsync.add.mockRejectedValue({
      response: { status: 409, data: { detail: 'You already have a server named good_name.' } },
    });
    renderTab();

    fireEvent.click(screen.getByRole('button', { name: /add connector/i }));
    expect(screen.getByText(/switches it on only in this workspace/i)).toBeInTheDocument();
    fireEvent.change(screen.getByTestId('mcp-entry'), { target: { value: 'npx -y @scope/thing' } });
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));

    await waitFor(() =>
      expect(screen.getByText('You already have a server named good_name.')).toBeInTheDocument(),
    );
  });

  it('sends "Set up NAME" to the account vault with the name prefilled', () => {
    listData = makeList([
      makeServer('needs', { status: 'needs_secret', missing_secrets: ['MY_API_KEY'] }),
    ]);
    renderTab();

    fireEvent.click(screen.getByText('setup-MY_API_KEY'));
    expect(navigate).toHaveBeenCalledWith('/plugins?tab=secrets&secret=MY_API_KEY');
  });

  it('names the account reach in the save label', () => {
    // The row sits in one workspace's list, but the edit rewrites the account
    // connector every workspace reads, and the plain "Save" did not say so.
    listData = makeList([makeServer('shared')]);
    renderTab();

    fireEvent.click(screen.getByText('edit-shared'));
    expect(screen.getByText('Edit connector')).toBeInTheDocument();
    expect(screen.getByTestId('mcp-submit')).toHaveTextContent('Save for all workspaces');
  });
});

describe('McpTab — auto-resolve pending servers', () => {
  it('probes a pending server once when the sandbox is running', async () => {
    listData = makeList([makeServer('pend', { status: 'pending' })]);
    mutateAsync.discover.mockResolvedValue({ status: 'connected', tools: [], error: '' });
    renderTab();

    // A fresh pending server shouldn't sit on a dead pill — it gets probed.
    await waitFor(() => expect(mutateAsync.discover).toHaveBeenCalledWith('pend'));
    expect(mutateAsync.discover).toHaveBeenCalledTimes(1);
  });

  it('does NOT probe when the sandbox is stopped (nothing to discover against)', async () => {
    listData = { ...makeList([makeServer('pend', { status: 'pending' })]), sandbox_running: false };
    renderTab();

    await waitFor(() => expect(screen.getByTestId('row-pend')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });

  it('does NOT probe a disabled pending server (it reads as Disabled)', async () => {
    listData = makeList([makeServer('off', { status: 'pending', enabled: false })]);
    renderTab();

    await waitFor(() => expect(screen.getByTestId('row-off')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });

  it('does NOT probe an OAuth row — discovery is host-side and the backend 409s', async () => {
    // The gate that keeps this and the list query's self-stopping poll in
    // agreement: probing here would 409, and counting it there would poll
    // forever on a server no probe can ever resolve.
    listData = makeList([
      makeServer('oauth_row', { origin: 'user', status: 'pending', oauth_status: 'connected' }),
    ]);
    renderTab();

    await waitFor(() => expect(screen.getByTestId('row-oauth_row')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });

  it('does NOT probe a builtin (always connected, process-global)', async () => {
    listData = makeList([makeServer('builtin_row', { origin: 'builtin', status: 'pending' })]);
    renderTab();

    await waitFor(() => expect(screen.getByTestId('row-builtin_row')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });

  it('does NOT re-probe a connected server', async () => {
    listData = makeList([makeServer('ok', { status: 'connected' })]);
    renderTab();

    await waitFor(() => expect(screen.getByTestId('row-ok')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });
});
