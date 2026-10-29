import type { ReactElement } from 'react';
import { describe, it, expect, vi, beforeEach, afterAll } from 'vitest';
import { act, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { ApiError } from '@/api/client';
import { LoopbackRequiredError } from '@/pages/ChatAgent/utils/api';
import type {
  CatalogServer,
  McpPendingStart,
  PendingSignIn,
  StartMcpOauthOptions,
} from '@/pages/ChatAgent/utils/api';
import type { DesktopBridge } from '@/lib/desktop';
import { httpCatalogServer } from '@/test/factories';
import { brokerageForUrl, type Brokerage } from '../brokerages';

/**
 * The brokerages tab, and what a brokerage keeps once it is an ordinary row.
 *
 * The two halves are tested together because the tab is a front door rather
 * than a tier: the row it creates goes on living in the Connectors list, and
 * everything a brokerage carries there has to be readable back off its
 * address. A test for either half alone would let them drift.
 */

const RH: Brokerage = {
  name: 'robinhood',
  label: 'Robinhood',
  url: 'https://agent.robinhood.com/mcp/trading',
  site: 'robinhood.com',
  description: 'Balances, positions, and order placement.',
  native_callback_only: true,
  exclusive_connection: false,
  // Shaped like the real curation in the two ways the surface reads: one broker
  // can place orders and the other cannot, so the group that is off by default
  // is present on exactly one of them.
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'trading', tone: 'danger', rung: true },
  ],
};
/** What Robinhood's connect grants unless the user says otherwise. */
const RH_DEFAULT = ['market_data', 'account'];
const IBKR: Brokerage = {
  name: 'ibkr',
  label: 'Interactive Brokers',
  url: 'https://api.ibkr.com/v1/api/mcp-public',
  site: 'interactivebrokers.com',
  description: 'Portfolio, positions, and draft orders.',
  native_callback_only: false,
  exclusive_connection: true,
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'staged_orders', tone: 'caution', rung: true },
  ],
};
const IBKR_DEFAULT = ['market_data', 'account', 'staged_orders'];

/** A brokerage's own catalog row. Named, always, since the tab joins on it. */
function catalogRow(over: Partial<CatalogServer> & { name: string }): CatalogServer {
  return httpCatalogServer({ url: '', description: '', ...over });
}

// Typed against the real bridge: the stub used to answer `beginMcpOAuth` with a
// string, which is not what the shell returns, and the next person to copy it
// would have written a stub the app could never receive.
// `undefined` is a browser. An object with no `beginMcpOAuth` is the third
// case and the one worth having a name for: a shell old enough to predate the
// loopback channel, which is present and still cannot finish one of these.
let shell: Partial<DesktopBridge> | undefined;
vi.mock('@/lib/desktop', () => ({
  get desktop() {
    return shell;
  },
  isDesktopShell: () => shell !== undefined,
  canBeginMcpOAuth: () => typeof shell?.beginMcpOAuth === 'function',
  beginMcpOAuth: async () => undefined,
}));

let tradingLevel: string | undefined;
vi.mock('@/hooks/useTradingPermission', () => ({
  useTradingPermission: () => ({
    data: tradingLevel ? { level: tradingLevel, agreement_version: 1 } : undefined,
  }),
}));

const toggleBrokerage = vi.fn(async () => ({}));
let shipped: Brokerage[] = [];
// False is the registry not having answered: in flight, or asked and failed.
let registryAnswered = true;
let catalogServers: CatalogServer[] = [];
const deleteServer = vi.fn(async () => ({}));
vi.mock('@/hooks/useMcpServers', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useMcpServers')>();
  return {
    ...actual,
    useBrokerages: () => ({
      data: registryAnswered ? shipped : undefined,
      isLoading: false,
      error: null,
    }),
    useToggleBrokerage: () => ({ mutateAsync: toggleBrokerage }),
    useMcpCatalog: () => ({
      data: { servers: catalogServers, max_servers: 50 },
      isLoading: false,
      error: null,
    }),
    useDeleteMcpCatalogServer: () => ({ mutateAsync: deleteServer, isPending: false }),
    useSetMcpServerEnabledInWorkspace: () => ({ mutateAsync: vi.fn(async () => ({})) }),
  };
});

vi.mock('../hooks/useWorkspaceOptions', () => ({
  useWorkspaceOptions: () => ({ workspaces: [], nameById: new Map() }),
  useHomeOption: () => undefined,
}));

// The connect lifecycle runs for real: the question a vendor's terms raise is
// the hook's, not this tab's, and a stub in its place would test the strip
// while leaving the gate itself unexercised. Only the calls that leave the
// browser are replaced: the reconnect of a saved row, and the sign-in that
// saves a broker with no row yet.
//
// `stillWanted` is the one thing the hook cannot answer for itself: the real
// module arms the shell's listener before either of its round trips and keeps
// the flow id, so it is the only place that can both ask whether the connect is
// still wanted and give back what it armed. The stubs answer the same way it
// does -- the URL, or null for a caller that took the connect back while those
// round trips were in flight.
const answerStart = (stillWanted?: () => boolean) =>
  stillWanted && !stillWanted()
    ? null
    : { authorize_url: 'https://vendor.test/authorize', state: 'st-1' };

/** The pending start's answer, which also names the row the callback saves.
 *  Every pending sign-in on this tab is a brokerage's. */
const answerPending = (
  request: PendingSignIn,
  stillWanted?: () => boolean,
): McpPendingStart | null => {
  const started = answerStart(stillWanted);
  if (!started || request.kind !== 'brokerage') return null;
  return { ...started, kind: 'brokerage', server: request.name };
};

type PendingOptions = Omit<StartMcpOauthOptions, 'expectedUrl'>;

const startConnect = vi.fn(
  async (_name: string, _returnTo?: string, options?: StartMcpOauthOptions) =>
    answerStart(options?.stillWanted),
);
const startPending = vi.fn(
  async (request: PendingSignIn, _returnTo?: string, options?: PendingOptions) =>
    answerPending(request, options?.stillWanted),
);
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>();
  return {
    ...actual,
    startMcpOauth: (name: string, returnTo?: string, options?: StartMcpOauthOptions) =>
      startConnect(name, returnTo, options),
    startPendingMcpOauth: (
      request: PendingSignIn,
      returnTo?: string,
      options?: PendingOptions,
    ) => startPending(request, returnTo, options),
  };
});

// Watched rather than rendered: no Toaster is mounted here, and what the tab
// decides is which toast to raise -- a failure, or the offer of a sign-in.
const toastSpy = vi.fn();
vi.mock('@/components/ui/use-toast', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/components/ui/use-toast')>();
  return {
    ...actual,
    toast: (props: Parameters<typeof actual.toast>[0]) => {
      toastSpy(props);
      return actual.toast(props);
    },
  };
});

/** A refusal shaped the way the API client rejects one. */
function apiError(status: number, detail: unknown) {
  return new ApiError(
    `Request failed with status code ${status}`,
    status >= 500 ? 'ERR_BAD_RESPONSE' : 'ERR_BAD_REQUEST',
    { status, statusText: '', data: { detail }, headers: {} },
  );
}

/**
 * Let every queued microtask and zero-delay timer run, so an assertion that
 * something did NOT happen has given it every chance to.
 */
const flush = () =>
  act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });

const realLocation = window.location;
const assign = vi.fn();
Object.defineProperty(window, 'location', {
  configurable: true,
  value: {
    href: 'http://localhost/plugins',
    origin: 'http://localhost',
    pathname: '/plugins',
    assign,
  },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

beforeEach(() => {
  shell = undefined;
  tradingLevel = undefined;
  shipped = [RH, IBKR];
  registryAnswered = true;
  catalogServers = [];
  toggleBrokerage.mockClear();
  startConnect.mockClear();
  startPending.mockClear();
  toastSpy.mockClear();
  deleteServer.mockClear();
  assign.mockClear();
  // A connect writes the real store, and its record outlives a test.
  sessionStorage.clear();
});

/** The connects that left this page, as a return without an answer reads them. */
const pendingMarkers = () => {
  const records: Array<{ server: string; from: string; left: boolean }> = JSON.parse(
    sessionStorage.getItem('mcp:connect-started') ?? '[]',
  );
  return records.filter((r) => r.left).map(({ server, from }) => ({ server, from }));
};

/** The page going, which is when a connect that left is marked: the stubbed
 *  `assign` navigates nowhere, so the test says when it would have. */
const leave = () => window.dispatchEvent(new Event('pagehide'));

/** The tab as the app shell mounts it, with the one dialog that asks a
 *  connect's question. */
async function renderTab() {
  const { Brokerages } = await import('../components/Brokerages');
  const { ConnectConsent } = await import('../components/ConnectConsent');
  return renderWithProviders(
    <>
      <Brokerages />
      <ConnectConsent />
    </>,
  );
}

/** Open a cell's ⋯ menu and choose one of its items by name. */
async function chooseFromMenu(cellLabel: string, item: string) {
  fireEvent.pointerDown(screen.getByLabelText(`Actions for ${cellLabel}`), {
    button: 0,
    ctrlKey: false,
  });
  const items = await screen.findAllByRole('menuitem');
  const found = items.find((el) => el.textContent?.startsWith(item));
  if (!found) throw new Error(`No menu item "${item}" in ${items.map((el) => el.textContent)}`);
  fireEvent.click(found);
}

/** The consent dialog, when a connect has raised its question first. */
function connectConfirm(): HTMLElement | null {
  return screen.queryByRole('dialog');
}

/**
 * Click Connect and take the dialog's default answer.
 *
 * Every brokerage raises one now, so the tests that are about what happens
 * after the click go through here rather than each spelling the answer out.
 */
function connectThrough(name: string) {
  fireEvent.click(screen.getByTestId(`brokerage-connect-${name}`));
  const dialog = connectConfirm();
  if (dialog) fireEvent.click(within(dialog).getByRole('button', { name: 'Connect' }));
}

describe('the brokerages tab', () => {
  it('lists every shipped broker, whether or not the user has one', async () => {
    catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url, oauth_status: 'connected' })];
    await renderTab();

    expect(screen.getByTestId('brokerage-robinhood')).toBeInTheDocument();
    expect(screen.getByTestId('brokerage-ibkr')).toBeInTheDocument();
    // Under the vendor's name, not the catalog row's.
    expect(screen.getByText('Robinhood')).toBeInTheDocument();
    expect(screen.getByText('Interactive Brokers')).toBeInTheDocument();
  });

  it('says which ones the user has not added', async () => {
    catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url })];
    await renderTab();

    expect(screen.getAllByText('Not added')).toHaveLength(1);
  });

  it('only offers the row lifecycle once there is a row', async () => {
    catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url })];
    await renderTab();

    // One menu, belonging to the broker that exists; the offer has nothing to
    // switch or remove yet.
    expect(
      screen.getByLabelText('Actions for Interactive Brokers'),
    ).toBeInTheDocument();
    expect(screen.queryByLabelText('Actions for Robinhood')).toBeNull();
    expect(screen.queryByRole('switch')).toBeNull();
  });

  describe('connecting', () => {
    // Nothing exists for a broker the user has not added, and nothing is
    // written for it here: the sign-in carries the request that creates the
    // row, and the callback saves it once the vendor has answered. A switch
    // flipped on the way past is a row enabled, and inherited by every
    // workspace, for a connect that may never finish.
    it('starts the sign-in that creates a broker with no row, and switches nothing on', async () => {
      await renderTab();
      connectThrough('ibkr');

      // The vendor's terms travel as the one flag the start actually needs, and
      // the return lands back on this tab rather than the MCP one. The grant is
      // the dialog's answer, which for a broker nobody has answered for is the
      // vendor's default.
      await waitFor(() =>
        expect(startPending).toHaveBeenCalledWith(
          { kind: 'brokerage', name: 'ibkr' },
          '/plugins?tab=brokerages',
          {
            vendorRefusesHostedCallback: false,
            stillWanted: expect.any(Function),
            grantedCapabilities: IBKR_DEFAULT,
          },
        ),
      );
      expect(startConnect).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    // The row is the user's now, and a reconnect is a reconnect of it: its own
    // address goes along as the gate the backend checks, and whether the row is
    // switched on is the callback's to settle once the vendor has answered.
    it('reconnects a broker that has a row through it, without switching it on here', async () => {
      catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url, enabled: false })];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(startConnect).toHaveBeenCalledWith('ibkr', '/plugins?tab=brokerages', {
          vendorRefusesHostedCallback: false,
          expectedUrl: IBKR.url,
          stillWanted: expect.any(Function),
          grantedCapabilities: IBKR_DEFAULT,
        }),
      );
      expect(startPending).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    // `bindMcpOAuth` answers false for a flow that timed out, was cancelled, or
    // went down with the shell, not only for one a second connect took over.
    // With nothing written before the vendor, a flow that never launched has
    // nothing to put back: the user is told why, and that is all.
    it('leaves nothing behind when the sign-in never launched', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      startPending.mockRejectedValueOnce(new LoopbackRequiredError('not-bound'));
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(toastSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            variant: 'destructive',
            title: 'Could not start the connection',
            description:
              'Another connection was started before this one finished. Try connecting again.',
          }),
        ),
      );
      expect(toggleBrokerage).not.toHaveBeenCalled();
      expect(assign).not.toHaveBeenCalled();
      expect(pendingMarkers()).toEqual([]);
    });

    // The return path reads the marker under the name the callback reports,
    // which is the one the start answered with, and on the page it left from.
    it('marks the connect under the name the sign-in will report', async () => {
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
      // Not before the page actually goes: a navigation can still be refused.
      expect(pendingMarkers()).toEqual([]);
      leave();
      expect(pendingMarkers()).toEqual([{ server: 'ibkr', from: '/plugins' }]);
    });

    // The toast is the one place the shell's hand-off is said, and it said the
    // server's id ("ibkr") where every other line on the surface names the
    // broker.
    it('names the broker when the desktop app takes the sign-in to the browser', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      await renderTab();
      connectThrough('ibkr');
      await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
      await waitFor(() =>
        expect(toastSpy).toHaveBeenCalledWith({
          title: 'Sign-in opened in your browser',
          description: 'Finish signing in to "Interactive Brokers" there. Nothing is saved until you do.',
        }),
      );
    });

    // In the desktop app the vendor's page is in the system browser and this
    // page stays, so the page going is a reload with the flow still live. Read
    // as backed out of, it said the sign-in was not finished, and spent the
    // record a failed finish reopens its form from.
    it('reads a reload in the desktop app as nothing, and the shell giving up as unfinished', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      const { settleConnectReturn } = await import('../connectReturn');
      await renderTab();
      connectThrough('ibkr');
      await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));

      leave();
      expect(settleConnectReturn('')).toBeNull();

      // Past the shell's timeout, which brings the window back unanswered.
      const later = Date.now() + 10 * 60_000;
      const clock = vi.spyOn(Date, 'now').mockReturnValue(later);
      try {
        expect(settleConnectReturn('')).toMatchObject({
          kind: 'abandoned',
          record: { server: 'ibkr' },
        });
      } finally {
        clock.mockRestore();
      }
    });

    it('leaves an already-live row alone and just reconnects', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: IBKR.url, oauth_status: 'needs_reauth' }),
      ];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(startConnect).toHaveBeenCalledWith('ibkr', '/plugins?tab=brokerages', {
          vendorRefusesHostedCallback: false,
          expectedUrl: IBKR.url,
          stillWanted: expect.any(Function),
          grantedCapabilities: IBKR_DEFAULT,
        }),
      );
      expect(startPending).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    // A token expiry is not a change of mind. `granted_capabilities` is
    // withheld on a row that can no longer be served -- deliberately, so
    // nothing badges a dead connection as able to place orders -- so seeding
    // the dialog from it opened a repair with every declined group re-ticked.
    it('reopens a repair on the groups the user last chose, not the defaults', async () => {
      catalogServers = [
        catalogRow({
          name: 'ibkr',
          url: IBKR.url,
          oauth_status: 'needs_reauth',
          granted_capabilities: null,
          remembered_capabilities: ['market_data'],
        }),
      ];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(startConnect).toHaveBeenCalledWith(
          'ibkr',
          '/plugins?tab=brokerages',
          expect.objectContaining({ grantedCapabilities: ['market_data'] }),
        ),
      );
    });

    it('offers the defaults to a connection nobody has answered for yet', async () => {
      catalogServers = [
        catalogRow({
          name: 'ibkr',
          url: IBKR.url,
          oauth_status: 'needs_reauth',
          remembered_capabilities: null,
        }),
      ];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(startConnect).toHaveBeenCalledWith(
          'ibkr',
          '/plugins?tab=brokerages',
          expect.objectContaining({ grantedCapabilities: IBKR_DEFAULT }),
        ),
      );
    });

    // `[]` is an answer, and the one that costs the most to lose: the user
    // declined every group. Read as "nothing stored" it becomes the defaults,
    // which is the whole set.
    it('reopens a repair on nothing when the user granted nothing', async () => {
      catalogServers = [
        catalogRow({
          name: 'ibkr',
          url: IBKR.url,
          oauth_status: 'needs_reauth',
          remembered_capabilities: [],
        }),
      ];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(startConnect).toHaveBeenCalledWith(
          'ibkr',
          '/plugins?tab=brokerages',
          expect.objectContaining({ grantedCapabilities: [] }),
        ),
      );
    });

    // Past the first write this is an ordinary catalog row, and the MCP tab can
    // point it at a local command. The backend takes an OAuth connect only for a
    // remote row, so the button that survives that edit is one whose every click
    // ends in a failure toast. It is the question the MCP tab already asks of
    // its own rows, asked here too.
    it('stops offering a connect once the row is no longer a remote one', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', transport: 'stdio', command: 'npx', url: null }),
      ];
      await renderTab();

      expect(screen.queryByTestId('brokerage-connect-ibkr')).toBeNull();
      // One button, not the row: everything else it owns is still its own.
      expect(screen.getByTestId('brokerage-ibkr')).toBeInTheDocument();
      // The row is redirected, so it wears its own name now.
      expect(screen.getByLabelText('Actions for ibkr')).toBeInTheDocument();
    });

    it('asks first when connecting costs the account its other AI connection', async () => {
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));

      // Nothing has happened yet: no sign-in started, and nothing written.
      expect(connectConfirm()).not.toBeNull();
      expect(startConnect).not.toHaveBeenCalled();
      expect(startPending).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('does nothing at all when the user backs out of that question', async () => {
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Cancel' }),
      );

      // The dialog leaves after its exit animation.
      await waitFor(() => expect(connectConfirm()).toBeNull());
      expect(startConnect).not.toHaveBeenCalled();
      expect(startPending).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('stops before the vendor when Cancel is pressed after the answer', async () => {
      // The dialog stays up while the flow is minted, and its Cancel stays live
      // throughout -- so the window is real, and what sits on the far side of
      // it is a vendor that drops whatever AI platform this account is
      // connected to now. Nothing may open behind a click that said stop.
      let release: () => void = () => {};
      startPending.mockImplementationOnce(async (request, _returnTo, options) => {
        await new Promise<void>((resolve) => {
          release = resolve;
        });
        return answerPending(request, options?.stillWanted);
      });
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(1));

      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Cancel' }),
      );
      release();

      await waitFor(() => expect(connectConfirm()).toBeNull());
      await flush();
      expect(assign).not.toHaveBeenCalled();
      // No marker either: the return path is owed nothing by a flow that never
      // left, and one left behind would apologise on the next ordinary visit.
      expect(pendingMarkers()).toEqual([]);
      // And no failure: the user is the one who stopped it.
      expect(toastSpy).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    // Only the open Plugins tab is mounted, so leaving one mid-connect unmounts
    // the hook that started the run without stopping the run, and the connect
    // the user then starts from the other tab is a different instance entirely.
    // The older run failing late is not the newer one failing: it must not
    // stand the newer connect down on its way out. Modelled by remounting
    // rather than by rendering the other tab, since a fresh instance is the
    // whole of what the switch produces.
    it('lets a connect from another tab go on when the one it took over fails', async () => {
      let failFirst: (e: Error) => void = () => {};
      startPending.mockImplementationOnce(
        () =>
          new Promise<null>((_resolve, reject) => {
            failFirst = reject;
          }),
      );

      const firstTab = await renderTab();
      connectThrough('ibkr');
      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(1));

      firstTab.unmount();
      // What the second tab finds is what the first left: no row, since nothing
      // is written before the vendor has answered.
      await renderTab();
      connectThrough('ibkr');

      failFirst(new LoopbackRequiredError('not-bound'));

      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    // The other half of the same takeover, and the half that used to reach the
    // vendor. `stillWanted` asked only whether SOMEBODY owned the row, and after
    // a takeover somebody does -- so the older run read the newer one's claim as
    // its own, marked the row, and opened a consent screen for a connect the
    // user had already replaced. Here the older start resolves rather than
    // failing, which is the ordinary case: phase 1 succeeds, just late.
    it('never reaches the vendor once a newer connect owns the row', async () => {
      let releaseFirst: () => void = () => {};
      const held = new Promise<void>((resolve) => {
        releaseFirst = resolve;
      });
      startPending.mockImplementationOnce(async (request, _returnTo, options) => {
        await held;
        return answerPending(request, options?.stillWanted);
      });

      const firstTab = await renderTab();
      connectThrough('ibkr');
      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(1));

      firstTab.unmount();
      await renderTab();
      connectThrough('ibkr');

      // The newer connect owns the row, and still waits: what decides which
      // connect the backend retires is which start lands last, so an older one
      // still out would retire the consent screen the user is about to see.
      await flush();
      expect(startPending).toHaveBeenCalledTimes(1);
      expect(assign).not.toHaveBeenCalled();

      releaseFirst();

      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(2));
      // The newer connect's navigation is the only one.
      await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
      leave();
      expect(pendingMarkers()).toEqual([{ server: 'ibkr', from: '/plugins' }]);
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('still goes out to the vendor when the answer stands', async () => {
      // The control for the cancel above: the same click on the same broker,
      // left alone, must reach the consent screen.
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      await waitFor(() =>
        expect(assign).toHaveBeenCalledWith('https://vendor.test/authorize'),
      );
      expect(startPending).toHaveBeenCalledTimes(1);
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('raises the terms only of a vendor that has them', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-robinhood'));

      // Still a question -- what the connection may do is asked of every
      // brokerage -- but not this one, which is another vendor's term and would
      // be a false claim about this account.
      expect(connectConfirm()).not.toBeNull();
      expect(
        within(connectConfirm()!).queryByText(/replaces whichever one is connected now/i),
      ).toBeNull();
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      // Asserted with its arguments, not merely that it ran:
      // `vendorRefusesHostedCallback` is what makes the backend mint this flow
      // against the shell's loopback callback, and it is the whole reason a
      // Robinhood connect can finish at all. Nothing else in the tree pins it.
      await waitFor(() =>
        expect(startPending).toHaveBeenCalledWith(
          { kind: 'brokerage', name: 'robinhood' },
          '/plugins?tab=brokerages',
          {
            vendorRefusesHostedCallback: true,
            stillWanted: expect.any(Function),
            grantedCapabilities: RH_DEFAULT,
          },
        ),
      );
      await waitFor(() => expect(connectConfirm()).toBeNull());
    });

    // The change the whole consent step exists for. A user who connects a
    // broker for quotes used to hand the agent an ungated path to placing real
    // orders, and the only way to see that had been to read the tool list.
    it('leaves real orders out of what a connect grants by default', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      await renderTab();
      connectThrough('robinhood');

      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(1));
      expect(startPending.mock.calls[0][2]?.grantedCapabilities).not.toContain('trading');
    });

    it('grants them when the user turns them on', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-robinhood'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('switch', {
          name: 'Enable Live orders',
        }),
      );
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(1));
      expect(startPending.mock.calls[0][2]?.grantedCapabilities).toContain('trading');
    });

    // Empty is an answer, and it is not the same as not asking: the backend
    // refuses a brokerage sign-in that names no selection, and reads one that
    // names `[]` as granted nothing. Sending the empty array is what keeps
    // "declined everything" a request the backend can take.
    it('sends an empty grant rather than none when everything is switched off', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-robinhood'));
      for (const name of ['Disable Market data and research', 'Disable Account and positions']) {
        fireEvent.click(within(connectConfirm()!).getByRole('switch', { name }));
      }
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      await waitFor(() => expect(startPending).toHaveBeenCalledTimes(1));
      expect(startPending.mock.calls[0][2]?.grantedCapabilities).toEqual([]);
    });

    // A switch cannot sign anyone in, so a row the backend will not switch on
    // without a connection is answered with the way to make one, not with a
    // failure: the toast carries the sign-in, and the user decides whether to
    // leave this page for the vendor's. The row reads connected here, as one
    // whose token lapsed since the list loaded does: the menu holds the switch
    // only for a row it already knows is unconnected.
    it('offers the sign-in when a switch is refused for want of one', async () => {
      catalogServers = [
        catalogRow({
          name: 'ibkr',
          url: IBKR.url,
          enabled: false,
          oauth_status: 'connected',
          remembered_capabilities: ['market_data'],
        }),
      ];
      toggleBrokerage.mockRejectedValueOnce(
        apiError(409, {
          code: 'mcp_sign_in_required',
          servers: [{ server: 'ibkr', url: IBKR.url }],
        }),
      );
      await renderTab();
      await chooseFromMenu('Interactive Brokers', 'Switch on');

      await waitFor(() => expect(toastSpy).toHaveBeenCalledTimes(1));
      const offer = toastSpy.mock.calls[0][0] as {
        variant?: string;
        title: string;
        action: ReactElement<{ onClick: () => void }>;
      };
      expect(offer.variant).toBeUndefined();
      expect(offer.title).toBe('Sign-in needed');
      expect(offer.title).not.toBe('Could not change this connector');
      // Nothing starts until the user takes the offer.
      expect(startConnect).not.toHaveBeenCalled();
      expect(startPending).not.toHaveBeenCalled();

      // Taking it is the ordinary connect, questions and all, on the row the
      // user already has.
      act(() => offer.action.props.onClick());
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );
      await waitFor(() =>
        expect(startConnect).toHaveBeenCalledWith(
          'ibkr',
          '/plugins?tab=brokerages',
          expect.objectContaining({
            expectedUrl: IBKR.url,
            grantedCapabilities: ['market_data'],
          }),
        ),
      );
      expect(startPending).not.toHaveBeenCalled();
    });

    it('still reports a switch refused for any other reason', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: IBKR.url, enabled: false, oauth_status: 'connected' }),
      ];
      toggleBrokerage.mockRejectedValueOnce(apiError(500, 'boom'));
      await renderTab();
      await chooseFromMenu('Interactive Brokers', 'Switch on');

      await waitFor(() =>
        expect(toastSpy).toHaveBeenCalledWith(
          expect.objectContaining({
            variant: 'destructive',
            title: 'Could not change this connector',
          }),
        ),
      );
      expect(toastSpy.mock.calls[0][0]).not.toHaveProperty('action');
    });

    it('does not start a flow the browser provably cannot finish', async () => {
      await renderTab();

      const connect = screen.getByTestId('brokerage-connect-robinhood');
      fireEvent.click(connect);
      expect(startConnect).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
      expect(screen.getByText('Needs the desktop app')).toBeInTheDocument();
    });

    it('names the trading permission and links to where it is changed', async () => {
      tradingLevel = 'autonomous';
      await renderTab();

      expect(screen.getByRole('heading', { name: 'Trading permission' })).toBeInTheDocument();
      expect(screen.getByText('Full autonomy')).toBeInTheDocument();
      expect(
        screen.getByRole('link', { name: 'Manage trading permission' }),
      ).toHaveAttribute('href', '/settings?tab=trading#trading-permission');
    });

    it('links to the download beside the note, without opening the row', async () => {
      await renderTab();

      // Said once, on the card beside the button it stops. The side column
      // used to repeat it, which was the same sentence twice on one screen.
      const links = screen.getAllByRole('link', { name: 'Download the desktop app' });
      expect(links).toHaveLength(1);
      const [onRow] = links;
      expect(onRow).toHaveAttribute('href', 'https://langalpha.ai/download');
      expect(onRow).toHaveAttribute('target', '_blank');
      expect(onRow).toHaveTextContent('Download');
      const bubbled = vi.fn();
      document.addEventListener('click', bubbled);
      fireEvent.click(onRow);
      document.removeEventListener('click', bubbled);
      expect(bubbled).not.toHaveBeenCalled();
    });

    it('says why to a screen reader, on a control it can still reach', async () => {
      // aria-disabled rather than disabled: a disabled button is not
      // focusable, so the user who most needs the reason is the one who can
      // never reach the control it belongs to.
      await renderTab();

      const connect = screen.getByTestId('brokerage-connect-robinhood');
      expect(connect).toHaveAttribute('aria-disabled', 'true');
      expect(connect).not.toBeDisabled();
      expect(
        document.getElementById(connect.getAttribute('aria-describedby')!),
      ).toHaveTextContent('Needs the desktop app');
    });

    it('warns on a connection that was revoked too', async () => {
      // A revoked row still needs the authorize flow, and this browser still
      // cannot finish it.
      catalogServers = [
        catalogRow({ name: 'robinhood', url: RH.url, oauth_status: 'revoked' }),
      ];
      await renderTab();

      expect(screen.getByText('Needs the desktop app')).toBeInTheDocument();
    });

    it('stops warning once the broker is connected', async () => {
      catalogServers = [
        catalogRow({ name: 'robinhood', url: RH.url, oauth_status: 'connected' }),
      ];
      await renderTab();

      expect(screen.queryByText('Needs the desktop app')).toBeNull();
    });

    it('offers it in the desktop shell, which has the loopback listener', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      await renderTab();

      expect(screen.getByTestId('brokerage-connect-robinhood')).toBeEnabled();
      expect(screen.queryByText('Needs the desktop app')).toBeNull();
    });
  });

  describe('a connection that was granted nothing', () => {
    it('says so when the grant is an empty list', async () => {
      catalogServers = [
        catalogRow({
          name: 'ibkr',
          url: IBKR.url,
          oauth_status: 'connected',
          granted_capabilities: [],
        }),
      ];
      await renderTab();

      expect(
        screen.getByText(/granted nothing/i),
      ).toBeInTheDocument();
    });

    it('says so when it stored no grant at all', async () => {
      // A brokerage connected before its tools were curated: the dialog had no
      // groups to offer, so nothing was stored. The relay refuses every call on
      // it exactly as it does for an empty list, and testing only for the empty
      // list left the one connection that permits nothing the one that says so.
      catalogServers = [
        catalogRow({
          name: 'ibkr',
          url: IBKR.url,
          oauth_status: 'connected',
          granted_capabilities: null,
        }),
      ];
      await renderTab();

      expect(
        screen.getByText(/granted nothing/i),
      ).toBeInTheDocument();
    });

    it('stays quiet on a broker with no connection to read', async () => {
      // Null before a connection is the offer, not a refusal.
      catalogServers = [];
      await renderTab();

      expect(screen.queryByText(/granted nothing/i)).toBeNull();
    });
  });

  describe('what it says before the click', () => {
    it('warns that an exclusive connection takes the account slot', async () => {
      await renderTab();
      expect(
        screen.getByText('Disconnects other AI apps linked to this account'),
      ).toBeInTheDocument();
    });

    it('stops warning once that connection is ours', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: IBKR.url, oauth_status: 'connected' }),
      ];
      await renderTab();
      expect(
        screen.queryByText('Disconnects other AI apps linked to this account'),
      ).toBeNull();
    });

    it('flags a row whose address the user has since pointed elsewhere', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: 'https://mcp.example.com/sse' }),
      ];
      await renderTab();
      expect(screen.getByText('Points at a custom address')).toBeInTheDocument();
    });

    it('flags a row pointed at the other shipped broker, not only at a stranger', async () => {
      // The address still resolves to a vendor, so a null test read this as
      // "unmoved" and the row went on calling itself Robinhood while the notes
      // and the connect button, which read the resolved vendor, had already
      // switched to what IBKR costs.
      catalogServers = [catalogRow({ name: 'robinhood', url: IBKR.url })];
      await renderTab();

      expect(screen.getByText('Points at a custom address')).toBeInTheDocument();
      expect(screen.getByText('robinhood')).toBeInTheDocument();
      expect(screen.queryByText('Robinhood')).toBeNull();
    });

    it('stops presenting such a row as the broker it no longer reaches', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: 'https://mcp.example.com/sse' }),
      ];
      await renderTab();

      // Neither the vendor's name nor its description is lent to an address
      // the user chose. The row goes back to being called what it is.
      expect(screen.queryByText('Interactive Brokers')).toBeNull();
      expect(screen.getByText('ibkr')).toBeInTheDocument();
      expect(screen.queryByText(IBKR.description)).toBeNull();
      // Including to a screen reader, which was still being told the vendor.
      expect(screen.getByLabelText('Actions for ibkr')).toBeInTheDocument();
    });

    it('gives a consequence more weight than a capability note', async () => {
      await renderTab();

      // The exclusive-connection warning costs the user something; "needs the
      // desktop app" does not. They sat at the same tertiary grey.
      const consequence = screen.getByText('Disconnects other AI apps linked to this account');
      const capability = screen.getByText('Needs the desktop app');
      expect(consequence).toHaveStyle({ color: 'var(--color-warning)' });
      expect(capability).toHaveStyle({ color: 'var(--color-text-tertiary)' });
    });

    // Flagging it is half the job. Everything the row says about the vendor is
    // a claim about wherever it now points, and the mark is the loudest of
    // them: a broker's logo over somebody else's endpoint is the one thing on
    // this page a user would be entitled to read as our word for it.
    it('claims nothing for the vendor once it points elsewhere, mark included', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: 'https://mcp.example.com/sse' }),
      ];
      const { container } = await renderTab();

      expect(container.querySelector('img[src*="/brokerages/ibkr/icon"]')).toBeNull();
      expect(
        screen.queryByText('Disconnects other AI apps linked to this account'),
      ).toBeNull();
      // The one beside it is untouched, so this is de-branding and not a
      // build with the art switched off.
      expect(
        container.querySelector('img[src*="/brokerages/robinhood/icon"]'),
      ).not.toBeNull();
    });
  });

  it('says so plainly when a build ships no brokers at all', async () => {
    shipped = [];
    await renderTab();
    expect(screen.getByText('This build ships no brokerage connectors.')).toBeInTheDocument();
  });
});

describe("the quirks after it is the user's own row", () => {
  const catalogConnect = vi.fn();

  async function row(
    url: string,
    oauthStatus: string | null = null,
    // The list resolves this and hands it down, so 'auto' does what the list
    // does. Both states are named rather than passed literally: `undefined` is
    // one of the values under test, and handing it to a defaulted parameter
    // silently takes the default instead of the value the test asked for.
    vendorArg: Brokerage | null | 'auto' | 'unanswered' = 'auto',
    registryUnavailable = false,
  ) {
    const vendor =
      vendorArg === 'auto'
        ? brokerageForUrl(url, shipped)
        : vendorArg === 'unanswered'
          ? undefined
          : vendorArg;
    catalogConnect.mockClear();
    const { ConnectorCell } = await import('../components/connectors/ConnectorCells');
    return renderWithProviders(
      <ConnectorCell
        server={catalogRow({
          name: 'whatever_they_called_it',
          url,
          oauth_status: oauthStatus as CatalogServer['oauth_status'],
        })}
        vendor={vendor}
        registryUnavailable={registryUnavailable}
        workspaces={[]}
        workspacesLoading={false}
        connecting={false}
        refreshing={false}
        onOpen={() => {}}
        onConnect={catalogConnect}
      />,
    );
  }

  it('follows the address, so a row they typed themselves still carries them', async () => {
    await row('https://agent.robinhood.com/mcp/trading');
    expect(screen.getByText('Needs the desktop app')).toBeInTheDocument();
  });

  it('follows a sibling path on the same host, which is still that vendor', async () => {
    const { container } = await row('https://api.ibkr.com/v1/api/something-else');
    expect(container.querySelector('img[src*="/brokerages/ibkr/icon"]')).not.toBeNull();
  });

  it('claims nothing for a host that merely looks like one', async () => {
    await row('https://agent.robinhood.com.example.test/mcp');
    expect(screen.queryByText('Needs the desktop app')).toBeNull();
  });

  it('drops the notes once connected, when there is nothing left to warn about', async () => {
    await row('https://agent.robinhood.com/mcp/trading', 'connected');
    expect(screen.queryByText('Needs the desktop app')).toBeNull();
  });

  it('refuses the click here too, not just on the tab that offered it', async () => {
    // The two surfaces once said the same sentence and only one of them meant
    // it: this row warned and stayed clickable, which walked the user into the
    // silent dead end the warning is about.
    await row('https://agent.robinhood.com/mcp/trading');

    const connect = screen.getByTestId('catalog-connect-whatever_they_called_it');
    fireEvent.click(connect);
    expect(catalogConnect).not.toHaveBeenCalled();
    expect(connect).toHaveAttribute('aria-disabled', 'true');
  });

  it('leaves the click alone when this build can finish the flow', async () => {
    shell = {
      beginMcpOAuth: async () => ({
        redirectUri: 'http://127.0.0.1:8790/mcp/callback',
        flowId: 'flow-1',
      }),
    };
    await row('https://agent.robinhood.com/mcp/trading');

    fireEvent.click(screen.getByTestId('catalog-connect-whatever_they_called_it'));
    expect(catalogConnect).toHaveBeenCalledTimes(1);
  });

  it('holds the click while the registry has not answered yet', async () => {
    // A backend older than the endpoint answers nothing, and so does the moment
    // before the query settles. Reading that as "this row has no vendor" is what
    // put a live button on a native-only row with no note beside it, so the gate
    // has to refuse the unknown rather than treat it as the absence of a quirk.
    shell = undefined;
    await row('https://agent.robinhood.com/mcp/trading', null, 'unanswered');

    const connect = screen.getByTestId('catalog-connect-whatever_they_called_it');
    fireEvent.click(connect);
    expect(catalogConnect).not.toHaveBeenCalled();
    expect(connect).toHaveAttribute('aria-disabled', 'true');
    // Nothing to say yet, so it must not point at a note that was never rendered.
    expect(connect).not.toHaveAttribute('aria-describedby');
  });

  it('points at the page once the registry has been asked and failed', async () => {
    // Held the same way either way, but no longer silently: an answer that is
    // never coming is something the page can explain, and the button says
    // where. One id for the whole list, because the cause is the page's -- a
    // plugin-owned row and one the user typed are held by the same outage.
    shell = undefined;
    await row('https://agent.robinhood.com/mcp/trading', null, 'unanswered', true);

    const connect = screen.getByTestId('catalog-connect-whatever_they_called_it');
    fireEvent.click(connect);
    expect(catalogConnect).not.toHaveBeenCalled();
    expect(connect).toHaveAttribute('aria-disabled', 'true');
    expect(connect).toHaveAttribute('aria-describedby', 'oauth-block-registry');
  });

  // The row's own page has to read the vendor as the cell that opened it does.
  // It once read an unanswered registry as "no vendor", so a broker's page
  // offered a live Connect beside a cell that held it, and a click there went
  // to the vendor before its terms could be asked.
  it("holds the row's page Connect as well while the registry has not answered", async () => {
    registryAnswered = false;
    const onConnect = vi.fn();
    const { ServerDetail } = await import('../components/ServerDetail');
    const { DetailPageProvider } = await import('../components/DetailPage');
    renderWithProviders(
      <DetailPageProvider value={{ section: 'Connectors' }}>
        <ServerDetail
          data={{
            origin: 'user',
            server: catalogRow({ name: 'whatever_they_called_it', url: IBKR.url, oauth_status: null }),
          }}
          onClose={() => {}}
          onConnect={onConnect}
        />
      </DetailPageProvider>,
    );

    const connect = screen.getAllByRole('button', { name: /^connect$/i })[0];
    fireEvent.click(connect);
    expect(onConnect).not.toHaveBeenCalled();
    expect(connect).toHaveAttribute('aria-disabled', 'true');
  });

  it('still connects a plain row once the registry answers with no vendor', async () => {
    // The other half of the same gate: `null` is a settled answer and must stay
    // as clickable as it was before the unknown state existed.
    await row('https://mcp.example.com/sse', null, null);

    fireEvent.click(screen.getByTestId('catalog-connect-whatever_they_called_it'));
    expect(catalogConnect).toHaveBeenCalledTimes(1);
  });
});
