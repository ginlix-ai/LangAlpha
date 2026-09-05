import { describe, it, expect, vi, beforeEach, afterAll } from 'vitest';
import { act, screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { LoopbackRequiredError } from '@/pages/ChatAgent/utils/api';
import type {
  CatalogServer,
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

const toggleBrokerage = vi.fn(async () => ({}));
let shipped: Brokerage[] = [];
let catalogServers: CatalogServer[] = [];
const deleteServer = vi.fn(async () => ({}));
vi.mock('@/hooks/useMcpServers', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useMcpServers')>();
  return {
    ...actual,
    useBrokerages: () => ({ data: shipped, isLoading: false, error: null }),
    useToggleBrokerage: () => ({ mutateAsync: toggleBrokerage }),
    useMcpCatalog: () => ({
      data: { servers: catalogServers, workspace_servers: [], max_servers: 50 },
      isLoading: false,
      error: null,
    }),
    useDeleteMcpCatalogServer: () => ({ mutateAsync: deleteServer, isPending: false }),
    useSetMcpServerEnabledInWorkspace: () => ({ mutateAsync: vi.fn(async () => ({})) }),
  };
});

vi.mock('../hooks/useWorkspaceOptions', () => ({
  useWorkspaceOptions: () => ({ workspaces: [], nameById: new Map() }),
}));

// The connect lifecycle runs for real: the question a vendor's terms raise is
// the hook's, not this tab's, and a stub in its place would test the strip
// while leaving the gate itself unexercised. Only the call that leaves the
// browser is replaced.
//
// `stillWanted` is the one thing the hook cannot answer for itself: the real
// module arms the shell's listener before either of its round trips and keeps
// the flow id, so it is the only place that can both ask whether the connect is
// still wanted and give back what it armed. The stub answers the same way it
// does -- the URL, or null for a caller that took the connect back while those
// round trips were in flight.
const answerStart = (stillWanted?: () => boolean) =>
  stillWanted && !stillWanted()
    ? null
    : { authorize_url: 'https://vendor.test/authorize', state: 'st-1' };

const startConnect = vi.fn(
  async (_name: string, _returnTo?: string, options?: StartMcpOauthOptions) =>
    answerStart(options?.stillWanted),
);
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>();
  return {
    ...actual,
    startMcpOauth: (name: string, returnTo?: string, options?: StartMcpOauthOptions) =>
      startConnect(name, returnTo, options),
  };
});

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
  value: { href: 'http://localhost/plugins', origin: 'http://localhost', assign },
});
afterAll(() => {
  Object.defineProperty(window, 'location', { configurable: true, value: realLocation });
});

beforeEach(() => {
  shell = undefined;
  shipped = [RH, IBKR];
  catalogServers = [];
  toggleBrokerage.mockClear();
  startConnect.mockClear();
  deleteServer.mockClear();
  assign.mockClear();
  // `markConnectStarted` writes the real store, and the marker outlives a test.
  sessionStorage.clear();
});

/** What the connect left behind for the return path to act on. */
const pendingMarkers = () =>
  JSON.parse(sessionStorage.getItem('mcp:connect-started') ?? '[]');

async function renderTab() {
  const { Brokerages } = await import('../components/Brokerages');
  return renderWithProviders(<Brokerages />);
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

    // One toggle and one kebab, both belonging to the broker that exists.
    expect(screen.getAllByRole('switch')).toHaveLength(1);
    expect(
      screen.getByLabelText('Actions for Interactive Brokers'),
    ).toBeInTheDocument();
  });

  describe('connecting', () => {
    it('creates the row enabled and goes straight on to the vendor', async () => {
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: true }),
      );
      // The vendor's terms travel as the one flag the start actually needs, and
      // the return lands back on this tab rather than the MCP one. Both are
      // read off the row's own URL, so the thing that decides whether this flow
      // needs the desktop shell is settled where the address is known. The
      // address travels too: the row this offer is about to create carries the
      // registry's, which is the one thing on this tab the user does not pick.
      expect(startConnect).toHaveBeenCalledWith('ibkr', '/plugins?tab=brokerages', {
        vendorRefusesHostedCallback: false,
        expectedUrl: IBKR.url,
        stillWanted: expect.any(Function),
        grantedCapabilities: IBKR_DEFAULT,
      });
    });

    // `bindMcpOAuth` answers false for a flow that timed out, was cancelled, or
    // went down with the shell, not only for one a second connect took over.
    // Reading every such refusal as "another connect owns this row" left a
    // brokerage switched on, and inherited by every workspace, with no
    // connection behind it and no consent screen ever opened.
    it('stands a row it just brought up back down when the flow never launched', async () => {
      shell = {
        beginMcpOAuth: async () => ({
          redirectUri: 'http://127.0.0.1:8790/mcp/callback',
          flowId: 'flow-1',
        }),
      };
      startConnect.mockRejectedValueOnce(new LoopbackRequiredError('not-bound'));
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: true }),
      );
      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: false }),
      );
    });

    it('enables a row the user had switched off, since an inert grant is revoked', async () => {
      catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url, enabled: false })];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: true }),
      );
    });

    // The rollback the lifecycle holds runs from a catch that sits before the
    // jump to the vendor, and every way the vendor can refuse happens after it.
    // So whether this connect is what brought the row up has to travel with the
    // marker, or the return has no way to know it owes the user the row back.
    it('records that this connect is what brought the row live', async () => {
      catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url, enabled: false })];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(pendingMarkers()).toEqual([{ server: 'ibkr', broughtLive: true }]),
      );
    });

    it('records a row that was already live as one it did not bring up', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: IBKR.url, oauth_status: 'needs_reauth' }),
      ];
      await renderTab();
      connectThrough('ibkr');

      await waitFor(() =>
        expect(pendingMarkers()).toEqual([{ server: 'ibkr', broughtLive: false }]),
      );
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
      expect(screen.getAllByRole('switch')).toHaveLength(1);
    });

    it('asks first when connecting costs the account its other AI connection', async () => {
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));

      // Nothing has happened yet -- not even the row, which connecting would
      // otherwise create on the way past.
      expect(connectConfirm()).not.toBeNull();
      expect(startConnect).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('does nothing at all when the user backs out of that question', async () => {
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Cancel' }),
      );

      expect(connectConfirm()).toBeNull();
      expect(startConnect).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('takes the connect back when Cancel is pressed after the answer', async () => {
      // The strip stays up while the row is enabled and the flow is minted, and
      // its Cancel stays live throughout -- so the window is real, and what sits
      // on the far side of it is a vendor that drops whatever AI platform this
      // account is connected to now. Nothing may open behind a click that said
      // stop, and the row the answer brought up has to go back down with it.
      let release: () => void = () => {};
      startConnect.mockImplementationOnce(async (_name, _returnTo, options) => {
        await new Promise<void>((resolve) => {
          release = resolve;
        });
        return answerStart(options?.stillWanted);
      });
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      await waitFor(() => expect(startConnect).toHaveBeenCalledTimes(1));
      expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: true });

      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Cancel' }),
      );
      release();

      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenLastCalledWith({
          name: 'ibkr',
          enabled: false,
        }),
      );
      expect(assign).not.toHaveBeenCalled();
      // No marker either: the return path is owed nothing by a flow that never
      // left, and one left behind would apologise on the next ordinary visit.
      expect(pendingMarkers()).toEqual([]);
    });

    // Only the open Plugins tab is mounted, so leaving one mid-connect unmounts
    // the hook that started the run without stopping the run, and the connect
    // the user then starts from the other tab is a different instance entirely.
    // The two used to keep separate ideas of who owned the row: the first run
    // came back to one where it was still the owner and switched off a row the
    // second connect had just brought live. Modelled by remounting rather than
    // by rendering the other tab, since a fresh instance is the whole of what
    // the switch produces.
    it('leaves the row alone when a connect from another tab has taken it over', async () => {
      let failFirst: (e: Error) => void = () => {};
      startConnect.mockImplementationOnce(
        () =>
          new Promise<null>((_resolve, reject) => {
            failFirst = reject;
          }),
      );

      const firstTab = await renderTab();
      connectThrough('ibkr');
      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: true }),
      );

      firstTab.unmount();
      // What the second tab finds: the row the first connect brought up, live,
      // so its own connect is a reconnect with nothing to prepare or undo.
      catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url })];
      toggleBrokerage.mockClear();
      await renderTab();
      connectThrough('ibkr');

      failFirst(new LoopbackRequiredError('not-bound'));

      await waitFor(() => expect(startConnect).toHaveBeenCalledTimes(2));
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
      startConnect.mockImplementationOnce(
        async (_name: string, _returnTo?: string, options?: StartMcpOauthOptions) => {
          await held;
          return answerStart(options?.stillWanted);
        },
      );

      const firstTab = await renderTab();
      connectThrough('ibkr');
      await waitFor(() =>
        expect(toggleBrokerage).toHaveBeenCalledWith({ name: 'ibkr', enabled: true }),
      );

      firstTab.unmount();
      catalogServers = [catalogRow({ name: 'ibkr', url: IBKR.url })];
      toggleBrokerage.mockClear();
      await renderTab();
      connectThrough('ibkr');

      // The newer connect owns the row, and still waits: what decides which
      // connect the backend retires is which start lands last, so an older one
      // still out would retire the consent screen the user is about to see.
      await flush();
      expect(startConnect).toHaveBeenCalledTimes(1);
      expect(assign).not.toHaveBeenCalled();

      releaseFirst();

      await waitFor(() => expect(startConnect).toHaveBeenCalledTimes(2));
      // The newer connect's navigation is the only one, and the row it is using
      // is still switched on.
      await waitFor(() => expect(assign).toHaveBeenCalledTimes(1));
      expect(toggleBrokerage).not.toHaveBeenCalled();
    });

    it('still goes out to the vendor when the answer stands', async () => {
      // The control for the cancel above: the same click on the same row, left
      // alone, must reach the consent screen exactly as it did before.
      await renderTab();
      fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
      fireEvent.click(
        within(connectConfirm()!).getByRole('button', { name: 'Connect' }),
      );

      await waitFor(() =>
        expect(assign).toHaveBeenCalledWith('https://vendor.test/authorize'),
      );
      expect(toggleBrokerage).toHaveBeenCalledExactlyOnceWith({
        name: 'ibkr',
        enabled: true,
      });
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
        expect(startConnect).toHaveBeenCalledWith('robinhood', '/plugins?tab=brokerages', {
          vendorRefusesHostedCallback: true,
          expectedUrl: RH.url,
          stillWanted: expect.any(Function),
          grantedCapabilities: RH_DEFAULT,
        }),
      );
      expect(connectConfirm()).toBeNull();
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

      await waitFor(() => expect(startConnect).toHaveBeenCalledTimes(1));
      expect(startConnect.mock.calls[0][2]?.grantedCapabilities).not.toContain('trading');
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

      await waitFor(() => expect(startConnect).toHaveBeenCalledTimes(1));
      expect(startConnect.mock.calls[0][2]?.grantedCapabilities).toContain('trading');
    });

    // Empty is an answer, and it is not the same as not asking: the backend
    // reads a brokerage that named no group as one granted nothing, and a
    // brokerage that named none because the field never arrived the same way.
    // Sending the empty array is what keeps those from being different things
    // on the wire.
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

      await waitFor(() => expect(startConnect).toHaveBeenCalledTimes(1));
      expect(startConnect.mock.calls[0][2]?.grantedCapabilities).toEqual([]);
    });

    it('does not start a flow the browser provably cannot finish', async () => {
      await renderTab();

      const connect = screen.getByTestId('brokerage-connect-robinhood');
      fireEvent.click(connect);
      expect(startConnect).not.toHaveBeenCalled();
      expect(toggleBrokerage).not.toHaveBeenCalled();
      expect(screen.getByText('Needs the desktop app')).toBeInTheDocument();
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

    it('warns up top too, including for a connection that was revoked', async () => {
      // The paragraph and the row read the same predicate. A revoked row still
      // needs the authorize flow, and this browser still cannot finish it.
      catalogServers = [
        catalogRow({ name: 'robinhood', url: RH.url, oauth_status: 'revoked' }),
      ];
      await renderTab();

      expect(
        screen.getByText(/only accept desktop apps/, { exact: false }),
      ).toBeInTheDocument();
    });

    it('stops warning up top once every such broker is connected', async () => {
      catalogServers = [
        catalogRow({ name: 'robinhood', url: RH.url, oauth_status: 'connected' }),
      ];
      await renderTab();

      expect(screen.queryByText(/only accept desktop apps/)).toBeNull();
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
        screen.getByText("Replaces this account's other AI connection"),
      ).toBeInTheDocument();
    });

    it('stops warning once that connection is ours', async () => {
      catalogServers = [
        catalogRow({ name: 'ibkr', url: IBKR.url, oauth_status: 'connected' }),
      ];
      await renderTab();
      expect(
        screen.queryByText("Replaces this account's other AI connection"),
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
      const consequence = screen.getByText("Replaces this account's other AI connection");
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
        screen.queryByText("Replaces this account's other AI connection"),
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
    const { McpCatalogRow } = await import('../components/McpCatalogRow');
    renderWithProviders(
      <McpCatalogRow
        server={catalogRow({
          name: 'whatever_they_called_it',
          url,
          oauth_status: oauthStatus as CatalogServer['oauth_status'],
        })}
        vendor={vendor}
        registryUnavailable={registryUnavailable}
        workspaces={[]}
        selection={{
          selecting: false,
          selected: new Set(),
          start: () => {},
          exit: () => {},
          toggle: () => {},
          setMany: () => {},
        }}
        connecting={false}
        refreshing={false}
        toggling={false}
        scopeBusy={false}
        onOpen={() => {}}
        onConnect={catalogConnect}
        onDisconnect={() => {}}
        onRefreshSchemas={() => {}}
        onEdit={() => {}}
        onRequestDelete={() => {}}
        onToggle={() => {}}
        onSetWorkspaceDisabled={() => {}}
        onMove={() => {}}
      />,
    );
  }

  it('follows the address, so a row they typed themselves still carries them', async () => {
    await row('https://agent.robinhood.com/mcp/trading');
    expect(screen.getByText('Needs the desktop app')).toBeInTheDocument();
  });

  it('follows a sibling path on the same host, which is still that vendor', async () => {
    await row('https://api.ibkr.com/v1/api/something-else');
    expect(
      screen.getByText("Replaces this account's other AI connection"),
    ).toBeInTheDocument();
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

  it('still connects a plain row once the registry answers with no vendor', async () => {
    // The other half of the same gate: `null` is a settled answer and must stay
    // as clickable as it was before the unknown state existed.
    await row('https://mcp.example.com/sse', null, null);

    fireEvent.click(screen.getByTestId('catalog-connect-whatever_they_called_it'));
    expect(catalogConnect).toHaveBeenCalledTimes(1);
  });
});
