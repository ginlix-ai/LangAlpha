import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook } from '@testing-library/react';
import { queryKeys } from '@/lib/queryKeys';
import { httpCatalogServer } from '@/test/factories';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import type { Brokerage } from '@/pages/Plugins/brokerages';
import { NAV_ITEMS } from '../navItems';
import { useNavItems } from '../useNavItems';

/**
 * Which primary nav items get drawn, which is not the same question as which
 * routes exist: /orders is a page about brokers, and someone who has connected
 * none is offered an entry into a list that can only ever be empty for them.
 *
 * The gate runs for real here -- only the queries behind it are stubbed, by key --
 * because the interesting part is which catalog row counts as a connected
 * broker, not the filter that reads the boolean.
 */

const MOOMOO: Brokerage = {
  name: 'moomoo',
  label: 'moomoo',
  url: 'https://mcp.moomoo.com/mcp',
  site: 'moomoo.com',
  description: 'Balances, positions, and order placement.',
  native_callback_only: false,
  exclusive_connection: false,
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'trading', tone: 'danger', rung: true },
  ],
};

let shipped: Brokerage[] | undefined;
let servers: CatalogServer[] | undefined;
/** How many ledger rows the one-row probe finds; `undefined` is still in flight. */
let ledgerRows: number | undefined;

/** What the shared cache holds for each query the gate asks. */
function cached(queryKey: readonly unknown[]) {
  const key = JSON.stringify(queryKey);
  if (key === JSON.stringify(queryKeys.brokerages.list())) return shipped;
  if (key === JSON.stringify(queryKeys.mcp.catalog())) {
    return servers && { servers, workspace_servers: [], max_servers: 50 };
  }
  if (key === JSON.stringify(queryKeys.orders.any())) {
    return ledgerRows === undefined ? undefined : { items: Array(ledgerRows).fill({}), next_cursor: null };
  }
  throw new Error(`the nav gate asked an unexpected query: ${key}`);
}

vi.mock('@tanstack/react-query', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@tanstack/react-query')>()),
  useQuery: ({ queryKey }: { queryKey: readonly unknown[] }) => ({ data: cached(queryKey) }),
}));

function keys() {
  return renderHook(() => useNavItems()).result.current.map((item) => item.key);
}

beforeEach(() => {
  shipped = [MOOMOO];
  servers = [];
  ledgerRows = 0;
});

describe('the primary nav', () => {
  it('draws every ungated item whatever the gate answers', () => {
    const ungated = NAV_ITEMS.filter((item) => !item.requires).map((item) => item.key);
    expect(keys()).toEqual(ungated);
    servers = [httpCatalogServer({ url: MOOMOO.url, oauth_status: 'connected' })];
    expect(keys()).toEqual(expect.arrayContaining(ungated));
  });

  it('shows Orders once a shipped brokerage is connected', () => {
    servers = [httpCatalogServer({ name: 'moomoo', url: MOOMOO.url, oauth_status: 'connected' })];
    expect(keys()).toContain('/orders');
    // In its declared place, not appended: the gate filters the list, it does
    // not rebuild it.
    expect(keys()).toEqual(NAV_ITEMS.map((item) => item.key));
  });

  it('hides Orders while the answer is still in flight', () => {
    servers = undefined;
    expect(keys()).not.toContain('/orders');
    servers = [httpCatalogServer({ url: MOOMOO.url, oauth_status: 'connected' })];
    shipped = undefined;
    expect(keys()).not.toContain('/orders');
  });

  it('hides Orders when the user has connected nothing and placed nothing', () => {
    expect(keys()).not.toContain('/orders');
  });

  it('shows Orders for a ledger with history and no broker left', () => {
    ledgerRows = 1;
    expect(keys()).toContain('/orders');
    // The broker answer is not even needed once the ledger has said yes.
    servers = undefined;
    shipped = undefined;
    expect(keys()).toContain('/orders');
  });

  it('shows Orders for a connected broker before the ledger has answered', () => {
    ledgerRows = undefined;
    servers = [httpCatalogServer({ url: MOOMOO.url, oauth_status: 'connected' })];
    expect(keys()).toContain('/orders');
  });

  it('hides Orders while only the ledger is still in flight and no broker is connected', () => {
    ledgerRows = undefined;
    expect(keys()).not.toContain('/orders');
  });

  it('does not count a connection that is not live', () => {
    for (const status of ['needs_reauth', 'revoked', 'refresh_ambiguous'] as const) {
      servers = [httpCatalogServer({ url: MOOMOO.url, oauth_status: status })];
      expect(keys()).not.toContain('/orders');
    }
    // Nor an added-but-never-connected row, which carries no status at all.
    servers = [httpCatalogServer({ url: MOOMOO.url })];
    expect(keys()).not.toContain('/orders');
  });

  it('does not count an ordinary connected server', () => {
    servers = [httpCatalogServer({ name: 'notion', oauth_status: 'connected' })];
    expect(keys()).not.toContain('/orders');
  });

  it('does not count a row the user pointed at another address', () => {
    // Same name, somewhere else: the address is what makes it the broker, the
    // same join the Plugins page uses to decide whose terms a row is under.
    servers = [
      httpCatalogServer({
        name: 'moomoo',
        url: 'https://mcp.example.com/mcp',
        oauth_status: 'connected',
      }),
    ];
    expect(keys()).not.toContain('/orders');
  });

  it('leaves the gated route itself alone', () => {
    // Hiding an entry is not a route guard: /orders stays in the shared config
    // that the active-state matcher reads, so the page is reachable by URL and
    // lights the nav up if the user is standing on it.
    expect(NAV_ITEMS.map((item) => item.key)).toContain('/orders');
  });
});
