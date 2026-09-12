import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { httpCatalogServer } from '@/test/factories';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import type { McpToolSummary } from '@/pages/ChatAgent/utils/api';
import type { Brokerage } from '../brokerages';

/**
 * Changing a whole group of a broker's tools at once. A moomoo connection
 * publishes 88 of them under headers of sixty-odd, so the assertions that
 * matter are that a selection reaches exactly the rows the user ticked, and
 * that applying it is ONE request rather than one per tool.
 */

const VENDOR: Brokerage = {
  name: 'moomoo',
  label: 'moomoo',
  url: 'https://mcp.example.com/moomoo',
  site: 'moomoo.com',
  description: '',
  native_callback_only: false,
  exclusive_connection: false,
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'trading', tone: 'danger', rung: true },
  ],
};

const TOOLS: McpToolSummary[] = [
  {
    name: 'quote_get_price',
    description: '',
    input_schema: {},
    capability: 'market_data',
    binding: 'ptc',
    binding_source: 'group',
    allowed: ['ptc', 'direct', 'both'],
    approval: false,
  },
  {
    name: 'quote_get_depth',
    description: '',
    input_schema: {},
    capability: 'market_data',
    binding: 'ptc',
    binding_source: 'group',
    allowed: ['ptc', 'direct', 'both'],
    approval: false,
  },
  {
    name: 'market_snapshot',
    description: '',
    input_schema: {},
    capability: 'market_data',
    binding: 'ptc',
    binding_source: 'group',
    allowed: ['ptc', 'direct', 'both'],
    approval: false,
  },
  // Pinned by the server, as every order tool is: no box, and no bulk change
  // may name either of them.
  {
    name: 'trading_order_cancel',
    description: '',
    input_schema: {},
    capability: 'trading',
    binding: 'direct',
    binding_source: 'policy',
    allowed: ['direct'],
    approval: true,
    order: { action: 'cancel', mode: 'live' },
  },
  {
    name: 'trading_order_place',
    description: '',
    input_schema: {},
    capability: 'trading',
    binding: 'direct',
    binding_source: 'policy',
    allowed: ['direct'],
    approval: true,
    order: { action: 'place', mode: 'live' },
  },
  // Restricted but not pinned -- a tool no capability group names is reachable
  // from the sandbox and nowhere else. It keeps its box, so it is the tool the
  // skip count is about.
  {
    name: 'vendor_beta_tool',
    description: '',
    input_schema: {},
    capability: null,
    always_denied: false,
    binding: 'ptc',
    binding_source: 'default',
    allowed: ['ptc'],
    approval: false,
  },
];

const patch = vi.fn().mockResolvedValue({});
vi.mock('@/hooks/useMcpServers', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useMcpServers')>();
  return {
    ...actual,
    useSetMcpServerBinding: () => ({ mutateAsync: patch, isPending: false }),
    useMcpCatalogServerTools: () => ({
      data: { tools: TOOLS, order_modes: ['live'], discovered_at: null },
      isLoading: false,
      isError: false,
    }),
    useBuiltinMcpServerTools: () => ({ data: undefined, isLoading: false, isError: false }),
    useBrokerages: () => ({ data: [VENDOR], isLoading: false, error: null }),
  };
});

async function renderDetail() {
  const { ServerDetail } = await import('../components/ServerDetail');
  return renderWithProviders(
    <ServerDetail
      data={{
        origin: 'brokerage',
        brokerage: VENDOR,
        server: httpCatalogServer({
          name: 'moomoo',
          url: VENDOR.url,
          oauth_status: 'connected',
          granted_capabilities: ['market_data', 'trading'],
          tool_binding: {},
          binding_preset: null,
        }),
      }}
      onClose={() => {}}
    />,
  );
}

const MARKET = 'Market data and research';

const filterBox = () => screen.getByRole('searchbox');
const groupBox = (label: string) =>
  screen.getByRole('checkbox', { name: `Select every tool in ${label}` });
const rowBox = (tool: string) =>
  screen.getByRole('checkbox', { name: `Select ${tool}` });
const bar = () => screen.getByTestId('tool-bulk-bar');
const barSegment = (label: string) =>
  within(
    within(bar()).getByRole('group', {
      name: 'How the agent reaches the selected tools',
    }),
  ).getByRole('button', { name: label });
/** The count beside a group header. By testid because the same capability
 *  label also names the consent section higher up the dialog. */
const groupCount = (key: string) => screen.getByTestId(`tool-count-${key}`).textContent;

beforeEach(() => {
  patch.mockClear();
  patch.mockResolvedValue({});
});

describe('filtering a long tool list', () => {
  it('narrows the rows and the count beside each group', async () => {
    await renderDetail();
    expect(groupCount('market_data')).toBe('3');
    expect(groupCount('trading')).toBe('2');

    fireEvent.change(filterBox(), { target: { value: 'depth' } });

    expect(screen.getByText('quote_get_depth')).toBeInTheDocument();
    expect(screen.queryByText('quote_get_price')).toBeNull();
    expect(groupCount('market_data')).toBe('1');
    // A group with nothing left is a header standing over no rows.
    expect(screen.queryByTestId('tool-count-trading')).toBeNull();
  });

  it('matches a substring in any case', async () => {
    await renderDetail();
    fireEvent.change(filterBox(), { target: { value: 'QUOTE_' } });

    expect(screen.getByText('quote_get_price')).toBeInTheDocument();
    expect(screen.getByText('quote_get_depth')).toBeInTheDocument();
    expect(screen.queryByText('market_snapshot')).toBeNull();
  });

  it('says so when the filter matches nothing', async () => {
    await renderDetail();
    fireEvent.change(filterBox(), { target: { value: 'zzz' } });

    expect(screen.getByText('No tools match that filter.')).toBeInTheDocument();
  });
});

describe('picking tools', () => {
  it('takes a whole group from its header box', async () => {
    await renderDetail();
    fireEvent.click(groupBox(MARKET));

    expect(within(bar()).getByText('3 selected')).toBeInTheDocument();
    expect(groupBox(MARKET)).toHaveAttribute('aria-checked', 'true');
  });

  // Tri-state, because "some of this group" is a real answer and a box that
  // could only say yes or no would report two thirds of a group as none of it.
  it('reads mixed while only part of the group is picked', async () => {
    await renderDetail();
    fireEvent.click(rowBox('quote_get_price'));

    expect(groupBox(MARKET)).toHaveAttribute('aria-checked', 'mixed');

    fireEvent.click(groupBox(MARKET));
    expect(groupBox(MARKET)).toHaveAttribute('aria-checked', 'true');
    expect(within(bar()).getByText('3 selected')).toBeInTheDocument();
  });

  it('clears the group from a full header box', async () => {
    await renderDetail();
    fireEvent.click(groupBox(MARKET));
    fireEvent.click(groupBox(MARKET));

    expect(screen.queryByTestId('tool-bulk-bar')).toBeNull();
  });

  // Select all is an action on what is in front of the user, so it reads the
  // filter: ticking rows the filter is hiding is how a user ends up applying a
  // change to tools they never saw.
  it('takes only the rows the filter is showing', async () => {
    await renderDetail();
    fireEvent.change(filterBox(), { target: { value: 'quote_' } });
    // The label counts what it would take, so the number is the filter's.
    fireEvent.click(screen.getByRole('button', { name: 'Select all 2' }));

    expect(within(bar()).getByText('2 selected')).toBeInTheDocument();
  });

  // The opposite rule, and deliberately so: a selection is keyed by name and
  // survives a filter change, or a group ticked and then searched through
  // would apply to fewer tools than the count the user just read.
  it('keeps a selection the filter no longer shows', async () => {
    await renderDetail();
    fireEvent.click(groupBox(MARKET));
    fireEvent.change(filterBox(), { target: { value: 'depth' } });

    expect(within(bar()).getByText('3 selected')).toBeInTheDocument();
  });

  // A pinned tool answers 422 to anything but its pin. It carries the pinned
  // label instead of a box, and nothing that selects in bulk may reach it. A
  // tool the server merely restricts is a different case and keeps its box.
  it('never draws a box on a tool the server pins', async () => {
    await renderDetail();

    expect(screen.queryByRole('checkbox', { name: 'Select trading_order_place' })).toBeNull();
    expect(screen.queryByRole('checkbox', { name: 'Select trading_order_cancel' })).toBeNull();
    expect(rowBox('vendor_beta_tool')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('checkbox', { name: 'Select every tool shown' }));
    // Four movable tools out of six: neither pinned order tool is one of them.
    expect(within(bar()).getByText('4 selected')).toBeInTheDocument();
  });

  it('drops the bar when the selection is cleared', async () => {
    await renderDetail();
    fireEvent.click(groupBox(MARKET));
    fireEvent.click(within(bar()).getByRole('button', { name: 'Clear selection' }));

    expect(screen.queryByTestId('tool-bulk-bar')).toBeNull();
  });
});

describe('applying one change to many tools', () => {
  it('sends a single patch naming every selected tool', async () => {
    await renderDetail();
    fireEvent.click(groupBox(MARKET));
    fireEvent.click(barSegment('Direct'));

    await waitFor(() => expect(patch).toHaveBeenCalledTimes(1));
    expect(patch).toHaveBeenCalledWith({
      name: 'moomoo',
      body: {
        tool_binding_set: {
          quote_get_price: 'direct',
          quote_get_depth: 'direct',
          market_snapshot: 'direct',
        },
      },
    });
  });

  // The server refuses the whole patch on the first pair it will not take, so
  // one tool that cannot move would cost the other three their change. It is
  // dropped from the write and counted in the bar instead.
  it('leaves out a tool the value is not allowed on, and says how many', async () => {
    await renderDetail();
    fireEvent.click(screen.getByRole('checkbox', { name: 'Select every tool shown' }));
    fireEvent.click(barSegment('Both'));

    await waitFor(() => expect(patch).toHaveBeenCalledTimes(1));
    expect(patch).toHaveBeenCalledWith({
      name: 'moomoo',
      body: {
        tool_binding_set: {
          quote_get_price: 'both',
          quote_get_depth: 'both',
          market_snapshot: 'both',
        },
      },
    });
    expect(within(bar()).getByText('1 skipped, not allowed')).toBeInTheDocument();
  });

  it('resets the selection as one unset list', async () => {
    await renderDetail();
    fireEvent.click(groupBox(MARKET));
    fireEvent.click(within(bar()).getByRole('button', { name: 'Reset to default' }));

    await waitFor(() => expect(patch).toHaveBeenCalledTimes(1));
    expect(patch).toHaveBeenCalledWith({
      name: 'moomoo',
      body: {
        tool_binding_unset: ['quote_get_price', 'quote_get_depth', 'market_snapshot'],
      },
    });
  });

  it('shows the server refusal of a bulk write in the words it sent', async () => {
    patch.mockRejectedValueOnce({
      response: { data: { detail: 'that binding is not available on this connection' } },
    });
    await renderDetail();
    fireEvent.click(groupBox(MARKET));
    fireEvent.click(barSegment('Direct'));

    expect(
      await screen.findByText('that binding is not available on this connection'),
    ).toBeInTheDocument();
  });
});

/**
 * The bulk surface is the one place a user can change sixty-four tools without
 * reading sixty-four rows, so its words have to survive a locale switch. The
 * tree-wide sweep only checks that a key resolves; this checks the two catalogs
 * both actually carry the lines this bar is made of.
 */
describe('the bulk copy', () => {
  it.each([
    ['en-US', enUS],
    ['zh-CN', zhCN],
  ])('carries every bulk and filter line in %s', (_name, catalog) => {
    const detail = (catalog as { plugins: { detail: Record<string, string> } }).plugins
      .detail;
    const keys = [
      'searchPlaceholder',
      'searchClear',
      'searchEmpty',
      'bulkSelectAll',
      'bulkSelectAllAria',
      'bulkSelectGroupAria',
      'bulkSelectToolAria',
      'bulkSelected',
      'bulkBindingAria',
      'bulkReset',
      'bulkClear',
      'bulkSkipped',
    ];
    for (const key of keys) {
      expect(typeof detail[key], key).toBe('string');
      expect(detail[key].length, key).toBeGreaterThan(0);
    }
    // The two that name a thing must keep their placeholder, or the line reads
    // as being about every tool at once.
    expect(detail.bulkSelectGroupAria).toContain('{{name}}');
    expect(detail.bulkSelectToolAria).toContain('{{name}}');
  });
});
