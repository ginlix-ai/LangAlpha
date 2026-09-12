import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { httpCatalogServer } from '@/test/factories';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import type {
  CatalogServer,
  McpOrderMode,
  McpToolSummary,
} from '@/pages/ChatAgent/utils/api';
import type { Brokerage } from '../brokerages';

/**
 * What the detail panel writes when someone changes how the agent reaches a
 * broker's tools. Every assertion is on the patch body, because precedence
 * lives on the server: the row preset, the per-tool override, and the reset
 * that removes it are three different writes and the panel must send the one
 * the control it drew stands for.
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
    { key: 'account', tone: 'caution' },
    { key: 'paper_trading', tone: 'caution', rung: true },
    { key: 'trading', tone: 'danger', rung: true },
  ],
};

const TOOLS: McpToolSummary[] = [
  {
    name: 'existing_tool',
    description: '',
    input_schema: {},
    capability: 'account',
    binding: 'direct',
    binding_source: 'override',
    allowed: ['ptc', 'direct', 'both'],
    approval: false,
  },
  {
    name: 'sim_trade_account_list',
    description: '',
    input_schema: {},
    capability: 'paper_trading',
    binding: 'direct',
    binding_source: 'group',
    approval: false,
  },
  {
    name: 'sim_trade_input_order',
    description: '',
    input_schema: {},
    capability: 'paper_trading',
    binding: 'direct',
    binding_source: 'policy',
    allowed: ['direct'],
    approval: false,
    order: { action: 'place', mode: 'paper' },
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
  // Restricted without being pinned: a curated vendor permits a tool no
  // capability group names, but only from the sandbox.
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

// The server answers both halves separately, and the gates read the first: the
// kinds of order the vendor has come off its curation, the tools off a
// discovery that may not have happened. A test swaps either one.
let tools: McpToolSummary[] = TOOLS;
let modes: McpOrderMode[] = ['live', 'paper'];

const patch = vi.fn().mockResolvedValue({});
vi.mock('@/hooks/useMcpServers', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useMcpServers')>();
  return {
    ...actual,
    useSetMcpServerBinding: () => ({ mutateAsync: patch, isPending: false }),
    useMcpCatalogServerTools: () => ({
      data: { tools, order_modes: modes, discovered_at: null },
      isLoading: false,
      isError: false,
    }),
    useBuiltinMcpServerTools: () => ({ data: undefined, isLoading: false, isError: false }),
    useBrokerages: () => ({ data: [VENDOR], isLoading: false, error: null }),
  };
});

function row(over: Partial<CatalogServer> = {}): CatalogServer {
  return httpCatalogServer({
    name: 'moomoo',
    url: VENDOR.url,
    oauth_status: 'connected',
    granted_capabilities: ['account', 'paper_trading', 'trading'],
    tool_binding: { existing_tool: 'direct' },
    binding_preset: null,
    order_approval: { live: true, paper: false, staged: true },
    ...over,
  });
}

async function renderDetail(over: Partial<CatalogServer> = {}) {
  const { ServerDetail } = await import('../components/ServerDetail');
  return renderWithProviders(
    <ServerDetail
      data={{ origin: 'brokerage', brokerage: VENDOR, server: row(over) }}
      onClose={() => {}}
    />,
  );
}

// The per-tool control is a segmented row, not a menu: one click is the whole
// change, which is the reason it replaced the select on a broker with 88 tools.
const bindingGroup = (tool: string) =>
  screen.getByRole('group', { name: `How the agent reaches ${tool}` });
const segment = (tool: string, label: string) =>
  within(bindingGroup(tool)).getByRole('button', { name: label });

beforeEach(() => {
  patch.mockClear();
  patch.mockResolvedValue({});
  tools = TOOLS;
  modes = ['live', 'paper'];
});

describe('the tool access section', () => {
  it('reads an unset preset as the direct switch already being on', async () => {
    await renderDetail();
    const toggle = screen.getByRole('switch', { name: /Use each tool group's default/ });

    expect(toggle).toHaveAttribute('aria-checked', 'true');
    fireEvent.click(toggle);
    await waitFor(() =>
      expect(patch).toHaveBeenCalledWith({
        name: 'moomoo',
        body: { binding_preset: 'ptc_only' },
      }),
    );
  });

  it('clears the preset to null when switched back on', async () => {
    await renderDetail({ binding_preset: 'ptc_only' });
    const toggle = screen.getByRole('switch', { name: /Use each tool group's default/ });

    expect(toggle).toHaveAttribute('aria-checked', 'false');
    fireEvent.click(toggle);
    await waitFor(() =>
      expect(patch).toHaveBeenCalledWith({
        name: 'moomoo',
        body: { binding_preset: null },
      }),
    );
  });

  // A row written before the preset lost its second value may still hold the
  // old string; anything other than `ptc_only` is the on position.
  it('reads a legacy stored preset as the switch being on', async () => {
    await renderDetail({ binding_preset: 'order_direct' as unknown as CatalogServer['binding_preset'] });
    const toggle = screen.getByRole('switch', { name: /Use each tool group's default/ });

    expect(toggle).toHaveAttribute('aria-checked', 'true');
  });

  // The approval gates are independent row-wide writes: they say whether an
  // order stops for the user, never where the call runs, so they must not
  // travel with `binding_preset`.
  it('lets the user stop being asked before every live order', async () => {
    await renderDetail();
    const toggle = screen.getByRole('switch', { name: /Ask before every live order/ });

    expect(toggle).toHaveAttribute('aria-checked', 'true');
    fireEvent.click(toggle);
    await waitFor(() =>
      expect(patch).toHaveBeenCalledWith({
        name: 'moomoo',
        body: { order_approval: { live: false } },
      }),
    );
  });

  // The whole point of splitting the switch: paper money and real money are
  // two different answers, and turning one on must not carry this tab's copy
  // of the other back to the server, which merges the mode it was sent.
  it('writes only the mode it changed', async () => {
    await renderDetail();
    const toggle = screen.getByRole('switch', { name: /Ask before every paper order/ });

    expect(toggle).toHaveAttribute('aria-checked', 'false');
    fireEvent.click(toggle);
    await waitFor(() =>
      expect(patch).toHaveBeenCalledWith({
        name: 'moomoo',
        body: { order_approval: { paper: true } },
      }),
    );
  });

  // One switch per kind of order the vendor has, loudest first whichever order
  // the server listed them in: which modes exist is its answer, which one the
  // user reads first is ours. This vendor stages nothing, so there is no
  // switch for a gate that would govern no call.
  it('draws a switch per order kind the vendor has, live first', async () => {
    modes = ['paper', 'live'];
    await renderDetail();
    const labels = screen
      .getAllByRole('switch')
      .map((el) => el.getAttribute('aria-label'));

    expect(labels).toEqual([
      "Disable Use each tool group's default",
      'Disable Ask before every live order',
      'Enable Ask before every paper order',
    ]);
  });

  it('offers no order gate on a server that places no order', async () => {
    modes = [];
    tools = TOOLS.filter((tool) => !tool.order);
    await renderDetail();

    expect(screen.queryByRole('switch', { name: /Ask before/ })).toBeNull();
    expect(
      screen.getByRole('switch', { name: /Use each tool group's default/ }),
    ).toBeInTheDocument();
  });

  // Why the gates read the vendor's curation rather than the tool list: a row
  // whose discovery has not run yet still places orders the moment it does,
  // and a switch that appeared only after the snapshot landed would leave the
  // gate unanswerable exactly while the user was setting the connection up.
  it('draws the gates before any tool has been discovered', async () => {
    tools = [];
    await renderDetail();

    expect(
      screen.getByRole('switch', { name: /Ask before every live order/ }),
    ).toHaveAttribute('aria-checked', 'true');
    expect(
      screen.getByRole('switch', { name: /Ask before every paper order/ }),
    ).toHaveAttribute('aria-checked', 'false');
  });

  // Absent is the mode's own default, not one answer for all three: a row
  // stored before the map existed still asks before a live order and still
  // does not ask before a paper one.
  it('reads an absent order_approval as each mode default', async () => {
    await renderDetail({ order_approval: undefined });

    expect(
      screen.getByRole('switch', { name: /Ask before every live order/ }),
    ).toHaveAttribute('aria-checked', 'true');
    expect(
      screen.getByRole('switch', { name: /Ask before every paper order/ }),
    ).toHaveAttribute('aria-checked', 'false');
  });

  // The override is written whole: the row's existing overrides travel with it,
  // so setting one tool never silently drops another's.
  // Only the tool the user touched travels. Carrying the rest of the map is
  // what let a second tab's edit be written back to the version this one read.
  it('writes only the tool it changed, never the stored map', async () => {
    await renderDetail();
    fireEvent.click(segment('sim_trade_account_list', 'Both'));

    await waitFor(() =>
      expect(patch).toHaveBeenCalledWith({
        name: 'moomoo',
        body: { tool_binding_set: { sim_trade_account_list: 'both' } },
      }),
    );
  });

  // The single-tool path stays one interaction: no menu to open, no second
  // click to commit. This is the assertion that a bulk affordance did not
  // quietly cost the common case an extra step.
  it('changes one tool in a single click', async () => {
    await renderDetail();
    fireEvent.click(segment('sim_trade_account_list', 'PTC'));

    await waitFor(() => expect(patch).toHaveBeenCalledTimes(1));
    expect(patch).toHaveBeenCalledWith({
      name: 'moomoo',
      body: { tool_binding_set: { sim_trade_account_list: 'ptc' } },
    });
  });

  it('draws the binding in force as the pressed segment', async () => {
    await renderDetail();

    expect(segment('existing_tool', 'Direct')).toHaveAttribute('aria-pressed', 'true');
    expect(segment('existing_tool', 'PTC')).toHaveAttribute('aria-pressed', 'false');
  });

  it('offers only Direct on a live order tool the server pins to direct', async () => {
    // The server answers `ptc` or `both` on a live order tool with a 422, so
    // the row says where the tool runs rather than let the user find out by
    // picking a value that comes straight back.
    await renderDetail();

    expect(segment('trading_order_place', 'PTC')).toBeDisabled();
    expect(segment('trading_order_place', 'Both')).toBeDisabled();
    expect(segment('trading_order_place', 'Direct')).not.toBeDisabled();
    expect(
      screen.getAllByText('Order tools run only as direct calls the app can show.').length,
    ).toBeGreaterThan(0);
  });

  // The enable rule is `allowed`, not the pin: an option outside the list is
  // disabled and one inside it is not, whichever direction the server chose.
  it('disables exactly the options outside allowed', async () => {
    await renderDetail();

    for (const name of ['PTC', 'Direct', 'Both']) {
      expect(segment('existing_tool', name)).not.toBeDisabled();
    }
    expect(segment('trading_order_place', 'Direct')).not.toBeDisabled();
    expect(segment('trading_order_place', 'PTC')).toBeDisabled();
  });

  // A response without `allowed` is a stale one, not a locked tool: every
  // option stays live so an older server never freezes the whole control.
  it('treats a missing allowed list as unrestricted', async () => {
    await renderDetail();

    for (const name of ['PTC', 'Direct', 'Both']) {
      expect(segment('sim_trade_account_list', name)).not.toBeDisabled();
    }
  });

  // `allowed` is the whole rule the row draws, on a restriction that is not a
  // pin: a tool no capability group names is reachable from the sandbox and
  // nowhere else, and the row still says which three answers exist.
  it('greys out the paths a restricted tool refuses without pinning it', async () => {
    await renderDetail();

    expect(segment('vendor_beta_tool', 'PTC')).not.toBeDisabled();
    expect(segment('vendor_beta_tool', 'Direct')).toBeDisabled();
    expect(segment('vendor_beta_tool', 'Both')).toBeDisabled();
  });

  // No badge on a tool that never asks, or every row would carry one.
  it('marks exactly the tools that ask first', async () => {
    await renderDetail();

    expect(screen.getAllByText('asks first')).toHaveLength(2);
  });

  // Which money a tool moves is the cost of the call, so it is said on the row
  // that decides how the call is made. A tool that touches no order carries no
  // badge, and a kind this vendor does not have appears nowhere.
  it('badges each order tool with the kind of order it acts on', async () => {
    await renderDetail();

    expect(screen.getAllByText('live')).toHaveLength(2);
    expect(screen.getAllByText('paper')).toHaveLength(1);
    expect(screen.queryByText('staged')).toBeNull();
  });

  it('resets an overridden tool by naming it, not by writing the default', async () => {
    await renderDetail();
    fireEvent.click(
      screen.getByRole('button', { name: 'Reset existing_tool to the row default' }),
    );

    await waitFor(() =>
      expect(patch).toHaveBeenCalledWith({
        name: 'moomoo',
        body: { tool_binding_unset: ['existing_tool'] },
      }),
    );
  });

  // A stored key may be a differently cased or padded spelling of the
  // discovered name. Reconciling that is the server's job now: it reads the
  // map folded, and the delta names the tool rather than carrying the map, so
  // the page has no spellings to reconcile. Covered by ``merge_overrides``.

  it('shows the server refusal in the words the server sent', async () => {
    patch.mockRejectedValueOnce({
      response: {
        data: {
          detail:
            "'trading_order_place' places live orders, so it runs only as a direct call and cannot be bound from the sandbox",
        },
      },
    });
    await renderDetail();
    fireEvent.click(segment('sim_trade_account_list', 'Both'));

    expect(
      await screen.findByText(
        "'trading_order_place' places live orders, so it runs only as a direct call and cannot be bound from the sandbox",
      ),
    ).toBeInTheDocument();
  });
});

/**
 * The three gates sit stacked in one list, and the only thing telling a reader
 * which money each one governs is its label. Two of them sharing a word would
 * be the single switch they were split out of, with the user answering for
 * real money while reading about paper. Pinned in both catalogs because the
 * tree-wide sweep only checks that a key resolves, not that it says something
 * different from the key above it.
 */
describe('the order gate copy', () => {
  it.each([
    ['en-US', enUS],
    ['zh-CN', zhCN],
  ])('gives every order kind its own words in %s', (_name, catalog) => {
    const detail = (
      catalog as { plugins: { detail: Record<string, string> } }
    ).plugins.detail;
    const written = ['Live', 'Paper', 'Staged'].flatMap((mode) => [
      detail[`orderApproval${mode}`],
      detail[`orderApproval${mode}Desc`],
      detail[`orderMode${mode}`],
    ]);
    for (const line of written) expect(typeof line).toBe('string');
    expect(new Set(written).size).toBe(written.length);
  });
});
