import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import { catalogProbe, httpCatalogServer } from '@/test/factories';
import { ApiError } from '@/api/client';
import { toast } from '@/components/ui/use-toast';
import { Discover } from '../Discover';
import { DetailPageProvider } from '../DetailPage';
import type { ConnectRequest } from '../../hooks/useMcpOauthActions';
import {
  getMarketplace,
  type MarketplaceEntry,
  type MarketplaceResponse,
} from '@/pages/ChatAgent/utils/api/marketplace';
import {
  getPlugins,
  installPluginFromUrl,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api/plugins';
import { getMcpCatalog } from '@/pages/ChatAgent/utils/api/mcp';

vi.mock('@/pages/ChatAgent/utils/api/marketplace', () => ({
  getMarketplace: vi.fn(),
}));

vi.mock('@/pages/ChatAgent/utils/api/plugins', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/plugins')>()),
  getPlugins: vi.fn().mockResolvedValue({ plugins: [], max_plugins: 20, remaining_slots: 20 }),
  installPluginFromUrl: vi.fn(),
}));

// What the sign-in offer reads: which vendor a server is, and its catalog row.
vi.mock('@/pages/ChatAgent/utils/api/brokerages', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/brokerages')>()),
  getBrokerages: vi.fn().mockResolvedValue([]),
}));

vi.mock('@/pages/ChatAgent/utils/api/mcp', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/mcp')>()),
  getMcpCatalog: vi.fn().mockResolvedValue({ servers: [], max_servers: 20 }),
}));

// The connect lifecycle leaves for the vendor's page; here it only records
// what it was asked to start.
const oauth = vi.hoisted(() => ({
  connectingName: null,
  refreshingName: null,
  pendingConfirm: null,
  connect: vi.fn(),
  cancelPending: vi.fn(),
  offerSignIn: vi.fn(),
  disconnect: vi.fn(),
  refreshSchemas: vi.fn(),
}));

vi.mock('../../hooks/useMcpOauthActions', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../hooks/useMcpOauthActions')>()),
  useMcpOauthActions: () => oauth,
}));

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

/** A refusal as the API client rejects it: the server's `detail` under
 *  `response.data`. */
function apiError(status: number, detail: Record<string, unknown>): ApiError {
  return new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_REQUEST', {
    data: { detail },
    status,
    statusText: '',
    headers: {},
  });
}

const keysRequired = () =>
  apiError(422, {
    code: 'plugin_secrets_required',
    message: 'wind needs keys to install.',
    secrets: [
      { name: 'WIND_KEY', label: 'Wind key', description: 'From the Wind console.' },
      { name: 'WIND_SECRET', label: 'Wind secret' },
    ],
  });

const term = (key: string, label: string) => ({ key, label, label_zh: null });

function entry(overrides: Partial<MarketplaceEntry>): MarketplaceEntry {
  return {
    name: 'vendor',
    description: '',
    category: 'data',
    license: null,
    publisher: null,
    homepage: null,
    icon_url: null,
    keywords: [],
    auth: null,
    requires: null,
    coverage: null,
    source_url: `https://github.com/acme/catalog/tree/main/plugins/${overrides.name}`,
    installed: false,
    plugin: null,
    update_available: false,
    ...overrides,
  };
}

const catalog: MarketplaceResponse = {
  configured: true,
  name: 'Acme plugins',
  description: 'Curated data vendors and research skills.',
  url: 'https://github.com/acme/catalog',
  vocabulary: {
    assets: [term('equity', 'Equities'), term('fund', 'Funds'), term('fx', 'FX')],
    regions: [
      term('us', 'United States'),
      term('cn', 'Mainland China'),
      term('hk', 'Hong Kong'),
      term('global', 'Global'),
    ],
    data: [],
  },
  entries: [
    entry({
      name: 'wind',
      publisher: { name: 'Wind Information', url: null, official: true },
      auth: 'api-key',
      requires: 'Wind AIFin Market key',
      license: 'Proprietary',
      coverage: { markets: { equity: ['cn', 'hk'], fund: ['cn'] }, data: ['prices'] },
    }),
    entry({
      name: 'polygon',
      coverage: { markets: { equity: ['us'] }, data: [] },
      installed: true,
      plugin: 'polygon',
    }),
    entry({ name: 'dcf-model', category: 'research' }),
    entry({ name: 'oddity', category: 'esoteric' }),
  ],
};

const installResponse: PluginInstallResponse = {
  plugin: {
    name: 'wind',
    version: '1.0.0',
    description: '',
    author: null,
    homepage: null,
    source_type: 'git',
    source_ref: catalog.entries[0].source_url,
    enabled: true,
    installed_at: null,
    updated_at: null,
    components: [{ kind: 'mcp', name: 'wind', key: 'wind' }],
  },
  report: {
    components: [
      {
        kind: 'mcp',
        key: 'wind',
        name: 'wind',
        renamed: false,
        status: 'created',
        reason: '',
        warnings: [],
      },
    ],
    diagnostics: [],
    secrets_created: [],
    secrets_required: [],
    dropped_files: [],
    servers_created: 1,
    skills_created: 0,
  },
};

const card = (name: string) => screen.queryByTestId(`discover-card-${name}`);

describe('Discover', () => {
  beforeEach(() => {
    vi.mocked(getMarketplace).mockResolvedValue(catalog);
    vi.mocked(installPluginFromUrl).mockReset();
    vi.mocked(toast).mockReset();
    oauth.connect.mockReset();
    vi.mocked(getPlugins).mockResolvedValue({ plugins: [], max_plugins: 20, remaining_slots: 20 });
    vi.mocked(getMcpCatalog).mockResolvedValue({ servers: [], max_servers: 20 });
  });

  it('splits the catalog into data vendors and skill collections, one vendor featured above', async () => {
    renderWithProviders(<Discover scope="plugins" />);
    expect(await screen.findByTestId('discover-featured-wind')).toBeInTheDocument();
    const headings = screen.getAllByRole('heading', { level: 2 }).map((h) => h.textContent);
    expect(headings).toEqual(['Data vendors', 'Skill collections']);
    // The featured entry is not repeated in the grid under it.
    expect(card('wind')).not.toBeInTheDocument();
    expect(card('polygon')).toBeInTheDocument();
    expect(card('dcf-model')).toBeInTheDocument();
    expect(card('oddity')).toBeInTheDocument();
  });

  it('shows an installed entry as a state, not an Install button', async () => {
    renderWithProviders(<Discover scope="plugins" />);
    const polygon = await screen.findByTestId('discover-card-polygon');
    expect(within(polygon).getByRole('img', { name: 'Installed' })).toBeInTheDocument();
    expect(within(polygon).queryByRole('button', { name: /install/i })).not.toBeInTheDocument();
  });

  it('says an installed entry has a newer version in place of its publisher line, keeping its check', async () => {
    vi.mocked(getMarketplace).mockResolvedValue({
      ...catalog,
      entries: catalog.entries.map((e) =>
        e.name === 'polygon' ? { ...e, update_available: true } : e,
      ),
    });
    vi.mocked(getPlugins).mockResolvedValue({
      plugins: [
        { ...installResponse.plugin, name: 'polygon', source_ref: catalog.entries[1].source_url, components: [] },
      ],
      max_plugins: 20,
      remaining_slots: 19,
    });
    renderWithProviders(<Discover scope="plugins" />);

    const polygon = await screen.findByTestId('discover-card-polygon');
    expect(await within(polygon).findByRole('button', { name: 'Update available' })).toBeInTheDocument();
    expect(within(polygon).getByRole('img', { name: 'Installed' })).toBeInTheDocument();
  });

  // An http connector nothing has checked offers Connect, but nothing has gone
  // wrong with it, so Yours and the rail leave it out of attention. The count
  // over the catalog's installed entries used to take it in.
  it('counts installed entries needing attention by the rule Yours and the rail use', async () => {
    const installedAs = (name: string) => ({
      ...installResponse.plugin,
      name,
      source_ref: `https://github.com/acme/catalog/tree/main/plugins/${name}`,
    });
    vi.mocked(getMarketplace).mockResolvedValue({
      ...catalog,
      entries: [...catalog.entries, entry({ name: 'factset', installed: true, plugin: 'factset' })],
    });
    vi.mocked(getPlugins).mockResolvedValue({
      plugins: [installedAs('polygon'), installedAs('factset')],
      max_plugins: 20,
      remaining_slots: 18,
    });
    vi.mocked(getMcpCatalog).mockResolvedValue({
      servers: [
        httpCatalogServer({ name: 'polygon', plugin_name: 'polygon' }),
        httpCatalogServer({
          name: 'factset',
          plugin_name: 'factset',
          probe: catalogProbe({ verdict: 'credential_rejected' }),
        }),
      ],
      max_servers: 20,
    });
    renderWithProviders(<Discover scope="plugins" />);

    expect(await screen.findByRole('button', { name: 'factset needs attention' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /need attention/ })).not.toBeInTheDocument();
  });

  // The catalog names the plugin that came from each entry. One of the same
  // name from somewhere else (an upload, another repository) is not the entry,
  // so the entry reads as not installed, and the server refuses its install
  // on the name: Install was a button that could only fail.
  it('holds Install where a plugin of that name came from elsewhere, and marks what the catalog installed', async () => {
    vi.mocked(getPlugins).mockResolvedValue({
      plugins: [
        { ...installResponse.plugin, name: 'wind', source_type: 'zip', source_ref: null },
        { ...installResponse.plugin, name: 'polygon', source_ref: catalog.entries[1].source_url },
      ],
      max_plugins: 20,
      remaining_slots: 18,
    });
    const reason = 'A plugin named wind is already installed from another source';
    const list = renderWithProviders(<Discover scope="plugins" />);

    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Install wind' })).toBeDisabled(),
    );
    expect(screen.getByRole('button', { name: 'Install wind' })).toHaveAttribute('title', reason);
    const polygon = card('polygon')!;
    expect(within(polygon).getByRole('img', { name: 'Installed' })).toBeInTheDocument();
    expect(within(polygon).queryByRole('button', { name: /install/i })).not.toBeInTheDocument();
    list.unmount();

    // On the entry's page the reason stands beside the way to that plugin.
    const page = (name: string) =>
      renderWithProviders(
        <DetailPageProvider value={{ section: 'Plugins' }}>
          <Discover scope="plugins" />
        </DetailPageProvider>,
        { route: `/plugins?detail=catalog:${name}` },
      );
    const wind = page('wind');
    const callout = await screen.findByRole('status');
    expect(callout).toHaveTextContent(reason);
    expect(within(callout).getByRole('button', { name: 'Open plugin' })).toBeInTheDocument();
    wind.unmount();

    page('polygon');
    expect(await screen.findByRole('button', { name: 'Open plugin' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Install polygon' })).not.toBeInTheDocument();
  });

  it('narrows by category, on Connectors by market, and says when a search finds nothing', async () => {
    const user = userEvent.setup();
    const first = renderWithProviders(<Discover scope="plugins" />);
    const chips = await screen.findByRole('group', { name: 'Filter the catalog' });
    await user.click(within(chips).getByRole('button', { name: /^Research/ }));
    expect(within(chips).getByRole('button', { name: /^Research/ })).toHaveAttribute(
      'aria-pressed',
      'true',
    );
    await waitFor(() => expect(card('polygon')).not.toBeInTheDocument());
    expect(card('dcf-model')).toBeInTheDocument();
    // A narrowed view is an answer, not a shop window: no featured entry.
    expect(screen.queryByTestId('discover-featured-wind')).not.toBeInTheDocument();
    first.unmount();

    // Every vendor is a data source, so Connectors filters by the markets they
    // name; only regions some vendor names become chips.
    const second = renderWithProviders(<Discover scope="connectors" />);
    const markets = await screen.findByRole('group', { name: 'Filter by market' });
    expect(within(markets).queryByRole('button', { name: /^Global/ })).not.toBeInTheDocument();
    expect(card('dcf-model')).not.toBeInTheDocument();
    await user.click(within(markets).getByRole('button', { name: /^Mainland China/ }));
    await waitFor(() => expect(card('polygon')).not.toBeInTheDocument());
    expect(card('wind')).toBeInTheDocument();
    second.unmount();

    renderWithProviders(<Discover scope="plugins" />, { route: '/plugins?q=zzz' });
    expect(await screen.findByText('Nothing in the catalog matches "zzz".')).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Clear search' }));
    expect(await screen.findByTestId('discover-card-oddity')).toBeInTheDocument();
  });

  it('installs from the entry source URL and opens the outcome', async () => {
    const user = userEvent.setup();
    vi.mocked(installPluginFromUrl).mockResolvedValue(installResponse);
    renderWithProviders(<Discover scope="plugins" />);

    await user.click(await screen.findByRole('button', { name: 'Install wind' }));
    // The first try sends no keys: the server says which, if any, it needs.
    expect(installPluginFromUrl).toHaveBeenCalledWith(catalog.entries[0].source_url, null, null);
    expect(await screen.findByText('wind installed')).toBeInTheDocument();
  });

  it('asks for the keys a refused install names, then installs with them', async () => {
    const user = userEvent.setup();
    vi.mocked(installPluginFromUrl)
      .mockRejectedValueOnce(keysRequired())
      .mockResolvedValueOnce(installResponse);
    renderWithProviders(<Discover scope="plugins" />);

    await user.click(await screen.findByRole('button', { name: 'Install wind' }));
    const dialog = await screen.findByRole('dialog', { name: 'Install wind' });
    expect(within(dialog).getByText('From the Wind console.')).toBeInTheDocument();
    expect(screen.queryByText('wind installed')).not.toBeInTheDocument();
    expect(toast).not.toHaveBeenCalled();

    // Every key is required: a blank one would only fail its server's check.
    const submit = within(dialog).getByRole('button', { name: 'Check and install' });
    expect(submit).toBeDisabled();
    await user.type(within(dialog).getByLabelText(/Wind key/), 'k-123');
    expect(submit).toBeDisabled();
    await user.type(within(dialog).getByLabelText(/Wind secret/), 's-456');
    expect(submit).toBeEnabled();

    await user.click(submit);
    await waitFor(() => expect(installPluginFromUrl).toHaveBeenCalledTimes(2));
    expect(installPluginFromUrl).toHaveBeenLastCalledWith(catalog.entries[0].source_url, null, {
      WIND_KEY: 'k-123',
      WIND_SECRET: 's-456',
    });
    expect(await screen.findByText('wind installed')).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Install wind' })).not.toBeInTheDocument(),
    );
  });

  it('shows why a server refused the keys in the form, keeping what was typed', async () => {
    const user = userEvent.setup();
    vi.mocked(installPluginFromUrl)
      .mockRejectedValueOnce(keysRequired())
      .mockRejectedValueOnce(
        apiError(422, {
          code: 'mcp_check_failed',
          message: 'A server did not pass its check.',
          failures: [
            { server: 'wind', verdict: 'credential_rejected', reason: 'The key was rejected.' },
          ],
        }),
      );
    renderWithProviders(<Discover scope="plugins" />);

    await user.click(await screen.findByRole('button', { name: 'Install wind' }));
    const dialog = await screen.findByRole('dialog', { name: 'Install wind' });
    await user.type(within(dialog).getByLabelText(/Wind key/), 'k-bad');
    await user.type(within(dialog).getByLabelText(/Wind secret/), 's-456');
    await user.click(within(dialog).getByRole('button', { name: 'Check and install' }));

    const failures = await within(dialog).findByTestId('mcp-check-failures');
    expect(failures).toHaveTextContent('wind: The key was rejected.');
    // Said where the keys can be changed, not in a toast, and nothing installed.
    expect(toast).not.toHaveBeenCalled();
    expect(screen.queryByText('wind installed')).not.toBeInTheDocument();
    expect(within(dialog).getByLabelText(/Wind key/)).toHaveValue('k-bad');
    expect(within(dialog).getByLabelText(/Wind secret/)).toHaveValue('s-456');
  });

  it('hands an install that needs a sign-in to the sign-in, installing nothing first', async () => {
    const user = userEvent.setup();
    const ledger = entry({ name: 'ledger', auth: 'oauth' });
    vi.mocked(getMarketplace).mockResolvedValue({
      ...catalog,
      entries: [...catalog.entries, ledger],
    });
    vi.mocked(installPluginFromUrl).mockRejectedValueOnce(
      apiError(409, {
        code: 'mcp_sign_in_required',
        message: 'Sign in to ledger first.',
        servers: [{ server: 'ledger', url: 'https://mcp.ledger.example/mcp' }],
      }),
    );
    renderWithProviders(<Discover scope="plugins" />);

    await user.click(await screen.findByRole('button', { name: 'Install ledger' }));
    await waitFor(() => expect(oauth.connect).toHaveBeenCalledTimes(1));
    const request = oauth.connect.mock.calls[0][0] as ConnectRequest;
    expect(request).toMatchObject({
      name: 'ledger',
      url: 'https://mcp.ledger.example/mcp',
      granted: null,
    });
    // The install rides with the sign-in, and its callback is what installs.
    expect(request.pending).toEqual({ kind: 'plugin', sourceUrl: ledger.source_url });
    expect(installPluginFromUrl).toHaveBeenCalledTimes(1);
    expect(screen.queryByText('ledger installed')).not.toBeInTheDocument();
    expect(toast).not.toHaveBeenCalled();

    // Waiting on the sign-in holds every other install.
    const card = screen.getByTestId('discover-card-ledger');
    expect(within(card).getByText('Signing in...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Install wind' })).toBeDisabled();

    // Taken back before the vendor's page: nothing to undo, and the list frees up.
    act(() => request.onNotStarted?.());
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Install wind' })).toBeEnabled(),
    );
    expect(within(card).queryByText('Signing in...')).not.toBeInTheDocument();
  });

  // Inside the desktop app the vendor's page opens in the system browser and
  // this page stays. The keys dialog used to hold the user, undismissable, for
  // as long as that page stayed open.
  it('lets go of the keys dialog once the sign-in has gone on in the browser', async () => {
    const user = userEvent.setup();
    const ledger = entry({ name: 'ledger', auth: 'oauth' });
    vi.mocked(getMarketplace).mockResolvedValue({
      ...catalog,
      entries: [...catalog.entries, ledger],
    });
    vi.mocked(installPluginFromUrl)
      .mockRejectedValueOnce(keysRequired())
      .mockRejectedValueOnce(
        apiError(409, {
          code: 'mcp_sign_in_required',
          message: 'Sign in to ledger first.',
          servers: [{ server: 'ledger', url: 'https://mcp.ledger.example/mcp' }],
        }),
      );
    renderWithProviders(<Discover scope="plugins" />);

    await user.click(await screen.findByRole('button', { name: 'Install ledger' }));
    const dialog = await screen.findByRole('dialog', { name: 'Install ledger' });
    await user.type(within(dialog).getByLabelText(/Wind key/), 'k-123');
    await user.type(within(dialog).getByLabelText(/Wind secret/), 's-456');
    await user.click(within(dialog).getByRole('button', { name: 'Check and install' }));
    await waitFor(() => expect(oauth.connect).toHaveBeenCalledTimes(1));
    const request = oauth.connect.mock.calls[0][0] as ConnectRequest;
    expect(request.pending).toEqual({
      kind: 'plugin',
      sourceUrl: ledger.source_url,
      secrets: { WIND_KEY: 'k-123', WIND_SECRET: 's-456' },
    });

    act(() => request.onHandedOff?.());
    await waitFor(() =>
      expect(screen.queryByRole('dialog', { name: 'Install ledger' })).not.toBeInTheDocument(),
    );
    // The card says where its install went, and the rest of the catalog is free.
    const card = screen.getByTestId('discover-card-ledger');
    expect(within(card).getByText('Signing in...')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Install wind' })).toBeEnabled();
  });
});
