import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { useLocation } from 'react-router';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { getMarketplace, type MarketplaceResponse } from '@/pages/ChatAgent/utils/api/marketplace';
import Plugins from '../Plugins';

vi.mock('@/pages/ChatAgent/utils/api/marketplace', () => ({
  getMarketplace: vi.fn(),
}));

// The page shell is under test here; each section's own list is not.
vi.mock('../components/shell/useShellData', () => ({ useShellData: () => ({}) }));
vi.mock('../components/shell/PluginsRail', () => ({ PluginsRail: () => null }));
vi.mock('../components/shell/YoursSummary', () => ({ YoursSummary: () => null }));
vi.mock('../components/WorkspacesLoadNote', () => ({ WorkspacesLoadNote: () => null }));
vi.mock('../components/PluginsList', () => ({ PluginsList: () => <p>your plugins</p> }));
vi.mock('../components/McpServers', () => ({ McpServers: () => <p>your connectors</p> }));
vi.mock('../components/SkillsList', () => ({ SkillsList: () => <p>your skills</p> }));
vi.mock('../components/Discover', () => ({ Discover: () => <p>the catalog</p> }));

// The Add menu inline: the Radix one opens on pointer events jsdom does not drive.
vi.mock('@/components/ui/dropdown-menu', () => ({
  DropdownMenu: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  DropdownMenuContent: ({ children }: { children: React.ReactNode }) => <div role="menu">{children}</div>,
  DropdownMenuSeparator: () => null,
  DropdownMenuItem: ({ children, onSelect }: { children: React.ReactNode; onSelect?: () => void }) => (
    <button role="menuitem" onClick={() => onSelect?.()}>
      {children}
    </button>
  ),
}));

function listing(configured: boolean): MarketplaceResponse {
  return {
    configured,
    name: null,
    description: null,
    url: null,
    vocabulary: { assets: [], regions: [], data: [] },
    entries: [],
  };
}

function Address() {
  return <output data-testid="address">{useLocation().search}</output>;
}

function renderPage(route: string) {
  const view = renderWithProviders(
    <>
      <Plugins />
      <Address />
    </>,
    { route },
  );
  const answered = () =>
    waitFor(() =>
      expect(view.queryClient.getQueryState(queryKeys.plugins.marketplace())?.status).not.toBe('pending'),
    );
  return { ...view, answered };
}

const address = () => new URLSearchParams(screen.getByTestId('address').textContent ?? '');
// The Yours / Discover switch: with the rail stubbed, its Discover button is
// the only one on the page.
const viewSwitch = () => screen.queryByRole('button', { name: 'Discover' });

beforeEach(() => {
  vi.mocked(getMarketplace).mockReset();
});

describe('the Plugins page on a server with no plugin catalog', () => {
  it('offers no way into Discover: no Yours / Discover switch, no catalog in the Add menu', async () => {
    vi.mocked(getMarketplace).mockResolvedValue(listing(false));
    const { answered } = renderPage('/plugins?tab=plugins');
    await answered();

    expect(screen.getByText('your plugins')).toBeInTheDocument();
    expect(viewSwitch()).not.toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: 'Browse catalog' })).not.toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: 'Install from a git URL or zip' })).toBeInTheDocument();
  });

  // A way in shown while the listing loads would be taken away on a server
  // without one, so it waits, and appears once the catalog is there.
  it('holds the switch back until the server says it has a catalog', async () => {
    let answer: (value: MarketplaceResponse) => void = () => {};
    vi.mocked(getMarketplace).mockReturnValue(new Promise((resolve) => (answer = resolve)));
    renderPage('/plugins?tab=mcp');

    expect(screen.getByText('your connectors')).toBeInTheDocument();
    expect(viewSwitch()).not.toBeInTheDocument();
    expect(screen.queryByRole('menuitem', { name: 'Browse catalog' })).not.toBeInTheDocument();

    answer(listing(true));
    expect(await screen.findByRole('button', { name: 'Discover' })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: 'Browse catalog' })).toBeInTheDocument();
  });

  it('keeps the way into a catalog that is set but cannot be read, where Discover offers the retry', async () => {
    vi.mocked(getMarketplace).mockRejectedValue(new Error('Request failed with status code 502'));
    renderPage('/plugins?tab=skills');

    expect(await screen.findByRole('button', { name: 'Discover' })).toBeInTheDocument();
  });

  it("opens a link into Discover as the section's own list, and moves the address there", async () => {
    vi.mocked(getMarketplace).mockResolvedValue(listing(false));
    const { answered } = renderPage('/plugins?tab=mcp&view=discover&q=fmp&detail=catalog:fmp');
    await answered();

    expect(await screen.findByText('your connectors')).toBeInTheDocument();
    expect(screen.queryByText('the catalog')).not.toBeInTheDocument();
    await waitFor(() => expect(address().has('view')).toBe(false));
    // The search stays; the catalog entry, which only Discover has, does not.
    expect(address().get('tab')).toBe('mcp');
    expect(address().get('q')).toBe('fmp');
    expect(address().has('detail')).toBe(false);
  });

  it('reads the old Discover tab the same way', async () => {
    vi.mocked(getMarketplace).mockResolvedValue(listing(false));
    const { answered } = renderPage('/plugins?tab=discover');
    await answered();

    expect(await screen.findByText('your plugins')).toBeInTheDocument();
    await waitFor(() => expect(address().get('tab')).toBe('plugins'));
  });
});
