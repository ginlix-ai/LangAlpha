import { describe, it, expect, vi } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { httpCatalogServer } from '@/test/factories';
import { resolveBrokerages, type Brokerage } from '../brokerages';
import { BrokerageCard, type BrokerageCardManage } from '../components/BrokerageCard';

/**
 * The card on its own, in the welcome flow's mode: no `manage`, so the offer
 * alone. The Brokerages tab's mode, with the menu and the page, is exercised
 * through the tab in `brokerageSurface`; what is pinned here is what the card
 * leaves out without it, and the one control it gains: a row that is switched
 * off has no menu to be switched back on from, so its foot does it.
 */

vi.mock('../hooks/useWorkspaceOptions', () => ({
  useWorkspaceOptions: () => ({ workspaces: [], nameById: new Map() }),
  useHomeOption: () => undefined,
}));

const IBKR: Brokerage = {
  name: 'ibkr',
  label: 'Interactive Brokers',
  url: 'https://api.ibkr.com/v1/api/mcp-public',
  site: 'interactivebrokers.com',
  description: 'Portfolio, positions, and draft orders.',
  native_callback_only: false,
  exclusive_connection: false,
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'staged_orders', tone: 'caution', rung: true },
  ],
};

/** Resolved the way every list resolves it, so the card is handed what it
 *  would be handed in the app rather than a hand-built answer. */
function broker(row: Partial<CatalogServer> | null) {
  const rows = row ? [httpCatalogServer({ name: 'ibkr', url: IBKR.url, description: '', ...row })] : [];
  return resolveBrokerages([IBKR], rows).get('ibkr')!;
}

/** Signed in, then switched off: its tokens are intact. */
const switchedOff: Partial<CatalogServer> = {
  oauth_status: 'connected',
  enabled: false,
  granted_capabilities: ['market_data'],
};

function renderCard({
  row = null as Partial<CatalogServer> | null,
  toggling = false,
  manage = undefined as BrokerageCardManage | undefined,
} = {}) {
  const onConnect = vi.fn();
  const onToggle = vi.fn();
  renderWithProviders(
    <BrokerageCard
      broker={broker(row)}
      connecting={false}
      toggling={toggling}
      onConnect={onConnect}
      onToggle={onToggle}
      manage={manage}
    />,
  );
  return { onConnect, onToggle };
}

function manageStub(): BrokerageCardManage {
  return {
    workspaces: [],
    refreshing: false,
    onDisconnect: vi.fn(),
    onRefreshSchemas: vi.fn(),
    onRequestRemove: vi.fn(),
    onOpenInMcpTab: vi.fn(),
    onOpen: vi.fn(),
  };
}

describe('BrokerageCard without manage', () => {
  it('is the offer alone: the name as text, no menu, and Connect as the one action', () => {
    const { onConnect, onToggle } = renderCard();

    // There is no broker's page to open from the welcome flow, so the name is
    // not a button that would open nothing.
    expect(screen.getByText('Interactive Brokers').tagName).toBe('SPAN');
    expect(screen.queryByRole('button', { name: 'Interactive Brokers' })).toBeNull();
    expect(screen.queryByLabelText('Actions for Interactive Brokers')).toBeNull();
    expect(screen.getByText('Not added')).toBeInTheDocument();
    expect(screen.queryByTestId('brokerage-switch-on-ibkr')).toBeNull();

    fireEvent.click(screen.getByTestId('brokerage-connect-ibkr'));
    expect(onConnect).toHaveBeenCalledTimes(1);
    expect(onToggle).not.toHaveBeenCalled();
  });

  it('keeps no menu on a row the user has, either', () => {
    renderCard({ row: switchedOff });
    expect(screen.queryByLabelText('Actions for Interactive Brokers')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Interactive Brokers' })).toBeNull();
  });

  it('switches a switched-off row back on from its foot, with no second sign-in', () => {
    const { onConnect, onToggle } = renderCard({ row: switchedOff });

    const button = screen.getByTestId('brokerage-switch-on-ibkr');
    expect(button).toHaveTextContent('Switch on');
    // In place of Connect, not beside it: the tokens are intact.
    expect(screen.queryByTestId('brokerage-connect-ibkr')).toBeNull();

    fireEvent.click(button);
    expect(onToggle).toHaveBeenCalledTimes(1);
    expect(onToggle).toHaveBeenCalledWith(true);
    expect(onConnect).not.toHaveBeenCalled();
  });

  it('holds the switch while one is in flight', () => {
    const { onToggle } = renderCard({ row: switchedOff, toggling: true });
    const button = screen.getByTestId('brokerage-switch-on-ibkr');
    expect(button).toBeDisabled();
    fireEvent.click(button);
    expect(onToggle).not.toHaveBeenCalled();
  });

  it('offers nothing to press on a row that is connected and on', () => {
    renderCard({ row: { ...switchedOff, enabled: true } });
    expect(screen.queryByTestId('brokerage-switch-on-ibkr')).toBeNull();
    expect(screen.queryByTestId('brokerage-connect-ibkr')).toBeNull();
    expect(screen.queryByLabelText('Actions for Interactive Brokers')).toBeNull();
  });

  it('asks a row still waiting on its sign-in to connect, not to switch on', () => {
    // A switch cannot sign anyone in, so a row with no live connection gets
    // Connect whether or not it is switched on.
    renderCard({ row: { oauth_status: 'needs_reauth', enabled: false } });
    expect(screen.getByTestId('brokerage-connect-ibkr')).toBeInTheDocument();
    expect(screen.queryByTestId('brokerage-switch-on-ibkr')).toBeNull();
  });
});

describe('BrokerageCard with manage', () => {
  // The contrast that makes the mode worth pinning: on the Brokerages tab the
  // switch lives in the menu, and the name opens the broker's page.
  it('keeps the switch in the menu and opens the page from the name', () => {
    const manage = manageStub();
    renderCard({ row: switchedOff, manage });

    expect(screen.queryByTestId('brokerage-switch-on-ibkr')).toBeNull();
    expect(screen.getByLabelText('Actions for Interactive Brokers')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Interactive Brokers' }));
    expect(manage.onOpen).toHaveBeenCalledTimes(1);
  });
});
