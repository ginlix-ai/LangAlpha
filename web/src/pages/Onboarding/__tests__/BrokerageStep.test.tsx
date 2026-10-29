/**
 * The welcome flow's brokerage step says what this window cannot connect and
 * asks the trading permission beside the brokers. A broker only the desktop
 * app can connect (Robinhood) is no card, since its Connect would do nothing:
 * it is named under the cards with a link to the download. A desktop app too
 * old to finish any sign-in gets its own note and no "nothing to connect"
 * line, which would be false. The trading permission is asked only once a
 * broker is connected, since before that there is no account for it to govern.
 * It is the Settings choice itself, so it saves as it does there and the
 * levels that let orders skip approval open the agreement instead of saving,
 * and it is left out until a level has been read.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, within } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { TradingPermission } from '@/types/api';

vi.mock('@/api/tradingPermission', () => ({
  getTradingPermission: vi.fn(),
  updateTradingPermission: vi.fn(),
}));
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

interface FakeBroker {
  b: { name: string };
  label: string;
  vendor: { name: string };
  row: null;
  connected: boolean;
}
const broker = (name: string, label: string): FakeBroker => ({
  b: { name },
  label,
  vendor: { name },
  row: null,
  connected: false,
});

const brokers = {
  rows: [] as FakeBroker[],
  desktopOnly: [] as FakeBroker[],
  shellOutdated: false,
  resolved: new Map(),
  connected: [] as FakeBroker[],
  loading: false,
  ready: true,
  offersError: null as unknown,
  catalogError: null as unknown,
  refetchOffers: vi.fn(),
  refetchCatalog: vi.fn(),
};
vi.mock('../welcome/useBrokers', () => ({ useBrokers: () => brokers }));
vi.mock('@/pages/Plugins/hooks/useBrokerageConnect', () => ({
  useBrokerageConnect: () => ({
    oauth: { connectingName: null },
    requestConnect: vi.fn(),
    toggle: vi.fn(),
    togglingName: null,
  }),
}));
vi.mock('@/pages/Plugins/hooks/useConnectLanding', () => ({
  useWelcomeLanding: () => null,
  spendConnectLanding: () => false,
}));
vi.mock('@/pages/Plugins/components/BrokerageCard', () => ({
  BrokerageCard: ({ broker }: { broker: FakeBroker }) => (
    <article data-testid={`brokerage-${broker.b.name}`}>{broker.label}</article>
  ),
}));
vi.mock('@/pages/ChatAgent/components/mcp/BrandMark', () => ({
  BrandMark: () => <span />,
}));

import { getTradingPermission, updateTradingPermission } from '@/api/tradingPermission';
import { BrokerageStep } from '../welcome/steps/BrokerageStep';

const mockGet = vi.mocked(getTradingPermission);
const mockUpdate = vi.mocked(updateTradingPermission);

const permission = (over: Partial<TradingPermission> = {}): TradingPermission => ({
  level: 'approve_each',
  agreement_version: 1,
  agreed_at: null,
  updated_at: null,
  ...over,
});

const NONE = 'No brokerage can be connected here yet.';

const onElsewhere = vi.fn();

function renderStep() {
  return renderWithProviders(
    <BrokerageStep labelledBy="brokerage-line" onElsewhere={onElsewhere} onPreparing={() => {}} />,
  );
}

describe('BrokerageStep', () => {
  beforeEach(() => {
    mockGet.mockReset();
    mockUpdate.mockReset();
    onElsewhere.mockReset();
    mockGet.mockResolvedValue(permission());
    Object.assign(brokers, {
      rows: [],
      desktopOnly: [],
      shellOutdated: false,
      connected: [],
      loading: false,
      offersError: null,
      catalogError: null,
    });
  });

  it('names a desktop-only broker with a download link and gives it no card', () => {
    brokers.rows = [broker('ibkr', 'Interactive Brokers')];
    brokers.desktopOnly = [broker('robinhood', 'Robinhood')];
    renderStep();

    const note = screen.getByTestId('welcome-desktop-note');
    expect(note).toHaveTextContent('Robinhood can only be connected from the desktop app.');
    const link = within(note).getByRole('link', { name: 'Download the desktop app' });
    expect(link).toHaveAttribute('href', 'https://langalpha.ai/download');
    expect(link).toHaveAttribute('target', '_blank');
    expect(screen.getByTestId('brokerage-ibkr')).toBeInTheDocument();
    expect(screen.queryByTestId('brokerage-robinhood')).not.toBeInTheDocument();
  });

  it('shows no desktop note when nothing is desktop-only and the app is current', () => {
    brokers.rows = [broker('ibkr', 'Interactive Brokers')];
    renderStep();
    expect(screen.queryByTestId('welcome-desktop-note')).not.toBeInTheDocument();
  });

  it('asks for an update, not "none", when the desktop app is too old', () => {
    brokers.shellOutdated = true;
    renderStep();

    const note = screen.getByTestId('welcome-desktop-note');
    expect(note).toHaveTextContent('Update the desktop app to connect a brokerage.');
    expect(within(note).getByRole('link', { name: 'Download the latest version' })).toBeInTheDocument();
    expect(screen.queryByText(NONE)).not.toBeInTheDocument();
  });

  it('says none can be connected, and asks no trading permission, when nothing is offered', () => {
    renderStep();
    expect(screen.getByText(NONE)).toBeInTheDocument();
    expect(screen.queryByRole('radiogroup', { name: 'Trading permission' })).not.toBeInTheDocument();
    expect(mockGet).not.toHaveBeenCalled();
  });

  it('asks no trading permission while brokers are offered but none is connected', async () => {
    brokers.rows = [broker('ibkr', 'Interactive Brokers')];
    renderStep();
    expect(screen.getByTestId('brokerage-ibkr')).toBeInTheDocument();
    expect(screen.queryByRole('radiogroup', { name: 'Trading permission' })).not.toBeInTheDocument();
    expect(mockGet).not.toHaveBeenCalled();
  });

  describe('continuing without connecting', () => {
    it('offers the way on under the cards when nothing is connected, and takes it on a click', () => {
      brokers.rows = [broker('ibkr', 'Interactive Brokers')];
      renderStep();
      const row = screen.getByTestId('welcome-elsewhere');
      expect(row).toHaveTextContent('Continue without connecting');
      fireEvent.click(row);
      expect(onElsewhere).toHaveBeenCalledTimes(1);
    });

    it('is left out while the brokers are still loading', () => {
      brokers.loading = true;
      renderStep();
      expect(screen.queryByTestId('welcome-elsewhere')).not.toBeInTheDocument();
    });

    it('is left out once a broker is connected', async () => {
      const moomoo = { ...broker('moomoo', 'moomoo'), connected: true };
      brokers.rows = [moomoo];
      brokers.connected = [moomoo];
      renderStep();
      await screen.findByRole('radiogroup', { name: 'Trading permission' });
      expect(screen.queryByTestId('welcome-elsewhere')).not.toBeInTheDocument();
    });

    it('is still offered when the list failed to load', () => {
      brokers.offersError = new Error('boom');
      renderStep();
      expect(screen.getByTestId('welcome-elsewhere')).toBeInTheDocument();
    });
  });

  describe('trading permission', () => {
    beforeEach(() => {
      const moomoo = { ...broker('moomoo', 'moomoo'), connected: true };
      brokers.rows = [broker('ibkr', 'Interactive Brokers'), moomoo];
      brokers.connected = [moomoo];
    });

    it('draws the four levels with the current one checked', async () => {
      renderStep();
      const group = await screen.findByRole('radiogroup', { name: 'Trading permission' });
      expect(within(group).getAllByRole('radio')).toHaveLength(4);
      expect(within(group).getByRole('radio', { name: /^Approve each order/ })).toBeChecked();
    });

    it('saves a level that keeps approval on the click', async () => {
      mockUpdate.mockResolvedValue(permission({ level: 'no_trading' }));
      renderStep();
      fireEvent.click(await screen.findByRole('radio', { name: /^No trading/ }));
      await vi.waitFor(() => expect(mockUpdate).toHaveBeenCalledWith({ level: 'no_trading' }));
    });

    it('opens the agreement instead of saving a level that skips approval', async () => {
      renderStep();
      fireEvent.click(await screen.findByRole('radio', { name: /^Full autonomy/ }));
      expect(await screen.findByRole('dialog')).toBeInTheDocument();
      expect(mockUpdate).not.toHaveBeenCalled();
    });

    it('leaves the section out while the level is still being read', async () => {
      mockGet.mockReturnValue(new Promise(() => {}));
      renderStep();
      await vi.waitFor(() => expect(mockGet).toHaveBeenCalled());
      expect(screen.queryByRole('radiogroup', { name: 'Trading permission' })).not.toBeInTheDocument();
    });
  });
});
