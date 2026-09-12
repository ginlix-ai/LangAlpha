import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { Brokerage } from '../brokerages';
import { BrokerageConsentDialog } from '../components/BrokerageConsentDialog';

/**
 * The switches a brokerage's consent dialog links, read off each group's
 * `requires`.
 *
 * The server refuses a selection that grants live orders without the account
 * access that follows them, so the dialog must not be able to confirm one from
 * either direction a user can reach it: ticking the dependent group, or
 * unticking the one it needs.
 */

const VENDOR: Brokerage = {
  name: 'robinhood',
  label: 'Robinhood',
  url: 'https://agent.robinhood.com/mcp/trading',
  site: 'robinhood.com',
  description: 'Robinhood brokerage account.',
  native_callback_only: true,
  exclusive_connection: false,
  capabilities: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'trading', tone: 'danger', rung: true, requires: ['account'] },
  ],
};

function openDialog(granted?: string[] | null) {
  const onConfirm = vi.fn();
  renderWithProviders(
    <BrokerageConsentDialog
      vendor={VENDOR}
      name="robinhood"
      granted={granted}
      pending={false}
      onConfirm={onConfirm}
      onCancel={vi.fn()}
    />,
  );
  return onConfirm;
}

const liveOrders = () => screen.getByRole('switch', { name: /Live orders/ });
const account = () => screen.getByRole('switch', { name: /Account and positions/ });
const confirm = () => fireEvent.click(screen.getByRole('button', { name: 'Connect' }));

describe('BrokerageConsentDialog', () => {
  it('opens on account access and without live orders', () => {
    openDialog();

    expect(account()).toHaveAttribute('aria-checked', 'true');
    expect(liveOrders()).toHaveAttribute('aria-checked', 'false');
  });

  it('turns on the account access live orders need', () => {
    const onConfirm = openDialog(['market_data']);

    fireEvent.click(liveOrders());

    expect(account()).toHaveAttribute('aria-checked', 'true');
    confirm();
    expect([...onConfirm.mock.calls[0][0]].sort()).toEqual([
      'account',
      'market_data',
      'trading',
    ]);
  });

  it('turns live orders off with the account access they need', () => {
    const onConfirm = openDialog(['market_data', 'account', 'trading']);

    fireEvent.click(account());

    expect(liveOrders()).toHaveAttribute('aria-checked', 'false');
    confirm();
    expect(onConfirm).toHaveBeenCalledWith(['market_data']);
  });

  it('opens a remembered choice the server would refuse on what can be in force', () => {
    // Stored before the groups were linked. Narrowed, never widened: account
    // stays off, and live orders go off with it.
    openDialog(['market_data', 'trading']);

    expect(liveOrders()).toHaveAttribute('aria-checked', 'false');
    expect(account()).toHaveAttribute('aria-checked', 'false');
  });

  it('says why only under the group that needs another', () => {
    openDialog();

    expect(
      screen.getAllByText('Needs Account and positions, so placed orders can be followed.'),
    ).toHaveLength(1);
  });
});
