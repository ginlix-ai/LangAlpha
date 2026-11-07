import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { InlineOptionsChainCard } from '../InlineOptionsChainCard';
import { OptionsChainPanel } from '../OptionsMoversPanels';

// Placeholder contracts and fabricated figures, in the shape quotes.py sends.
const artifact = {
  type: 'options_chain',
  underlying: 'ZZZZ',
  contract_type: 'call',
  currency: 'USD',
  results: [
    {
      ticker: 'O:ZZZZ261120C00330000', contract_type: 'call', strike_price: 330, expiration_date: '2026-11-20',
      session: { close: 15, change: -3.9, change_percent: -20.8, volume: 9, last_updated: '2026-11-06 09:50:02', stale: false },
    },
    {
      ticker: 'O:ZZZZ261120C00332500', contract_type: 'call', strike_price: 332.5, expiration_date: '2026-11-20',
      session: { close: 17.48, change: 0, change_percent: 0, volume: 14, low: 16.9, high: 18.2, last_updated: '2026-11-05 16:10:02', stale: true },
    },
  ],
};

describe('InlineOptionsChainCard', () => {
  it('dims a contract not traded this session and dates its last print', () => {
    render(<InlineOptionsChainCard artifact={artifact} />);

    const stale = screen.getByText('$17.48');
    expect(stale.closest('.term-stale')).not.toBeNull();
    expect(stale.closest('td')).toHaveAttribute('title', 'Not traded this session. Last print 2026-11-05 16:10 New York time');

    const traded = screen.getByText('$15.00');
    expect(traded.closest('.term-stale')).toBeNull();
    expect(traded.closest('td')).not.toHaveAttribute('title');

    const volume = screen.getByText('14');
    expect(volume.closest('.term-stale')).not.toBeNull();
    expect(volume.closest('td')).toHaveAttribute('title', 'Not traded this session. Last print 2026-11-05 16:10 New York time');
    expect(screen.getByText('9').closest('.term-stale')).toBeNull();
  });
});

describe('OptionsChainPanel', () => {
  it('dims the stale contract\'s price, moves, volume and range', () => {
    const { container } = render(<OptionsChainPanel data={artifact} />);

    expect(screen.getByText('$17.48').closest('.term-stale')).not.toBeNull();
    expect(screen.getByText('$15.00').closest('.term-stale')).toBeNull();
    expect(container.querySelectorAll('.term-stale')).toHaveLength(5);
    expect(container.querySelector('.term-stale .term-range')).not.toBeNull();
  });
});
