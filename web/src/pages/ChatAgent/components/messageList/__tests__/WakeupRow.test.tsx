/**
 * What a wakeup's transcript row says. A price target's notice from before
 * wakeups is read into a wakeup's notice where it arrives, and its row reads
 * as a price alert always has: the alert in words from the notice's own
 * fields, and a generic label when the notice names nothing. The model-facing
 * query text is never the source.
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { NotificationMessage, WakeupArrival } from '@/types/chat';
import { deliveredNoticeRow, wakeTurnOpener } from '../../../session/wakeupEvents';
import { WakeupRow } from '../WakeupRow';

function arrival(row: NotificationMessage | null): WakeupArrival {
  if (!row?.wakeup) throw new Error('no wakeup row');
  return row.wakeup;
}

// Neutral placeholder tickers and made-up prices only.
const delivered = (market_alert: Record<string, unknown>) =>
  arrival(deliveredNoticeRow({ content: '', market_alert } as never, new Date(0)));

describe('WakeupRow, a price target from before wakeups', () => {
  it('names the wake from the notice, as a price alert', () => {
    const wake = arrival(wakeTurnOpener(3, {
      market_target: {
        target_id: 'tg-1', notice: 'hit', symbol: 'NVDA', kind: 'above', value: 205,
        fired_price: 205.12, fired_at: '2030-01-01T15:05:00Z',
      },
    }, new Date(0), 'market_target'));
    const { container } = renderWithProviders(<WakeupRow wakeup={wake} />);
    expect(screen.getByText(/^Price alert woke the agent: NVDA crossed above 205\.00 · 205\.12 at /)).toBeInTheDocument();
    expect(container.querySelector('[data-market-alert="wake"]')).not.toBeNull();
    expect(container.querySelector('[data-wakeup]')).toBeNull();
  });

  it('falls back to the generic wake label when the notice names only its target', () => {
    const wake = arrival(wakeTurnOpener(3, { market_target: { target_id: 'tg-1' } }, new Date(0), 'market_target'));
    renderWithProviders(<WakeupRow wakeup={wake} />);
    expect(screen.getByText('Price alert woke the agent')).toBeInTheDocument();
  });

  it('keeps the price alert row for a wake that names no notice at all', () => {
    const wake = arrival(wakeTurnOpener(3, {}, new Date(0), 'market_target'));
    const { container } = renderWithProviders(<WakeupRow wakeup={wake} />);
    expect(screen.getByText('Price alert woke the agent')).toBeInTheDocument();
    expect(container.querySelector('[data-market-alert="wake"]')).not.toBeNull();
  });

  it('says a delivered move from its own fields', () => {
    const row = delivered({ target_id: 'x', notice: 'hit', symbol: 'TSLA', kind: 'pct_down', value: 3, pct_from: 'previous_close' });
    const { container } = renderWithProviders(<WakeupRow wakeup={row} />);
    expect(screen.getByText('TSLA moved -3% from previous close')).toBeInTheDocument();
    expect(container.querySelector('[data-market-alert="delivered"]')).not.toBeNull();
  });

  it('says a target is about to expire', () => {
    const row = delivered({ target_id: 'x', notice: 'expiring', symbol: 'NVDA', kind: 'below', value: 180 });
    renderWithProviders(<WakeupRow wakeup={row} />);
    expect(screen.getByText('NVDA ≤ 180 target expires soon')).toBeInTheDocument();
  });
});

describe('WakeupRow, an order update', () => {
  const ORDER_NOTICE = {
    wakeup_id: 'wk-o', short_id: 'w_o1', label: null, kind: 'order', notice: 'fire',
    seq: 1, total: 1, last: true, skipped: 0, fired_at: '2030-01-01T15:05:00Z',
  };

  it('names an update that arrived without its order by the wakeup\'s own name', () => {
    const row = arrival(deliveredNoticeRow({ content: '', wakeup: ORDER_NOTICE } as never, new Date(0)));
    renderWithProviders(<WakeupRow wakeup={row} />);
    expect(screen.getByText(/w_o1/)).toBeInTheDocument();
  });

  it('prefers the label the agent gave it', () => {
    const row = arrival(deliveredNoticeRow({ content: '', wakeup: { ...ORDER_NOTICE, label: 'the ACME buy' } } as never, new Date(0)));
    renderWithProviders(<WakeupRow wakeup={row} />);
    expect(screen.getByText(/the ACME buy/)).toBeInTheDocument();
  });
});
