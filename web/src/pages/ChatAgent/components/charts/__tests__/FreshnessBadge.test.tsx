import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { FreshnessBadge, freshnessTooltipLines } from '../FreshnessBadge';
import i18n from '@/i18n';
import type { Freshness } from '@/types/market';

const f = (partial: Partial<Freshness>): Freshness =>
  ({ label: 'unknown', measured: false, ...partial }) as Freshness;

describe('<FreshnessBadge />', () => {
  it('renders a realtime pill for a live row', () => {
    render(<FreshnessBadge freshness={f({ label: 'live', measured: true })} />);
    expect(screen.getByText('Realtime')).toBeInTheDocument();
  });

  it('renders nothing when the row carries no freshness fields', () => {
    const { container } = render(<FreshnessBadge />);
    expect(container.firstChild).toBeNull();
  });

  it('renders the localized copy for each badge state', () => {
    const { unmount } = render(<FreshnessBadge tier="delayed_15m" />);
    expect(screen.getByText('Delayed 15 min')).toBeInTheDocument();
    unmount();

    const second = render(<FreshnessBadge freshness={f({ label: 'delayed', measured: true, lag_s: 300 })} />);
    expect(screen.getByText('Delayed 5 min')).toBeInTheDocument();
    second.unmount();

    const third = render(<FreshnessBadge tier="eod" />);
    expect(screen.getByText('Last close')).toBeInTheDocument();
    third.unmount();

    render(<FreshnessBadge freshness={f({ label: 'stale', measured: true })} />);
    expect(screen.getByText('Stale')).toBeInTheDocument();
  });

  it('keeps a delay neutral and gives the warning color to stale and incomplete rows', () => {
    const dotColor = (el: HTMLElement) => (el.querySelector('[aria-hidden]') as HTMLElement).style.background;
    const delayed = render(<FreshnessBadge tier="delayed_15m" />);
    expect(dotColor(delayed.container)).toBe('var(--color-text-tertiary)');
    delayed.unmount();

    const stale = render(<FreshnessBadge freshness={f({ label: 'stale', measured: true })} />);
    expect(dotColor(stale.container)).toBe('var(--color-warning)');
    stale.unmount();

    const incomplete = render(<FreshnessBadge freshness={f({ label: 'incomplete', measured: true })} />);
    expect(dotColor(incomplete.container)).toBe('var(--color-warning)');
  });

  it('takes focus only when it has a tooltip to open, and names a bare dot as a graphic', () => {
    // One line: the pill's words are everything, so nothing to focus.
    const plain = render(<FreshnessBadge tier="delayed_15m" />);
    expect(plain.container.querySelector('[tabindex]')).toBeNull();
    plain.unmount();

    const withSource = render(<FreshnessBadge tier="delayed_15m" source="fmp" />);
    expect(withSource.container.querySelector('[tabindex="0"]')).not.toBeNull();
    withSource.unmount();

    render(<FreshnessBadge tier="delayed_15m" source="fmp" compact />);
    expect(screen.getByRole('img', { name: 'Source: FMP. Delayed 15 min' })).toHaveAttribute('tabindex', '0');
  });
});

describe('freshnessTooltipLines', () => {
  const t = i18n.getFixedT('en-US');
  const NOW = Date.UTC(2026, 6, 14, 20, 0);

  it('names the source, the state and the print time for a quote row', () => {
    expect(
      freshnessTooltipLines(t, {
        tier: 'realtime',
        freshness: f({ label: 'live', measured: true, source: 'tushare' }),
        asOfLocal: '2026-07-14 10:44:00 CST',
        printed: true,
      }, 'en-US', NOW),
    ).toEqual(['Source: Tushare', 'Realtime', '2026-07-14 10:44:00 CST']);
  });

  it('says the clock is a retrieval time when the provider gave no print', () => {
    const lines = freshnessTooltipLines(t, {
      tier: 'delayed_15m',
      freshness: f({ label: 'unknown', measured: false, source: 'fmp' }),
      asOfLocal: '2026-07-14 10:44:40 HKT',
      printed: false,
    }, 'en-US', NOW);
    expect(lines).toEqual(['Source: FMP', 'Delayed 15 min', 'Retrieved 2026-07-14 10:44:40 HKT']);
  });

  it('prints the last bar in venue time for a chart series', () => {
    // Same New York day as the bar, so it prints as a bare time.
    const lines = freshnessTooltipLines(t, {
      source: 'fmp',
      freshness: f({ label: 'delayed', measured: true, lag_s: 900, interval: '5min', actual_latest: Date.UTC(2026, 6, 14, 19, 45) }),
      chartSymbol: 'ACME',
    }, 'en-US', NOW);
    // Source, state, last bar; the state line uses the pill's own words.
    expect(lines[0]).toBe('Source: FMP');
    expect(lines[1]).toBe('Delayed 15 min');
    // 19:45 UTC is 15:45 in New York (EDT); any other zone is a different clock.
    expect(lines[2]).toBe('last bar 15:45');
  });
});
