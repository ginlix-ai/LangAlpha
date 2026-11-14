import { act, fireEvent, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { TickerLogo } from '../TickerLogo';

describe('TickerLogo', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  it('asks once more after the retry delay, then keeps the letter', () => {
    const { container } = render(<TickerLogo symbol="AAPL" />);
    const img = () => container.querySelector('img');
    expect(container.querySelector('.ticker-logo')?.getAttribute('data-letter')).toBe('A');

    fireEvent.error(img()!);
    expect(img()).toBeNull();

    act(() => { vi.advanceTimersByTime(60_000); });
    expect(img()?.getAttribute('src')).toBe('/api/v1/market-data/logos/AAPL?size=128');

    fireEvent.error(img()!);
    act(() => { vi.advanceTimersByTime(600_000); });
    expect(img()).toBeNull();
  });

  it('starts over for another symbol', () => {
    const { container, rerender } = render(<TickerLogo symbol="AAPL" />);
    const first = container.querySelector('img');
    rerender(<TickerLogo symbol="MSFT" />);
    // A new element, so no browser paints Apple's logo while Microsoft's loads.
    expect(container.querySelector('img')).not.toBe(first);
    fireEvent.error(container.querySelector('img')!);
    rerender(<TickerLogo symbol="NVDA" />);
    expect(container.querySelector('img')?.getAttribute('src')).toContain('/logos/NVDA');
  });

  it('asks nothing for an index', () => {
    const { container } = render(<TickerLogo symbol="^GSPC" />);
    expect(container.querySelector('img')).toBeNull();
  });
});
