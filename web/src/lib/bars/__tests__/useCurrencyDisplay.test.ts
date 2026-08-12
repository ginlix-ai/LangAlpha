import { describe, it, expect } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useCurrencyDisplay } from '../useCurrencyDisplay';

function render(symbol: string) {
  return renderHook(({ s }) => useCurrencyDisplay(s), { initialProps: { s: symbol } });
}

describe('useCurrencyDisplay', () => {
  it('initializes USD from a plain US ticker', () => {
    const { result } = render('AAPL');
    expect(result.current.displayCurrency).toEqual({ code: 'USD', decimals: 2 });
  });

  it('derives the listing currency from an exchange suffix', () => {
    const { result } = render('TSCO.L');
    expect(result.current.displayCurrency.code).toBe('GBP');
  });

  it('onCurrencyMeta upgrades currency + decimals and mirrors the ref', () => {
    const { result } = render('AAPL');
    act(() => result.current.onCurrencyMeta({ currency: 'HKD', displayDecimals: 3 }));
    expect(result.current.displayCurrency).toEqual({ code: 'HKD', decimals: 3 });
    expect(result.current.priceFormatRef.current).toEqual({ code: 'HKD', decimals: 3 });
  });

  it('onCurrencyMeta falls back to the suffix when currency is absent', () => {
    const { result } = render('TSCO.L');
    act(() => result.current.onCurrencyMeta({ displayDecimals: 0 }));
    expect(result.current.displayCurrency).toEqual({ code: 'GBP', decimals: 0 });
  });

  it('does not reset when re-rendered with the same symbol', () => {
    const { result, rerender } = render('AAPL');
    act(() => result.current.onCurrencyMeta({ currency: 'EUR', displayDecimals: 4 }));
    rerender({ s: 'AAPL' });
    expect(result.current.displayCurrency).toEqual({ code: 'EUR', decimals: 4 });
  });

  it('resets to the suffix default when the symbol changes', () => {
    const { result, rerender } = render('AAPL');
    act(() => result.current.onCurrencyMeta({ currency: 'EUR', displayDecimals: 4 }));
    expect(result.current.displayCurrency.code).toBe('EUR');
    rerender({ s: 'TSCO.L' });
    expect(result.current.displayCurrency).toEqual({ code: 'GBP', decimals: 2 });
  });

  it('formatPrice tracks the live currency and decimals', () => {
    const { result } = render('AAPL');
    expect(result.current.formatPrice(12.3)).toBe('$12.30');
    act(() => result.current.onCurrencyMeta({ currency: 'GBP', displayDecimals: 3 }));
    expect(result.current.formatPrice(12.3)).toBe('£12.300');
  });
});
