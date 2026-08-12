import { useCallback, useEffect, useRef, useState } from 'react';
import type { RefObject } from 'react';
import { currencyForSymbol, formatPrice, resolveDisplayCurrency } from './currencyDisplay';

export interface DisplayCurrency {
  code: string;
  decimals: number;
}

/** Currency/decimals as they arrive from a loader or delta-poll payload. */
export interface CurrencyMeta {
  currency?: string;
  displayDecimals?: number;
}

export interface UseCurrencyDisplay {
  /** Reactive currency + decimals for headers and child props. */
  displayCurrency: DisplayCurrency;
  /**
   * Mirrors {@link displayCurrency} for chart price-formatter closures that are
   * created once at series-creation time — the formatter reads the ref so the
   * axis follows the currency without re-creating the series.
   */
  priceFormatRef: RefObject<DisplayCurrency>;
  /** Format a price in the current currency. Reads the ref; referentially stable. */
  formatPrice: (value: number) => string;
  /**
   * Upgrade the currency from protocol metadata (initial load or delta poll).
   * Designed to plug straight into `useLiveBars`' `onMeta`.
   */
  onCurrencyMeta: (meta: CurrencyMeta | null | undefined) => void;
}

/**
 * Currency-aware price display for a chart symbol. Owns the {displayCurrency
 * state + mirrored ref} pair, resets to the symbol-suffix default when the
 * symbol changes, and exposes `onCurrencyMeta` to upgrade from the protocol's
 * `price_currency` / `display_decimals`. See ./currencyDisplay for the suffix
 * heuristic and the shared formatter.
 */
export function useCurrencyDisplay(symbol: string): UseCurrencyDisplay {
  const [displayCurrency, setDisplayCurrency] = useState<DisplayCurrency>(
    () => ({ code: currencyForSymbol(symbol), decimals: 2 }),
  );
  const priceFormatRef = useRef(displayCurrency);
  priceFormatRef.current = displayCurrency;

  // Live symbol, synced during render, so `onCurrencyMeta` resolves the suffix
  // fallback against the current symbol even when it fires mid-transition.
  const symbolRef = useRef(symbol);
  symbolRef.current = symbol;

  // Reset to the symbol-suffix default when the symbol actually changes — not on
  // the initial mount (the useState initializer already seeded it). The loader
  // upgrades it from protocol meta on success via `onCurrencyMeta`.
  const prevSymbolRef = useRef(symbol);
  useEffect(() => {
    if (prevSymbolRef.current === symbol) return;
    prevSymbolRef.current = symbol;
    setDisplayCurrency({ code: currencyForSymbol(symbol), decimals: 2 });
  }, [symbol]);

  const onCurrencyMeta = useCallback((meta: CurrencyMeta | null | undefined) => {
    setDisplayCurrency(resolveDisplayCurrency(symbolRef.current, meta));
  }, []);

  const format = useCallback(
    (value: number) => formatPrice(value, priceFormatRef.current.code, priceFormatRef.current.decimals),
    [],
  );

  return { displayCurrency, priceFormatRef, formatPrice: format, onCurrencyMeta };
}
