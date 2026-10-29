/**
 * Pure adapters that turn a canonical quote row (raw snapshot) into the shapes
 * legacy hooks return. Kept side-effect-free and framework-free so they are
 * trivially unit-testable and reusable by any consumer of the quote layer.
 */
import type { StockPrice } from '@/types/market';
import type { QuoteRow } from './quoteBatcher';

/** Every spelling a batch row answers: the one it shows, plus the spellings
 *  the request used for it. A caller keyed on either finds the row. */
export function snapshotRowSpellings(row: { symbol: string; requested?: string[] | null }): string[] {
  return [row.symbol, ...(row.requested ?? [])];
}

/**
 * Decimals a row's prices are quoted to, mirroring the server's
 * `display_decimals_for`: CN funds tick in 0.001 CNY (the chart shows 3.912),
 * everything else keeps cents. The snapshot row carries no decimals of its own.
 */
export function quoteDecimals(row: { asset_class?: string | null; currency?: string | null }): number {
  return row.asset_class === 'fund' && (row.currency ?? '').toUpperCase() === 'CNY' ? 3 : 2;
}

export function roundQuote(value: number, decimals: number): number {
  const f = 10 ** decimals;
  return Math.round(value * f) / f;
}

/**
 * Map a raw snapshot quote row → the transformed `StockPrice` the watchlist and
 * portfolio hooks consume. Byte-for-byte equivalent to the per-symbol transform
 * in `getStockPrices` so migrated hooks produce identical rows.
 */
export function snapshotToStockPrice(symbol: string, snap: QuoteRow | undefined | null): StockPrice {
  if (snap && snap.price != null) {
    const dp = quoteDecimals(snap);
    const change = snap.change ?? 0;
    const changePct = snap.change_percent ?? 0;
    return {
      symbol,
      currency: snap.currency ?? null,
      price: roundQuote(snap.price, dp),
      change: roundQuote(change, dp),
      changePercent: Math.round(changePct * 100) / 100,
      isPositive: change >= 0,
      quoteAvailable: true,
      previousClose: snap.previous_close ?? null,
      earlyTradingChangePercent: snap.early_trading_change_percent ?? null,
      lateTradingChangePercent: snap.late_trading_change_percent ?? null,
    };
  }
  return { symbol, price: 0, change: 0, changePercent: 0, isPositive: true, quoteAvailable: false };
}
