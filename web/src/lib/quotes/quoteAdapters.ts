/**
 * Pure adapters that turn a canonical quote row (raw snapshot) into the shapes
 * legacy hooks return. Kept side-effect-free and framework-free so they are
 * trivially unit-testable and reusable by any consumer of the quote layer.
 */
import type { StockPrice } from '@/types/market';
import type { QuoteRow } from './quoteBatcher';

/**
 * Map a raw snapshot quote row → the transformed `StockPrice` the watchlist and
 * portfolio hooks consume. Byte-for-byte equivalent to the per-symbol transform
 * in `getStockPrices` so migrated hooks produce identical rows.
 */
export function snapshotToStockPrice(symbol: string, snap: QuoteRow | undefined | null): StockPrice {
  if (snap && snap.price != null) {
    const change = snap.change ?? 0;
    const changePct = snap.change_percent ?? 0;
    return {
      symbol,
      price: Math.round(snap.price * 100) / 100,
      change: Math.round(change * 100) / 100,
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
