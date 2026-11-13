/**
 * Pure adapters that turn a canonical quote row (raw snapshot) into the shapes
 * legacy hooks return. Kept side-effect-free and framework-free so they are
 * trivially unit-testable and reusable by any consumer of the quote layer.
 */
import { quotePlaces } from '@/lib/bars/currencyDisplay';
import type { StockPrice } from '@/types/market';
import type { QuoteRow } from './quoteBatcher';

/** Every spelling a batch row answers: the one it shows, plus the spellings
 *  the request used for it. A caller keyed on either finds the row. */
export function snapshotRowSpellings(row: { symbol: string; requested?: string[] | null }): string[] {
  return [row.symbol, ...(row.requested ?? [])];
}

/**
 * Map a raw snapshot quote row → the `StockPrice` the watchlist and portfolio
 * hooks consume. Figures pass through unrounded, with the listing's places
 * (lib/bars `quotePlaces`) worked out here from the row's own currency and
 * asset class, since a cent-rounded 0.0483 reads 0.05 in every total computed
 * from it.
 */
export function snapshotToStockPrice(symbol: string, snap: QuoteRow | undefined | null): StockPrice {
  // A new day's row has no close until its regular session prints, so through
  // pre-market the last official price is still the previous close, unmoved.
  // Its `change` then measures the pre-market move, which `session` carries.
  const unprinted = snap != null && snap.price == null && snap.previous_close != null && snap.previous_close > 0;
  const price = unprinted ? snap.previous_close : snap?.price;
  if (snap && price != null) {
    const change = unprinted ? 0 : (snap.change ?? 0);
    return {
      symbol,
      currency: snap.currency ?? null,
      price,
      change,
      changePercent: unprinted ? 0 : (snap.change_percent ?? 0),
      isPositive: change >= 0,
      quoteAvailable: true,
      places: quotePlaces(symbol, { currency: snap.currency, assetClass: snap.asset_class, price: snap.previous_close ?? price }),
      session: {
        previousClose: snap.previous_close ?? null,
        regularClose: snap.regular_close ?? null,
        regularTradingChange: snap.regular_trading_change ?? null,
        lastMinuteClose: snap.last_minute_close ?? null,
        earlyTradingChange: snap.early_trading_change ?? null,
        lateTradingChange: snap.late_trading_change ?? null,
        earlyTradingChangePercent: snap.early_trading_change_percent ?? null,
        lateTradingChangePercent: snap.late_trading_change_percent ?? null,
      },
    };
  }
  return { symbol, price: 0, change: 0, changePercent: 0, isPositive: true, quoteAvailable: false, places: quotePlaces(symbol, {}) };
}
