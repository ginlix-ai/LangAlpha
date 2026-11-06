import { currencyForSymbol } from '@/lib/bars/exchanges';
import type { StockSearchHit } from '@/lib/marketUtils';

/**
 * The currency a holding's cost is entered in: the one the app quotes the
 * listing in. Search names a pence-quoted London line in pence (GBp), which the
 * app quotes in pounds, so a cost typed against the app's price is pounds; a
 * London line traded in dollars keeps its USD. Absent, the server takes the
 * listing's.
 */
export function holdingCurrency(hit: StockSearchHit): string | undefined {
  const code = hit.currency?.trim();
  if (code === 'GBp' || code?.toUpperCase() === 'GBX') return currencyForSymbol(hit.symbol) ?? undefined;
  return code || undefined;
}

/** A listing on a venue the app does not know has no currency to default to,
 *  and the server refuses a holding written without one, so the user names it. */
export function asksHoldingCurrency(hit: StockSearchHit): boolean {
  return !holdingCurrency(hit) && currencyForSymbol(hit.symbol) === null;
}

/** A typed currency as the server takes it: an ISO code upper-cased, and a
 *  pence code (GBp, GBX) as typed, so the server stores its cost in pounds. */
export function typedCurrencyCode(typed: string): string | undefined {
  const code = typed.trim();
  if (code === 'GBp' || code.toUpperCase() === 'GBX') return code;
  return code.toUpperCase() || undefined;
}

let isoCurrencies: Set<string> | null = null;

/** An ISO code this browser knows, else a pence code. A browser without
 *  `Intl.supportedValuesOf` takes any three letters. */
export function isCurrencyCode(code: string | undefined): boolean {
  if (!code) return false;
  if (code === 'GBp' || code.toUpperCase() === 'GBX') return true;
  if (!isoCurrencies) {
    try {
      isoCurrencies = new Set(Intl.supportedValuesOf('currency'));
    } catch {
      return /^[A-Z]{3}$/.test(code);
    }
  }
  return isoCurrencies.has(code);
}
