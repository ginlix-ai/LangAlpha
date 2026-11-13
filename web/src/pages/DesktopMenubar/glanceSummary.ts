/**
 * The menu bar panel's arithmetic, kept pure so the menu bar lines computed in
 * the background and the panel drawn in the foreground read one answer.
 */
import { percentMove } from '@/pages/ChatAgent/components/charts/termChange';
import {
  normalizePortfolioCurrency,
  summarizePortfolioByCurrency,
} from '@/pages/Dashboard/utils/portfolioSummary';
import type { MarketPhase, SessionQuote } from '@/lib/marketUtils';

/** A pre-market or after-hours move (`PM`/`AH`), measured off the official price beside it. */
export type ExtendedMove = Omit<NonNullable<SessionQuote['ext']>, 'type'>;

/**
 * A holding at its official price (`sessionQuote`'s headline): in an extended
 * session the last close, its move beside it in `ext`, so the day's figures
 * never count the extended move twice.
 */
export interface HoldingQuote {
  symbol: string;
  quantity: number;
  /** Null when the symbol has no quote. */
  price: number | null;
  /** Today's official move per share, in the quote's currency; null before the open. */
  change: number | null;
  changePercent: number | null;
  currency: string;
  ext?: ExtendedMove | null;
}

export interface RankedHolding extends HoldingQuote {
  value: number;
  /** Share of its currency's total, 0 to 100. */
  weight: number;
}

export interface PortfolioDay {
  currency: string;
  totalValue: number;
  /** Null when no holding has a move today yet (before the open). */
  dayChange: number | null;
  /** Against yesterday's value; null when that was nothing, or there is no move yet. */
  dayChangePct: number | null;
  /** The holdings' extended-hours move together, against their official value; null outside one. */
  ext: { label: string; pct: number } | null;
  /** This currency's holdings, largest first. */
  holdings: RankedHolding[];
  /** The other currencies held, which the panel names but does not add up. */
  otherCurrencies: string[];
}

/**
 * Today's change across the holdings, in the primary currency: the one
 * `summarizePortfolioByCurrency` lists first (dollars, else the first code
 * alphabetically), since sums across currencies mean nothing. Null when no
 * holding has a quote.
 */
export function summarizePortfolioDay(rows: readonly HoldingQuote[]): PortfolioDay | null {
  const priced = rows.filter((r) => r.price != null && Number.isFinite(r.price) && Number.isFinite(r.quantity));
  const summaries = summarizePortfolioByCurrency(
    priced.map((r) => ({
      symbol: r.symbol,
      marketValue: r.quantity * (r.price as number),
      currency: r.currency,
      quoteAvailable: true,
    })),
  ).filter((s) => s.totalValue !== 0);
  const primary = summaries[0];
  if (!primary) return null;

  const mine = priced.filter((r) => normalizePortfolioCurrency(r.currency) === primary.currency);
  const moved = mine.filter((r) => r.change != null && Number.isFinite(r.change));
  const dayChange = moved.length ? moved.reduce((sum, r) => sum + r.quantity * (r.change as number), 0) : null;
  const before = dayChange == null ? null : primary.totalValue - dayChange;
  // A holding with no extended print has not moved off its close.
  const extended = mine.filter((r) => r.ext && r.ext.change != null && Number.isFinite(r.ext.change));
  const extChange = extended.reduce((sum, r) => sum + r.quantity * (r.ext!.change as number), 0);
  const holdings = mine
    .map((r) => {
      const value = r.quantity * (r.price as number);
      return { ...r, value, weight: primary.totalValue ? (value / primary.totalValue) * 100 : 0 };
    })
    .sort((a, b) => b.value - a.value);

  return {
    currency: primary.currency,
    totalValue: primary.totalValue,
    dayChange,
    dayChangePct: dayChange != null && before != null && before > 0 ? (dayChange / before) * 100 : null,
    ext:
      extended.length && primary.totalValue > 0
        ? { label: extended[0].ext!.label, pct: (extChange / primary.totalValue) * 100 }
        : null,
    holdings,
    otherCurrencies: summaries.slice(1).map((s) => s.currency),
  };
}

/** A move as the menu bar prints it: `+0.78%`, `-1.20%`, `0.00%`. */
export function signedPercent(value: number): string {
  const { sign, size } = percentMove(value);
  return `${sign}${size}`;
}

const OPEN_AT = 9 * 60 + 30;
const CLOSE_AT = 16 * 60;
const AFTER_ENDS_AT = 20 * 60;

/**
 * Minutes until the session the market is in ends (the open, for pre-market),
 * from the New York wall clock as `HH:MM`. The status says which session it
 * is; the regular hours are the clock's guess at when it ends, so a half day
 * reads an hour or three long. Null when there is nothing to count down to.
 */
export function minutesLeftInPhase(phase: MarketPhase, etClock: string): number | null {
  const match = /^(\d{1,2}):(\d{2})/.exec(etClock);
  if (!match) return null;
  // Some engines print midnight as 24:00 under a 24-hour clock.
  const minutes = (Number(match[1]) % 24) * 60 + Number(match[2]);
  const end = phase === 'open' ? CLOSE_AT : phase === 'pre' ? OPEN_AT : phase === 'after' ? AFTER_ENDS_AT : null;
  if (end == null) return null;
  const left = end - minutes;
  return left > 0 ? left : null;
}
