import type { TFunction } from 'i18next';
import { fixed2 } from '@/lib/format';
import type { PriceTriggerConfig } from '@/types/automation';

// ── The terms a price trigger is set in ────────────────────

export const PRICE_CONDITION_TYPES = [
  { value: 'price_above', labelKey: 'automation.priceConditionAbove' },
  { value: 'price_below', labelKey: 'automation.priceConditionBelow' },
  { value: 'pct_change_above', labelKey: 'automation.pctChangeAbove' },
  { value: 'pct_change_below', labelKey: 'automation.pctChangeBelow' },
] as const;

export const PRICE_REFERENCE_OPTIONS = [
  { value: 'previous_close', labelKey: 'automation.refPreviousClose' },
  { value: 'day_open', labelKey: 'automation.refDayOpen' },
] as const;

export const RETRIGGER_MODES = [
  { value: 'one_shot', labelKey: 'automation.retriggerOneShot' },
  { value: 'recurring', labelKey: 'automation.retriggerRecurring' },
] as const;

/** The shortest cooldown the server takes, in minutes: four hours. */
export const MIN_COOLDOWN_MINUTES = 240;

export function isPctCondition(type: string): boolean {
  return type === 'pct_change_above' || type === 'pct_change_below';
}

export function isPriceTriggerConfig(v: unknown): v is PriceTriggerConfig {
  return (
    v != null &&
    typeof v === 'object' &&
    'symbol' in v &&
    typeof (v as Record<string, unknown>).symbol === 'string' &&
    'conditions' in v &&
    Array.isArray((v as Record<string, unknown>).conditions)
  );
}

export function formatPriceTrigger(triggerConfig: PriceTriggerConfig | null | undefined, t: TFunction): string {
  if (!triggerConfig || !isPriceTriggerConfig(triggerConfig)) return t('automation.price.alert');
  const symbol = triggerConfig.symbol || '???';
  const condition = triggerConfig.conditions?.[0];
  if (!condition) return t('automation.price.symbolAlert', { symbol });
  const value = Number(condition.value).toFixed(2);
  switch (condition.type) {
    case 'price_above':
      return t('automation.price.above', { symbol, price: `$${value}` });
    case 'price_below':
      return t('automation.price.below', { symbol, price: `$${value}` });
    case 'pct_change_above':
    case 'pct_change_below':
      return t(condition.reference === 'day_open' ? 'automation.price.moveFromOpen' : 'automation.price.moveFromClose', {
        symbol,
        arrow: condition.type === 'pct_change_above' ? '\u2191' : '\u2193',
        pct: value,
      });
    default:
      return t('automation.price.symbolAlert', { symbol });
  }
}

export function formatRetriggerMode(triggerConfig: PriceTriggerConfig | null | undefined, t: TFunction): string {
  const retrigger = triggerConfig?.retrigger;
  if (retrigger?.mode !== 'recurring') return t('automation.retriggerOneShot');
  if (!retrigger.cooldown_seconds) return t('automation.price.recurringDaily');
  const hours = Math.round(retrigger.cooldown_seconds / 3600);
  return hours > 0 ? t('automation.price.recurringEvery', { hours }) : t('automation.retriggerRecurring');
}

// ── Live distance to the trigger ───────────────────────────

/** The indices a price trigger can watch, by the bare names the server
 *  stores (its `_INDEX_SYMBOLS`). */
export const INDEX_SYMBOLS: ReadonlyArray<{ symbol: string; name: string }> = [
  { symbol: 'SPX', name: 'S&P 500' },
  { symbol: 'DJI', name: 'Dow Jones Industrial Average' },
  { symbol: 'COMP', name: 'Nasdaq Composite' },
  { symbol: 'NDX', name: 'Nasdaq 100' },
  { symbol: 'RUT', name: 'Russell 2000' },
  { symbol: 'VIX', name: 'CBOE Volatility Index' },
];

/** Index quotes come back keyed by the provider's legacy spelling, which is
 *  not always the one the automation stores. */
const INDEX_QUOTE_SYMBOL: Record<string, string> = { SPX: 'GSPC', COMP: 'IXIC' };
const INDEX_NAMES = new Set([...INDEX_SYMBOLS.map((i) => i.symbol), ...Object.values(INDEX_QUOTE_SYMBOL)]);

/** A symbol as the server stores it: upper case, with no `^` or `I:` prefix.
 *  The server refuses either prefix rather than strip it, so the form strips
 *  it before sending. */
export function normalizeSymbol(symbol: string): string {
  return symbol.trim().toUpperCase().replace(/^(\^|I:)/, '');
}

/** What `PriceTriggerConfig.symbol` accepts once normalized: one to ten
 *  characters, and a ticker has no spaces. */
export function isValidSymbol(symbol: string): boolean {
  return /^\S{1,10}$/.test(normalizeSymbol(symbol));
}

export function isIndexSymbol(symbol: string): boolean {
  return INDEX_NAMES.has(normalizeSymbol(symbol));
}

export interface WatchedQuote {
  /** The spelling to ask the quote layer for. */
  symbol: string;
  isIndex: boolean;
}

export function watchedQuote(cfg: PriceTriggerConfig | null | undefined): WatchedQuote | null {
  if (!isPriceTriggerConfig(cfg)) return null;
  const symbol = normalizeSymbol(cfg.symbol);
  if (!symbol) return null;
  const isIndex = cfg.market === 'index' || INDEX_NAMES.has(symbol);
  return { symbol: isIndex ? (INDEX_QUOTE_SYMBOL[symbol] ?? symbol) : symbol, isIndex };
}

export interface QuoteLike {
  price?: number | null;
  previous_close?: number | null;
  open?: number | null;
}

/**
 * How far the price has moved from the point a condition measures against:
 * the day open for a move-from-open, else the previous close, which for a
 * price level is simply the day's change.
 */
export function quoteMove(
  cfg: PriceTriggerConfig,
  quote: QuoteLike | undefined,
): { pct: number; from: 'previous_close' | 'day_open' } | null {
  const price = quote?.price;
  if (price == null || !(price > 0)) return null;
  const condition = cfg.conditions[0];
  const fromOpen = condition?.reference === 'day_open' && isPctCondition(condition.type);
  const ref = fromOpen ? quote?.open : quote?.previous_close;
  if (ref == null || !(ref > 0)) return null;
  return { pct: ((price - ref) / ref) * 100, from: fromOpen ? 'day_open' : 'previous_close' };
}

/**
 * Where the watched value stands against the first condition, in the
 * condition's own unit: dollars for a price level, percentage points for a
 * move. `remaining` is how far it still has to travel; zero or less means the
 * condition holds now. Mirrors the monitor's comparison (a move-down threshold
 * is stored as a positive magnitude and compared against its negation).
 */
export interface MeterReading {
  unit: 'price' | 'pct';
  current: number;
  threshold: number;
  remaining: number;
  /** Remaining distance relative to the current price, for a price level. */
  remainingPct: number | null;
}

export function meterReading(
  cfg: PriceTriggerConfig | null | undefined,
  quote: QuoteLike | undefined,
): MeterReading | null {
  if (!isPriceTriggerConfig(cfg)) return null;
  const condition = cfg.conditions[0];
  const price = quote?.price;
  if (!condition || price == null || !(price > 0)) return null;
  const value = Number(condition.value);
  if (!Number.isFinite(value)) return null;

  switch (condition.type) {
    case 'price_above':
    case 'price_below': {
      const remaining = condition.type === 'price_above' ? value - price : price - value;
      return { unit: 'price', current: price, threshold: value, remaining, remainingPct: (remaining / price) * 100 };
    }
    case 'pct_change_above':
    case 'pct_change_below': {
      const move = quoteMove(cfg, quote);
      if (!move) return null;
      const threshold = condition.type === 'pct_change_above' ? value : -value;
      const remaining = condition.type === 'pct_change_above' ? threshold - move.pct : move.pct - threshold;
      return { unit: 'pct', current: move.pct, threshold, remaining, remainingPct: null };
    }
    default:
      return null;
  }
}

/** The words for how far a watched value still has to go. */
export function distanceLabel(reading: MeterReading | null, t: TFunction, locale: string): string {
  if (!reading) return '';
  if (reading.remaining <= 0) return t('automation.conditionMet');
  if (reading.unit === 'price' && reading.remainingPct != null) {
    return t('automation.awayPct', { value: fixed2(reading.remainingPct, locale) });
  }
  return t('automation.awayPts', { value: fixed2(reading.remaining, locale) });
}
