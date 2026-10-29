/**
 * Dual-name resolution for listings that carry both a local-market and an
 * English name (currently CN/HK; the shape is market-agnostic). Primary is
 * local-first — 腾讯控股 before Tencent Holdings — and the secondary renders
 * only when it adds information.
 */
import type { StockSearchHit } from '@/lib/marketUtils';

export interface DualNameInput {
  name?: string | null;
  nameLocal?: string | null;
  nameEn?: string | null;
}

export interface DualName {
  /** The name to lead with, or the fallback (usually the ticker) when nothing names the listing. */
  primary: string;
  secondary: string | null;
  /** False when `primary` is only the fallback, so a reader showing the ticker beside it prints it once. */
  named: boolean;
}

export function resolveDualName(input: DualNameInput, fallback = ''): DualName {
  // A name that only repeats the ticker names nothing.
  const clean = (n: string | null | undefined): string | null => {
    const v = n?.trim();
    return v && v.toUpperCase() !== fallback.trim().toUpperCase() ? v : null;
  };
  const name = clean(input.name);
  const nameLocal = clean(input.nameLocal);
  const nameEn = clean(input.nameEn);
  const primary = nameLocal ?? name ?? nameEn;
  const secondary = [nameEn, name].find((n) => n && n !== primary) ?? null;
  return primary ? { primary, secondary, named: true } : { primary: fallback, secondary: null, named: false };
}

/** What a picked search result tells a header before the symbol's own quote
 *  lands: both spellings of the name, and the venue. */
export interface SymbolDisplayOverride {
  name: string | null;
  nameLocal?: string;
  nameEn?: string;
  exchange: string;
}

export function displayOverrideFromHit(hit: StockSearchHit): SymbolDisplayOverride {
  return {
    name: hit.name || null,
    nameLocal: hit.nameLocal,
    nameEn: hit.nameEn,
    exchange: hit.exchangeShortName || hit.stockExchange || '',
  };
}
