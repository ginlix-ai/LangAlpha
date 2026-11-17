import { sessionQuote, type MarketStatusData, type SessionRow } from '@/lib/marketUtils';
import { displaySpelling } from '@/lib/bars';
import { costBasis, summarizePortfolioByCurrency, type CurrencyPortfolioSummary } from '@/pages/Dashboard/utils/portfolioSummary';
import type { PortfolioRow } from '@/pages/Dashboard/hooks/usePortfolioData';

export type QuoteListTab = 'watchlist' | 'holdings';
export type QuoteListSession = 'pre' | 'post';
export type SortKey = 'symbol' | 'price' | 'pct' | 'value' | 'pl';
export interface SortState { key: SortKey; dir: 'asc' | 'desc' }

export interface QuoteListRow {
  /** The watchlist item or holding id, which removing it takes. */
  id: string;
  symbol: string;
  /** The figure the row leads with: the extended print in pre or post market, else the last price. */
  price: number | null;
  /** `price`'s move since the previous close, in every session. */
  pct: number | null;
  /** Which extended session `price` is from, when it is from one. */
  ext: 'pre' | 'post' | null;
  /** The official close and its move, which an extended row gives up its place to. */
  close: { price: number | null; pct: number | null };
  places: number;
  currency: string | null;
  /** A holding's quantity at the price shown, so a pre-market drop shows before the open; null on the watchlist. */
  value: number | null;
  plPct: number | null;
}

/**
 * One column of the table: its heading, the figure it sorts by and how that
 * figure prints. The tab's columns drive the headings, the cells and the sort
 * alike, so a column is added in one place.
 */
export type Column =
  | { key: 'symbol'; label: string; print: 'symbol' }
  | { key: Exclude<SortKey, 'symbol'>; label: string; print: 'price' | 'pct' | 'money'; value: (row: QuoteListRow) => number | null };

const SYMBOL: Column = { key: 'symbol', label: 'marketView.quoteList.col.symbol', print: 'symbol' };
const MOVE = (label: string): Column => ({ key: 'pct', label, print: 'pct', value: (r) => r.pct });

export const COLUMNS: Record<QuoteListTab, readonly Column[]> = {
  watchlist: [
    SYMBOL,
    { key: 'price', label: 'marketView.quoteList.col.last', print: 'price', value: (r) => r.price },
    MOVE('marketView.quoteList.col.chg'),
  ],
  holdings: [
    SYMBOL,
    { key: 'value', label: 'marketView.quoteList.col.value', print: 'money', value: (r) => r.value },
    MOVE('marketView.quoteList.col.today'),
    { key: 'pl', label: 'marketView.quoteList.col.pl', print: 'pct', value: (r) => r.plPct },
  ],
};

/** Two spellings of one listing (`brk.b`, `BRK-B`), as the chart and a saved item may hold it. */
export function sameListing(a: string, b: string): boolean {
  return displaySpelling(a) === displaySpelling(b);
}

/** A socket print laid over a polled quote. */
interface LivePrint {
  price: number;
  changePercent?: number | null;
}

interface SourceRow extends SessionRow {
  symbol: string;
  places: number;
  currency?: string | null;
  quoteAvailable?: boolean;
}

/** A listed symbol as the table shows it, its figure the one the session makes current. */
export function quoteRow(row: SourceRow, id: string, status: MarketStatusData | null, ws: LivePrint | undefined): QuoteListRow {
  const live = ws ? { ...row, price: ws.price, changePercent: ws.changePercent ?? row.changePercent } : row;
  const hasQuote = row.quoteAvailable !== false || ws != null;
  const { headline, ext } = sessionQuote(status, live, { livePrice: ws?.price ?? null, shortLabels: true });
  // A pre-market move is declared against the previous close and an
  // after-hours one against today's, so the latter is re-based: otherwise a
  // holding's Today shows only the evening's move beside a value that holds
  // the whole day's. With no previous close to re-base on, there is no day's
  // move to show.
  const prevClose = ext?.type === 'post' && headline.price != null && headline.change != null ? headline.price - headline.change : null;
  const pct = !ext ? headline.pct
    : ext.type === 'pre' ? ext.pct
      : prevClose ? ((ext.price - prevClose) / prevClose) * 100 : null;
  return {
    id,
    symbol: row.symbol,
    price: hasQuote ? (ext?.price ?? headline.price) : null,
    pct: hasQuote ? pct : null,
    ext: ext?.type ?? null,
    close: { price: hasQuote ? headline.price : null, pct: hasQuote ? headline.pct : null },
    places: row.places,
    currency: row.currency ?? null,
    value: null,
    plPct: null,
  };
}

/** Holdings valued at the price each row shows, with their totals per currency. */
export function valueHoldings(
  holdings: readonly PortfolioRow[],
  toRow: (h: PortfolioRow) => QuoteListRow,
): { rows: QuoteListRow[]; summaries: CurrencyPortfolioSummary[] } {
  const valued = holdings.map((h) => {
    const row = toRow(h);
    const quantity = Number(h.quantity ?? 0);
    const value = row.price != null ? quantity * row.price : null;
    const basis = costBasis({ ...h, quantity });
    const plPct = value != null && basis != null && basis > 0 ? ((value - basis) / basis) * 100 : null;
    return { holding: { ...h, quantity, marketValue: value, quoteAvailable: value != null }, row: { ...row, value, plPct } };
  });
  return {
    rows: valued.map((v) => v.row),
    summaries: summarizePortfolioByCurrency(valued.map((v) => v.holding)),
  };
}

/**
 * The extended session the rows' figures come from, named once above them;
 * none when every row shows a regular-session price. It follows the rows, not
 * the US clock, which neither knows a list of Hong Kong names is trading nor
 * that the evening's after-hours prints still stand overnight.
 */
export function listSession(rows: readonly QuoteListRow[]): QuoteListSession | null {
  if (rows.some((r) => r.ext === 'pre')) return 'pre';
  if (rows.some((r) => r.ext === 'post')) return 'post';
  return null;
}

/**
 * A heading click sorts by that column, a second click flips it, and a third
 * hands the list back to the order the user built it in. Names sort A to Z
 * first, figures largest first.
 */
export function nextSort(cur: SortState | null, key: SortKey): SortState | null {
  const first: SortState['dir'] = key === 'symbol' ? 'asc' : 'desc';
  if (cur?.key !== key) return { key, dir: first };
  if (cur.dir === first) return { key, dir: first === 'asc' ? 'desc' : 'asc' };
  return null;
}

/** Rows in the column's order; a row with no figure goes last whichever way it runs. */
export function sortRows(rows: QuoteListRow[], sort: SortState | null, columns: readonly Column[]): QuoteListRow[] {
  const column = sort && columns.find((c) => c.key === sort.key);
  if (!sort || !column) return rows;
  const sign = sort.dir === 'asc' ? 1 : -1;
  if (column.print === 'symbol') return [...rows].sort((a, b) => sign * a.symbol.localeCompare(b.symbol));
  return [...rows].sort((a, b) => {
    const x = column.value(a);
    const y = column.value(b);
    if (x == null || y == null) return x == null ? (y == null ? 0 : 1) : -1;
    return sign * (x - y);
  });
}
