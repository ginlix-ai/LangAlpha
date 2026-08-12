/**
 * Thin protocol client for the CMDP progressive bars endpoint.
 *
 * `fetchBarsSeries` hits `GET /api/v1/market-data/bars/{instrument}` and returns
 * the raw series envelope. A 404 (endpoint not deployed yet) or a network error
 * (no HTTP response) throws {@link BarsNotAvailableError} so callers can fall
 * back to the legacy full-fetch path; any other HTTP error propagates untouched.
 */
import { api } from '@/api/client';
import { utcMsToChartSec } from '@/lib/utils';

import type {
  BarsCache,
  BarsResponse,
  ChartBar,
  LoaderMeta,
  SchemaId,
  SeriesHeader,
  SeriesRecord,
} from './marketProtocol';

/** Thrown when the protocol endpoint is unreachable (404 / network) — the
 *  signal for callers to fall back to the legacy loader. */
export class BarsNotAvailableError extends Error {
  readonly instrument: string;
  readonly schema: string;
  readonly status?: number;

  constructor(instrument: string, schema: string, status?: number) {
    super(`bars endpoint unavailable for ${instrument} (${schema})${status ? ` [${status}]` : ''}`);
    this.name = 'BarsNotAvailableError';
    this.instrument = instrument;
    this.schema = schema;
    this.status = status;
  }
}

export interface FetchBarsOptions {
  /** Epoch-ms high-water mark — returns only records newer than this (delta poll). */
  after?: number;
  /**
   * Opaque pagination cursor for older pages. No caller sets it yet — staged for
   * scroll-back pagination (paging older history), not dead.
   */
  before?: string;
  /** e.g. `'index'` — routes indices to the correct provider. */
  assetClass?: string;
  signal?: AbortSignal;
}

function isAbort(err: unknown): boolean {
  return err instanceof Error && (err.name === 'CanceledError' || err.name === 'AbortError');
}

export async function fetchBarsSeries(
  instrument: string,
  schema: SchemaId | string,
  opts: FetchBarsOptions = {},
): Promise<BarsResponse> {
  const { after, before, assetClass, signal } = opts;
  const params: Record<string, string> = { schema: String(schema) };
  if (after != null) params.after = String(after);
  if (before != null) params.before = before;
  if (assetClass) params.asset_class = assetClass;

  try {
    const { data } = await api.get(
      `/api/v1/market-data/bars/${encodeURIComponent(instrument)}`,
      { params, signal },
    );
    if (!data || typeof data !== 'object' || !data.series || !Array.isArray(data.series.records)) {
      // Malformed / empty body — treat as unavailable so the caller falls back.
      throw new BarsNotAvailableError(instrument, String(schema));
    }
    return data as BarsResponse;
  } catch (err) {
    if (err instanceof BarsNotAvailableError) throw err;
    if (isAbort(err)) throw err;
    const status = (err as { response?: { status?: number } })?.response?.status;
    // 404 (not deployed) or a transport failure (no response) → fall back.
    if (status === 404 || status == null) {
      throw new BarsNotAvailableError(instrument, String(schema), status);
    }
    throw err;
  }
}

/** A raw OHLCV row from either the protocol series or the legacy REST envelope.
 *  Numerics arrive as numbers or numeric strings; time is epoch-ms. */
export interface RawBarRow {
  time?: number | null;
  ts_event?: number | null;
  open?: number | string | null;
  high?: number | string | null;
  low?: number | string | null;
  close?: number | string | null;
  volume?: number | string | null;
  [key: string]: unknown;
}

/**
 * Coerce a raw watermark (number | numeric-string | anything else) to epoch-ms
 * or null. Shared by the protocol header and legacy envelope meta parsers.
 */
export function coerceWatermark(raw: unknown): number | null {
  const wm = typeof raw === 'number'
    ? raw
    : (typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : null);
  return wm != null && !Number.isNaN(wm) ? wm : null;
}

/**
 * Map raw OHLCV rows → the chart's native bar shape: epoch-ms → market-local
 * chart seconds (venue wall clock encoded as fake UTC), coerce numerics
 * (missing → 0), drop zero-time rows, sort + dedupe by time. Shared by the
 * protocol loader ({@link toChartBars}) and the legacy REST loader so both
 * pipelines normalize identically. OHLC are `Number(x) || 0` so they are never
 * NaN — only the `time > 0` guard filters.
 *
 * `tz` is deliberately required: every conversion for a symbol must use the
 * same venue timezone (`timezoneForSymbol`) or merge-by-time breaks silently.
 */
export function rowsToChartBars(rows: ReadonlyArray<RawBarRow> | undefined | null, tz: string): ChartBar[] {
  if (!Array.isArray(rows)) return [];
  return rows
    .map((r) => ({
      time: utcMsToChartSec(r.time ?? r.ts_event, tz),
      open: Number(r.open) || 0,
      high: Number(r.high) || 0,
      low: Number(r.low) || 0,
      close: Number(r.close) || 0,
      volume: Number(r.volume) || 0,
    }))
    .filter((b) => b.time > 0)
    .sort((a, b) => a.time - b.time)
    .filter((b, i, arr) => i === 0 || b.time !== arr[i - 1].time);
}

/**
 * Map protocol records to the chart's native bar shape. See {@link rowsToChartBars}.
 */
export function toChartBars(records: SeriesRecord[] | undefined | null, tz: string): ChartBar[] {
  return rowsToChartBars(records, tz);
}

/** Normalize a series header (+ cache block) into the shared {@link LoaderMeta}. */
export function headerToMeta(
  header: SeriesHeader | undefined | null,
  cache?: BarsCache | null,
): LoaderMeta {
  return {
    watermark: coerceWatermark(header?.watermark),
    complete: true,
    marketPhase: (header?.market_phase as string) ?? cache?.market_phase ?? null,
    nextChangeAt: typeof cache?.next_change_at === 'number' ? cache.next_change_at : null,
    currency: (header?.price_currency as string) || undefined,
    displayDecimals: typeof header?.display_decimals === 'number' ? header.display_decimals : undefined,
    revision: typeof header?.revision === 'number' ? header.revision : undefined,
    cached: cache?.cached,
  };
}
