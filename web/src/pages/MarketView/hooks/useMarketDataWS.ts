/**
 * useMarketDataWS — WebSocket hook for real-time market data from ginlix-data
 * via the langalpha proxy endpoint.
 *
 * Returns:
 *   prices          Map<symbol, PriceUpdate>
 *   connectionStatus  'connecting' | 'connected' | 'disconnected' | 'reconnecting' | 'disabled'
 *   subscribe(symbols)    subscribe to symbol aggregates
 *   unsubscribe(symbols)  unsubscribe from symbol aggregates
 */
import { useState, useRef, useCallback, useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { getMarketDataWSUrl } from '../utils/api';
import { getAccessToken } from '@/lib/authToken';
import { writeQuoteFromWs } from '@/lib/quotes';
import { stockKey, type QuoteRow } from '@/lib/quotes/quoteBatcher';
import { queryKeys } from '@/lib/queryKeys';
import { timezoneForSymbol, usClassShare } from '@/lib/bars/exchanges';
import { getExtendedHoursType } from '@/lib/bars/marketSession';
import { dateStrInTz, utcMsToChartSec } from '@/lib/utils';
// Reconnect parameters
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;
const BACKOFF_MULTIPLIER = 2;
const STALE_TIMEOUT_MS = 45000; // close if no message in 45s
const HIDDEN_CLOSE_DELAY_MS = 60000; // close 60s after page hidden
// A print older than this is no longer live: the socket stays up on
// keepalives after the close, so its being connected says nothing about data.
const TICK_MAX_AGE_MS = 5 * 60000;
const TICK_SWEEP_MS = 30000;

// The feed spells a US class share as Massive does (BRK.B) whichever spelling
// subscribed to it (BRK-B), so a print belongs to each subscribed spelling.
function subscribedSpellings(subscribed: Map<string, number>, symbol: string): string[] {
  const share = usClassShare(symbol);
  if (!share) return [symbol];
  const spellings = [share.join('.'), share.join('-')].filter((s) => subscribed.has(s));
  return spellings.length > 0 ? spellings : [symbol];
}

export interface BarData {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface PriceUpdate {
  symbol: string;
  price: number;
  /** The day's regular-session open, high and low; null until a source states them. */
  open: number | null;
  high: number | null;
  low: number | null;
  close: number;
  volume: number;
  /** Against the previous close; null when the symbol's quote row states none. */
  change: number | null;
  changePercent: number | null;
  timestamp: number;
  barData: BarData;
}

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'reconnecting' | 'disabled';
export type DataLevel = 'second' | 'minute' | null;

export interface UseMarketDataWSReturn {
  prices: Map<string, PriceUpdate>;
  connectionStatus: ConnectionStatus;
  dataLevel: DataLevel;
  ginlixDataEnabled: boolean;
  subscribe: (symbols: string[]) => void;
  unsubscribe: (symbols: string[]) => void;
}

interface SessionData {
  date: string;
  open: number | null;
  high: number | null;
  low: number | null;
  volume: number;
}

export default function useMarketDataWS(): UseMarketDataWSReturn {
  const queryClient = useQueryClient();
  const [prices, setPrices] = useState<Map<string, PriceUpdate>>(() => new Map());
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>('disconnected');
  const [dataLevel, setDataLevel] = useState<DataLevel>(null);
  const [ginlixDataEnabled, setGinlixDataEnabled] = useState(true); // assume enabled until preflight says otherwise

  const wsRef = useRef<WebSocket | null>(null);
  const subscribedRef = useRef<Map<string, number>>(new Map()); // symbol → refCount
  const backoffRef = useRef(INITIAL_BACKOFF_MS);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const staleTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hiddenTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const intentionalCloseRef = useRef(false);
  const disabledRef = useRef(false);
  const mountedRef = useRef(true);

  // Session OHLCV tracking per symbol (reset on new trading day)
  const sessionDataRef = useRef<Map<string, SessionData>>(new Map());
  // When each symbol's last print arrived, by this clock: the sweep below
  // ages prints out on it, so a skewed client clock cannot.
  const receivedAtRef = useRef<Map<string, number>>(new Map());

  const resetStaleTimer = useCallback((): void => {
    if (staleTimerRef.current) clearTimeout(staleTimerRef.current);
    staleTimerRef.current = setTimeout(() => {
      // No message received in STALE_TIMEOUT_MS — connection likely dead
      console.warn('[WS] stale timer fired — no message in', STALE_TIMEOUT_MS, 'ms');
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.close(4000, 'Stale connection');
      }
    }, STALE_TIMEOUT_MS);
  }, []);

  const processMessage = useCallback((event: MessageEvent) => {
    resetStaleTimer();

    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return; // keepalive or unparseable
    }

    // ginlix-data sends aggregate bars with shape:
    // { ev: "AM", sym: "AAPL", o, h, l, c, v, s, e, ... }
    // or wrapped: { type: "aggregate", symbol, data: { ... } }
    let symbol: string | undefined, open: number, high: number, low: number, close: number, volume: number, timestamp: number;

    if (msg.ev === 'AM' || msg.ev === 'A') {
      // Raw aggregate
      symbol = msg.sym;
      open = msg.o;
      high = msg.h;
      low = msg.l;
      close = msg.c;
      volume = msg.v;
      timestamp = msg.s || msg.e || Date.now();
    } else if (msg.type === 'aggregate' && msg.data) {
      symbol = msg.symbol || msg.data.sym || msg.data.symbol;
      const d = msg.data;
      open = d.open ?? d.o;
      high = d.high ?? d.h;
      low = d.low ?? d.l;
      close = d.close ?? d.c;
      volume = d.volume ?? d.v;
      timestamp = d.time ?? d.timestamp ?? d.s ?? d.e ?? Date.now();
    } else {
      // Unrecognised message (status, keepalive, etc.) — ignore
      // Unrecognised — silently drop
      return;
    }

    if (!symbol || close == null) {
      console.warn('[WS] aggregate missing symbol/close:', { symbol, close });
      return;
    }
    const spellings = subscribedSpellings(subscribedRef.current, symbol);

    // Session OHLCV tracking
    // Timestamp is Unix ms from ginlix-data; convert to chart seconds in the
    // symbol's venue wall clock — MUST match how REST bars are encoded or the
    // chart's merge-by-time breaks. (WS feeds are US-only today, so this is
    // ET in practice.)
    const tsMs = typeof timestamp === 'number' ? timestamp : Date.now();
    const venueTz = timezoneForSymbol(symbol);
    const barTime = utcMsToChartSec(tsMs, venueTz);
    const barDate = dateStrInTz(tsMs, venueTz);
    // The symbol's quote row (REST, kept live by the write-through below) holds
    // the day's figures the socket cannot: the previous close every change is
    // measured against, and the regular session's open, high and low before
    // the first tick. Only a row printed on the tick's venue day speaks for it.
    const rows = spellings
      .map((s) => queryClient.getQueryData<QuoteRow | null>(queryKeys.quote.detail(stockKey(s))))
      .filter((r): r is QuoteRow => r != null);
    const rowToday = rows.find((r) => r.as_of != null && dateStrInTz(r.as_of, venueTz) === barDate) ?? null;
    const row = rowToday ?? rows[0];
    let session = sessionDataRef.current.get(symbol);
    if (!session || session.date !== barDate) {
      session = { date: barDate, open: null, high: null, low: null, volume: 0 };
      sessionDataRef.current.set(symbol, session);
    }
    // Folded in on every print, since the row may land after the first one;
    // its open is the official one, so it replaces a first print's. A zero is
    // a row with no figure yet, not a price.
    if (rowToday) {
      if (rowToday.open) session.open = rowToday.open;
      if (rowToday.high && (session.high == null || rowToday.high > session.high)) session.high = rowToday.high;
      if (rowToday.low && (session.low == null || rowToday.low < session.low)) session.low = rowToday.low;
    }
    // Pre- and after-hours prints move the price but not the day's range.
    if (getExtendedHoursType(barTime) == null) {
      session.open ??= open;
      if (session.high == null || high > session.high) session.high = high;
      if (session.low == null || low < session.low) session.low = low;
    }
    session.volume += volume || 0;

    // A row from an earlier day (a tab left open overnight stops polling the
    // symbol it streams) closed that day at its regular close. Truthiness (not
    // != null): a 0 prevClose would divide to Infinity and poison the shared
    // quote cache via the write-through.
    const prevClose = rowToday ? rowToday.previous_close : row?.regular_close;
    const change = prevClose ? close - prevClose : null;
    const changePercent = prevClose && change != null ? (change / prevClose) * 100 : null;

    const priceUpdate: PriceUpdate = {
      symbol: symbol!,
      price: close,
      open: session.open,
      high: session.high,
      low: session.low,
      close,
      volume: session.volume,
      change,
      changePercent,
      timestamp,
      barData: { time: barTime, open, high, low, close, volume },
    };

    spellings.forEach((s) => receivedAtRef.current.set(s, Date.now()));
    setPrices((prev) => {
      const next = new Map(prev);
      spellings.forEach((s) => next.set(s, { ...priceUpdate, symbol: s }));
      return next;
    });

    // Write-through into the shared quote cache so REST and WS agree. Only the
    // live fields (price/change) are merged; name/previous_close/day-OHLC from
    // the REST snapshot are preserved, and an unwatched symbol is never seeded.
    spellings.forEach((s) => writeQuoteFromWs(queryClient, s, { price: close, change, changePercent }));
  }, [resetStaleTimer, queryClient]);

  /**
   * Preflight check: hit the dedicated HTTP status endpoint to see if the
   * WS proxy feature is enabled on the server.  This avoids the noisy
   * browser console error from a failed WebSocket handshake when the
   * backend has the feature disabled.
   */
  const checkEndpointAvailable = useCallback(async (): Promise<boolean> => {
    try {
      const wsUrl = getMarketDataWSUrl('stock');
      // /ws/v1/market-data/aggregates/stock → /ws/v1/market-data/status
      const statusUrl = wsUrl.replace(/^ws/, 'http').replace(/\/aggregates\/.*$/, '/status');
      const res = await fetch(statusUrl, { method: 'GET', signal: AbortSignal.timeout(5000) });
      if (!res.ok) return false;
      const data = await res.json();
      return data.enabled !== false;
    } catch {
      // Network error / timeout = server down or unreachable
      return false;
    }
  }, []);

  const connect = useCallback(async (): Promise<void> => {
    if (disabledRef.current || !mountedRef.current) return;
    if (wsRef.current && wsRef.current.readyState <= WebSocket.OPEN) return;

    setConnectionStatus('connecting');
    intentionalCloseRef.current = false;

    // On first attempt, probe via HTTP to avoid noisy browser console errors
    if (backoffRef.current === INITIAL_BACKOFF_MS) {
      const available = await checkEndpointAvailable();
      console.info('[WS] preflight check:', available ? 'available' : 'NOT available');
      if (!mountedRef.current) return;
      if (!available) {
        disabledRef.current = true;
        setGinlixDataEnabled(false);
        setConnectionStatus('disabled');
        return;
      }
    }

    // From the shared cache, so this inherits the pre-expiry margin it never had
    // on its own. Without it a tab resuming past expiry presented the same dead
    // token on every reconnect, for as long as the hook lived.
    const token = await getAccessToken();
    if (!mountedRef.current) return;
    const base = getMarketDataWSUrl('stock');
    const sep = base.includes('?') ? '&' : '?';
    const url = token ? `${base}${sep}token=${token}` : base;

    let ws;
    try {
      ws = new WebSocket(url);
    } catch {
      scheduleReconnect();
      return;
    }
    wsRef.current = ws;

    ws.onopen = () => {
      if (!mountedRef.current) { ws.close(); return; }
      console.info('[WS] connected to', url.replace(/token=[^&]+/, 'token=***'));
      setConnectionStatus('connected');
      // Derive data level from the WS URL interval param
      const wsInterval = new URL(base, location.origin).searchParams.get('interval') || 'second';
      setDataLevel(wsInterval === 'second' ? 'second' : 'minute');
      backoffRef.current = INITIAL_BACKOFF_MS;
      resetStaleTimer();

      // Re-subscribe any symbols that were subscribed before reconnect
      if (subscribedRef.current.size > 0) {
        const msg = JSON.stringify({
          action: 'subscribe',
          symbols: [...subscribedRef.current.keys()],
        });
        console.info('[WS] re-subscribing on open:', msg);
        ws.send(msg);
      } else {
        console.info('[WS] no symbols to re-subscribe on open');
      }
    };

    ws.onmessage = processMessage;

    ws.onclose = (event) => {
      console.info('[WS] closed: code=%d reason=%s', event.code, event.reason);
      if (staleTimerRef.current) clearTimeout(staleTimerRef.current);
      wsRef.current = null;
      // A tick from a closed socket is history: the next connection is live
      // only once it delivers one of its own.
      setPrices((prev) => (prev.size ? new Map() : prev));

      if (!mountedRef.current) return;

      // Already marked disabled (e.g. by onerror) — don't override
      if (disabledRef.current) {
        setConnectionStatus('disabled');
        return;
      }

      // Auth failure (1008) -- mark as disabled, don't reconnect.
      //
      // Unreachable against this backend, and left as it was rather than built
      // on: `authenticate_websocket` closes 1008 *before* `accept()`, which
      // uvicorn turns into an HTTP 403 rejection of the upgrade, and a rejected
      // handshake reaches the browser as 1006 with no code of its own. A
      // refused credential therefore lands on the ordinary reconnect path
      // below and backs off to 30s, which is also the only thing it can do:
      // the browser deliberately does not tell a page why a handshake failed,
      // so an auth failure here is indistinguishable from the server being
      // down. Recovery is the cached token above renewing itself.
      if (event.code === 1008) {
        disabledRef.current = true;
        setConnectionStatus('disabled');
        return;
      }

      if (intentionalCloseRef.current) {
        setConnectionStatus('disconnected');
        return;
      }

      setConnectionStatus('reconnecting');
      scheduleReconnect();
    };

    ws.onerror = () => {
      // onerror is always followed by onclose — onclose handles reconnect/disable
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [processMessage, resetStaleTimer, checkEndpointAvailable]);

  const scheduleReconnect = useCallback((): void => {
    if (disabledRef.current || !mountedRef.current) return;
    if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
    const jitter = Math.random() * 500;
    const delay = Math.min(backoffRef.current + jitter, MAX_BACKOFF_MS);
    backoffRef.current = Math.min(backoffRef.current * BACKOFF_MULTIPLIER, MAX_BACKOFF_MS);
    reconnectTimerRef.current = setTimeout(() => {
      reconnectTimerRef.current = null;
      connect();
    }, delay);
  }, [connect]);

  const disconnect = useCallback((): void => {
    intentionalCloseRef.current = true;
    setDataLevel(null);
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    if (staleTimerRef.current) {
      clearTimeout(staleTimerRef.current);
      staleTimerRef.current = null;
    }
    if (wsRef.current) {
      wsRef.current.close(1000, 'Client disconnect');
      wsRef.current = null;
    }
  }, []);

  const subscribe = useCallback((symbols: string[]): void => {
    if (!Array.isArray(symbols) || symbols.length === 0) return;
    const upper = symbols.map((s) => s.toUpperCase());
    const newSymbols: string[] = [];
    upper.forEach((s) => {
      const prev = subscribedRef.current.get(s) || 0;
      subscribedRef.current.set(s, prev + 1);
      if (prev === 0) newSymbols.push(s);
    });

    if (newSymbols.length > 0 && wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      const msg = JSON.stringify({ action: 'subscribe', symbols: newSymbols });
      console.info('[WS] subscribe sent:', msg);
      wsRef.current.send(msg);
    } else if (newSymbols.length > 0) {
      console.warn('[WS] subscribe deferred (ws not open), symbols:', newSymbols, 'readyState:', wsRef.current?.readyState);
    }
  }, []);

  const unsubscribe = useCallback((symbols: string[]): void => {
    if (!Array.isArray(symbols) || symbols.length === 0) return;
    const upper = symbols.map((s) => s.toUpperCase());
    const removedSymbols: string[] = [];
    upper.forEach((s) => {
      const count = subscribedRef.current.get(s) || 0;
      if (count <= 1) {
        subscribedRef.current.delete(s);
        removedSymbols.push(s);
      } else {
        subscribedRef.current.set(s, count - 1);
      }
    });

    if (removedSymbols.length > 0 && wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ action: 'unsubscribe', symbols: removedSymbols }));
    }
  }, []);

  // Auto-connect on mount
  useEffect(() => {
    mountedRef.current = true;
    connect();
    return () => {
      mountedRef.current = false;
      disconnect();
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Drop prints past TICK_MAX_AGE_MS, so a symbol that stopped trading (the
  // close, a halt, a thin name) falls back to its quote row instead of
  // reading Live with its last print.
  useEffect(() => {
    const sweep = setInterval(() => {
      const cutoff = Date.now() - TICK_MAX_AGE_MS;
      setPrices((prev) => {
        const fresh = [...prev].filter(([sym]) => (receivedAtRef.current.get(sym) ?? 0) >= cutoff);
        return fresh.length === prev.size ? prev : new Map(fresh);
      });
    }, TICK_SWEEP_MS);
    return () => clearInterval(sweep);
  }, []);

  // Page visibility: close after 60s hidden, reconnect on visible
  useEffect(() => {
    const handleVisibility = (): void => {
      if (document.hidden) {
        hiddenTimerRef.current = setTimeout(() => {
          disconnect();
        }, HIDDEN_CLOSE_DELAY_MS);
      } else {
        if (hiddenTimerRef.current) {
          clearTimeout(hiddenTimerRef.current);
          hiddenTimerRef.current = null;
        }
        // Reconnect if not connected and not disabled
        if (!disabledRef.current && (!wsRef.current || wsRef.current.readyState > WebSocket.OPEN)) {
          backoffRef.current = INITIAL_BACKOFF_MS * 2;  // skip preflight on visibility reconnect
          connect();
        }
      }
    };

    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      document.removeEventListener('visibilitychange', handleVisibility);
      if (hiddenTimerRef.current) clearTimeout(hiddenTimerRef.current);
    };
  }, [connect, disconnect]);

  return { prices, connectionStatus, dataLevel, ginlixDataEnabled, subscribe, unsubscribe };
}
