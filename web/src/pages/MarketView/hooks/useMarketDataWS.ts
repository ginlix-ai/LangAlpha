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
import { getMarketDataWSUrl, getWSAuthToken } from '../utils/api';
import { utcMsToChartSec, utcMsToETDate } from '@/lib/utils';
// Reconnect parameters
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;
const BACKOFF_MULTIPLIER = 2;
const STALE_TIMEOUT_MS = 45000; // close if no message in 45s
const HIDDEN_CLOSE_DELAY_MS = 60000; // close 60s after page hidden

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
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  change: number;
  changePercent: number;
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
  setPreviousClose: (symbol: string, price: number) => void;
  setDayOpen: (symbol: string, price: number) => void;
}

interface SessionData {
  date: string;
  open: number;
  high: number;
  low: number;
  volume: number;
}

export default function useMarketDataWS(): UseMarketDataWSReturn {
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

  // Snapshot-seeded reference data for accurate change% calculation
  const previousCloseRef = useRef<Map<string, number>>(new Map()); // symbol → previousClose price
  const dayOpenRef = useRef<Map<string, number>>(new Map()); // symbol → actual market open price

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

    // Session OHLCV tracking
    // Timestamp is Unix ms from ginlix-data; convert to Unix seconds for chart
    const tsMs = typeof timestamp === 'number' ? timestamp : Date.now();
    const barTime = utcMsToChartSec(tsMs);
    const barDate = utcMsToETDate(tsMs);
    let session = sessionDataRef.current.get(symbol);
    if (!session || session.date !== barDate) {
      // New trading day
      session = { date: barDate, open, high, low, volume: 0 };
      sessionDataRef.current.set(symbol, session);
    }
    if (high > session.high) session.high = high;
    if (low < session.low) session.low = low;
    session.volume += volume || 0;

    // Use snapshot previousClose for accurate change%; fall back to session open
    const prevClose = previousCloseRef.current.get(symbol);
    const change = prevClose != null ? (close - prevClose) : (close - session.open);
    const changePercent = prevClose != null
      ? parseFloat(((change / prevClose) * 100).toFixed(2))
      : session.open ? parseFloat((((close - session.open) / session.open) * 100).toFixed(2)) : 0;

    // Use snapshot dayOpen if available (actual market open, not first WS tick)
    const effectiveOpen = dayOpenRef.current.get(symbol) ?? session.open;

    const priceUpdate: PriceUpdate = {
      symbol: symbol!,
      price: Math.round(close * 100) / 100,
      open: effectiveOpen,
      high: session.high,
      low: session.low,
      close,
      volume: session.volume,
      change: Math.round(change * 100) / 100,
      changePercent,
      timestamp,
      barData: { time: barTime, open, high, low, close, volume },
    };

    setPrices((prev) => {
      const next = new Map(prev);
      next.set(symbol, priceUpdate);
      return next;
    });
  }, [resetStaleTimer]);

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

    const token = await getWSAuthToken();
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

      if (!mountedRef.current) return;

      // Already marked disabled (e.g. by onerror) — don't override
      if (disabledRef.current) {
        setConnectionStatus('disabled');
        return;
      }

      // Auth failure (1008) — mark as disabled, don't reconnect
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

  const setPreviousClose = useCallback((symbol: string, price: number): void => {
    if (symbol && price != null) previousCloseRef.current.set(symbol.toUpperCase(), price);
  }, []);

  const setDayOpen = useCallback((symbol: string, price: number): void => {
    if (symbol && price != null) dayOpenRef.current.set(symbol.toUpperCase(), price);
  }, []);

  return { prices, connectionStatus, dataLevel, ginlixDataEnabled, subscribe, unsubscribe, setPreviousClose, setDayOpen };
}
