/**
 * The lines beside the menu bar icon. They show while the panel is closed, so
 * unlike the panel they are worked out whether or not the page is on screen:
 * when the settings change, and whenever the shell asks (`onTrayRefresh`),
 * which it does every 15 seconds while a US session trades and every five
 * minutes when none does. A hidden page's own timers run once a minute at
 * best; the shell's asks are not throttled. The reads go through the same
 * quote, holdings and status entries the panel reads, so a fresh read there
 * spares one here.
 */
import { useEffect, useRef } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { portfolioHoldingsQuery } from '@/hooks/membershipQueries';
import { useLocale } from '@/hooks/useLocale';
import { canSetTrayItems, onTrayRefresh, setTrayItems, type MenubarItem } from '@/lib/desktop';
import { DEFAULT_MENUBAR, itemKey, useDesktopSettings } from '@/lib/desktopSettings';
import { indexName } from '@/lib/indices';
import { marketPhase, marketStatusQuery, type MarketStatusData } from '@/lib/marketUtils';
import { quoteKey, quoteQueryOptions, stockKey, type QuoteRow } from '@/lib/quotes';
import { summarizePortfolioDay } from './glanceSummary';
import { splitQuote } from './quoteSplit';
import { formatTrayLine, trayRefreshMs, type TrayFigure } from './trayText';
import { holdingQuotes } from './useGlanceData';

/** Under the shell's quickest ask, so each one reads afresh and a panel read in between counts. */
const FRESH_MS = 10_000;
/** Where nothing asks: no line known yet to keep asking for. */
const FALLBACK_EVERY_MS = 60_000;

/**
 * A fresh read, or the last row when the refresh failed: the panel keeps that
 * row on screen, and a line left without it would sum the portfolio over the
 * holdings that happened to answer. With no row to fall back on the failure
 * stands, so the line keeps what it said rather than a partial sum.
 */
async function readQuote(qc: QueryClient, symbol: string, isIndex: boolean): Promise<QuoteRow | undefined> {
  const key = quoteKey(symbol, isIndex);
  if (!key) return undefined;
  const options = quoteQueryOptions(qc, key, { isIndex, staleTime: FRESH_MS });
  const row = await qc.fetchQuery(options).catch((err: unknown) => {
    const cached = qc.getQueryData(options.queryKey);
    if (cached === undefined) throw err;
    return cached;
  });
  return row ?? undefined;
}

async function figureFor(
  qc: QueryClient,
  item: MenubarItem,
  status: MarketStatusData | null,
  labels: { portfolio: string; index: (symbol: string) => string | null },
): Promise<TrayFigure | null> {
  if (item.kind === 'portfolio') {
    const holdings = await qc.fetchQuery({ ...portfolioHoldingsQuery, staleTime: FRESH_MS });
    const quotes: Record<string, QuoteRow | undefined> = {};
    await Promise.all(
      holdings.map(async (h) => {
        const symbol = stockKey(h.symbol);
        if (symbol) quotes[symbol] = await readQuote(qc, symbol, false);
      }),
    );
    const day = summarizePortfolioDay(holdingQuotes(holdings, quotes, status));
    if (!day) return null;
    return {
      label: labels.portfolio,
      price: null,
      pct: day.dayChangePct,
      ext: day.ext ? { label: day.ext.label, price: null, pct: day.ext.pct } : null,
      percentOnly: true,
    };
  }
  const isIndex = item.kind === 'index';
  const key = quoteKey(item.symbol, isIndex);
  const q = splitQuote(key, await readQuote(qc, item.symbol, isIndex), status);
  if (!q.quoteAvailable) return null;
  return {
    label: (isIndex && labels.index(key)) || key,
    price: q.price,
    pct: q.pct,
    ext: q.ext,
    places: q.places,
  };
}

export function useTrayItems(): void {
  const queryClient = useQueryClient();
  const { t } = useTranslation();
  const locale = useLocale();
  const { view, failed } = useDesktopSettings();
  const menubar = view?.menubar ?? (failed ? DEFAULT_MENUBAR : null);
  // A line whose read failed keeps what it last said rather than vanishing for a beat.
  const lastText = useRef(new Map<string, string>());

  const itemsKey = menubar && JSON.stringify(menubar.items);
  const show = menubar?.show;
  const extendedHours = menubar?.extendedHours;

  useEffect(() => {
    // Nothing to work out for a shell that cannot show it, or before it says what to show.
    if (itemsKey == null || show == null || extendedHours == null || !canSetTrayItems()) return;
    const items = JSON.parse(itemsKey) as MenubarItem[];
    const labels = { portfolio: t('menubar.tray.portfolio'), index: (symbol: string) => indexName(symbol, 'short', t) };
    const remembered = lastText.current;
    for (const id of [...remembered.keys()]) if (!items.some((i) => itemKey(i) === id)) remembered.delete(id);

    if (items.length === 0) {
      setTrayItems([], { refreshMs: trayRefreshMs('closed') });
      return;
    }

    let live = true;
    let retry: ReturnType<typeof setTimeout> | null = null;
    let sent = '';
    // The shell can ask again while a slow read is out; only the newest ask
    // may write, so an older one ending late never puts back older lines.
    let latest = 0;
    const update = async () => {
      const run = ++latest;
      if (retry) clearTimeout(retry);
      retry = null;
      // Null only when the read was cancelled; the lines then go without the session.
      const status = await queryClient.fetchQuery({ ...marketStatusQuery(), staleTime: FRESH_MS }).catch(() => null);
      const reads = await Promise.all(
        items.map(async (item) => {
          try {
            const figure = await figureFor(queryClient, item, status, labels);
            return { id: itemKey(item), read: true, text: figure ? formatTrayLine(figure, { show, extendedHours }, locale) : null };
          } catch {
            return { id: itemKey(item), read: false, text: null };
          }
        }),
      );
      if (!live || run !== latest) return;
      const lines = reads.map(({ id, read, text }) => {
        // Offline or signed out mid-read: keep what the line said until a read lands.
        if (!read) return remembered.get(id) ?? null;
        if (text) remembered.set(id, text);
        else remembered.delete(id);
        return text;
      });
      const texts = lines.filter((l): l is string => !!l).map((text) => ({ text }));
      const options = { refreshMs: trayRefreshMs(marketPhase(status)) };
      const payload = JSON.stringify([texts, options]);
      if (payload !== sent) {
        sent = payload;
        setTrayItems(texts, options);
      }
      // The shell stops asking once it has no line.
      if (texts.length === 0) retry = setTimeout(() => void update(), FALLBACK_EVERY_MS);
    };
    void update();
    const off = onTrayRefresh(() => void update());
    return () => {
      live = false;
      off();
      if (retry) clearTimeout(retry);
    };
  }, [itemsKey, show, extendedHours, locale, t, queryClient]);
}
