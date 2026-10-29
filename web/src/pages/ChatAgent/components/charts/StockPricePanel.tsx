import React, { useEffect, useMemo, useRef } from 'react';
import { CandlestickSeries, HistogramSeries, LineSeries } from 'lightweight-charts';
import type { IChartApi, ISeriesApi, LogicalRange, UTCTimestamp } from 'lightweight-charts';
import { useTranslation } from 'react-i18next';
import {
  dedupeMergeByTime,
  fetchStockData,
  formatMoney,
  INTERVAL_LABEL,
  RANGE_CHANGE_DEBOUNCE_MS,
  rangeBeforeOldest,
  SCROLL_CHUNK_DAYS,
  SCROLL_LOAD_THRESHOLD,
  type ChartBar,
} from '@/lib/bars';
import { compactNumberFixed2, formatCalendarDayRange } from '@/lib/format';
import { chartSecToDateStr } from '@/lib/utils';
import { useLocale } from '@/hooks/useLocale';
import { calculateMA } from '@/pages/MarketView/utils/chartHelpers';
import { canvasChartOptions, candleData, candleOptions, mountCanvasChart, useCanvasTheme, type CanvasTheme } from './canvasChart';
import { readStockPrices, type PanelProps, type StockPrices } from './cardReaders';
import { FreshnessBadge } from './FreshnessBadge';
import { MarketLink } from './marketLinks';
import { TermCard, TermChange, TermKV } from './TermCard';
import { TermPanelHead } from './TermPanel';

const PRICE_CHART_HEIGHT = 360;
/** The averages a daily chart draws: the first in ink, as the panel's charts draw a first series. */
const MA_PERIODS = [20, 50] as const;
const MA_ORANGE = '#f59e0b';
const DAY_SEC = 86_400;

/** Candles, volume under them and, on a daily chart, the averages: drawn again whole as bars arrive. */
function priceSeries(chart: IChartApi, ct: CanvasTheme, daily: boolean): (bars: ChartBar[]) => void {
  const candles = chart.addSeries(CandlestickSeries, candleOptions(ct));
  // Volume in the bottom fifth.
  const volume = chart.addSeries(HistogramSeries, { priceFormat: { type: 'volume' }, priceScaleId: 'volume' });
  chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
  const averages: Array<[number, ISeriesApi<'Line'>]> = daily
    ? MA_PERIODS.map((period, i) => [
        period,
        chart.addSeries(LineSeries, { color: i === 0 ? ct.ma : MA_ORANGE, lineWidth: 1, priceLineVisible: false, lastValueVisible: false }),
      ])
    : [];
  return (bars) => {
    candles.setData(candleData(bars));
    volume.setData(bars.map((b, i) => ({
      time: b.time as UTCTimestamp,
      value: b.volume || 0,
      color: i > 0 && b.close >= bars[i - 1].close ? ct.upA : ct.downA,
    })));
    for (const [period, series] of averages) {
      series.setData(calculateMA(bars, period).map((p) => ({ time: p.time as UTCTimestamp, value: p.value })));
    }
  };
}

/**
 * Reaching the first bar loads the window before it, merged in time order. A
 * window that holds nothing older ends it, and a request still out dies with
 * the chart. Only once the reader moves the chart: it opens fitted, its first
 * bar already in view, and that alone asks for nothing. Returns the
 * unsubscribe.
 */
function loadEarlierAtEdge(
  chart: IChartApi,
  prices: Pick<StockPrices, 'symbol' | 'interval'>,
  initial: ChartBar[],
  draw: (bars: ChartBar[]) => void,
): () => void {
  const { symbol, interval } = prices;
  const chunk = SCROLL_CHUNK_DAYS[interval];
  if (!symbol || !chunk) return () => {};
  const loads = new AbortController();
  let bars = initial;
  let fetching = false;
  let exhausted = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const loadEarlier = async () => {
    const oldest = bars[0]?.time;
    if (fetching || exhausted || oldest == null) return;
    fetching = true;
    try {
      const { fromStr, toStr } = rangeBeforeOldest(oldest, chunk);
      const result = await fetchStockData(symbol, interval, fromStr, toStr, { signal: loads.signal });
      if (loads.signal.aborted) return;
      // A daily bar is its day, whatever hour the feed stamps it at.
      const fetched = interval === '1day' ? result.data.map((b) => ({ ...b, time: b.time - (b.time % DAY_SEC) })) : result.data;
      const { merged } = dedupeMergeByTime(bars, fetched);
      if (merged[0].time >= oldest) {
        if (!result.error) exhausted = true;
        return;
      }
      bars = merged;
      draw(bars);
    } catch (err) {
      console.warn('Detail chart scroll-load failed:', err);
    } finally {
      fetching = false;
    }
  };

  let moved = false;
  const onMove = () => {
    moved = true;
  };
  const el = chart.chartElement();
  const moves = ['wheel', 'pointerdown', 'touchstart'] as const;
  for (const type of moves) el.addEventListener(type, onMove, { capture: true, passive: true });

  const onRange = (range: LogicalRange | null) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      if (moved && range && range.from <= SCROLL_LOAD_THRESHOLD) void loadEarlier();
    }, RANGE_CHANGE_DEBOUNCE_MS);
  };
  chart.timeScale().subscribeVisibleLogicalRangeChange(onRange);
  return () => {
    clearTimeout(timer);
    loads.abort();
    for (const type of moves) el.removeEventListener(type, onMove, { capture: true });
    chart.timeScale().unsubscribeVisibleLogicalRangeChange(onRange);
  };
}

export function StockPriceChart({ data }: PanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const ct = useCanvasTheme();
  const containerRef = useRef<HTMLDivElement>(null);
  // Held across renders: the chart is drawn from these, and a new array would redraw it.
  const prices = useMemo(() => readStockPrices(data), [data]);
  const { symbol, interval, chartBars, currency } = prices;
  const daily = interval === '1day';

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !chartBars.length) return;
    const { chart, dispose } = mountCanvasChart(el, PRICE_CHART_HEIGHT, {
      ...canvasChartOptions(ct, !daily),
      handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true },
    });
    const draw = priceSeries(chart, ct, daily);
    draw(chartBars);
    chart.timeScale().fitContent();
    const stopLoading = loadEarlierAtEdge(chart, prices, chartBars, draw);
    return () => {
      stopLoading();
      dispose();
    };
  }, [prices, chartBars, daily, ct]);

  if (!chartBars.length) {
    return <div className="term-t3" style={{ padding: 16 }}>{t('toolArtifact.noPriceData')}</div>;
  }

  // The card's line and dates, a size up: the last daily close and the
  // period's move, as the stats measure them, over the intraday bars drawn.
  const lastBar = chartBars[chartBars.length - 1];
  const firstDay = prices.sent ? prices.sent.first : chartSecToDateStr(chartBars[0].time);
  const lastDay = prices.sent ? prices.sent.last : chartSecToDateStr(lastBar.time);
  const lastClose = prices.sent ? prices.sent.lastClose : lastBar.close;

  return (
    <>
      <TermPanelHead title={symbol ?? ''} aside={<MarketLink symbol={symbol} />} />
      <TermCard
        title={t('toolArtifact.price')}
        sub={INTERVAL_LABEL[interval] ?? interval}
        meta={firstDay && lastDay ? formatCalendarDayRange(firstDay, lastDay, locale) : undefined}
      >
        <div className="term-line" style={{ marginBottom: 10 }}>
          {lastClose != null && <span className="term-big term-hero">{formatMoney(lastClose, currency, locale)}</span>}
          <TermChange value={prices.changePct} />
          {/* The chart below may be drawn from another publisher than the daily close. */}
          {prices.chartFreshness && (
            <FreshnessBadge freshness={prices.chartFreshness} source={prices.chartSource} chartSymbol={symbol ?? ''} />
          )}
          {daily && (
            <span className="term-keys">
              <LineKey color="var(--color-term-text-secondary)">MA20</LineKey>
              <LineKey color={MA_ORANGE}>MA50</LineKey>
            </span>
          )}
        </div>
        <div ref={containerRef} style={{ width: '100%', height: PRICE_CHART_HEIGHT }} />
        <StockStats prices={prices} />
      </TermCard>
    </>
  );
}

/** A line series' key, its stroke drawn: `── MA20`. */
function LineKey({ color, children }: { color: string; children: React.ReactNode }): React.ReactElement {
  return (
    <span className="term-key">
      <span className="term-key-mark" style={{ background: color }} aria-hidden="true" />
      {children}
    </span>
  );
}

/** The period's figures under the chart. Its move is on the line above. */
function StockStats({ prices }: { prices: StockPrices }): React.ReactElement | null {
  const locale = useLocale();
  const { t } = useTranslation();
  const { high, low, avgVolume, volatility, ma20, ma50 } = prices;
  const money = (n: number) => formatMoney(n, prices.currency, locale);

  const pairs = [
    high != null && [t('toolArtifact.periodHigh'), money(high)] as const,
    low != null && [t('toolArtifact.periodLow'), money(low)] as const,
    avgVolume != null && [t('toolArtifact.avgVolume'), compactNumberFixed2(avgVolume, locale)] as const,
    volatility != null && [t('toolArtifact.volatility'), `${(volatility * 100).toFixed(1)}%`] as const,
    ma20 != null && ['MA20', money(ma20)] as const,
    ma50 != null && ['MA50', money(ma50)] as const,
  ].filter((p) => !!p);
  if (!pairs.length) return null;

  return (
    <div style={{ marginTop: 12 }}>
      <TermKV pairs={pairs} cols={3} />
    </div>
  );
}
