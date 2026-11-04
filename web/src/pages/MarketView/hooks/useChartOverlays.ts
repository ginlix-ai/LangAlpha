import { useEffect, useMemo, useRef, type RefObject } from 'react';
import { createSeriesMarkers } from 'lightweight-charts';
import type { ISeriesApi, ISeriesMarkersPluginApi, SeriesMarker, Time } from 'lightweight-charts';
import type { ChartDataPoint } from '@/types/market';
import { getChartTheme } from '../utils/chartConstants';

interface EarningsEntry {
  date?: string;
  fiscalDateEnding?: string;
  epsActual?: number | null;
  epsEstimate?: number | null;
  [key: string]: unknown;
}

interface GradeEntry {
  date?: string;
  action?: string;
  [key: string]: unknown;
}

interface OverlayData {
  grades?: GradeEntry[];
  [key: string]: unknown;
}

interface OverlayVisibility {
  earnings?: boolean;
  grades?: boolean;
  [key: string]: boolean | undefined;
}

const DAY_SECONDS = 86400;

/**
 * Where an event date lands on a series: the first bar at or after the date's
 * midnight, or null for no marker. Bar times are the venue's wall clock
 * written as UTC seconds, the same encoding new Date('YYYY-MM-DD') gives, so
 * a weekend date lands on the next session's first bar. A date before the
 * loaded range or past the last bar gets no marker.
 */
function barSnapper(chartData: ChartDataPoint[]): (dateStr: string) => number | null {
  const n = chartData.length;
  const first = n ? Math.floor(chartData[0].time / DAY_SECONDS) * DAY_SECONDS : 0;
  return (dateStr) => {
    const target = Math.floor(new Date(dateStr).getTime() / 1000);
    if (n === 0 || isNaN(target) || target < first) return null;
    let lo = 0;
    let hi = n;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (chartData[mid].time < target) lo = mid + 1;
      else hi = mid;
    }
    return lo < n ? chartData[lo].time : null;
  };
}

type OverlayMarker = SeriesMarker<Time>;

const VALID_MARKER_SHAPES: ReadonlySet<string> = new Set([
  'arrowUp',
  'arrowDown',
  'circle',
  'square',
]);

/**
 * Manages series markers on the candlestick series.
 * Combines earnings surprises, analyst grade changes, and caller-supplied
 * agent markers into one markers plugin (``setMarkers`` replaces the full
 * list each call, so all sources must merge here). The plugin is created
 * once per series and reused, since each ``createSeriesMarkers`` call would
 * attach another primitive to the series.
 */
export function useChartOverlays(
  candlestickSeriesRef: RefObject<ISeriesApi<'Candlestick'> | null>,
  chartData: ChartDataPoint[] | null,
  earningsData: EarningsEntry[] | null,
  overlayData: OverlayData | null,
  overlayVisibility: OverlayVisibility | null,
  symbol: string | null,
  extraMarkers: OverlayMarker[] = [],
  theme: 'dark' | 'light' = 'dark'
): void {
  const pluginRef = useRef<{ series: ISeriesApi<'Candlestick'>; plugin: ISeriesMarkersPluginApi<Time> } | null>(null);
  const snapToBar = useMemo(() => barSnapper(chartData ?? []), [chartData]);

  useEffect(() => {
    const series = candlestickSeriesRef.current;
    const setMarkers = (markers: OverlayMarker[]): void => {
      if (!series) return;
      try {
        if (pluginRef.current?.series !== series) {
          pluginRef.current = { series, plugin: createSeriesMarkers(series) };
        }
        pluginRef.current.plugin.setMarkers(markers);
      } catch (_) {
        /* series may be disposed */
      }
    };
    if (!series || !chartData || chartData.length === 0) {
      setMarkers([]);
      return;
    }

    const markers: OverlayMarker[] = [];
    // Themed profit/loss pair — canvas markers can't read CSS variables, so
    // they go through the resolved chart theme, never hex literals.
    const { upColor, downColor, text: neutralColor } = getChartTheme(theme);

    // Earnings markers
    if (overlayVisibility?.earnings && earningsData && Array.isArray(earningsData)) {
      earningsData.forEach((e: EarningsEntry) => {
        const date = e.date || e.fiscalDateEnding;
        if (!date) return;
        const time = snapToBar(date);
        if (!time) return;

        if (e.epsActual == null || e.epsEstimate == null) {
          markers.push({ time: time as Time, position: 'aboveBar', shape: 'circle', color: neutralColor, text: 'E' });
          return;
        }

        const isBeat = e.epsActual >= e.epsEstimate;
        markers.push({
          time: time as Time,
          position: isBeat ? 'belowBar' : 'aboveBar',
          shape: isBeat ? 'arrowUp' : 'arrowDown',
          color: isBeat ? upColor : downColor,
          text: 'E',
        });
      });
    }

    // Grade change markers
    if (overlayVisibility?.grades && overlayData?.grades && Array.isArray(overlayData.grades)) {
      overlayData.grades.forEach((g: GradeEntry) => {
        const date = g.date;
        if (!date) return;
        const time = snapToBar(date);
        if (!time) return;

        const isUpgrade = g.action === 'upgrade' || g.action === 'Upgrade';
        markers.push({
          time: time as Time,
          position: isUpgrade ? 'belowBar' : 'aboveBar',
          shape: isUpgrade ? 'arrowUp' : 'arrowDown',
          color: isUpgrade ? upColor : downColor,
          text: isUpgrade ? '\u2191' : '\u2193',
        });
      });
    }

    // Merge caller-supplied agent markers
    if (extraMarkers && extraMarkers.length > 0) {
      markers.push(...extraMarkers);
    }

    // Drop any marker without a valid shape/time. setMarkers replaces the whole
    // list and throws on a malformed entry, so one bad agent marker would
    // otherwise blank every marker here — earnings and grades included.
    const safeMarkers = markers.filter(
      (m) => VALID_MARKER_SHAPES.has(m.shape) && Number.isFinite(m.time as number),
    );

    // Sort markers by time (required by lightweight-charts)
    safeMarkers.sort((a, b) => (a.time as number) - (b.time as number));

    setMarkers(safeMarkers);

    return () => { setMarkers([]); };
  }, [candlestickSeriesRef, chartData, snapToBar, earningsData, overlayData, overlayVisibility, symbol, extraMarkers, theme]);
}
