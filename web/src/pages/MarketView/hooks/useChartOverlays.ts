import { useEffect, type RefObject } from 'react';
import type { ISeriesApi, Time } from 'lightweight-charts';
import type { ChartDataPoint } from '@/types/market';

interface EarningsEntry {
  date?: string;
  fiscalDateEnding?: string;
  actualEarningResult?: number;
  estimatedEarning?: number;
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

/**
 * Binary search to find the nearest chart bar time for a given date string.
 * Returns the closest time that exists in chartData.
 */
function snapToNearestBar(chartData: ChartDataPoint[], dateStr: string): number | null {
  if (!chartData || chartData.length === 0) return null;

  // Convert date string to unix timestamp (seconds)
  const target = Math.floor(new Date(dateStr).getTime() / 1000);
  if (isNaN(target)) return null;

  let lo = 0;
  let hi = chartData.length - 1;

  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (chartData[mid].time < target) lo = mid + 1;
    else hi = mid;
  }

  // Check neighbours for closest match
  if (lo > 0) {
    const diffLo = Math.abs(chartData[lo].time - target);
    const diffPrev = Math.abs(chartData[lo - 1].time - target);
    if (diffPrev < diffLo) lo = lo - 1;
  }

  return chartData[lo].time;
}

type OverlayMarker = {
  time: Time;
  position: 'aboveBar' | 'belowBar' | 'inBar';
  shape: 'arrowUp' | 'arrowDown' | 'circle' | 'square';
  color: string;
  text?: string;
};

/**
 * Manages series markers on the candlestick series.
 * Combines earnings surprises, analyst grade changes, and caller-supplied
 * agent markers into a single ``setMarkers`` call (LWC replaces the full
 * list each call, so all sources must merge here).
 */
export function useChartOverlays(
  candlestickSeriesRef: RefObject<ISeriesApi<'Candlestick'> | null>,
  chartData: ChartDataPoint[] | null,
  earningsData: EarningsEntry[] | null,
  overlayData: OverlayData | null,
  overlayVisibility: OverlayVisibility | null,
  symbol: string | null,
  extraMarkers: OverlayMarker[] = []
): void {
  useEffect(() => {
    const series = candlestickSeriesRef.current;
    if (!series || !chartData || chartData.length === 0) {
      if (series) {
        try { series.setMarkers([]); } catch (_) { /* series may be disposed */ }
      }
      return;
    }

    const markers: OverlayMarker[] = [];

    // Earnings markers
    if (overlayVisibility?.earnings && earningsData && Array.isArray(earningsData)) {
      earningsData.forEach((e: EarningsEntry) => {
        const date = e.date || e.fiscalDateEnding;
        if (!date) return;
        const time = snapToNearestBar(chartData, date);
        if (!time) return;

        const isBeat = e.actualEarningResult != null && e.estimatedEarning != null
          ? e.actualEarningResult >= e.estimatedEarning
          : true;

        markers.push({
          time: time as Time,
          position: isBeat ? 'belowBar' : 'aboveBar',
          shape: isBeat ? 'arrowUp' : 'arrowDown',
          color: isBeat ? '#10b981' : '#ef4444',
          text: 'E',
        });
      });
    }

    // Grade change markers
    if (overlayVisibility?.grades && overlayData?.grades && Array.isArray(overlayData.grades)) {
      overlayData.grades.forEach((g: GradeEntry) => {
        const date = g.date;
        if (!date) return;
        const time = snapToNearestBar(chartData, date);
        if (!time) return;

        const isUpgrade = g.action === 'upgrade' || g.action === 'Upgrade';
        markers.push({
          time: time as Time,
          position: isUpgrade ? 'belowBar' : 'aboveBar',
          shape: isUpgrade ? 'arrowUp' : 'arrowDown',
          color: isUpgrade ? '#22d3ee' : '#f87171',
          text: isUpgrade ? '\u2191' : '\u2193',
        });
      });
    }

    // Merge caller-supplied agent markers
    if (extraMarkers && extraMarkers.length > 0) {
      markers.push(...extraMarkers);
    }

    // Sort markers by time (required by lightweight-charts)
    markers.sort((a, b) => (a.time as number) - (b.time as number));

    try {
      series.setMarkers(markers);
    } catch (_) {
      /* series may be disposed */
    }

    return () => {
      if (series) {
        try { series.setMarkers([]); } catch (_) { /* already cleaned */ }
      }
    };
  }, [candlestickSeriesRef, chartData, earningsData, overlayData, overlayVisibility, symbol, extraMarkers]);
}
