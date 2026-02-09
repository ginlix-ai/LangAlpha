import { useEffect } from 'react';

/**
 * Binary search to find the nearest chart bar time for a given date string.
 * Returns the closest time that exists in chartData.
 */
function snapToNearestBar(chartData, dateStr) {
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

/**
 * Manages series markers on the candlestick series.
 * Combines earnings surprises and analyst grade changes into markers.
 */
export function useChartOverlays(candlestickSeriesRef, chartData, earningsData, overlayData, overlayVisibility, symbol) {
  useEffect(() => {
    const series = candlestickSeriesRef.current;
    if (!series || !chartData || chartData.length === 0) {
      if (series) {
        try { series.setMarkers([]); } catch (_) { /* series may be disposed */ }
      }
      return;
    }

    const markers = [];

    // Earnings markers
    if (overlayVisibility?.earnings && earningsData && Array.isArray(earningsData)) {
      earningsData.forEach((e) => {
        const date = e.date || e.fiscalDateEnding;
        if (!date) return;
        const time = snapToNearestBar(chartData, date);
        if (!time) return;

        const isBeat = e.actualEarningResult != null && e.estimatedEarning != null
          ? e.actualEarningResult >= e.estimatedEarning
          : true;

        markers.push({
          time,
          position: isBeat ? 'belowBar' : 'aboveBar',
          shape: isBeat ? 'arrowUp' : 'arrowDown',
          color: isBeat ? '#10b981' : '#ef4444',
          text: 'E',
        });
      });
    }

    // Grade change markers
    if (overlayVisibility?.grades && overlayData?.grades && Array.isArray(overlayData.grades)) {
      overlayData.grades.forEach((g) => {
        const date = g.date;
        if (!date) return;
        const time = snapToNearestBar(chartData, date);
        if (!time) return;

        const isUpgrade = g.action === 'upgrade' || g.action === 'Upgrade';
        markers.push({
          time,
          position: isUpgrade ? 'belowBar' : 'aboveBar',
          shape: isUpgrade ? 'arrowUp' : 'arrowDown',
          color: isUpgrade ? '#22d3ee' : '#f87171',
          text: isUpgrade ? '\u2191' : '\u2193',
        });
      });
    }

    // Sort markers by time (required by lightweight-charts)
    markers.sort((a, b) => a.time - b.time);

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
  }, [candlestickSeriesRef, chartData, earningsData, overlayData, overlayVisibility, symbol]);
}
