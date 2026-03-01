import { useEffect, useRef } from 'react';
import { LineStyle } from 'lightweight-charts';

/**
 * Manages horizontal price lines on the candlestick series.
 * Draws 52-week high/low, day high/low, previous close, and analyst price targets.
 */
export function useChartAnnotations(candlestickSeriesRef, stockMeta, quoteData, priceTargets, annotationsVisible, symbol) {
  const priceLinesRef = useRef([]);

  useEffect(() => {
    // Clear all existing price lines
    const series = candlestickSeriesRef.current;
    if (series) {
      priceLinesRef.current.forEach((line) => {
        try { series.removePriceLine(line); } catch (_) { /* already removed */ }
      });
    }
    priceLinesRef.current = [];

    if (!annotationsVisible || !series) return;

    const lines = [];

    const addLine = (price, title, color, lineStyle) => {
      if (price == null || isNaN(price)) return;
      const line = series.createPriceLine({
        price,
        title,
        color,
        lineWidth: 1,
        lineStyle,
        axisLabelVisible: true,
        lineVisible: true,
      });
      lines.push(line);
    };

    // 52-week high/low from stock meta
    if (stockMeta) {
      addLine(stockMeta.fiftyTwoWeekHigh, '52W High', 'rgba(16,185,129,0.5)', LineStyle.Dashed);
      addLine(stockMeta.fiftyTwoWeekLow, '52W Low', 'rgba(239,68,68,0.5)', LineStyle.Dashed);
    }

    // Day range + previous close from quote data
    if (quoteData) {
      addLine(quoteData.dayHigh, 'Day High', 'rgba(34,211,238,0.4)', LineStyle.Dotted);
      addLine(quoteData.dayLow, 'Day Low', 'rgba(251,191,36,0.4)', LineStyle.Dotted);
      addLine(quoteData.previousClose, 'Prev Close', 'rgba(139,143,163,0.5)', LineStyle.LargeDashed);
    }

    // Price targets from analyst data
    if (priceTargets) {
      addLine(priceTargets.targetHigh, 'PT High', 'rgba(34,211,238,0.4)', LineStyle.Dotted);
      addLine(priceTargets.targetLow, 'PT Low', 'rgba(251,191,36,0.4)', LineStyle.Dotted);
      addLine(priceTargets.targetConsensus, 'PT Consensus', 'rgba(168,85,247,0.5)', LineStyle.Dashed);
    }

    priceLinesRef.current = lines;

    return () => {
      if (series) {
        lines.forEach((line) => {
          try { series.removePriceLine(line); } catch (_) { /* already removed */ }
        });
      }
    };
  }, [candlestickSeriesRef, stockMeta, quoteData, priceTargets, annotationsVisible, symbol]);
}
