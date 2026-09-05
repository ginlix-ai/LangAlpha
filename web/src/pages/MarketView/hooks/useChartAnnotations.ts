import { useEffect, useRef, type RefObject } from 'react';
import { LineStyle, type ISeriesApi, type IPriceLine } from 'lightweight-charts';

interface QuoteData {
  yearHigh?: number;
  yearLow?: number;
  dayHigh?: number;
  dayLow?: number;
  previousClose?: number;
  [key: string]: unknown;
}

interface PriceTargets {
  targetHigh?: number;
  targetLow?: number;
  targetConsensus?: number;
  [key: string]: unknown;
}

/**
 * Manages horizontal price lines on the candlestick series.
 * Draws 52-week high/low, day high/low, previous close, and analyst price targets.
 */
export function useChartAnnotations(
  candlestickSeriesRef: RefObject<ISeriesApi<'Candlestick'> | null>,
  _stockMeta: unknown,
  quoteData: QuoteData | null,
  priceTargets: PriceTargets | null,
  annotationsVisible: boolean,
  symbol: string | null
): void {
  const priceLinesRef = useRef<IPriceLine[]>([]);

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

    const lines: IPriceLine[] = [];

    const addLine = (price: number | undefined, title: string, color: string, lineStyle: LineStyle): void => {
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

    // 52-week high/low from quote data (real values from FMP quote API)
    if (quoteData) {
      addLine(quoteData.yearHigh, '52W High', 'rgba(63,185,80,0.5)', LineStyle.Dashed);
      addLine(quoteData.yearLow, '52W Low', 'rgba(248,81,73,0.5)', LineStyle.Dashed);
    }

    // Day range + previous close from quote data
    if (quoteData) {
      addLine(quoteData.dayHigh, 'Day High', 'rgba(34,211,238,0.4)', LineStyle.Dotted);
      addLine(quoteData.dayLow, 'Day Low', 'rgba(248,81,73,0.4)', LineStyle.Dotted);
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
  }, [candlestickSeriesRef, quoteData, priceTargets, annotationsVisible, symbol]);
}
