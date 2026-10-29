import { createChart, ColorType, LineStyle } from 'lightweight-charts';
import type { CandlestickData, CandlestickSeriesPartialOptions, ChartOptions, DeepPartial, IChartApi, UTCTimestamp } from 'lightweight-charts';
import type { ChartBar } from '@/lib/bars';
import { useTheme } from '@/contexts/ThemeContext';
import { createThemeResolver, useThemeTokens } from '@/lib/themeTokens';

// Canvas charts (lightweight-charts) take color strings and cannot resolve CSS
// variables, so each slot names the token it comes from and is resolved off
// <html> at paint time. A non-`--` entry is a one-off no token carries. These
// charts are drawn only in the detail panel, so they take its rules and type.
type CanvasSlot = 'grid' | 'edge' | 'text' | 'ink' | 'ma' | 'up' | 'down' | 'upA' | 'downA';
export type CanvasTheme = Record<CanvasSlot, string>;

const CANVAS_SOURCES: Record<'dark' | 'light', CanvasTheme> = {
  dark: {
    grid: '--color-term-rule',
    edge: '--color-term-edge',
    text: '--color-term-text-tertiary',
    ink: '--color-term-text',
    ma: '--color-term-text-secondary',
    // Terminal-mint candles: an accent that exists nowhere else in the system,
    // so there is no token to point at. Its volume tint follows it.
    up: '#0FEDBE',
    down: '--color-loss',
    upA: 'rgba(15, 237, 190, 0.3)',
    // Volume tints need 30% opacity; no *-soft/-border token carries that in
    // both themes, so they stay literal derivations of profit/loss.
    downA: 'rgba(248, 81, 73, 0.3)',
  },
  light: {
    grid: '--color-term-rule',
    edge: '--color-term-edge',
    text: '--color-term-text-tertiary',
    ink: '--color-term-text',
    ma: '--color-term-text-secondary',
    up: '--color-profit',
    down: '--color-loss',
    upA: 'rgba(26, 127, 55, 0.3)',
    downA: 'rgba(207, 34, 46, 0.3)',
  },
};

/** Literal mirror of the tokens above — the jsdom / pre-stamp path. */
const CANVAS_FALLBACKS: Record<'dark' | 'light', CanvasTheme> = {
  dark: {
    grid: '#2E3033', edge: '#3A3C40', text: 'rgba(230, 230, 228, 0.62)', ink: '#E6E6E4', ma: '#9B9FA6',
    up: '#0FEDBE', down: '#F85149', upA: 'rgba(15, 237, 190, 0.3)', downA: 'rgba(248, 81, 73, 0.3)',
  },
  light: {
    grid: '#E3E6EA', edge: '#CDD2D9', text: '#8A919C', ink: '#15181D', ma: '#5B6370',
    up: '#1A7F37', down: '#CF222E', upA: 'rgba(26, 127, 55, 0.3)', downA: 'rgba(207, 34, 46, 0.3)',
  },
};

const resolveCanvasTheme = createThemeResolver(CANVAS_SOURCES, CANVAS_FALLBACKS);

/** The panel's canvas colors, stable per theme so a chart's effect keys off them. */
export function useCanvasTheme(): CanvasTheme {
  const { theme } = useTheme();
  return useThemeTokens(resolveCanvasTheme, theme);
}

const MONO_FALLBACK = "'JetBrains Mono', Menlo, ui-monospace, monospace";

/**
 * The panel's furniture on a canvas chart: no fill of its own, so the panel's
 * ground shows through, dashed rules across, and the axes in the panel's mono
 * type. The canvas takes a font stack as a string, so it is read off the
 * token; call it where the DOM is current (an effect), never in render.
 */
export function canvasChartOptions(ct: CanvasTheme, timeVisible: boolean): DeepPartial<ChartOptions> {
  const font = getComputedStyle(document.documentElement).getPropertyValue('--font-mono').trim();
  const crosshairLine = { color: ct.text, labelBackgroundColor: ct.ink };
  return {
    layout: {
      background: { type: ColorType.Solid, color: 'transparent' },
      textColor: ct.text,
      fontFamily: font || MONO_FALLBACK,
      fontSize: 11,
      // MarketView hides the same mark; in a panel it sits over the bars.
      attributionLogo: false,
    },
    grid: {
      vertLines: { visible: false },
      horzLines: { color: ct.grid, style: LineStyle.Dashed },
    },
    crosshair: { vertLine: crosshairLine, horzLine: crosshairLine },
    rightPriceScale: { borderColor: ct.edge },
    // Both edges held: no scrolling past the first or last bar, and zooming
    // out stops once every bar fits, so a reader playing with the chart never
    // finds empty space either side and takes it for missing data. The price
    // chart still reaches its first bar, which loads the bars before it.
    timeScale: { borderColor: ct.edge, timeVisible, fixLeftEdge: true, fixRightEdge: true },
  };
}

export function candleOptions(ct: CanvasTheme): CandlestickSeriesPartialOptions {
  return {
    upColor: ct.up, downColor: ct.down,
    borderUpColor: ct.up, borderDownColor: ct.down,
    wickUpColor: ct.up, wickDownColor: ct.down,
  };
}

/** Bars as a candlestick series takes them: chart seconds are its UTC timestamps. */
export function candleData(bars: readonly ChartBar[]): CandlestickData<UTCTimestamp>[] {
  return bars.map((b) => ({ time: b.time as UTCTimestamp, open: b.open, high: b.high, low: b.low, close: b.close }));
}

/** A chart filling `el`'s width at `height`, kept to that width as it resizes; `dispose` removes both. */
export function mountCanvasChart(
  el: HTMLElement,
  height: number,
  options: DeepPartial<ChartOptions>,
): { chart: IChartApi; dispose: () => void } {
  const chart = createChart(el, { ...options, width: el.clientWidth, height });
  const ro = new ResizeObserver(() => chart.applyOptions({ width: el.clientWidth }));
  ro.observe(el);
  return {
    chart,
    dispose: () => {
      ro.disconnect();
      chart.remove();
    },
  };
}
