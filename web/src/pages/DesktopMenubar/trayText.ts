/**
 * The lines beside the menu bar icon, as text. The shell lays them out and
 * knows nothing of quotes, so every word of a line is decided here. The bar
 * is monochrome, so direction is a glyph (▲▼), never a color.
 */
import type { MenubarSettings } from '@/lib/desktopSettings';
import { createFormatter, grouped2 } from '@/lib/format';
import type { MarketPhase } from '@/lib/marketUtils';

/** What one item has to say: its official figure, and the extended-hours one beside it. */
export interface TrayFigure {
  /** `NVDA`, `GSPC`, or the portfolio's short label. */
  label: string;
  price: number | null;
  pct: number | null;
  /** The pre-market or after-hours move, labelled `PM` / `AH`. */
  ext: { label: string; price: number | null; pct: number } | null;
  /** Decimal places a price takes; 2 unless the listing quotes finer. */
  places?: number;
  /** A portfolio prints its move alone: its worth does not belong in the menu bar. */
  percentOnly?: boolean;
}

const price4 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 4 });

/** `▲5.60%`, `▼0.4%`, and a move that rounds to nothing unmarked: `0.00%`. */
export function trayPercent(value: number, digits: 1 | 2): string {
  const size = Math.abs(value).toFixed(digits);
  if (Number(size) === 0) return `${size}%`;
  return `${value > 0 ? '▲' : '▼'}${size}%`;
}

export function trayPrice(value: number, locale: string, places = 2): string {
  return places > 2 ? price4(value, locale) : grouped2(value, locale);
}

const finite = (n: number | null | undefined): n is number => n != null && Number.isFinite(n);

/**
 * One line, `NVDA ▼5.60%` / `NVDA 163.99` / `NVDA 163.99 ▼5.6%`, with the
 * extended move standing in when asked for and there is one: `NVDA AH ▲2.08%`.
 * A figure that is not known yet falls back to the one that is (a price before
 * the day has a change); null when neither is, so a guess never shows.
 */
export function formatTrayLine(
  figure: TrayFigure,
  { show, extendedHours }: Pick<MenubarSettings, 'show' | 'extendedHours'>,
  locale: string,
): string | null {
  const useExt = extendedHours && figure.ext != null;
  const head = useExt ? `${figure.label} ${figure.ext!.label}` : figure.label;
  const price = useExt ? figure.ext!.price : figure.price;
  const pct = useExt ? figure.ext!.pct : figure.pct;
  const places = figure.places ?? 2;

  if (figure.percentOnly) return finite(pct) ? `${head} ${trayPercent(pct, 2)}` : null;
  if (show === 'both' && finite(price) && finite(pct)) {
    return `${head} ${trayPrice(price, locale, places)} ${trayPercent(pct, 1)}`;
  }
  if (show !== 'price' && finite(pct)) return `${head} ${trayPercent(pct, 2)}`;
  if (finite(price)) return `${head} ${trayPrice(price, locale, places)}`;
  if (finite(pct)) return `${head} ${trayPercent(pct, 2)}`;
  return null;
}

/**
 * How often the shell should ask for fresh lines: often while any US session
 * trades, rarely when none does, and in between while the session is not
 * known (the status read failed), so the next ask can find out.
 */
export function trayRefreshMs(phase: MarketPhase): number {
  if (phase === 'open' || phase === 'pre' || phase === 'after') return 15_000;
  return phase === 'closed' ? 300_000 : 60_000;
}
