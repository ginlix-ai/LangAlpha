/**
 * What the chart already draws (its RSI pane, its moving averages, its
 * volume), so a layer's copy of it is folded in or skipped rather than drawn
 * twice. Each call is a note in the render report.
 */

import type { ChartLayer } from '../../types/chartLayer';

/** What the chart draws on its own, so a layer does not draw it a second time. */
export interface BuiltinIndicators {
  /** Period of the built-in RSI pane, or null when it is not showing. */
  rsiPeriod: number | null;
  /** Periods of the simple moving averages on price. */
  maPeriods: readonly number[];
  /** The volume histogram along the price pane's floor. */
  volume: boolean;
}

/** The period an RSI series or pane title names: "RSI 14", "RSI(14)", "RSI" (14, the convention). */
export function rsiPeriodOf(title: string | null | undefined): number | null {
  const m = (title ?? '').match(/^\s*RSI\s*\(?\s*(\d{1,3})?\s*\)?\s*$/i);
  if (!m) return null;
  return m[1] ? Number(m[1]) : 14;
}

/** The period a simple moving average's title names ("SMA 20", "MA20", "MA (50)"); an EMA is not one. */
export function smaPeriodOf(title: string | null | undefined): number | null {
  const m = (title ?? '').match(/^\s*S?MA\s*[-(]?\s*(\d{1,3})\s*\)?\s*$/i);
  return m ? Number(m[1]) : null;
}

function isVolumeTitle(title: string | null | undefined): boolean {
  return /^\s*vol(?:ume)?\s*$/i.test(title ?? '');
}

export interface BuiltinPlan {
  /** Layer panes drawn into the built-in RSI pane instead of a pane of their own. */
  intoRsi: Set<string>;
  /** Layer panes not drawn at all (a volume pane beside the chart's own volume). */
  dropPanes: Set<string>;
  /** Layer series the chart already draws. */
  skipSeries: Set<string>;
  notes: string[];
}

/**
 * Which parts of a layer the chart already shows. Only an evident duplicate
 * counts: the same indicator under its usual name with the built-in's period
 * (RSI 14 beside the chart's RSI (14), SMA 20 beside MA20, a Volume histogram).
 * An EMA, an RSI of another period, or a series a fill needs is drawn as given.
 * A layer pane that merges into the RSI pane keeps its other series and its
 * levels there, provided they share RSI's 0–100 scale.
 */
export function planBuiltinDuplicates(layer: ChartLayer, builtins: BuiltinIndicators): BuiltinPlan {
  const plan: BuiltinPlan = { intoRsi: new Set(), dropPanes: new Set(), skipSeries: new Set(), notes: [] };
  const series = layer.series;
  const filled = new Set(layer.fills.flatMap((f) => [f.a, f.b]));
  const rsi = builtins.rsiPeriod;
  for (const pane of layer.panes) {
    const inPane = series.filter((s) => s.pane === pane.id);
    if (!inPane.length) continue;
    const title = pane.title || inPane[0].title;
    if (builtins.volume && inPane.every((s) => s.type === 'histogram' && isVolumeTitle(s.title) && !filled.has(s.id))) {
      plan.dropPanes.add(pane.id);
      for (const s of inPane) plan.skipSeries.add(s.id);
      plan.notes.push(`layer pane '${title}' not drawn: the chart's own volume shows it`);
      continue;
    }
    if (rsi == null) continue;
    let dup = inPane.filter((s) => s.type !== 'histogram' && rsiPeriodOf(s.title) === rsi && !filled.has(s.id));
    const paneIsRsi = rsiPeriodOf(pane.title) === rsi;
    if (!dup.length && paneIsRsi && inPane.length === 1 && inPane[0].type === 'line' && !filled.has(inPane[0].id)) {
      dup = inPane;
    }
    if (!dup.length && !paneIsRsi) continue;
    const rest = inPane.filter((s) => !dup.includes(s));
    const fits = rest.every((s) => s.data.every((pt) => !Number.isFinite(pt.value) || (pt.value >= 0 && pt.value <= 100)));
    if (!fits) continue;
    plan.intoRsi.add(pane.id);
    for (const s of dup) plan.skipSeries.add(s.id);
    plan.notes.push(
      `layer pane '${title}' merged into built-in RSI (${rsi})` +
        (dup.length ? `; its ${dup.map((s) => `'${s.title}'`).join(', ')} not drawn twice` : ''),
    );
  }
  for (const s of series) {
    if ((s.pane || 'price') !== 'price' || filled.has(s.id)) continue;
    const ma = s.type === 'line' ? smaPeriodOf(s.title) : null;
    if (ma != null && builtins.maPeriods.includes(ma)) {
      plan.skipSeries.add(s.id);
      plan.notes.push(`layer series '${s.title}' not drawn: built-in MA${ma} shows it`);
    } else if (s.type === 'histogram' && builtins.volume && isVolumeTitle(s.title)) {
      plan.skipSeries.add(s.id);
      plan.notes.push(`layer series '${s.title}' not drawn: the chart's own volume shows it`);
    }
  }
  return plan;
}
