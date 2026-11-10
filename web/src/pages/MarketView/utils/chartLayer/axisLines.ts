/**
 * A layer's levels and alerts as price lines: lines at one price share one
 * axis label, named with a bell when an alert rides on it.
 */

import type { LineStyleName } from '../../types/chartLayer';
import type { ElementHandle } from './scene';

/** Characters of a level's name kept beside its price on the axis; hover shows the rest. */
const AXIS_TITLE_MAX = 16;

function axisTitle(title: string | null | undefined, max = AXIS_TITLE_MAX): string {
  const t = (title ?? '').trim();
  return t.length > max ? `${t.slice(0, max - 1).trimEnd()}…` : t;
}

/** One price line a layer asks for: a level, or an alert's trigger. */
export interface AxisLineSpec extends ElementHandle {
  /** Which series hosts it (a pane key): lines merge only on the same scale. */
  host: string;
  price: number;
  color: string;
  style: LineStyleName | 'sparse';
  title: string;
  /** Set for an alert: the direction it fires on. */
  alert: 'above' | 'below' | 'either' | null;
}

export interface MergedAxisLine {
  /** The element the line explains on hover: the first level, else the first alert. */
  key: string;
  host: string;
  price: number;
  color: string;
  style: LineStyleName | 'sparse';
  title: string;
  members: AxisLineSpec[];
}

const ALERT_BELL = '\u{1F514}';

/** The lines whose names a merged label shows: its levels, or its alerts when it has no level. */
function namedMembers(group: readonly AxisLineSpec[]): AxisLineSpec[] {
  const levels = group.filter((g) => !g.alert);
  return levels.length ? levels : [...group];
}

/** The lines whose names the merged label shows cut short (`axisTitle`), for the render report. */
export function cutAxisTitles(line: MergedAxisLine): AxisLineSpec[] {
  return namedMembers(line.members).filter((g) => {
    const t = g.title.trim();
    return t !== '' && !line.title.includes(t);
  });
}

/** Two lines closer than this are one price on the axis: a tick, or 0.05% for a large price. */
function axisMergeTolerance(price: number): number {
  return Math.max(0.01, Math.abs(price) * 5e-4);
}

/**
 * Lines at the same price share one axis label. A level and the alert that
 * watches it are one idea, and two pills at one price only push the rest of
 * the axis stack apart. The merged line takes the level's color and style
 * and its name, with a bell (and the alert's direction) in front when an
 * alert rides on it.
 */
export function mergeAxisLines(lines: readonly AxisLineSpec[]): MergedAxisLine[] {
  const out: MergedAxisLine[] = [];
  const byHost = new Map<string, AxisLineSpec[]>();
  for (const l of lines) {
    if (!Number.isFinite(l.price)) continue;
    const list = byHost.get(l.host);
    if (list) list.push(l);
    else byHost.set(l.host, [l]);
  }
  for (const [host, list] of byHost) {
    const sorted = [...list].sort((a, b) => a.price - b.price);
    let group: AxisLineSpec[] = [];
    const flush = () => {
      if (!group.length) return;
      const levels = group.filter((g) => !g.alert);
      const alerts = group.filter((g) => g.alert);
      const lead = levels[0] ?? alerts[0];
      const names = [...new Set(namedMembers(group).map((g) => g.title.trim()).filter(Boolean))];
      let title = names.length > 1 ? names.map((n) => axisTitle(n, 10)).join(' · ') : axisTitle(names[0] ?? '');
      if (alerts.length) {
        const dirs = new Set(alerts.map((a) => a.alert));
        const glyph = dirs.size === 1 ? (dirs.has('above') ? '↑' : dirs.has('below') ? '↓' : '↕') : '↕';
        title = `${ALERT_BELL}${glyph}${title ? ` ${title}` : ''}`;
      }
      out.push({ key: lead.key, host, price: lead.price, color: lead.color, style: lead.style, title, members: group });
      group = [];
    };
    for (const l of sorted) {
      if (group.length && l.price - group[0].price > axisMergeTolerance(group[0].price)) flush();
      group.push(l);
    }
    flush();
  }
  return out;
}
