import { CHART_SURFACE_MIN_WIDTH } from '@/pages/MarketView/components/chartSurfaceLayout';
import { DEFAULT_PANEL_WIDTH, PREVIEW_MAX_RATIO } from './detailWidth';
import type { PanelTarget } from './types';
import type { FileTab } from './useFileTabs';

/**
 * How wide the file panel stands for what it has in front. Each kind of tab
 * reads best at its own width, so the panel moves to the front tab's width as
 * tabs change, both ways: Status is a list beside the chat, a document wants
 * a page, and a chart or a running app wants the room. A drag sets the width
 * for its kind and is kept, so a narrow Status stays narrow after a chart.
 * A tool result opens at the width its own content asks for, which differs
 * call by call, so its drag lasts while the panel stays open.
 */
export type PanelWidthKind = 'status' | 'document' | 'surface' | 'detail';

/** What the panel can have in front, or be landing: a tab's kind or a target's. */
export type PanelTabKind = FileTab['kind'] | PanelTarget['kind'];

/** The kinds a dragged width is kept for. */
type KeptKind = Exclude<PanelWidthKind, 'detail'>;

const DEFAULT_WIDTH: Record<KeptKind, number> = {
  status: 420,
  document: DEFAULT_PANEL_WIDTH,
  surface: DEFAULT_PANEL_WIDTH,
};

/**
 * A tab's kind, or a landing's, as the width it stands at. Nothing seen yet
 * reads as a document. Every kind is named, so a new one has to pick.
 */
export function panelWidthKind(kind: PanelTabKind | null): PanelWidthKind {
  switch (kind) {
    case 'status':
      return 'status';
    case 'chart':
    case 'preview':
    case 'browser':
      return 'surface';
    case 'tool':
    case 'sources':
      return 'detail';
    case null:
    case 'empty':
    case 'file':
    case 'settings':
    case 'memory':
    case 'memo':
      return 'document';
    default:
      return kind satisfies never;
  }
}

export type KeptWidths = Partial<Record<KeptKind, number>>;

// A layout preference of this browser's, naming no workspace or thread, so
// it is not scoped to the signed-in user (see `userStorage`).
const STORAGE_KEY = 'filePanel.widths';

/** The widths dragged before, each one a positive number or left out. */
export function readKeptWidths(): KeptWidths {
  try {
    const raw: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '{}');
    if (!raw || typeof raw !== 'object') return {};
    const kept: KeptWidths = {};
    for (const kind of Object.keys(DEFAULT_WIDTH) as KeptKind[]) {
      const value = (raw as Record<string, unknown>)[kind];
      if (typeof value === 'number' && Number.isFinite(value) && value > 0) kept[kind] = Math.round(value);
    }
    return kept;
  } catch {
    return {};
  }
}

/** The kept widths with a dragged one set for its kind. A tool result's
 *  width is not kept, so its kind is not one of these. */
export function withKeptWidth(kept: KeptWidths, kind: KeptKind, width: number): KeptWidths {
  return { ...kept, [kind]: Math.round(width) };
}

/** Saves the kept widths for the next visit. */
export function saveKeptWidths(kept: KeptWidths): void {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(kept)); } catch { /* the widths still hold for this visit */ }
}

/**
 * The width, cap and floor the panel stands at for the tab in front. A chart
 * is never narrower than its surface's floor, below which its toolbar
 * collides, and the divider stops there too. `detailWidth` is what the last
 * tool result asked for.
 */
export function panelWidthFor(
  tabKind: PanelTabKind | null,
  kept: KeptWidths,
  detailWidth: number,
): { width: number; maxRatio?: number; minWidth: number } {
  const kind = panelWidthKind(tabKind);
  if (kind === 'detail') return { width: detailWidth, minWidth: 0 };
  const width = kept[kind] ?? DEFAULT_WIDTH[kind];
  if (kind !== 'surface') return { width, minWidth: 0 };
  const minWidth = tabKind === 'chart' ? CHART_SURFACE_MIN_WIDTH : 0;
  return { width: Math.max(width, minWidth), maxRatio: PREVIEW_MAX_RATIO, minWidth };
}
