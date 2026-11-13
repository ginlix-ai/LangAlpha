/**
 * The desktop shell's settings as the web reads and edits them: the limits
 * the shell clamps to, the defaults, one identity per menu bar item, and the
 * hook both the Settings section and the menu bar panel draw from.
 */
import { useEffect, useState } from 'react';
import { isIndexFamilySpelling } from '@/lib/bars/exchanges';
import {
  getDesktopSettings,
  onDesktopSettings,
  setDesktopSettings,
  type DesktopSettings,
  type DesktopSettingsPatch,
  type DesktopSettingsView,
  type MenubarItem,
  type PanelSectionId,
  type QuoteRef,
} from '@/lib/desktop';
import { indexName, isBareIndex } from '@/lib/indices';
import { normalizeIndexKey } from '@/lib/marketUtils';

export type MenubarSettings = DesktopSettings['menubar'];
export type PanelSettings = DesktopSettings['panel'];

/** The most the shell keeps beside the icon. */
export const MAX_MENUBAR_ITEMS = 8;
export const MAX_PANEL_TILES = 4;
/** The shell clamps to 2..30 and 1..10; offered as choices so a pick is never clamped behind the user's back. */
export const ROTATE_SECONDS = [2, 3, 4, 5, 6, 8, 10, 15, 20, 30] as const;
export const WATCHLIST_ROWS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as const;

export const SECTION_IDS: readonly PanelSectionId[] = ['market', 'portfolio', 'watchlist', 'agent'];

export const DEFAULT_MENUBAR: MenubarSettings = {
  enabled: true,
  items: [],
  style: 'rotate',
  rotateSeconds: 4,
  show: 'change',
  extendedHours: true,
};

export const DEFAULT_PANEL: PanelSettings = {
  sections: SECTION_IDS.map((id) => ({ id, visible: true })),
  tiles: [
    { symbol: 'GSPC', kind: 'index' },
    { symbol: 'IXIC', kind: 'index' },
    { symbol: 'DJI', kind: 'index' },
    { symbol: 'VIX', kind: 'index' },
  ],
  watchlistId: null,
  watchlistRows: 5,
};

export const PORTFOLIO_ITEM: MenubarItem = { kind: 'portfolio' };

/**
 * A symbol as the quote API asks for it: uppercase, an index bare (`^GSPC`
 * is `GSPC`). Without a `kind`, a caret spelling or one of the bare US
 * families reads as an index and anything else as a stock.
 */
export function quoteRef(symbol: string, kind?: QuoteRef['kind']): QuoteRef {
  const spelled = symbol.trim().toUpperCase();
  const bare = normalizeIndexKey(spelled);
  const resolved = kind ?? (isIndexFamilySpelling(spelled) || isBareIndex(bare) ? 'index' : 'stock');
  return { symbol: resolved === 'index' ? bare : spelled, kind: resolved };
}

/** One key per thing, so a list holds each once and a drag can name what it moved. */
export function itemKey(item: MenubarItem): string {
  return 'symbol' in item ? `${item.kind}:${item.symbol.toUpperCase()}` : 'portfolio';
}

export function hasItem(items: readonly MenubarItem[], item: MenubarItem): boolean {
  const key = itemKey(item);
  return items.some((it) => itemKey(it) === key);
}

/** The list with `item` at its end, or the same list when it already holds it or is full. */
export function withItem<T extends MenubarItem>(items: readonly T[], item: T, max: number): T[] {
  if (items.length >= max || hasItem(items, item)) return [...items];
  return [...items, item];
}

/** The name a Settings chip shows: the portfolio, an index's long name (else its caret spelling), or the ticker. */
export function itemLabel(item: MenubarItem, t: (key: string) => string): string {
  if (!('symbol' in item)) return t('settings.desktop.itemPortfolio');
  if (item.kind === 'index') return indexName(item.symbol, 'long', t) ?? `^${item.symbol}`;
  return item.symbol;
}

/** The given choices, plus the current value where the shell holds one they leave out. */
export function choicesWith(choices: readonly number[], current: number): number[] {
  return choices.includes(current) ? [...choices] : [...choices, current].sort((a, b) => a - b);
}

/** The shell repairs every field it stores; this only fills a group it left out. */
export function normalize(view: DesktopSettingsView): DesktopSettingsView {
  return {
    ...view,
    menubar: { ...DEFAULT_MENUBAR, ...view.menubar },
    panel: { ...DEFAULT_PANEL, ...view.panel },
  };
}

/** Applies a one-level patch the way the shell does. Arrays replace whole. */
function applyPatch(view: DesktopSettingsView, patch: DesktopSettingsPatch): DesktopSettingsView {
  const next = { ...view } as DesktopSettingsView & Record<string, unknown>;
  for (const [key, value] of Object.entries(patch)) {
    const current = next[key];
    next[key] =
      value && typeof value === 'object' && !Array.isArray(value) && current && typeof current === 'object'
        ? { ...(current as object), ...(value as object) }
        : value;
  }
  return next;
}

export interface DesktopSettingsState {
  /** Null until the shell answers, and where it cannot. */
  view: DesktopSettingsView | null;
  /** The shell gave no answer: a browser, or a shell without settings. */
  failed: boolean;
  /** Moves the view at once, then takes the shell's answer as the truth, or
   *  reads it again when the save got none. */
  save: (patch: DesktopSettingsPatch) => void;
}

/**
 * The shell's settings, followed as they change anywhere (Settings, the
 * icon's menu, a pin in the panel), so every surface draws the view the
 * shell sent last.
 */
export function useDesktopSettings(): DesktopSettingsState {
  const [view, setView] = useState<DesktopSettingsView | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let live = true;
    void getDesktopSettings().then((answer) => {
      if (!live) return;
      if (answer) setView(normalize(answer));
      else setFailed(true);
    });
    const off = onDesktopSettings((answer) => {
      if (live) setView(normalize(answer));
    });
    return () => {
      live = false;
      off();
    };
  }, []);

  const save = (patch: DesktopSettingsPatch) => {
    setView((current) => (current ? applyPatch(current, patch) : current));
    void setDesktopSettings(patch).then((answer) => {
      // The shell's answer wins, including a shortcut it could not register.
      if (answer) {
        setView(normalize(answer));
        return;
      }
      // Not taken: draw what the shell holds again, not a change it never kept.
      void getDesktopSettings().then((held) => {
        if (held) setView(normalize(held));
      });
    });
  };

  return { view, failed, save };
}
