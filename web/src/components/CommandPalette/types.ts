import type { ComponentType } from 'react';

type Icon = ComponentType<{ className?: string; 'aria-hidden'?: boolean }>;

/** Something a row can do. */
export interface PaletteAction {
  label: string;
  icon: Icon;
  run: () => void;
  /** Starts a thread where new work goes, so it waits for that place to be known. */
  startsThread?: boolean;
}

export interface PaletteItem {
  /** Unique across the whole list: the selection follows it, not a position. */
  id: string;
  icon: Icon;
  title: string;
  /** More text the row is found by but never shows: aliases, a description, a section. */
  keywords?: string;
  /** Spans of `title` the query matched, bolded. */
  spans?: [number, number][];
  /** A ticker's symbol, set in the data face before the title. */
  symbol?: string;
  /** Muted text after the title: where it lives, when, what it is. */
  meta?: string;
  /** Amber glyph: only the row that hands work to the agent. */
  accent?: boolean;
  /** The first is what Enter does, and the footer names it; the rest are offered on →. */
  actions: [PaletteAction, ...PaletteAction[]];
}

export interface PaletteGroup {
  id: string;
  label: string;
  items: PaletteItem[];
}
