import type { ComponentType } from 'react';

type Icon = ComponentType<{ className?: string; 'aria-hidden'?: boolean }>;

/**
 * A kind of result the palette can show alone, asked for by a prefix typed
 * first (`$` tickers, `/` files, `#` thread titles, `"` what was said, `@`
 * workspaces, `>` the app's own commands) or opened from its group.
 */
export type Kind = 'tickers' | 'files' | 'threads' | 'turns' | 'workspaces' | 'actions';

/** A workspace or a thread the search keeps to (Tab on its row). */
export type Place =
  | { kind: 'workspace'; workspaceId: string; name: string }
  | { kind: 'thread'; threadId: string; workspaceId: string; title: string };

/**
 * What a view shows alone, and how it was asked for: one kind, by its prefix
 * or opened from its group (a drill-in), or one ticker and the threads and
 * conversations that name it.
 */
export type Lens = { kind: Kind; via: 'prefix' | 'drill' } | { kind: 'ticker'; symbol: string; via: 'drill' };

/** One view of the palette: everything or one lens, everywhere or within a place. */
export interface ViewSpec {
  place: Place | null;
  lens: Lens | null;
}

/** Something a row can do: run (the palette closes first), or open another view of the palette over this one. */
export type PaletteAction = {
  /** Unique within its row: what waits for it names it by this, and the list keys it. */
  id: string;
  label: string;
  icon: Icon;
} & (
  | {
      effect: 'run';
      run: () => void;
      /** Starts a thread where new work goes, so it waits for that place to be known. */
      startsThread?: boolean;
    }
  | { effect: 'open'; view: Lens }
);

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
  /** A second line: the words a conversation matched, after who said them. */
  detail?: { label: string; text: string; spans?: [number, number][] };
  /** What the preview pane shows while the row is selected. */
  preview?: PreviewTarget;
  /** What Tab narrows the search to: the workspace or thread the row is. */
  place?: Place;
  /** The first is what Enter (and, when it opens a view, →) does, and the footer names it; the rest are offered on →. */
  actions: [PaletteAction, ...PaletteAction[]];
}

export interface PaletteGroup {
  id: string;
  label: string;
  items: PaletteItem[];
  /** What its header opens, for a pointer: this kind of result alone. */
  open?: Lens;
}

export type PreviewTarget =
  /** The turn a conversation match is in. */
  | { kind: 'turn'; threadId: string; turnIndex: number; side: 'user' | 'agent' }
  /** A thread's latest exchange. */
  | { kind: 'thread'; threadId: string }
  /** A ticker's price and change. */
  | { kind: 'ticker'; symbol: string };
