/** A selection in the page, which follows the text as it scrolls. */
export interface SelectionAnchor {
  kind: 'selection';
  id: number;
  text: string;
  range: Range;
  /** Dragged toward the start of the text, so it ended at the range's start. */
  backward: boolean;
}

/** Monaco draws its own selection and reports a rectangle instead. */
export interface RectAnchor {
  kind: 'rect';
  id: number;
  text: string;
  rect: { left: number; top: number; bottom: number };
}

/** The whole of an element the pointer rests on, with no selection of its own. */
export interface BlockAnchor {
  kind: 'block';
  id: number;
  text: string;
  block: HTMLElement;
  range: Range;
}

/** An image has no text, only itself. */
export interface ImageAnchor {
  kind: 'image';
  id: number;
  image: HTMLImageElement;
}

export type TextAnchor = SelectionAnchor | RectAnchor | BlockAnchor;
export type QuoteAnchor = TextAnchor | ImageAnchor;

let nextAnchorId = 1;

export function makeQuoteAnchor<A extends QuoteAnchor>(fields: Omit<A, 'id'>): A {
  return { ...fields, id: nextAnchorId++ } as A;
}

/** Marks the toolbar, the popover and the menu, whose presses are not selections. */
export const QUOTE_SURFACE_ATTR = 'data-selection-quote';

export const surfaceProps = { [QUOTE_SURFACE_ATTR]: '' };

export function isQuoteSurface(target: EventTarget | null): boolean {
  return target instanceof Element && !!target.closest(`[${QUOTE_SURFACE_ATTR}]`);
}

export function lineCountOf(text: string): number {
  return (text.match(/\n/g) || []).length + 1;
}
