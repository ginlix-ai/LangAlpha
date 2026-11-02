/**
 * The page bridge's messages, both ways: what the viewer posts to a framed
 * HTML document (`ViewerToPage`), and what the document's bridge
 * (`src/server/preview_proxy/bridge/*.js`) reports back, read at the boundary
 * by `parsePageMessage`.
 *
 * Read defensively: the document is code the agent wrote, and anything in
 * the frame can post as the bridge does. A message that does not parse is
 * dropped whole, and the viewer acts only on what the owner then does in its
 * own UI; nothing the page says creates a comment. The bridge's sources
 * import the wire types below in JSDoc, so `tsc -b` holds both ends to one
 * shape (`web/tsconfig.bridge.json`).
 */
import * as z from 'zod/mini';
import type {
  AreaAnchor,
  ArtifactCommentAnchor,
  ArtifactCommentAreaElement,
  ElementAnchor,
  TextAnchor,
} from '@/types/artifacts';

/**
 * How long after load a document that never goes still (an animation driven
 * from script) is taken as settled anyway. The bridge waits this long
 * (`bridge/core.js`), and a viewer swapping documents waits a little longer.
 */
export const SETTLE_CAP_MS = 5000;

// --- The viewer to the page ---

/** A spot to find on the document; `mark` keeps its words marked while its box is open. */
export interface PageAnchorItem {
  id: string;
  anchor: ArtifactCommentAnchor;
  mark?: boolean;
}

/** What the viewer posts to the document. The read-only bridge answers the first four. */
export type ViewerToPage =
  /** The app's CSS variables, as `resolveThemeVars()` writes them. */
  | { type: 'page:theme'; css: string }
  /** Where the reader was on the document this one replaced. */
  | { type: 'page:restore-scroll'; x: number; y: number }
  /** The section a reference pointed at, by the id or name a fragment matches. */
  | { type: 'page:scroll-to'; fragment: string }
  | { type: 'page:scroll-to'; id: string }
  | { type: 'page:comment-mode'; on: boolean }
  | { type: 'page:anchors'; items: PageAnchorItem[] }
  /** Report the pointer's next move over the page (`page:pointer-in`). */
  | { type: 'page:pointer-watch' }
  | { type: 'page:clear-selection' };

// --- The page to the viewer ---

/** A box in the frame's viewport, which is also the overlay's. */
export interface FrameRect {
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface PageHover {
  rect: FrameRect;
  label: string | null;
  /** The pointer is over text: `rect` is the word under it, which a click takes, and no element is outlined. */
  word: boolean;
}

export interface PagePick {
  anchor: ArtifactCommentAnchor;
  rect: FrameRect;
  /** The document's title at the pick, which the comment keeps; null when it has none. */
  title: string | null;
}

export interface PageClick extends PagePick {
  /** The press whose click this was, or null for a click a key made. */
  press: number | null;
}

export interface PageAreaDrag {
  rect: FrameRect;
  /** The press the drag began with. */
  press: number | null;
}

export interface PageSelection extends PageClick {
  /**
   * Made in comment mode, by a drag over words or a click on one: the box
   * opens on it at once, with no chip, paired with `press` as a pick is.
   * Otherwise the reader selected words outside the mode, which the chip offers.
   */
  compose: boolean;
}

/** What the document's bridge reports, as the viewer reads it. */
export type PageToViewer =
  /** The bridge is listening; `path` is the workspace file the document is, which a link can change. */
  | { type: 'page:ready'; path: string | null }
  /** The colour the document paints behind itself, null for none. */
  | { type: 'page:ground'; color: string | null }
  | { type: 'page:scroll'; x: number; y: number; moving: boolean }
  /** Loaded, its requests back, and still: a spot it does not show now is not coming. */
  | { type: 'page:settled' }
  /** Where each spot the viewer asked about sits now, null where it is not on the page. */
  | { type: 'page:positions'; items: { id: string; rect: FrameRect | null }[] }
  /** In comment mode, what is under the pointer; null when nothing is. */
  | { type: 'page:hover'; hover: PageHover | null }
  /** A press on the page, in either mode, numbered from 1 for each document. */
  | { type: 'page:press'; press: number }
  /** An element clicked in comment mode. */
  | ({ type: 'page:pick' } & PageClick)
  /** Several elements dragged around in comment mode. */
  | ({ type: 'page:area' } & PageClick)
  /** The rectangle being dragged out, null once the drag ended or was called off. */
  | { type: 'page:area-drag'; drag: PageAreaDrag | null }
  /** Words the reader selected, null once they are not. */
  | { type: 'page:selection'; selection: PageSelection | null }
  /** Esc or C pressed inside the frame, which the viewer takes as it takes them outside. */
  | { type: 'page:key'; key: 'Escape' | 'c' }
  | { type: 'page:pointer-in' };

/** Larger than any viewport, small enough that arithmetic on it stays exact. */
const MAX_COORD = 1e6;
/**
 * The longest path a comment or a version can name (the server's path check
 * on both); a hello naming a longer one is not believed.
 */
export const MAX_DOC_PATH = 1024;
/**
 * The comment API's limits per anchor field. A longer value is cut rather
 * than refused: the anchor is a description of the spot, and its opening is
 * what matters.
 */
export const ANCHOR_LIMITS = { path: 1000, label: 300, quote: 2000, prefix: 200, suffix: 200 } as const;
const MAX_HOVER_LABEL = 300;
/** The API's limit on a comment's title, the document's when it was made. */
export const MAX_TITLE = 300;
export const MAX_POSITIONS = 500;
/** The API's limits on an area: the elements it lists, and how many it counts. */
export const MAX_AREA_ELEMENTS = 8;
export const MAX_AREA_COUNT = 500;
/** A computed colour as the browser writes one; anything else is not trusted into a style. */
const RGB = /^rgba?\(\s*\d{1,3}(\.\d+)?\s*,\s*\d{1,3}(\.\d+)?\s*,\s*\d{1,3}(\.\d+)?\s*(,\s*(0|1|0?\.\d+)\s*)?\)$/;

/** Text as the page sent it, without control characters, cut to `limit`; empty is absent. */
function cleanText(v: string, limit: number): string | undefined {
  // eslint-disable-next-line no-control-regex
  const text = v.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '').trim();
  if (!text) return undefined;
  if (text.length <= limit) return text;
  // Never end on half a surrogate pair: the server cannot store one alone.
  let cut = limit - 1;
  const last = text.charCodeAt(cut - 1);
  if (last >= 0xd800 && last <= 0xdbff) cut -= 1;
  return `${text.slice(0, cut)}…`;
}

/** The path a hello names, if it is one a workspace file could have. */
function cleanPath(path: string): string | undefined {
  // eslint-disable-next-line no-control-regex
  if (!path || path.length > MAX_DOC_PATH || /[\u0000-\u001f\u007f]/.test(path)) return undefined;
  return path.replace(/^\/+/, '');
}

/** The object without the keys whose value is undefined, as the API stores an anchor. */
function defined<T extends object>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined)) as T;
}

/** A field the page got wrong is left out, rather than costing the message. */
function lenient<T extends z.ZodMiniType>(schema: T) {
  return z.catch(z.optional(schema), undefined);
}

/** A text field, cleaned and cut to `limit`; null and anything not text are absent. */
function text(limit: number) {
  return lenient(z.pipe(z.nullable(z.string()), z.transform((v) => (v === null ? undefined : cleanText(v, limit)))));
}

const coord = z.number().check(z.gte(-MAX_COORD), z.lte(MAX_COORD));
const extent = z.number().check(z.gte(0), z.lte(MAX_COORD));
const unit = z.number().check(z.gte(0), z.lte(1));
const frameRect = z.object({ x: coord, y: coord, width: extent, height: extent });
const pressNumber = z.int().check(z.gt(0));
/** Where on the anchored box the pin sits, as fractions of it. */
const point = z.object({ x: unit, y: unit });

const spotFields = {
  path: text(ANCHOR_LIMITS.path),
  label: text(ANCHOR_LIMITS.label),
  quote: text(ANCHOR_LIMITS.quote),
  prefix: text(ANCHOR_LIMITS.prefix),
  suffix: text(ANCHOR_LIMITS.suffix),
  point: lenient(point),
};

/** A clicked element, found again by its path or its text. */
const elementAnchor = z.pipe(
  z.object({ kind: z.literal('element'), ...spotFields }).check(z.refine((a) => !!(a.path || a.quote))),
  z.transform((a): ElementAnchor => defined(a)),
);

/** Selected words, found again by them. */
const textAnchor = z.pipe(
  z.object({ kind: z.literal('text'), ...spotFields }).check(z.refine((a) => !!a.quote)),
  z.transform((a): TextAnchor => defined(a)),
);

const areaElement = z.pipe(
  z.object({ path: text(ANCHOR_LIMITS.path), label: text(ANCHOR_LIMITS.label), quote: text(ANCHOR_LIMITS.quote) })
    .check(z.refine((e) => !!(e.path || e.quote))),
  z.transform((e): ArtifactCommentAreaElement => defined(e)),
);

/**
 * An area as the page described it: at least two elements it can be found
 * by again, the first few listed, and the count of them all. Anything the API
 * would refuse fails the whole area, rather than leaving a smaller one the
 * reader never drew.
 */
const areaAnchor = z.pipe(
  z.object({
    kind: z.literal('area'),
    elements: z.array(areaElement).check(z.minLength(2), z.maxLength(MAX_AREA_ELEMENTS)),
    count: z.int().check(z.lte(MAX_AREA_COUNT)),
    label: text(ANCHOR_LIMITS.label),
    point: lenient(point),
    /** Where the area sat, as fractions of the document. */
    rect: lenient(z.object({ x: unit, y: unit, w: unit, h: unit })),
  }).check(z.refine((a) => a.count >= a.elements.length)),
  z.transform((a): AreaAnchor => defined(a)),
);

const title = text(MAX_TITLE);
const press = lenient(pressNumber);

/** A click's or an area's comment spot: the anchor, its box, and the press it pairs with. */
function click<K extends 'page:pick' | 'page:area'>(type: K, anchor: typeof elementAnchor | typeof areaAnchor) {
  return z.pipe(
    z.object({ type: z.literal(type), anchor, rect: frameRect, title, press }),
    z.transform((m) => ({ type: m.type, anchor: m.anchor, rect: m.rect, title: m.title ?? null, press: m.press ?? null })),
  );
}

/** One per type: a nested union where a type's message can also say `clear`. */
const pageToViewer = z.discriminatedUnion('type', [
  z.pipe(
    z.object({ type: z.literal('page:ready'), v: z.optional(z.number()), path: lenient(z.pipe(z.string(), z.transform(cleanPath))) }),
    z.transform((m) => ({ type: m.type, path: m.path ?? null })),
  ),
  z.object({ type: z.literal('page:ground'), color: z.nullable(z.string().check(z.regex(RGB))) }),
  z.object({ type: z.literal('page:scroll'), x: coord, y: coord, moving: z.boolean() }),
  z.object({ type: z.literal('page:settled') }),
  z.pipe(
    z.object({ type: z.literal('page:positions'), items: z.array(z.unknown()) }),
    z.transform((m) => ({
      type: m.type,
      items: m.items.slice(0, MAX_POSITIONS).flatMap((item) => {
        const read = z.object({ id: z.string(), rect: z.catch(z.nullable(frameRect), null) }).safeParse(item);
        return read.success ? [read.data] : [];
      }),
    })),
  ),
  z.pipe(
    z.discriminatedUnion('clear', [
      z.object({ type: z.literal('page:hover'), clear: z.literal(true) }),
      z.object({
        type: z.literal('page:hover'),
        clear: z.optional(z.literal(false)),
        rect: frameRect,
        word: lenient(z.boolean()),
        label: text(MAX_HOVER_LABEL),
      }),
    ]),
    z.transform((m) => ({
      type: m.type,
      hover: m.clear ? null : { rect: m.rect, label: m.word ? null : (m.label ?? null), word: m.word === true },
    })),
  ),
  z.object({ type: z.literal('page:press'), press: pressNumber }),
  click('page:pick', elementAnchor),
  click('page:area', areaAnchor),
  z.pipe(
    z.discriminatedUnion('clear', [
      z.object({ type: z.literal('page:area-drag'), clear: z.literal(true) }),
      z.object({ type: z.literal('page:area-drag'), clear: z.optional(z.literal(false)), rect: frameRect, press }),
    ]),
    z.transform((m) => ({ type: m.type, drag: m.clear ? null : { rect: m.rect, press: m.press ?? null } })),
  ),
  z.pipe(
    z.discriminatedUnion('clear', [
      z.object({ type: z.literal('page:selection'), clear: z.literal(true) }),
      z.object({
        type: z.literal('page:selection'),
        clear: z.optional(z.literal(false)),
        anchor: textAnchor,
        rect: frameRect,
        title,
        press,
        compose: lenient(z.boolean()),
      }),
    ]),
    z.transform((m) => ({
      type: m.type,
      selection: m.clear
        ? null
        : { anchor: m.anchor, rect: m.rect, title: m.title ?? null, press: m.press ?? null, compose: m.compose === true },
    })),
  ),
  z.object({ type: z.literal('page:key'), key: z.enum(['Escape', 'c']) }),
  z.object({ type: z.literal('page:pointer-in') }),
]);

/**
 * What the bridge sends, as it sends it. Every message also carries `v`, the
 * bridge's version, which the viewer does not read.
 */
export type PageToViewerWire = z.input<typeof pageToViewer>;

/** A message from the document, or null for one not to trust. */
export function parsePageMessage(data: unknown): PageToViewer | null {
  const read = pageToViewer.safeParse(data);
  return read.success ? read.data : null;
}
