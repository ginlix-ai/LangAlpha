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
 * import the wire types below in JSDoc, so the bridge's own typecheck
 * (`make test-bridge`) holds both ends to one shape.
 */
import * as z from 'zod/mini';
import { MAX_PAGE_LOADS } from '@/lib/pageLoads';
import { parseRange } from '@/pages/ChatAgent/utils/a1';
import type {
  AreaAnchor,
  ArtifactCommentAnchor,
  ArtifactCommentAreaElement,
  BlockAnchor,
  CellsAnchor,
  ElementAnchor,
  PartEntry,
  TextAnchor,
} from '@/types/artifacts';

/**
 * How long after load a document that never goes still (an animation driven
 * from script) is taken as settled anyway. The bridge waits this long
 * (`bridge/core.js`), and a viewer swapping documents waits a little longer.
 */
export const SETTLE_CAP_MS = 5000;

// --- The viewer to the page ---

/**
 * A spot to find on the document; `mark` keeps its words marked while its box
 * is open. `draft` is the comment being written, whose parts a modifier click
 * adds to.
 */
export interface PageAnchorItem {
  id: string;
  anchor: ArtifactCommentAnchor;
  mark?: boolean;
  draft?: boolean;
}

/**
 * What the viewer posts to the document. A read-only document has no comment
 * parts, so it answers only the theme, the scroll and the address.
 */
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
  | { type: 'page:clear-selection' }
  /**
   * Take `part`, an entry of the draft's area as the box showed it, out of the
   * selection. The page answers with the parts left, as an `extend` pick, and
   * takes nothing out once the draft no longer lists that part.
   */
  | { type: 'page:drop-part'; part: PartEntry }
  /** The next tap adds a part to the draft: a touch screen has no modifier to hold. */
  | { type: 'page:extend-next'; on: boolean }
  /**
   * The press `press` opens nothing: it closes what the viewer has open, or
   * asks before a written draft goes. What it picks is not the draft's, so
   * its parts are not kept for a modifier click to grow.
   */
  | { type: 'page:declined'; press: number }
  /**
   * The comment whose card is open, null for none: besides the draft, the
   * only spot whose parts are outlined, so the only other one `page:positions`
   * measures them for. A page that never hears it reports the draft's alone,
   * and one that ignores it may report every spot's.
   */
  | { type: 'page:outlined'; id: string | null }
  /** The document's address under a renewed grant, which it moves to without loading again. */
  | { type: 'page:address'; src: string };

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

export interface PagePressed extends PagePick {
  /** The press whose click this was, or null for a click a key made. */
  press: number | null;
}

export interface PageClick extends PagePressed {
  /**
   * The draft's whole selection, grown or shrunk by a modifier click or the
   * box's "Add a part": it takes the open draft's place, its box and words
   * staying, rather than opening a new one.
   */
  extend: boolean;
}

export interface PageAreaDrag {
  rect: FrameRect;
  /** The press the drag began with. */
  press: number | null;
}

export interface PageSelection extends PagePressed {
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
  /**
   * Where each spot the viewer asked about sits now, null where it is not on
   * the page. An area that lists every part also says where each part shows,
   * for the draft and the spot `page:outlined` names.
   */
  | { type: 'page:positions'; items: { id: string; rect: FrameRect | null; parts?: FrameRect[] }[] }
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
  | { type: 'page:pointer-in' }
  /**
   * In comment mode, whether a click on the page adds a part to the draft:
   * Cmd (Ctrl elsewhere) is held over it (`held`), or "Add a part" asked for
   * the next tap (`next`). Sent on each change.
   */
  | { type: 'page:adding'; held: boolean; next: boolean }
  /** A link to another workspace file in the owner's view: the path, and its section or empty. */
  | { type: 'page:open-file'; path: string; fragment: string }
  /** Every workspace file the document has loaded in the owner's view, and the file it is. Sent once it settles, then as it grows. */
  | { type: 'page:loads'; paths: string[]; path: string }
  /**
   * A frame inside a page reporting what it loaded, by the address of its own
   * document. From the viewer's own frame, it says a script or a refresh took
   * the frame to that file without the view, so it carries the nested bridge.
   */
  | { type: 'page:frame-loads'; doc: string };

/** Larger than any viewport, small enough that arithmetic on it stays exact. */
const MAX_COORD = 1e6;
/**
 * The longest path a comment or a version can name (the server's path check
 * on both); a hello naming a longer one is not believed.
 */
export const MAX_DOC_PATH = 1024;
/** A served address: a prefix with its grant, then a workspace path. */
const MAX_ADDRESS = 4096;
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
/**
 * The API's limits on an area: the elements it lists, and how many it counts.
 * An area lists each part, picked or dragged around; a drag over more than
 * `MAX_AREA_ELEMENTS` counts the rest.
 */
export const MAX_AREA_ELEMENTS = 64;
export const MAX_AREA_COUNT = 500;
/** The API's limit on the cell ranges one comment names. */
export const MAX_CELL_RANGES = 64;
/** 1-based source lines, one or a span, as the API takes them. */
const SOURCE_LINES = /^[1-9][0-9]{0,6}(-[1-9][0-9]{0,6})?$/;
/** A sheet's cell or range of cells, as the API takes it. */
const CELL_RANGE = /^[A-Z]{1,3}[1-9][0-9]{0,6}(:[A-Z]{1,3}[1-9][0-9]{0,6})?$/;
/** A computed colour as the browser writes one; anything else is not trusted into a style. */
const RGB = /^rgba?\(\s*\d{1,3}(\.\d+)?\s*,\s*\d{1,3}(\.\d+)?\s*,\s*\d{1,3}(\.\d+)?\s*(,\s*(0|1|0?\.\d+)\s*)?\)$/;

/** Text as the page sent it, without control characters, cut to `limit`; empty is absent. `keepEnds` keeps spaces at its ends, which a name may hold. */
function cleanText(v: string, limit: number, keepEnds = false): string | undefined {
  // eslint-disable-next-line no-control-regex
  const kept = v.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '');
  const text = keepEnds ? kept : kept.trim();
  if (!text.trim()) return undefined;
  if (text.length <= limit) return text;
  // Never end on half a surrogate pair: the server cannot store one alone.
  let cut = limit - 1;
  const last = text.charCodeAt(cut - 1);
  if (last >= 0xd800 && last <= 0xdbff) cut -= 1;
  return `${text.slice(0, cut)}…`;
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
function text(limit: number, keepEnds = false) {
  return lenient(z.pipe(z.nullable(z.string()), z.transform((v) => (v === null ? undefined : cleanText(v, limit, keepEnds)))));
}

/** A path a workspace file could have. */
// eslint-disable-next-line no-control-regex
const filePath = z.string().check(z.maxLength(MAX_DOC_PATH), z.regex(/^[^\u0000-\u001f\u007f]+$/));
/** The file a document says it is, in a hello or a report. */
const docPath = z.pipe(filePath, z.transform((path) => path.replace(/^\/+/, '')));

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

/** Lines as the API takes them: a span runs forwards. */
const sourceLines = z.string().check(z.regex(SOURCE_LINES), z.refine((v) => {
  const [first, last = first] = v.split('-').map(Number);
  return first <= last;
}));

/** Selected words, found again by them; in a text file, on the lines or the page they are on. */
const textAnchor = z.pipe(
  z.object({
    kind: z.literal('text'),
    ...spotFields,
    lines: lenient(sourceLines),
    page: lenient(z.int().check(z.gte(1), z.lte(100_000))),
  }).check(z.refine((a) => !!a.quote)),
  z.transform((a): TextAnchor => defined(a)),
);

/** A Markdown block, by its lines and its text. */
const blockAnchor = z.pipe(
  z.object({
    kind: z.literal('block'),
    lines: sourceLines,
    label: text(ANCHOR_LIMITS.label),
    quote: text(ANCHOR_LIMITS.quote),
    point: lenient(point),
  }),
  z.transform((a): BlockAnchor => defined(a)),
);

/** Cells of a sheet, by their ranges, each with the values it showed. */
const cellsAnchor = z.pipe(
  z.object({
    kind: z.literal('cells'),
    // A sheet is found again by its exact name, which may end in a space ("Q3 ").
    sheet: text(ANCHOR_LIMITS.label, true),
    ranges: z.array(z.pipe(
      // Within a sheet's last column and row, as the API takes it.
      z.object({ range: z.string().check(z.regex(CELL_RANGE), z.refine((v) => parseRange(v) !== null)), values: text(ANCHOR_LIMITS.quote) }),
      z.transform((r) => defined(r)),
    )).check(z.minLength(1), z.maxLength(MAX_CELL_RANGES)),
    point: lenient(point),
  }),
  z.transform((a): CellsAnchor => defined(a)),
);

const areaElement = z.pipe(
  z.object({
    lines: lenient(sourceLines),
    path: text(ANCHOR_LIMITS.path),
    label: text(ANCHOR_LIMITS.label),
    quote: text(ANCHOR_LIMITS.quote),
  }).check(z.refine((e) => !!(e.path || e.quote || e.lines))),
  z.transform((e): ArtifactCommentAreaElement => defined(e)),
);

/**
 * An area as the page described it: at least two elements it can be found
 * by again, up to the limit listed, and the count of them all. Anything the API
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
/** What one click picks: a page's element, a Markdown block, or cells. */
const pickAnchor = z.union([elementAnchor, blockAnchor, cellsAnchor]);

/** A click's or an area's comment spot: the anchor, its box, the press it pairs with, and whether it extends the draft. */
function click<K extends 'page:pick' | 'page:area'>(type: K, anchor: typeof pickAnchor | typeof areaAnchor) {
  return z.pipe(
    z.object({ type: z.literal(type), anchor, rect: frameRect, title, press, extend: lenient(z.boolean()) }),
    z.transform((m) => ({
      type: m.type, anchor: m.anchor, rect: m.rect, title: m.title ?? null, press: m.press ?? null, extend: m.extend === true,
    })),
  );
}

/** One per type: a nested union where a type's message can also say `clear`. */
const pageToViewer = z.discriminatedUnion('type', [
  z.pipe(
    z.object({ type: z.literal('page:ready'), v: z.optional(z.number()), path: lenient(docPath) }),
    z.transform((m) => ({ type: m.type, path: m.path ?? null })),
  ),
  z.object({ type: z.literal('page:ground'), color: z.nullable(z.string().check(z.regex(RGB))) }),
  z.object({ type: z.literal('page:scroll'), x: coord, y: coord, moving: z.boolean() }),
  z.object({ type: z.literal('page:settled') }),
  z.object({ type: z.literal('page:open-file'), path: filePath, fragment: z.string().check(z.maxLength(MAX_DOC_PATH)) }),
  z.pipe(
    z.object({ type: z.literal('page:frame-loads'), doc: z.string().check(z.maxLength(MAX_ADDRESS)) }),
    z.transform((m) => ({ type: m.type, doc: m.doc })),
  ),
  z.pipe(
    z.object({ type: z.literal('page:loads'), path: docPath, paths: z.array(z.unknown()) }),
    z.transform((m) => ({
      type: m.type,
      path: m.path,
      // One odd name loses that name, not the report.
      paths: m.paths.slice(0, MAX_PAGE_LOADS).flatMap((p) => {
        const read = filePath.safeParse(p);
        return read.success ? [read.data] : [];
      }),
    })),
  ),
  z.pipe(
    z.object({ type: z.literal('page:positions'), items: z.array(z.unknown()) }),
    z.transform((m) => ({
      type: m.type,
      items: m.items.slice(0, MAX_POSITIONS).flatMap((item) => {
        const read = z.object({
          id: z.string(),
          rect: z.catch(z.nullable(frameRect), null),
          parts: lenient(z.array(frameRect).check(z.maxLength(MAX_AREA_ELEMENTS))),
        }).safeParse(item);
        if (!read.success) return [];
        const { id, rect, parts } = read.data;
        return [parts?.length ? { id, rect, parts } : { id, rect }];
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
  click('page:pick', pickAnchor),
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
  z.object({ type: z.literal('page:adding'), held: z.boolean(), next: z.boolean() }),
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
