import { PREFIXES } from './prefixes';
import type { Kind, Lens, Place, ViewSpec } from './types';

/**
 * A view on the palette's stack, holding what the reader left it with: going
 * back to it puts the field, the selection and the lead back as they were.
 */
export interface Frame {
  spec: ViewSpec;
  query: string;
  /** The row the reader moved to, by id, so rows arriving above it never move the selection out from under them. */
  chosenId: string | null;
  /**
   * Until they move, the first row leads, and once the view's rows settle
   * for a query the one leading keeps the lead: the server's finds arrive
   * later, some above it, and Enter must not start to mean another row a
   * beat after the reader saw this one lead.
   */
  lead: { query: string; id: string } | null;
}

export const ROOT: ViewSpec = { place: null, lens: null };

const frame = (spec: ViewSpec, query = ''): Frame => ({ spec, query, chosenId: null, lead: null });

const BY_PREFIX = new Map(Object.entries(PREFIXES).map(([kind, char]) => [char, kind as Kind]));
/** What a workspace holds, so what its scope narrows to; there the other prefixes are text. */
const IN_WORKSPACE: ReadonlySet<Kind> = new Set<Kind>(['files', 'threads', 'turns']);

export const isDrill = (spec: ViewSpec) => spec.lens?.via === 'drill';
const isPrefix = (spec: ViewSpec) => spec.lens?.via === 'prefix';
const placeKey = (place: Place) => (place.kind === 'workspace' ? `ws:${place.workspaceId}` : `thread:${place.threadId}`);

/** `text` past its prefix: the prefix said again, and the spaces after, say nothing more. */
function pastPrefix(text: string, char: string): string {
  let at = 0;
  while (at < text.length && (text[at] === char || /\s/.test(text[at]))) at += 1;
  return text.slice(at);
}

/**
 * What the field holds once `text` is typed (or pasted) into a view. A
 * prefix that starts the field at the top level (spaces before it aside)
 * asks for its kind alone: it becomes the chip and the rest is the query. In
 * a workspace's scope only what a workspace holds narrows it (`/ # "`). In a
 * kind's own scope its prefix typed again is dropped. Anywhere else, a
 * drill-in included, a prefix is text.
 */
export function typeInto(spec: ViewSpec, text: string): { enter: Kind | null; text: string } {
  const lead = text.trimStart();
  const kind = BY_PREFIX.get(lead[0]);
  if (!kind || isDrill(spec)) return { enter: null, text };
  const char = PREFIXES[kind];
  if (!spec.lens && (!spec.place || (spec.place.kind === 'workspace' && IN_WORKSPACE.has(kind)))) {
    const rest = pastPrefix(lead, char);
    // A quoted phrase pasted whole: its closing quote is not a word.
    return { enter: kind, text: kind === 'turns' && rest.endsWith('"') ? rest.slice(0, -1) : rest };
  }
  if (spec.lens?.kind === kind) return { enter: null, text: pastPrefix(lead, char) };
  return { enter: null, text };
}

/** The stack once `text` is typed into the top view's field: a prefix opens its kind's view over it. */
export function typeText(frames: readonly Frame[], text: string): readonly Frame[] {
  const top = frames[frames.length - 1];
  const { enter, text: rest } = typeInto(top.spec, text);
  if (enter) return [...frames, frame({ place: top.spec.place, lens: { kind: enter, via: 'prefix' } }, rest)];
  return [...frames.slice(0, -1), { ...top, query: rest, chosenId: null }];
}

/**
 * The stack once a lens is opened over the top view (Show all, a group's
 * header, a ticker's threads), in the same place. The field keeps its words,
 * except in a ticker's view, which finds what names the ticker.
 */
export function openLens(frames: readonly Frame[], lens: Lens): readonly Frame[] {
  const top = frames[frames.length - 1];
  return [...frames, frame({ place: top.spec.place, lens }, lens.kind === 'ticker' ? '' : top.query)];
}

/**
 * The stack once a row's place is entered, or the same stack when Tab there
 * would change nothing. A kind asked for by its prefix is a filter, not a
 * place, so the place takes its spot and leaving returns to where the kind
 * was typed. A drill-in stays under it, so leaving returns to it. Places do
 * not nest, except that a thread entered from its workspace's scope sits
 * above it; any other place replaces the stack.
 */
export function enterPlace(frames: readonly Frame[], place: Place): readonly Frame[] {
  const top = frames[frames.length - 1].spec;
  if (!top.lens && top.place && placeKey(top.place) === placeKey(place)) return frames;
  if (isPrefix(top)) return enterPlace(frames.slice(0, -1), place);
  const nests =
    !top.place ||
    isDrill(top) ||
    (top.place.kind === 'workspace' && place.kind === 'thread' && place.workspaceId === top.place.workspaceId);
  return nests ? [...frames, frame({ place, lens: null })] : [frames[0], frame({ place, lens: null })];
}

/** Whether Tab on a row with this place would narrow the search, for the footer to offer it. */
export function canEnter(frames: readonly Frame[], place: Place | undefined): place is Place {
  return !!place && enterPlace(frames, place) !== frames;
}

/**
 * The stack once the scope the last chip names is left, with any drill-in
 * opened over it; null where no chip is shown.
 */
export function leave(frames: readonly Frame[]): readonly Frame[] | null {
  let at = frames.length - 1;
  while (at > 0 && isDrill(frames[at].spec)) at -= 1;
  return at > 0 ? frames.slice(0, at) : null;
}

/**
 * One view back. Out of a drill-in the results come back as they were, or
 * for what was typed since (a ticker's view takes its words with it);
 * otherwise out of the scope. Null at the top level.
 */
export function back(frames: readonly Frame[]): readonly Frame[] | null {
  const top = frames[frames.length - 1];
  if (!isDrill(top.spec)) return leave(frames);
  const below = frames[frames.length - 2];
  const same = top.spec.lens?.kind === 'ticker' || top.query === below.query;
  return [...frames.slice(0, -2), same ? below : { ...below, query: top.query, chosenId: null }];
}

/** The view whose chips the field shows: the top one, or the scope a drill-in was opened in. */
export function scopeOf(frames: readonly Frame[]): ViewSpec {
  return frames.findLast((entry) => !isDrill(entry.spec))?.spec ?? ROOT;
}

/** What the server's search is limited to in a place, if anything. */
export interface SearchWithin {
  workspaceId?: string;
  threadId?: string;
}

export function searchWithin(place: Place | null): SearchWithin {
  if (place?.kind === 'workspace') return { workspaceId: place.workspaceId };
  if (place?.kind === 'thread') return { threadId: place.threadId };
  return {};
}
