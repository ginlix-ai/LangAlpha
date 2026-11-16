/**
 * What each kind of anchor is to the comment UI, in one table: a new kind
 * does not compile until every facet says what it does for it.
 */
import type { TFunction } from 'i18next';
import type {
  AreaAnchor, ArtifactCommentAnchor, BlockAnchor, CellsAnchor, PartEntry,
} from '@/types/api';
import { oneLine } from './commentModel';

type Kind = ArtifactCommentAnchor['kind'];
type Heading = { label: string; quote: string | null };

interface Facets<A extends ArtifactCommentAnchor> {
  /** What names the spot in a line: its label, or where cells are. */
  place(anchor: A): string | undefined;
  /** The entries it lists its parts by, in its order, when it lists every part it holds; else null. */
  partEntries(anchor: A): PartEntry[] | null;
  /** Each part by a short name, for a spot that is a list of parts. */
  partLabels(anchor: A): string[] | null;
  /** A new comment's spot as its box names it, and what it holds under the name. */
  heading(t: TFunction, anchor: A): Heading;
  /** Where a comment is, as its card's spot line says it. */
  phrase(t: TFunction, anchor: A): string;
  /** How the layer outlines it: as one element, each part of a list, or not at all (words are marked in the page). */
  outline: 'one' | 'list' | 'words';
}

/** Cells by where they are, as a sheet names them: "Model!D3:D6, F3:F6", or the ranges alone in a CSV. */
function cellsPlace(anchor: CellsAnchor): string {
  const names = anchor.ranges.map((r) => r.range).join(', ');
  if (!anchor.sheet) return names;
  const sheet = /^[A-Za-z_][A-Za-z0-9_.]*$/.test(anchor.sheet) ? anchor.sheet : `'${anchor.sheet.replace(/'/g, "''")}'`;
  return `${sheet}!${names}`;
}

/**
 * A Markdown block by its words: blocks under one heading share it as their
 * label, so the label alone would name every paragraph there alike.
 */
function blockName(block: { lines?: string; label?: string; quote?: string }): string {
  return block.quote ?? block.label ?? block.lines ?? '';
}

/** A block named by nothing in the file (a line outside any function) is named by its lines: "On line 6", "On lines 6-8". */
function onLines(t: TFunction, lines: string): string {
  return lines.includes('-') ? t('pageViewer.comments.onLines', { lines }) : t('pageViewer.comments.onLine', { line: lines });
}

/** An area by its size, the composer's header: "Selected area · 3 elements". */
export function areaTitle(t: TFunction, anchor: Pick<ArtifactCommentAnchor, 'count' | 'elements'>): string {
  return t('pageViewer.comments.areaTitle', { count: anchor.count ?? anchor.elements?.length ?? 0 });
}

const onLabel = (t: TFunction, label: string) => t('pageViewer.comments.onLabel', { label });
const onElement = (t: TFunction) => t('pageViewer.comments.onElement');

/** A spot named by its label where it has one, else as `unnamed` names it. */
function byLabel<A extends { label?: string }>(unnamed: (t: TFunction, anchor: A) => string) {
  return {
    place: (a: A) => a.label,
    heading: (t: TFunction, a: A): Heading => ({ label: a.label ? onLabel(t, a.label) : unnamed(t, a), quote: null }),
    phrase: (t: TFunction, a: A) => (a.label ? onLabel(t, oneLine(a.label, 60)) : unnamed(t, a)),
  };
}

const KINDS = {
  element: {
    ...byLabel(onElement),
    partEntries: () => null,
    partLabels: (a) => [a.label ?? a.quote ?? a.path ?? ''],
    outline: 'one',
  },
  named: { ...byLabel(onElement), partEntries: () => null, partLabels: () => null, outline: 'words' },
  // Words are named by the place they are in (the tile's label, else the
  // nearest heading, or a PDF's page) and quoted: they are what the owner means.
  text: {
    place: (a) => a.label,
    partEntries: () => null,
    partLabels: () => null,
    heading: (t, a) => ({
      label: a.label ? onLabel(t, a.label) : a.page ? t('pageViewer.comments.onPage', { page: a.page }) : t('pageViewer.comments.onText'),
      quote: a.quote ? t('chat.artifactComments.message.quote', { quote: a.quote }) : null,
    }),
    phrase: (t, a) => (a.quote ? t('pageViewer.comments.onQuote', { quote: oneLine(a.quote, 36) }) : t('pageViewer.comments.onText')),
    outline: 'words',
  },
  block: {
    ...byLabel((t, a: BlockAnchor) => onLines(t, a.lines)),
    partEntries: () => null,
    partLabels: (a) => [blockName(a)],
    outline: 'one',
  },
  // A drag too large to list counts the parts past its first ones, and is no list a click grows.
  area: {
    ...byLabel<AreaAnchor>(areaTitle),
    partEntries: (a) => (a.elements.length >= 2 && a.count === a.elements.length ? a.elements : null),
    partLabels: (a) => a.elements.map((e) => (e.lines ? blockName(e) : (e.label ?? e.quote ?? e.path ?? ''))),
    heading: (t, a) => ({
      label: listsEvery(a) ? t('pageViewer.comments.partsTitle', { count: a.elements.length }) : areaTitle(t, a),
      quote: a.label ?? null,
    }),
    outline: 'list',
  },
  // Cells have no label: their place names them. A range is listed by its
  // name and the start of what it showed, as the name alone reads the same
  // in every sheet.
  cells: {
    place: cellsPlace,
    partEntries: (a) => a.ranges,
    partLabels: (a) => a.ranges.map((r) => (r.values ? `${r.range} · ${oneLine(r.values, 24)}` : r.range)),
    heading: (t, a) => ({ label: onLabel(t, cellsPlace(a)), quote: a.ranges[0]?.values ?? null }),
    phrase: (t, a) => onLabel(t, oneLine(cellsPlace(a), 60)),
    outline: 'list',
  },
} satisfies { [K in Kind]: Facets<Extract<ArtifactCommentAnchor, { kind: K }>> };

function facets<A extends ArtifactCommentAnchor>(anchor: A): Facets<A> {
  return KINDS[anchor.kind] as unknown as Facets<A>;
}

export function anchorLabel(anchor: ArtifactCommentAnchor): string | undefined {
  return facets(anchor).place(anchor);
}

export function partEntries(anchor: ArtifactCommentAnchor): PartEntry[] | null {
  return facets(anchor).partEntries(anchor);
}

/**
 * An area that lists every part it holds, or a sheet's cells, which list
 * every range: what a modifier click grows or shrinks, and a chip's close
 * takes a part out of. As the page's bridge decides it (`listsEvery`).
 */
export function listsEvery(anchor: ArtifactCommentAnchor): boolean {
  return partEntries(anchor) !== null;
}

export function outlineOf(anchor: ArtifactCommentAnchor): Facets<ArtifactCommentAnchor>['outline'] {
  return facets(anchor).outline;
}

/**
 * The parts a spot is a list of, by their labels, in the order the anchor
 * lists them: an element's comment is a list of one, an area that lists
 * every part it holds a list of those. A modifier click adds to such a list
 * or takes a part out. Null for words, and for an area too large to list,
 * which a modifier click starts afresh.
 */
export function partLabels(anchor: ArtifactCommentAnchor): string[] | null {
  const kind = facets(anchor);
  return kind.outline === 'list' && !listsEvery(anchor) ? null : kind.partLabels(anchor);
}

export function draftHeading(t: TFunction, anchor: ArtifactCommentAnchor): Heading {
  return facets(anchor).heading(t, anchor);
}

export function spotPhrase(t: TFunction, anchor: ArtifactCommentAnchor): string {
  return facets(anchor).phrase(t, anchor);
}
