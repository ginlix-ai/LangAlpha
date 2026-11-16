/**
 * What a viewer drawn in the app tells `ViewerPage` about its own content:
 * how a comment is found again, and what a click (`spots`) or selected words
 * (`words`) save, where the viewer takes them. The page does the rest of the
 * comment protocol the same way for every viewer, as a framed HTML file's
 * bridge does it for a page. What the strategies share is in `spotKit`.
 */
import type { AreaAnchor, ArtifactCommentAnchor, BlockAnchor, CellsAnchor, TextAnchor } from '@/types/artifacts';

/** A box in client coordinates. */
export interface Edges {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

/**
 * Where a stored anchor is now: each listed part's spots (null where not
 * found), the words it quotes, or the element it waits in until the viewer
 * draws it, which `show` does on a scroll to it.
 */
export type Found = { parts: (Element[] | null)[] } | { range: Range } | { parked: Element; show?: () => void };

/** What a click in the app saves: a Markdown block or a run of code, a range of cells, or several as an area. */
export type SpotAnchor = BlockAnchor | CellsAnchor | AreaAnchor;

/** What a click or a drag picks, in a viewer whose content has spots: a Markdown block, a line of code, a range of cells. */
export interface SpotPicker {
  /** The spot a click on `target` picks, or null where nothing takes a comment. */
  spotAt(target: Element): Element | null;
  /** The spots a box dragged over the content touches, in document order. */
  spotsIn(box: Edges): Element[];
  /** What the outline under the pointer names a spot by. */
  labelOf(spot: Element): string | null;
  /**
   * The anchor that one spot, or several in document order, save. Several
   * are listed one by one, in the order given, so the page can grow or
   * shrink the list part by part; past the API's limit the rest are counted.
   */
  anchorOf(spots: Element[]): SpotAnchor | null;
  /** The boxes a spot occupies now, in client coordinates; none while it is not laid out. */
  rectsOf(spot: Element): DOMRect[];
  /**
   * Spots in document order cut into the parts an anchor of them lists, in a
   * viewer where one part spans several (a run of code lines). Left out,
   * each spot is a part.
   */
  partsIn?(spots: Element[]): Element[][];
  /**
   * Whether `part` holds `spot`, itself included, where spots nest by more
   * than the DOM: a sheet's range stands as an element of its own, and holds
   * the cells and ranges inside it. Left out, the DOM's nesting.
   */
  holds?(part: Element, spot: Element): boolean;
}

export interface SpotStrategy {
  /** Where an anchor is now, or null where nothing of it is. */
  find(anchor: ArtifactCommentAnchor): Found | null;
  /**
   * A pass of `find`s starts, after which the content may have changed: a
   * strategy that reads the whole content once for every anchor of a pass
   * lets go of the last pass's reading here.
   */
  fresh?(): void;
  /** Left out where nothing is clicked (a PDF's page): every drag is then the browser's own selection. */
  spots?: SpotPicker;
  /** The anchor of words selected in the content, already grown to whole words; left out where words take none (a sheet's cells). */
  words?(range: Range): TextAnchor | null;
  /** The part of selected words one comment holds, where that is less than all (a PDF's, the page they start on); left out, all of them. */
  clip?(range: Range): Range;
}
