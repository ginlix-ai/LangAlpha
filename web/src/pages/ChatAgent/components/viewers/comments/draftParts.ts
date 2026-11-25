/**
 * The parts of the comment being written, when it is a list of them: one
 * spot's comment is a list of one, an area that lists every part a list of
 * those. A modifier click grows or shrinks the list, a chip's close in the
 * box takes one out, and each change is sent as the whole new list.
 */
import { MAX_AREA_ELEMENTS } from '../html/pageProtocol';
import type { ArtifactCommentAnchor, PartEntry } from '@/types/artifacts';
import { outlineOf, partEntries, partKey } from '../../artifactComments/anchorKinds';
import type { PageAnchors } from './pageAnchors';
import type { SpotPicker } from './spotStrategy';

function inOrder(a: Element, b: Element): number {
  return a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1;
}

/** Whether two parts `anchor` lists are the same part. */
function samePart(anchor: ArtifactCommentAnchor, a: PartEntry, b: PartEntry): boolean {
  return partKey(anchor, a) === partKey(anchor, b);
}

/** How a viewer's spots make up parts (`SpotPicker.partsIn`, `holds`), its defaults filled in. */
interface PartModel {
  partsIn(spots: Element[]): Element[][];
  holds(part: Element, spot: Element): boolean;
}

export function partModel(picker: SpotPicker | undefined): PartModel {
  return {
    partsIn: (spots) => picker?.partsIn?.(spots) ?? spots.map((s) => [s]),
    holds: (part, spot) => (picker?.holds ? picker.holds(part, spot) : part === spot || part.contains(spot)),
  };
}

/**
 * The parts with `added` joined, in document order. A part around others
 * takes their place: one comment does not hold a quote and a line of it
 * both. Past `limit` parts the newest are left out, never a part already
 * picked. Spots join a part at a time, so a drag over thousands of lines is
 * one step.
 */
export function joined(base: Element[], added: Element[], model: PartModel, limit = MAX_AREA_ELEMENTS): Element[] {
  let out = base.slice();
  for (const step of model.partsIn([...new Set(added)].sort(inOrder))) {
    const have = new Set(out);
    const joining = step.filter((el) => !have.has(el));
    if (!joining.length) continue;
    const rest = out.filter((p) => !joining.some((el) => model.holds(p, el) || model.holds(el, p)));
    const next = [...rest, ...joining];
    // There are never more parts than spots, so the parts are counted only past the limit's worth of spots.
    if (rest.length >= limit && model.partsIn(next.sort(inOrder)).length > limit) break;
    out = next;
  }
  return out.sort(inOrder);
}

export class DraftParts {
  /**
   * The parts this page sent for the draft, in the order its anchor lists
   * them. They hold until a spot starts a new comment: found again from the
   * anchor, a part could be another block with the same words, and the
   * viewer's echo of the new draft lags a quick second click.
   */
  private own: Element[] | null = null;
  /** The anchor of those spots, which lists them. */
  private sent: ArtifactCommentAnchor | null = null;
  private readonly model: PartModel;

  constructor(private readonly anchors: PageAnchors, private readonly picker: SpotPicker | undefined) {
    this.model = partModel(picker);
  }

  /** What was just sent for the draft: its spots, and the anchor that lists them. */
  keep(spots: Element[] | null, anchor: ArtifactCommentAnchor | null): void {
    this.own = spots;
    this.sent = anchor;
  }

  private ownLive(): Element[] | null {
    if (this.own && !this.own.every((el) => el.isConnected)) this.keep(null, null);
    return this.own;
  }

  /**
   * The draft's parts not on the page now, in a viewer that keeps them
   * through a change (`SpotPicker.withAway`): a chart takes a mark off the
   * page while it is out of view, and it is still the draft's. Elsewhere a
   * part not found again is gone (the file changed under the draft), and none
   * are kept.
   */
  away(): PartEntry[] {
    if (!this.picker?.withAway || this.ownLive()) return [];
    const found = this.found();
    const listed = found && partEntries(found.anchor);
    return listed && listed.length === found.parts.length ? listed.filter((_, i) => found.parts[i] === null) : [];
  }

  /** The draft's parts found again from its anchor: one spot's, or each of a list of every part. Null for words, and an area too large to list. */
  private found(): { parts: (Element[] | null)[]; anchor: ArtifactCommentAnchor } | null {
    const draft = this.anchors.draft();
    const found = draft?.found;
    if (!draft || !found) return null;
    // Every part off the page, in a viewer that keeps them: a list of none found.
    if ('parked' in found && this.picker?.withAway) {
      const listed = partEntries(draft.anchor);
      return listed ? { parts: listed.map(() => null), anchor: draft.anchor } : null;
    }
    if (!('parts' in found)) return null;
    return outlineOf(draft.anchor) === 'list' && !partEntries(draft.anchor) ? null : { parts: found.parts, anchor: draft.anchor };
  }

  /** The draft's parts: what was sent for it, else found again from its anchor. Empty when nothing is being written. */
  current(): Element[] {
    const own = this.ownLive();
    if (own) return own.slice();
    return [...new Set(this.found()?.parts.flatMap((part) => part ?? []))].sort(inOrder);
  }

  /** `base` with `added` joined (`joined`), as this viewer cuts its spots into parts. */
  join(base: Element[], added: Element[], limit?: number): Element[] {
    return joined(base, added, this.model, limit);
  }

  /**
   * The draft's parts without `part`, an entry its anchor lists: the spots
   * left, and the parts kept off the page (`away`). Nothing when it lists no
   * such part (a press on a list since changed), or `part` is the last one:
   * the last part stays, as a document's does. A part not found again and
   * not kept goes with it, as on any change: only what is here can be sent.
   */
  without(part: PartEntry): { spots: Element[]; away: PartEntry[] } {
    const away = this.away();
    const own = this.ownLive();
    const found = own ? null : this.found();
    const anchor = own ? this.sent : (found?.anchor ?? null);
    if (!anchor) return { spots: [], away };
    const none = { spots: [], away: [] };
    const rest = away.filter((entry) => !samePart(anchor, entry, part));
    if (rest.length < away.length) {
      // A part off the page leaves alone; the spots stay as they are.
      const spots = this.current();
      return spots.length || rest.length ? { spots, away: rest } : none;
    }
    const listed = partEntries(anchor);
    const parts = own ? this.model.partsIn(own) : (found?.parts ?? []);
    const at = listed ? listed.findIndex((entry) => samePart(anchor, entry, part)) : -1;
    if (at === -1 || parts.length !== listed?.length) return none;
    const spots = parts.filter((p, i): p is Element[] => i !== at && p !== null).flat();
    return spots.length || away.length ? { spots, away } : none;
  }

  /** The parts after a modifier click on `spot`: the part it is in leaves, else it joins. Null when nothing changes: the last part stays. */
  toggled(spot: Element): Element[] | null {
    const base = this.current();
    const at = base.findIndex((p) => this.model.holds(p, spot));
    if (at === -1) return this.join(base, [spot]);
    // Parts kept off the page count: the one on it may leave them.
    if (base.length < 2 && !this.away().length) return null;
    base.splice(at, 1);
    return base;
  }

  /** While a click adds parts, the part `spot` is in: a click anywhere on it takes it out, so all of it is outlined. */
  around(spot: Element): Element {
    const parts = this.current();
    return parts.length < 2 ? spot : (parts.find((p) => this.model.holds(p, spot)) ?? spot);
  }
}
