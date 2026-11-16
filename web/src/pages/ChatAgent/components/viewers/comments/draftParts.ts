/**
 * The parts of the comment being written, when it is a list of them: one
 * spot's comment is a list of one, an area that lists every part a list of
 * those. A modifier click grows or shrinks the list, a chip's close in the
 * box takes one out, and each change is sent as the whole new list.
 */
import { MAX_AREA_ELEMENTS } from '../html/pageProtocol';
import type { PartEntry } from '@/types/artifacts';
import { outlineOf, partEntries } from '../../artifactComments/anchorKinds';
import type { PageAnchors } from './pageAnchors';
import type { SpotPicker } from './spotStrategy';

function inOrder(a: Element, b: Element): number {
  return a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1;
}

function samePart(a: PartEntry, b: PartEntry): boolean {
  if ('range' in a || 'range' in b) return 'range' in a && 'range' in b && a.range === b.range;
  return a.path === b.path && a.lines === b.lines && a.quote === b.quote;
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
  private listed: PartEntry[] | null = null;
  private readonly model: PartModel;

  constructor(private readonly anchors: PageAnchors, picker: SpotPicker | undefined) {
    this.model = partModel(picker);
  }

  /** What was just sent for the draft: its spots, and the entries an area listed for them. */
  keep(spots: Element[] | null, listed: PartEntry[] | null): void {
    this.own = spots;
    this.listed = listed;
  }

  private ownLive(): Element[] | null {
    if (this.own && !this.own.every((el) => el.isConnected)) this.keep(null, null);
    return this.own;
  }

  /** The draft's parts found again from its anchor: one spot's, or each of a list of every part. Null for words, and an area too large to list. */
  private found(): { parts: (Element[] | null)[]; listed: PartEntry[] | null } | null {
    const draft = this.anchors.draft();
    const found = draft?.found;
    if (!draft || !found || !('parts' in found)) return null;
    const listed = partEntries(draft.anchor);
    return outlineOf(draft.anchor) === 'list' && !listed ? null : { parts: found.parts, listed };
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
   * The draft's parts without `part`, an entry its anchor lists; none when it
   * lists no such part (a press on a list since changed). A part not found
   * again goes with it, as on any change: only what is here can be sent.
   */
  without(part: PartEntry): Element[] {
    const own = this.ownLive();
    const found = own ? null : this.found();
    const listed = own ? this.listed : (found?.listed ?? null);
    const parts = own ? this.model.partsIn(own) : (found?.parts ?? []);
    const at = listed ? listed.findIndex((entry) => samePart(entry, part)) : -1;
    if (at === -1 || parts.length !== listed?.length) return [];
    return parts.filter((p, i): p is Element[] => i !== at && p !== null).flat();
  }

  /** The parts after a modifier click on `spot`: the part it is in leaves, else it joins. Null when nothing changes: the last part stays. */
  toggled(spot: Element): Element[] | null {
    const base = this.current();
    const at = base.findIndex((p) => this.model.holds(p, spot));
    if (at === -1) return this.join(base, [spot]);
    if (base.length < 2) return null;
    base.splice(at, 1);
    return base;
  }

  /** While a click adds parts, the part `spot` is in: a click anywhere on it takes it out, so all of it is outlined. */
  around(spot: Element): Element {
    const parts = this.current();
    return parts.length < 2 ? spot : (parts.find((p) => this.model.holds(p, spot)) ?? spot);
  }
}
