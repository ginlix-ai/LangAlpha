/**
 * Boxes drawn on a picture: an image, or a PDF's pages. A box is saved as
 * fractions of the surface it is on, so it lands in the same place at any
 * zoom, and is a spot like a Markdown block is: an element the page lays on
 * the surface (`.comment-region`), which the layer outlines, lists as a part
 * and takes out again as it does any other spot. The elements hold nothing
 * the viewer draws; they only mark where each box is.
 */
import type { ArtifactCommentAnchor, ArtifactCommentRegionBox, RegionAnchor } from '@/types/artifacts';
import { MAX_REGION_BOXES } from '../../html/pageProtocol';
import type { Edges, Found, SpotPicker, SpotStrategy } from '../spotStrategy';

/** What a box is drawn on, and the PDF page it is, if any. */
export interface Surface {
  el: HTMLElement;
  page?: number;
}

export interface RegionSurfaces {
  /** The surface under (x, y), in client coordinates. */
  at(x: number, y: number): Surface | null;
  /** The surface boxes on `page` (none on an image) sit on, or the element they wait in until it is laid out; null where there is no such page. */
  of(page: number | undefined): Surface | { parked: Element } | null;
  /** Whether a click drops a point, besides a drag drawing a box. */
  points: boolean;
}

/** Marks what the page lays over the content itself, which a change to the content is not. */
export const OWN_ATTR = 'data-comment-own';
const LAYER_CLASS = 'comment-regions';
const BOX_CLASS = 'comment-region';
/** The least a box is across, in pixels: a shorter drag is a slip, not a box. */
const MIN_BOX_PX = 6;
/** How large a point is outlined and pinned, in pixels. */
const POINT_PX = 14;

const round3 = (n: number) => Math.round(n * 1000) / 1000;
/** A box's side, never rounded to nothing: on a surface over 12,000px a 6px side would be, and a box of no width is no box. */
const side = (n: number) => Math.max(0.001, round3(n));
const clamp01 = (n: number) => Math.min(1, Math.max(0, n));

function keyOf(b: ArtifactCommentRegionBox): string {
  return `${b.page ?? ''}|${b.x}|${b.y}|${b.w}|${b.h}`;
}

/** Whether a change the content observer saw is only the page's own boxes being laid or moved. */
export function ownChange(record: MutationRecord): boolean {
  const own = (n: Node) => n instanceof Element && !!n.closest(`[${OWN_ATTR}]`);
  if (record.type !== 'childList') return own(record.target);
  const nodes = [...record.addedNodes, ...record.removedNodes];
  return nodes.length > 0 && nodes.every((n) => n instanceof Element && n.hasAttribute(OWN_ATTR));
}

/** A box's anchor entry, read off the element that marks it. */
export function boxOf(spot: Element): ArtifactCommentRegionBox | null {
  const raw = spot instanceof HTMLElement ? spot.dataset.box : undefined;
  if (!raw) return null;
  try {
    return JSON.parse(raw) as ArtifactCommentRegionBox;
  } catch {
    return null;
  }
}

/**
 * The region picker and finder over `surfaces`. The elements marking boxes
 * are laid as anchors are found and boxes drawn, and the end of a pass of
 * finds lets go of the ones it did not name.
 */
export function regionSpots(surfaces: RegionSurfaces, base: SpotStrategy | null = null): SpotStrategy {
  const laid = new Map<string, { el: HTMLElement; pass: number }>();
  let pass = 0;

  const layerOf = (surface: Element): Element => {
    const found = [...surface.children].find((c) => c.classList.contains(LAYER_CLASS));
    if (found) return found;
    const layer = document.createElement('div');
    layer.className = LAYER_CLASS;
    layer.setAttribute(OWN_ATTR, '');
    surface.appendChild(layer);
    return layer;
  };

  /** The element marking `box` on `surface`, laid there if it is not; kept through this pass and `keepFor` more. */
  const lay = (surface: Surface, box: ArtifactCommentRegionBox, keepFor = 0): HTMLElement => {
    const key = keyOf(box);
    const had = laid.get(key);
    let el = had?.el;
    if (!el || !surface.el.contains(el)) {
      el?.remove();
      el = document.createElement('div');
      el.className = BOX_CLASS;
      el.setAttribute(OWN_ATTR, '');
      el.dataset.box = JSON.stringify(box);
      const point = box.w === 0;
      el.dataset.point = point ? 'true' : 'false';
      Object.assign(el.style, {
        left: `${box.x * 100}%`,
        top: `${box.y * 100}%`,
        width: point ? '0' : `${box.w * 100}%`,
        height: point ? '0' : `${box.h * 100}%`,
      });
      layerOf(surface.el).appendChild(el);
    }
    laid.set(key, { el, pass: Math.max(pass + keepFor, had?.pass ?? 0) });
    return el;
  };

  /** A box in client coordinates on `surface`, as fractions of it; null for one too small to be meant. */
  const fractions = (surface: Surface, box: Edges): ArtifactCommentRegionBox | null => {
    const s = surface.el.getBoundingClientRect();
    if (!s.width || !s.height) return null;
    const left = Math.max(box.left, s.left);
    const top = Math.max(box.top, s.top);
    const right = Math.min(box.right, s.right);
    const bottom = Math.min(box.bottom, s.bottom);
    if (right - left < MIN_BOX_PX || bottom - top < MIN_BOX_PX) return null;
    // The size first, then a start that keeps it on the surface: a start rounded to its far edge would not.
    const w = side(Math.min(1, (right - left) / s.width));
    const h = side(Math.min(1, (bottom - top) / s.height));
    const x = Math.min(round3(clamp01((left - s.left) / s.width)), round3(1 - w));
    const y = Math.min(round3(clamp01((top - s.top) / s.height)), round3(1 - h));
    return { ...(surface.page ? { page: surface.page } : {}), x, y, w, h };
  };

  const picker: SpotPicker = {
    spotAt: (target) => target.closest(`.${BOX_CLASS}`),
    spotsIn: () => [],
    labelOf: () => null,
    anchorOf(spots) {
      const boxes = spots.flatMap((s) => boxOf(s) ?? []).slice(0, MAX_REGION_BOXES);
      // All on pages or none: a picture has no pages, and a PDF's every box is on one.
      return boxes.length ? { kind: 'region', boxes } : null;
    },
    rectsOf(spot) {
      if (!spot.isConnected) return [];
      const r = spot.getBoundingClientRect();
      if (spot instanceof HTMLElement && spot.dataset.point === 'true') {
        // About its centre: while the add key is held a point takes clicks around it, and its box grows.
        const half = POINT_PX / 2;
        return [new DOMRect(r.left + r.width / 2 - half, r.top + r.height / 2 - half, POINT_PX, POINT_PX)];
      }
      return [r];
    },
    draw(box, from) {
      const surface = surfaces.at(from.x, from.y);
      const drawn = surface && fractions(surface, box);
      // Kept through the next pass, which may come before the layer names it as the draft.
      // Too thin to be a box: where a click drops a point, the drag does too, from where it began.
      const spot = surface && drawn ? lay(surface, drawn, 1) : picker.pointAt?.(from.x, from.y);
      return spot ? [spot] : [];
    },
    pointAt(x, y) {
      if (!surfaces.points) return null;
      const surface = surfaces.at(x, y);
      const s = surface?.el.getBoundingClientRect();
      if (!surface || !s?.width || !s.height) return null;
      const at = { x: round3(clamp01((x - s.left) / s.width)), y: round3(clamp01((y - s.top) / s.height)) };
      return lay(surface, { ...(surface.page ? { page: surface.page } : {}), ...at, w: 0, h: 0 }, 1);
    },
  };

  const findRegion = (anchor: RegionAnchor): Found | null => {
    const parts: (Element[] | null)[] = [];
    for (const box of anchor.boxes) {
      const on = surfaces.of(box.page);
      if (on && 'parked' in on) return { parked: on.parked };
      parts.push(on ? [lay(on, box)] : null);
    }
    return parts.some(Boolean) ? { parts } : null;
  };

  return {
    ...base,
    fresh() {
      base?.fresh?.();
      pass += 1;
    },
    settled() {
      base?.settled?.();
      for (const [key, { el, pass: kept }] of laid) {
        if (kept >= pass) continue;
        el.remove();
        laid.delete(key);
      }
    },
    spots: picker,
    find(anchor: ArtifactCommentAnchor) {
      return anchor.kind === 'region' ? findRegion(anchor) : (base?.find(anchor) ?? null);
    },
  };
}
