/**
 * The shapes a layer the agent drew holds, as a comment's element mark finds
 * one again. A layer is republished whole, so a shape is looked for by its id
 * when it has one, else its name, and only a shape with neither by its place
 * in its kind's list, which holds while the agent leaves that list alone.
 */
import type { ChartElementMark } from '@/types/artifacts';
import type { ElementIdentity } from '@/pages/MarketView/utils/chartLayer/elements';
import { markName } from './chartMarks';

type Shape = [key: string, identity: ElementIdentity];

/**
 * The shapes drawn, by layer and kind. Every mark is found again on each
 * repaint, and filtering the whole map per mark took a few milliseconds a
 * frame on a chart with several full layers and many comments; the layers
 * compile a new map whenever they change, so one index per map is enough.
 */
const indexes = new WeakMap<ReadonlyMap<string, ElementIdentity>, Map<string, Shape[]>>();

const bucket = (layer: string, kind: string) => `${layer}\u0000${kind}`;

function shapesOf(identities: ReadonlyMap<string, ElementIdentity>, layer: string, kind: string): readonly Shape[] {
  let index = indexes.get(identities);
  if (!index) {
    index = new Map();
    for (const shape of identities) {
      const at = bucket(shape[1].layer, shape[1].element_kind);
      const list = index.get(at);
      if (list) list.push(shape);
      else index.set(at, [shape]);
    }
    indexes.set(identities, index);
  }
  return index.get(bucket(layer, kind)) ?? [];
}

/** Where a mark's shape is among the shapes drawn now (`identities`, by element key), as its key; null where its layer, or the shape, is gone. */
export function findElement(identities: ReadonlyMap<string, ElementIdentity>, mark: ChartElementMark): string | null {
  const shapes = shapesOf(identities, mark.layer, mark.element_kind);
  // An id names one shape: with it gone, so is the shape, whatever now sits in its place.
  if (mark.element_id) return shapes.find(([, s]) => s.element_id === mark.element_id)?.[0] ?? null;
  const sameType = (s: ElementIdentity) => s.element_type === mark.element_type;
  // So does a name: another shape in its place is not it.
  if (mark.element_name) {
    return shapes.find(([, s]) => !!s.element_name && markName(s.element_name) === mark.element_name && (!mark.element_type || sameType(s)))?.[0] ?? null;
  }
  return shapes.find(([, s]) => s.index === mark.index && sameType(s))?.[0] ?? null;
}
