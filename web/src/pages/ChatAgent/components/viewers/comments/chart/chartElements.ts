/**
 * The shapes a layer the agent drew holds, as a comment's element mark finds
 * one again. A layer is republished whole, so a shape is looked for by its id
 * when it has one, else its name, and only a shape with neither by its place
 * in its kind's list, which holds while the agent leaves that list alone.
 */
import type { ChartElementMark } from '@/types/artifacts';
import type { ElementIdentity } from '@/pages/MarketView/utils/chartLayer/elements';
import { markName } from './chartMarks';

/** Where a mark's shape is among the shapes drawn now (`identities`, by element key), as its key; null where its layer, or the shape, is gone. */
export function findElement(identities: ReadonlyMap<string, ElementIdentity>, mark: ChartElementMark): string | null {
  const shapes = [...identities].filter(([, s]) => s.layer === mark.layer && s.element_kind === mark.element_kind);
  // An id names one shape: with it gone, so is the shape, whatever now sits in its place.
  if (mark.element_id) return shapes.find(([, s]) => s.element_id === mark.element_id)?.[0] ?? null;
  const sameType = (s: ElementIdentity) => s.element_type === mark.element_type;
  // So does a name: another shape in its place is not it.
  if (mark.element_name) {
    return shapes.find(([, s]) => !!s.element_name && markName(s.element_name) === mark.element_name && (!mark.element_type || sameType(s)))?.[0] ?? null;
  }
  return shapes.find(([, s]) => s.index === mark.index && sameType(s))?.[0] ?? null;
}
