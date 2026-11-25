/**
 * A chart's marks as values: the prices and names they keep, and which two
 * are the same mark, whatever picture one of them has gained since it was picked.
 */
import type { ChartMark } from '@/types/artifacts';
import { clip, MAX_LABEL } from '../spotKit';

/** A price as a mark keeps it and the UI names it: six significant digits, as the layer read-back writes prices. */
export function markPrice(price: number): number {
  return Number(price.toPrecision(6));
}

/**
 * A shape's name as a mark keeps it: cut to the API's limit, which a label
 * drawing's text can run past. A shape is found again by its name cut the
 * same way.
 */
export function markName(name: string): string {
  return clip(name, MAX_LABEL);
}

/**
 * What tells one mark from another: its place, and for an element, which
 * element of which layer. One with no id is also told by its type and name,
 * as two shapes without ids can share a place in their kind's list.
 */
export function markKey(mark: ChartMark): string {
  switch (mark.type) {
    case 'region':
      return `region|${mark.start}|${mark.end}|${mark.low}|${mark.high}`;
    case 'point':
      return `point|${mark.time}|${mark.price}`;
    case 'element': {
      const key = `element|${mark.layer}|${mark.element_kind}|${mark.element_id ?? ''}|${mark.index}|${mark.time}|${mark.price}`;
      return mark.element_id ? key : `${key}|${mark.element_type ?? ''}|${mark.element_name ? markName(mark.element_name) : ''}`;
    }
    default:
      return mark satisfies never;
  }
}
