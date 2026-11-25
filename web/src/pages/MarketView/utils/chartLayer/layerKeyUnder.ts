import type { ISeriesApi, MouseEventParams, SeriesType, Time } from 'lightweight-charts';

import type { ChartLayerPrimitive } from './ChartLayerPrimitive';

/**
 * The element key of the layer shape under the pointer, by lightweight-charts'
 * own hover test: a price line or marker carries its key as its id, a series
 * maps back through `seriesKeys`, and on the price pane the primitive's
 * last-frame shapes answer for the drawings. The first key `accept` takes
 * wins, so a shape it passes over does not hide one beneath.
 */
export function layerKeyUnder(
  param: MouseEventParams<Time>,
  seriesKeys: ReadonlyMap<ISeriesApi<SeriesType>, string>,
  primitive: ChartLayerPrimitive | null,
  accept: (key: string) => boolean = () => true,
): string | null {
  const info = param.hoveredInfo;
  const id = info?.objectId ?? param.hoveredObjectId;
  if (typeof id === 'string' && accept(id)) return id;
  const series = info?.series ?? param.hoveredSeries;
  if (series) {
    const key = seriesKeys.get(series);
    if (key && accept(key)) return key;
  }
  if (primitive && param.point && (param.paneIndex ?? 0) === 0) {
    const hit = primitive.hitAt(param.point.x, param.point.y);
    if (hit && accept(hit.key)) return hit.key;
  }
  return null;
}
