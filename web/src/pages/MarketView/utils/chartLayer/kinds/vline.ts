import type { VLineDrawing } from '../../../types/chartLayer';
import { CHIP_H, EDGE } from '../canvasLabels';
import { dashFor } from '../format';
import type { SceneBase } from '../scene';
import { timeToIndex } from '../timeGrid';
import type { DrawingKind } from './types';

export interface VLineScene extends SceneBase {
  kind: 'vline';
  i: number;
  dash: number[];
}

export const vline: DrawingKind<VLineDrawing, VLineScene> = {
  nameKey: 'marketView.layerExplain.types.vline',
  times: (d) => [d.time],
  anchors: (d) => [{ time: d.time, price: NaN }],
  resolve(d, { base, grid }) {
    const i = timeToIndex(grid, d.time);
    if (i == null) return null;
    return { ...base, kind: 'vline', i, dash: dashFor(d.style, 'dashed') };
  },
  extent: (s) => ({ i0: s.i, i1: s.i, p0: -Infinity, p1: Infinity, openLeft: false, openRight: false, anyPrice: true }),
  paintOver(p, d) {
    const x = p.X(d.i);
    if (x == null || x < -2 || x > p.w + 2) return;
    p.line({ x, y: 0 }, { x, y: p.h }, d.color, p.strokeWidth(d, 1), d.dash, 0.85);
    p.seg(d.key, { x, y: 0 }, { x, y: p.h }, 4);
    if (d.label) p.nameChip(d, d.label, { x, y: EDGE + CHIP_H / 2 + 18, align: 'center' }, 55);
  },
};
