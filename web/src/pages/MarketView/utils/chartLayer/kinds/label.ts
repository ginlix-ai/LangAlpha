import type { LabelDrawing } from '../../../types/chartLayer';
import { extentOf, type SceneBase } from '../scene';
import type { IP } from '../timeGrid';
import type { DrawingKind } from './types';

export interface LabelScene extends SceneBase {
  kind: 'label';
  at: IP;
  text: string;
  position: 'above' | 'below' | 'left' | 'right';
}

export const label: DrawingKind<LabelDrawing, LabelScene> = {
  nameKey: 'marketView.layerExplain.types.label',
  times: (d) => [d.at.time],
  anchors: (d) => [d.at],
  resolve(d, { base, P }) {
    const at = P(d.at);
    if (!at || !d.text) return null;
    return { ...base, kind: 'label', at, text: d.text, position: d.position ?? 'above' };
  },
  extent: (s) => extentOf([s.at]),
  paintOver(p, d) {
    const pt = p.P(d.at);
    if (!pt) return;
    p.dot(pt, 2.5, d.color);
    const off = 14;
    const pos = d.position;
    const x = pos === 'left' ? pt.x - 8 : pos === 'right' ? pt.x + 8 : pt.x;
    const y = pos === 'above' ? pt.y - off : pos === 'below' ? pt.y + off : pt.y;
    const align = pos === 'left' ? 'right' : pos === 'right' ? 'left' : 'center';
    p.hit({ key: d.key, t: 'dot', c: pt, r: 6 });
    p.nameChip(d, d.text, { x, y, align }, 100, { movable: false });
  },
};
