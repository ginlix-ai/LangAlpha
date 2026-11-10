/**
 * "Ask the agent about this layer": the context pill a chart's layer, or one
 * element of it, becomes in the composer beside the chart (`ChartComposer`).
 * Like a chart's "Add to context" button it is `source: 'chart'`, which the
 * composer sends as a `<details>` block under the message. The pill carries
 * pointers, not data: the agent has the layer's id and fetches the rest itself.
 */

import type { ContextPayload } from '@/components/ui/chat-input.types';
import type { LayerElementRef } from '../types/chartLayer';

export interface LayerAskRequest {
  symbol: string;
  timeframe: string;
  layerId: string;
  layerTitle: string;
  summary?: string | null;
  /** The element asked about; absent when the whole layer is. */
  element?: LayerElementRef;
}

/** What a composer's `addContext` takes, for a chart: a chip it adds to the draft. */
export type LayerAskContext = Extract<ContextPayload, { kind: 'chip' }> & { source: 'chart' };

const oneLine = (s: string) => s.replace(/\s+/g, ' ').trim();

/** The pill for a request: its label names what was picked, its body tells the agent where to look. */
export function layerAskContext(req: LayerAskRequest): LayerAskContext {
  const where = `${req.symbol} · ${req.timeframe}`;
  const el = req.element;
  const what = el
    ? (el.label ? `${el.kind} "${oneLine(el.label)}"` : el.kind)
    : null;
  const label = [where, oneLine(req.layerTitle), what && oneLine(what)].filter(Boolean).join(' · ');
  const lines = [
    `The user is asking about a chart layer you published on the ${where} chart${what ? `, specifically its ${what}` : ''}.`,
    `Layer: "${oneLine(req.layerTitle)}" (id ${req.layerId}).`,
  ];
  if (req.summary) lines.push(`Your summary of it: ${oneLine(req.summary)}`);
  if (el?.note) lines.push(`Your note on that element: ${oneLine(el.note)}`);
  if (el?.anchors?.length) {
    lines.push(`It sits at ${el.anchors.map((a) => `${a.time} @ ${a.price}`).join(', ')}.`);
  }
  return { kind: 'chip', label, snippet: lines.join('\n'), source: 'chart' };
}
