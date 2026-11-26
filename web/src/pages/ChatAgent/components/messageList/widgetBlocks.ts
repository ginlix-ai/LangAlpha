import type { WidgetDraft } from '@/types/chat';
import { WIDGET_TOOL } from '../../session/stream/toolCallChunks';
import { callIdOfWidgetSegment, widgetSegmentId } from '../../session/stream/widgetIds';
import type { HtmlWidgetRenderBlock } from './buildRenderBlocks';
import type { ContentSegmentRecord, ToolCallProcessRecord } from './types';

export interface WidgetPlacement {
  /** By call id: the block drawn where that ShowWidget call sits, in place of its row. */
  atCall: Map<string, HtmlWidgetRenderBlock>;
  /** The widget segments drawn at their call, so not where they arrived. */
  absorbed: Set<string>;
  /** Widgets whose call is still being written, drawn after everything else. */
  trailing: HtmlWidgetRenderBlock[];
}

/** The block of a widget segment drawn where it arrived. */
export function widgetSegmentBlock(segment: ContentSegmentRecord): HtmlWidgetRenderBlock {
  return { type: 'html_widget', key: segment.widgetId!, widgetId: segment.widgetId!, widget: segment };
}

/**
 * Where a message's widgets draw. A widget draws where its call sits, so its
 * preview and the widget that replaces it are one block, under one key, in
 * one place, whatever landed between the call and its artifact (a parallel
 * call's row). Older threads named the artifact apart from the call; their
 * widget stays where it arrived. A draft whose call has not landed draws at
 * the end, where the call will land.
 */
export function placeWidgets(
  groupedSegments: ContentSegmentRecord[],
  toolCallProcesses: Record<string, ToolCallProcessRecord>,
  drafts: Record<string, WidgetDraft> | undefined,
): WidgetPlacement {
  const atCall = new Map<string, HtmlWidgetRenderBlock>();
  const absorbed = new Set<string>();
  // By the call that drew each: the artifact is named after its call.
  const segments = new Map<string, ContentSegmentRecord>();
  for (const s of groupedSegments) {
    const callId = s.type === 'html_widget' ? callIdOfWidgetSegment(s.widgetId) : null;
    if (callId) segments.set(callId, s);
  }
  for (const s of groupedSegments) {
    if (s.type !== 'tool_call' || !s.toolCallId || toolCallProcesses[s.toolCallId]?.toolName !== WIDGET_TOOL) continue;
    const widgetId = widgetSegmentId(s.toolCallId);
    const widget = segments.get(s.toolCallId);
    const preview = drafts?.[s.toolCallId];
    if (!widget && !preview) continue;
    if (widget) absorbed.add(widgetId);
    atCall.set(s.toolCallId, { type: 'html_widget', key: widgetId, widgetId, ...(widget ? { widget } : { preview }) });
  }
  const trailing: HtmlWidgetRenderBlock[] = [];
  for (const [key, preview] of Object.entries(drafts ?? {})) {
    if (toolCallProcesses[key]) continue;
    const widgetId = widgetSegmentId(key);
    trailing.push({ type: 'html_widget', key: widgetId, widgetId, preview });
  }
  return { atCall, absorbed, trailing };
}
