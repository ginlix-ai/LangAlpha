/**
 * Artifacts about the workspace rather than the transcript: a file the agent
 * wrote, a preview server it started, a chart layer it stored. They are
 * honored from either lane, since a subagent's changes the same workspace the
 * panels and charts show; the transcript draws nothing for them (a layer's
 * card comes from its call's tool result).
 */

import { applyChartLayerEvent } from '@/pages/MarketView/utils/chartLayerCache';
import type { ChartLayerArtifactPayload } from '@/pages/MarketView/types/chartLayer';
import type { SSEEvent } from '../types';
import type { StreamRuntime } from '../runtime';

/** Apply `event` if it is one of these. False for any other artifact. */
export function applySideChannelArtifact(
  event: SSEEvent,
  rt: Pick<StreamRuntime, 'queryClient' | 'onFileArtifact' | 'onPreviewUrl'>,
  refs: { isReconnect?: boolean },
): boolean {
  switch (event.artifact_type) {
    case 'chart_layer':
      void applyChartLayerEvent(rt.queryClient, event.payload as ChartLayerArtifactPayload | undefined, {
        replay: refs.isReconnect === true,
      });
      return true;
    case 'file_operation':
      rt.onFileArtifact?.(event);
      return true;
    case 'preview_url': {
      const payload = (event.payload || {}) as Record<string, unknown>;
      rt.onPreviewUrl?.({
        url: '', // resolved by ChatView via the authenticated endpoint
        port: payload.port as number,
        title: payload.title as string | undefined,
        command: payload.command as string | undefined,
        path: payload.path as string | undefined,
        loading: true,
      });
      return true;
    }
    default:
      return false;
  }
}
