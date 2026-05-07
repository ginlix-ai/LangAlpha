import { createContext, useContext } from 'react';
import type { SubagentTelemetry } from '../utils/resolveSubagentTelemetry';

export type SubagentTelemetryResolver = (subagentId: string) => SubagentTelemetry | undefined;

// Consumed at the leaf (SubagentTaskMessageContent) so live token-tick
// re-renders bypass the memoized MessageBubble / MessageContentSegments
// trees instead of busting their memo on every SSE event.
export const SubagentTelemetryContext = createContext<SubagentTelemetryResolver | null>(null);

export function useSubagentTelemetry(subagentId: string | undefined): SubagentTelemetry | undefined {
  const resolver = useContext(SubagentTelemetryContext);
  if (!resolver || !subagentId) return undefined;
  return resolver(subagentId);
}
