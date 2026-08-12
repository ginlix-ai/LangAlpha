/**
 * Compatibility barrel for the split stream-event handlers (permanent).
 *
 * The implementations live in `session/stream/` (main transcript),
 * `session/subagents/` (task cards), and siblings here. Consumers and the
 * vi.mock partials keep this import path; new session modules must import
 * the leaves directly, never this barrel.
 */

export { provenanceRecordKey, provenanceEventToRecord } from '../../session/stream/provenance';
export {
  handleReasoningSignal,
  handleReasoningContent,
  handleTextContent,
  handleToolCalls,
  handleToolCallResult,
  handleProvenance,
  handleTodoUpdate,
  handleHtmlWidget,
  handleToolCallChunks,
} from '../../session/stream/mainEventHandlers';
export {
  isSubagentEvent,
  handleSubagentMessageChunk,
  handleSubagentToolCallChunks,
  handleSubagentToolCalls,
  handleSubagentToolCallResult,
  handleTaskSteeringAccepted,
} from '../../session/subagents/liveEventHandlers';
export { getOrCreateTaskRefs } from '../../session/streamRefs';
export { coerceSymbols, handleMarketWatchUpdate, type MarketWatchState } from '../../session/marketWatchEvents';
