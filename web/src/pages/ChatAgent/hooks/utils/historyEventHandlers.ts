/**
 * Compatibility barrel for the history replay handlers (permanent).
 *
 * The implementation lives in `session/history/historyHandlers.ts`. Consumers
 * (including SharedChatView outside ChatAgent) and the vi.mock partials keep
 * this import path; new session modules must import the leaf directly.
 */

export {
  isSubagentHistoryEvent,
  handleHistoryUserMessage,
  handleHistoryReasoningSignal,
  handleHistoryReasoningContent,
  handleHistoryTextContent,
  handleHistoryToolCalls,
  handleHistoryToolCallResult,
  handleHistorySteeringDelivered,
  handleHistoryTaskArtifactStatus,
  handleHistoryTodoUpdate,
  handleHistoryHtmlWidget,
} from '../../session/history/historyHandlers';
