import type { StructuredError } from '@/utils/rateLimitError';
import { withoutPending } from './toolCallChunks';

type MessageEnd = 'completed' | 'stopped' | 'failed' | 'paused' | 'disconnected';

/** Ending a client stream is not proof of server settlement. Only terminal
 * observations get a local clock; replay's completedAt remains authoritative. */
export function finalizeAssistantMessage<T extends object>(
  message: T,
  end: MessageEnd,
  observedAt = Date.now(),
): T & { isStreaming: false; completionObservedAt?: number } {
  const previous = 'completionObservedAt' in message ? message.completionObservedAt : undefined;
  const terminal = end === 'completed' || end === 'stopped' || end === 'failed';
  return withoutPending({
    ...message,
    isStreaming: false,
    completionObservedAt: terminal
      ? typeof previous === 'number' ? previous : observedAt
      : undefined,
    ...(end === 'stopped' ? { stopped: true } : {}),
    ...(end === 'failed' ? { error: true } : {}),
  });
}

/** Settles a turn that failed. Whatever it streamed stays its reply, and the
 * failure rides apart from it as `structuredError`, the one card the bubble
 * draws for it, so no renderer has to guess which text is the error. A
 * failure shown somewhere else (an internal error's banner) passes none. */
export function failAssistantMessage<T extends object>(
  message: T,
  failure?: StructuredError | null,
  observedAt = Date.now(),
): T & { isStreaming: false; completionObservedAt?: number } {
  const failed = finalizeAssistantMessage(message, 'failed', observedAt);
  return failure ? { ...failed, structuredError: failure } : failed;
}
