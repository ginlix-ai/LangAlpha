import type { MessageRecord } from '../hooks/utils/types';

/**
 * The message that made a tool call, which is not always the one being written
 * when the answer arrives.
 *
 * A call stopped at a gate is answered in the turn *after* the one that made
 * it, so its result reaches the client attributed to a fresh assistant message
 * that has no record of the call. Pairing on that message alone drops the
 * result, and with it the artifact: an approved order kept its row in the
 * timeline but lost the receipt that is its only record of what the brokerage
 * did. Searched newest first, so a regenerated turn's copy of a call wins over
 * the one it replaced.
 */
export function ownerOfToolCall(
  messages: MessageRecord[],
  toolCallId: string,
): string | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const processes = messages[i].toolCallProcesses as Record<string, unknown> | undefined;
    if (processes && processes[toolCallId]) return messages[i].id as string;
  }
  return null;
}
