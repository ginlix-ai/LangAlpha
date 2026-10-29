/**
 * What a `delegate_to_analyst` result says about its hand-off, read once for
 * both places that draw one from it: a card the user approved (the result
 * patches its fields) and a card approved in advance (`preapprovedCards.ts`,
 * which has no other source).
 */
import type { PTCAgentProposalState } from '@/types/chat';

export type DispatchResultFields = Pick<
  PTCAgentProposalState,
  'thread_id' | 'workspace_id' | 'run_id' | 'steer_id' | 'report_back' | 'dispatch_failed' | 'dispatch_unknown'
>;

/** The JSON object a tool result is, else null. The leading-brace test keeps
 *  plain-text results (a decline) and every other tool's from being parsed. */
export function jsonObjectOf(content: unknown): Record<string, unknown> | null {
  if (typeof content !== 'string' || !content.startsWith('{')) return null;
  try {
    const parsed: unknown = JSON.parse(content);
    return parsed && typeof parsed === 'object' ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

/** A tool's result as the stream and a call's record carry it: the content,
 *  which the model reads, and the artifact beside it, which it does not. */
export interface ToolResultLike {
  content?: unknown;
  artifact?: unknown;
}

const id = (value: unknown): string | undefined => (typeof value === 'string' && value ? value : undefined);

/**
 * A dispatch that fails after the user approved it stays approved and says it
 * failed, and keeps the thread an unknown outcome names, where the run may
 * have started, so the card can still follow it. A result that is not the
 * dispatch's own JSON started nothing: the call was cancelled when the chat
 * changed agents, or the tool errored. The run the dispatch started is the
 * hand-off's own, which the card reads by id. A hand-off steered into a run
 * already going also names its steer, in the result's artifact rather than
 * its content, which the model reads.
 */
export function dispatchResultFields(tool: ToolResultLike): DispatchResultFields {
  const result = jsonObjectOf(tool.content);
  if (!result) return { dispatch_failed: true };
  // Only an explicit true started it: the server writes a boolean `success` on
  // every JSON result of the dispatch, and a decline is plain text.
  const failed = result.success !== true;
  const threadId = id(result.thread_id);
  const workspaceId = id(result.workspace_id);
  const runId = failed ? undefined : id(result.run_id);
  const { artifact } = tool;
  const steer = artifact && typeof artifact === 'object' && 'steer_id' in artifact ? artifact.steer_id : undefined;
  const steerId = failed ? undefined : id(steer);
  return {
    ...(threadId ? { thread_id: threadId } : {}),
    ...(workspaceId ? { workspace_id: workspaceId } : {}),
    ...(runId ? { run_id: runId } : {}),
    ...(steerId ? { steer_id: steerId } : {}),
    // Whether a report-back is actually wired, which the card's switch only asked for.
    ...(typeof result.report_back === 'boolean' ? { report_back: result.report_back } : {}),
    ...(failed ? { dispatch_failed: true } : {}),
    ...(failed && result.outcome === 'unknown_retained' ? { dispatch_unknown: true } : {}),
  };
}
