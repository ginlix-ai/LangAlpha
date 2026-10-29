/**
 * Cards for a hand-off or new workspace the user approved in advance. No
 * approval request carried the call, so no proposal holds its card: it is
 * drawn from the tool's own result, which the server marks `preapproved`
 * (`src/tools/secretary/approvals.py`).
 */

import type { PTCAgentProposalState } from '@/types/chat';
import { dispatchResultFields, jsonObjectOf, type ToolResultLike } from './dispatchResult';

const HANDOFF_TOOLS = new Set(['delegate_to_analyst', 'ptc_agent']);

export type PreapprovedCard =
  | { type: 'ptc_agent'; proposal: PTCAgentProposalState }
  | { type: 'create_workspace'; proposal: Record<string, unknown> };

export interface ToolCallLike {
  toolName?: unknown;
  toolCall?: { id?: unknown; args?: Record<string, unknown> } | null;
  toolCallResult?: ToolResultLike | null;
}

export function preapprovedCard(
  toolName: unknown,
  args: Record<string, unknown> | undefined,
  tool: ToolResultLike,
  toolCallId?: string,
): PreapprovedCard | null {
  const isHandoff = HANDOFF_TOOLS.has(toolName as string);
  if (!isHandoff && toolName !== 'manage_workspaces') return null;
  const result = jsonObjectOf(tool.content);
  if (result?.preapproved !== true) return null;
  if (!isHandoff) {
    if (result.success !== true) return null;
    return {
      type: 'create_workspace',
      proposal: { status: 'approved', workspace_name: result.workspace_name, workspace_description: result.workspace_description },
    };
  }
  // A hand-off that failed after it was approved still draws its card, as
  // one approved on its card does: approved, and failed to start. A
  // continuation that failed still names its thread, which ran other turns;
  // only an unknown outcome leaves the card following it, for a run that may
  // yet turn up there.
  const { report_back = false, ...fields } = dispatchResultFields(tool);
  return {
    type: 'ptc_agent',
    proposal: {
      status: 'approved',
      question: typeof args?.question === 'string' ? args.question : '',
      ...(typeof result.workspace_name === 'string' ? { workspace_name: result.workspace_name } : {}),
      ...fields,
      report_back,
      ...(toolCallId ? { tool_call_id: toolCallId } : {}),
    },
  };
}

/** Whether a tool result is a hand-off, made without asking, that reports back. */
export function preapprovedReportBack(content: unknown): boolean {
  const result = jsonObjectOf(content);
  // A hand-off steered into a running analyst turn reports back as one dispatched.
  const handedOff = result?.status === 'dispatched' || result?.status === 'steered';
  return result?.preapproved === true && handedOff && result.report_back === true;
}

// By result record, which the stream replaces rather than edits: the
// transcript rebuilds on every chunk, and a card that kept its identity is one
// the rebuild can reuse instead of redrawing.
const byResult = new WeakMap<object, PreapprovedCard | null>();

/** The card a finished secretary call draws, or null when it asked first or
 *  failed before anything was approved. */
export function preapprovedCardOf(proc: ToolCallLike | undefined): PreapprovedCard | null {
  const result = proc?.toolCallResult;
  if (!result) return null;
  let card = byResult.get(result);
  if (card === undefined) {
    const id = proc.toolCall?.id;
    card = preapprovedCard(proc.toolName, proc.toolCall?.args, result, typeof id === 'string' ? id : undefined);
    byResult.set(result, card);
  }
  return card;
}
