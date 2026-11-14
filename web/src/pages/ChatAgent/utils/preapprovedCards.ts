/**
 * Cards drawn from a call's own result rather than from a proposal: a
 * hand-off or new workspace the user approved in advance, which the server
 * marks `preapproved` (`src/tools/secretary/approvals.py`), and a workspace
 * `la workspaces create` made, whose approval the app asked for outside the
 * chat. A new workspace is worth a look of its own, so it leaves the activity
 * group for the card a workspace created in the chat has always drawn.
 */

import type { PTCAgentProposalState } from '@/types/chat';
import { dispatchResultFields, jsonObjectOf, type ToolResultLike } from './dispatchResult';
import { bashFailed, laStepOf } from './laCommand';

const HANDOFF_TOOLS = new Set(['delegate_to_analyst', 'ptc_agent']);

export type PreapprovedCard =
  | { type: 'ptc_agent'; proposal: PTCAgentProposalState }
  | { type: 'create_workspace'; proposal: Record<string, unknown> };

export interface ToolCallLike {
  toolName?: unknown;
  toolCall?: { id?: unknown; args?: Record<string, unknown> } | null;
  toolCallResult?: ToolResultLike | null;
  isFailed?: unknown;
  _liveState?: unknown;
}

const UUID = '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
/** `Created workspace <name> (<id>).`, the line `la workspaces create` prints. */
const CREATED_LINE = new RegExp(`\\((${UUID})\\)\\.\\s*$`, 'im');

/**
 * The id of the workspace a create printed, as its line or its `--json` row,
 * so the line follows that workspace through a rename and never another
 * created later under its old name.
 */
function createdWorkspaceId(content: unknown): string | undefined {
  if (typeof content !== 'string') return undefined;
  const row = jsonObjectOf(content);
  if (typeof row?.workspace_id === 'string') return row.workspace_id;
  return CREATED_LINE.exec(content)?.[1];
}

/**
 * The card for a workspace `la workspaces create` made, named as the command
 * named it. Only a command that ended well within its call: a failure, a
 * decline in the app included, keeps its row, and so does one moved to the
 * background, whose call returned before the workspace existed.
 */
function laWorkspaceCard(proc: ToolCallLike, result: ToolResultLike): PreapprovedCard | null {
  const step = laStepOf(proc.toolName as string | undefined, proc.toolCall?.args);
  if (step?.kind !== 'createWorkspace') return null;
  if (proc.isFailed === true || proc._liveState === 'failed') return null;
  if (bashFailed(result.content)) return null;
  if ((result.artifact as { command_id?: unknown } | null | undefined)?.command_id != null) return null;
  const ref = step.target?.type === 'workspace' ? step.target.ref : null;
  if (!ref || !('name' in ref)) return null;
  const workspaceId = createdWorkspaceId(result.content);
  return {
    type: 'create_workspace',
    proposal: {
      status: 'approved',
      workspace_name: ref.name,
      ...(step.description ? { workspace_description: step.description } : {}),
      ...(workspaceId ? { workspace_id: workspaceId } : {}),
    },
  };
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
      ...(typeof args?.model === 'string' && args.model ? { model: args.model } : {}),
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

/** The card a finished call draws in its own place: a secretary call made
 *  without asking, or a workspace `la` created. Null when it asked first, or
 *  failed before anything was approved or created. */
export function preapprovedCardOf(proc: ToolCallLike | undefined): PreapprovedCard | null {
  const result = proc?.toolCallResult;
  if (!result) return null;
  let card = byResult.get(result);
  if (card === undefined) {
    const id = proc.toolCall?.id;
    card = laWorkspaceCard(proc, result)
      ?? preapprovedCard(proc.toolName, proc.toolCall?.args, result, typeof id === 'string' ? id : undefined);
    byResult.set(result, card);
  }
  return card;
}
