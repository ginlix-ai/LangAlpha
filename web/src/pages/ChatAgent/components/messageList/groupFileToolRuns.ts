import type { ActivityItem, ToolActivityItem } from './activityTypes';
import { categorizeTool } from '../toolDisplayConfig';
import { calledOn } from '../../utils/agentPaths';

/** The verbs whose consecutive calls read as one act on a set of files. */
export const GROUPABLE_FILE_TOOLS = new Set(['Read', 'Edit', 'Write']);

/**
 * Splits a settled timeline into runs: a run is either one item, or several
 * consecutive calls of the same file verb. An agent reads four inputs in a
 * row, or edits one report eight times, and the timeline should say so in
 * one line rather than eight. Anything between two calls, a thought or a
 * different tool, ends the run: the order of events is what a timeline is for.
 */
export function groupFileToolRuns(items: ActivityItem[]): ActivityItem[][] {
  const runs: ActivityItem[][] = [];
  for (const item of items) {
    const last = runs[runs.length - 1];
    if (
      last
      && item.type === 'tool_call'
      && ordinarySettledFile(item)
      && ordinarySettledFile(last[0])
      && last[0].type === 'tool_call'
      && last[0].toolName === item.toolName
      // A settled failure carries `_liveState: 'completed'` like any other, so
      // nothing above this line separates it from the calls that worked.
      // `FileToolGroupRow` badges a group from `items.some(isFailed)`, so one
      // failure among four good reads reads as five failed files, and an
      // expanded Edit group shows a rejected replacement beside changes that
      // landed. Grouping is for calls that read as one act; a failure is its
      // own act.
      && failed(last[0]) === failed(item)
      // A run's pills name its files once each, and a pill opens its file on
      // this computer or, for a call on another, the call: so a run keeps to
      // one computer.
      && calledOn(last[0].toolCall?.args) === calledOn(item.toolCall?.args)
    ) {
      last.push(item);
      continue;
    }
    runs.push([item]);
  }
  return runs;
}

function failed(item: ActivityItem): boolean {
  return item.type === 'tool_call' && (item.isFailed === true || item._liveState === 'failed');
}

function ordinarySettledFile(item: ActivityItem): boolean {
  if (item.type !== 'tool_call' || item._liveState !== 'completed' || !filePathOf(item)) return false;
  const category = categorizeTool(item.toolName, item.toolCall, item.root);
  return GROUPABLE_FILE_TOOLS.has(item.toolName) && (category === 'fileRead' || category === 'fileEdit');
}

export function filePathOf(item: ToolActivityItem): string | null {
  const args = item.toolCall?.args;
  if (!args) return null;
  const path = args.file_path || args.filePath || args.path || args.filename;
  return typeof path === 'string' ? path : null;
}

export function editStrings(item: { toolCall?: { args?: unknown } }): { oldStr: string; newStr: string } {
  const args = (item.toolCall?.args || {}) as Record<string, unknown>;
  return {
    oldStr: (args.old_string || args.oldString || '') as string,
    newStr: (args.new_string || args.newString || '') as string,
  };
}
