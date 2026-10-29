import type { ToolCallProcessRecord } from '../ToolCallDetailView';
import { isTaskTool } from '../toolDisplayConfig';

/** What a tool result opens at before its record is there to size it. */
const UNSIZED_DETAIL_WIDTH = 550;
/** What the file panel opens at before a drag has said otherwise. */
export const DEFAULT_PANEL_WIDTH = 850;

/** How wide a tool result wants the panel: a filing or a file read needs the room, a data card does not. */
export function detailPanelWidth(toolCallProcess: ToolCallProcessRecord | null): number {
  if (!toolCallProcess) return UNSIZED_DETAIL_WIDTH;
  const toolName = toolCallProcess.toolName || '';
  const artifactType = toolCallProcess.toolCallResult?.artifact?.type;
  if (artifactType === 'sec_filing' || toolName === 'Read') return DEFAULT_PANEL_WIDTH;
  if (isTaskTool(toolName)) return 750;
  return artifactType === 'company_overview' || artifactType === 'automations' ? 480 : 650;
}
