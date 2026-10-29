import React from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { cn } from '@/lib/utils';
import { XCircle } from 'lucide-react';
import { getDisplayName, isTaskTool } from './toolDisplayConfig';
import { ToolIcon } from './ToolIcon';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import { useTranslation } from 'react-i18next';
import ToolCallDetailView, { type ToolCallProcessRecord, type SubagentInfo } from './ToolCallDetailView';
import { taskCardStatusKind } from './taskStatusUi';
import { isToolCallFailed } from './filePanel/toolCallFailure';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';

interface DetailPanelProps {
  toolCallProcess: ToolCallProcessRecord | null;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
}

/**
 * A tool call's result. On desktop it is the body of a Files tab, whose strip
 * already names it; on mobile it is the body of a bottom sheet, which has no
 * strip, so the header names it here and the sheet owns dismissal.
 */
function DetailPanel({ toolCallProcess, onOpenFile, onOpenSubagentTask }: DetailPanelProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const root = useWorkspaceFolders()?.rootDir;

  if (!toolCallProcess) return null;

  const toolName = toolCallProcess.toolName || '';
  const toolArgs = toolCallProcess.toolCall?.args;
  const isTask = isTaskTool(toolName);
  const artifact = toolCallProcess.toolCallResult?.artifact;
  const displayName = isTask
    ? t('toolArtifact.subagentTask')
    : getDisplayName(toolName, t, toolArgs, { artifact, root });
  const subagentType = isTask ? ((toolCallProcess.toolCall?.args?.subagent_type as string) || 'general-purpose') : '';
  // Status only — a Task's reply exists from the moment it is dispatched, so
  // `content` marked every running task "completed" and contradicted the
  // status chip the panel body renders. Liveness is the question the icon
  // answers, not completion: the body below picks the same way, and a failed
  // or stopped task is as done working as a finished one.
  const isSubagentLive =
    isTask && taskCardStatusKind(toolCallProcess._subagentStatus) === 'running';
  const isFailed = isToolCallFailed(toolCallProcess);

  return (
    <div
      className={cn('term-panel-host', !isMobile && 'h-full flex flex-col')}
      style={{ backgroundColor: 'transparent' }}
    >
      {isMobile && (
        <div className="flex items-center justify-between px-4 py-3 shrink-0">
          <div className="flex items-center gap-2 min-w-0">
            {isTask ? (
              <img src={isSubagentLive ? iconRoboSing : iconRobo} alt="Subagent" className="w-5 h-5 shrink-0" />
            ) : (
              <ToolIcon
                toolName={toolName}
                args={toolArgs}
                className="h-4 w-4 shrink-0"
                style={{ color: isFailed ? 'var(--color-loss)' : 'var(--color-accent-primary)' }}
              />
            )}
            <span
              className="font-semibold truncate"
              style={{ color: isFailed ? 'var(--color-loss)' : 'var(--color-text-primary)', fontSize: '0.875rem' }}
            >
              {displayName}
            </span>
            {isFailed && (
              <XCircle
                className="h-4 w-4 shrink-0"
                aria-label={t('toolArtifact.a11y.toolCallFailed')}
                style={{ color: 'var(--color-loss)' }}
              />
            )}
            {isTask && subagentType && (
              <span style={{ color: 'var(--Labels-Tertiary)', fontSize: '0.8125rem' }}>
                · {subagentType}
              </span>
            )}
            {!isTask && (toolCallProcess.toolCall?.args?.symbol as string | undefined) && (
              <span style={{ color: 'var(--Labels-Tertiary)', fontSize: '0.8125rem' }}>
                · {toolCallProcess.toolCall!.args!.symbol as string}
              </span>
            )}
          </div>
        </div>
      )}

      <ToolCallDetailView
        toolCallProcess={toolCallProcess}
        onOpenFile={onOpenFile}
        onOpenSubagentTask={onOpenSubagentTask}
      />
    </div>
  );
}

export default DetailPanel;
