import React from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { X, Zap } from 'lucide-react';
import { getDisplayName, getToolIcon } from './toolDisplayConfig';
import Markdown from './Markdown';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import { useTranslation } from 'react-i18next';
import ToolCallDetailView, { type ToolCallProcessRecord, type SubagentInfo } from './ToolCallDetailView';

interface PlanData {
  description?: string;
  [key: string]: unknown;
}

interface DetailPanelProps {
  toolCallProcess: ToolCallProcessRecord | null;
  planData?: PlanData | null;
  onClose: () => void;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
}

function DetailPanel({ toolCallProcess, planData, onClose, onOpenFile, onOpenSubagentTask }: DetailPanelProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();

  // Plan detail view
  if (planData) {
    return (
      <div
        className={isMobile ? '' : 'h-full flex flex-col'}
        style={{
          backgroundColor: 'transparent',
          ...(!isMobile && { borderLeft: '1px solid var(--color-border-muted)' }),
        }}
      >
        <div
          className="flex items-center justify-between px-4 py-3 flex-shrink-0"
          style={!isMobile ? { borderBottom: '1px solid var(--color-border-muted)' } : undefined}
        >
          <div className="flex items-center gap-2 min-w-0">
            <Zap className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
            <span
              className="font-semibold truncate"
              style={{ color: 'var(--color-text-primary)', fontSize: 14 }}
            >
              {t('toolArtifact.planDetails')}
            </span>
          </div>
          {!isMobile && (
            <button
              onClick={onClose}
              className="p-1 rounded hover:bg-foreground/10 transition-colors flex-shrink-0"
              style={{ color: 'var(--Labels-Secondary)' }}
            >
              <X className="h-4 w-4" />
            </button>
          )}
        </div>
        <div
          className={`${isMobile ? '' : 'flex-1 overflow-y-auto'} px-4 py-4`}
          style={!isMobile ? { minHeight: 0 } : undefined}
        >
          <Markdown variant="panel" content={planData.description || t('toolArtifact.noPlanDescription')} className="text-sm" />
        </div>
      </div>
    );
  }

  if (!toolCallProcess) return null;

  const toolName = toolCallProcess.toolName || '';
  const toolArgs = toolCallProcess.toolCall?.args;
  const isTaskTool = toolName === 'Task' || toolName === 'task';
  const displayName = isTaskTool ? t('toolArtifact.subagentTask') : getDisplayName(toolName, t, toolArgs);
  const IconComponent = getToolIcon(toolName, toolArgs);
  const artifact = toolCallProcess.toolCallResult?.artifact;
  const content = toolCallProcess.toolCallResult?.content;
  const subagentType = isTaskTool ? ((toolCallProcess.toolCall?.args?.subagent_type as string) || 'general-purpose') : '';
  const isSubagentCompleted = isTaskTool && (toolCallProcess._subagentStatus === 'completed' || !!content);

  return (
    <div
      className={isMobile && artifact?.type !== 'sec_filing' ? '' : 'h-full flex flex-col'}
      style={{
        backgroundColor: 'transparent',
        ...(!isMobile && { borderLeft: '1px solid var(--color-border-muted)' }),
      }}
    >
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-3 flex-shrink-0"
        style={!isMobile ? { borderBottom: '1px solid var(--color-border-muted)' } : undefined}
      >
        <div className="flex items-center gap-2 min-w-0">
          {isTaskTool ? (
            <img src={isSubagentCompleted ? iconRobo : iconRoboSing} alt="Subagent" className="w-5 h-5 flex-shrink-0" />
          ) : (
            <IconComponent className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
          )}
          <span
            className="font-semibold truncate"
            style={{ color: 'var(--color-text-primary)', fontSize: 14 }}
          >
            {displayName}
          </span>
          {isTaskTool && subagentType && (
            <span style={{ color: 'var(--Labels-Tertiary)', fontSize: 13 }}>
              — {subagentType}
            </span>
          )}
          {!isTaskTool && (toolCallProcess.toolCall?.args?.symbol as string | undefined) && (
            <span style={{ color: 'var(--Labels-Tertiary)', fontSize: 13 }}>
              — {toolCallProcess.toolCall!.args!.symbol as string}
            </span>
          )}
        </div>
        {!isMobile && (
          <button
            onClick={onClose}
            className="p-1 rounded hover:bg-foreground/10 transition-colors flex-shrink-0"
            style={{ color: 'var(--Labels-Secondary)' }}
          >
            <X className="h-4 w-4" />
          </button>
        )}
      </div>

      <ToolCallDetailView
        toolCallProcess={toolCallProcess}
        onOpenFile={onOpenFile}
        onOpenSubagentTask={onOpenSubagentTask}
      />
    </div>
  );
}

export default DetailPanel;
