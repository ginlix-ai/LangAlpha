import React, { memo } from 'react';
import { Wrench } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { categorizeTool, getDisplayName, getToolIcon, getPreparingText } from '../toolDisplayConfig';
import { ToolIcon } from '../ToolIcon';
import { completedToolTitle, completedToolSummary, liveToolLabel } from './activitySummary';
import { filePathOf } from './groupFileToolRuns';
import { classifyAgentPath } from '../../utils/agentPaths';
import { RunningTitle, RowIcon, FailedIconBadge } from './ActivityRowParts';
import { SwapCell } from '@/components/ui/crossfade';
import type { ToolActivityItem, PreparingCall } from './activityTypes';
import { ToolCallCommandState } from '../commands/CommandState';
import { isCommandTool } from '../commands/commandWords';

const RUNNING_VERB_KEY: Record<string, string> = { Read: 'read', Edit: 'edit', Write: 'write' };

/** What a running row says. A file call whose path is already known shows the
 *  present-tense verb beside the same pill its settled row will keep, so the
 *  settle changes one word and nothing moves. Anything else shows the full
 *  running phrase the live zone uses. */
function runningLine(item: ToolActivityItem, t: (key: string) => string): { verb: string; pill: string | null } {
  const verbKey = RUNNING_VERB_KEY[item.toolName || ''];
  const filePath = verbKey ? filePathOf(item) : null;
  if (verbKey && filePath && classifyAgentPath(filePath, item.root).kind === 'file') {
    return { verb: t(`toolArtifact.running.${verbKey}`), pill: completedToolSummary(item, t) || filePath.split('/').pop() || null };
  }
  return { verb: liveToolLabel(item, t), pill: null };
}

export function RunningLine({ item, onOpenPill }: { item: ToolActivityItem; onOpenPill?: () => void }): React.ReactElement {
  const { t } = useTranslation();
  const { verb, pill } = runningLine(item, t);
  return (
    <>
      <RunningTitle label={verb} />
      {pill && (
        <span className="titem-pill-wrap">
          <button type="button" onClick={onOpenPill} className="obj">{pill}</button>
        </span>
      )}
    </>
  );
}

export function PreparingTimelineRow({ tc }: { tc: PreparingCall }): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const displayName = toolName ? getDisplayName(toolName, t) : t('toolArtifact.toolCall');
  const IconComponent: LucideIcon = toolName ? getToolIcon(toolName) : Wrench;
  const prepText = getPreparingText(toolName, tc.argsLength, t);

  return (
    <div className="titem">
      <div className="titem-icon">
        <RowIcon running><IconComponent className="h-4 w-4" /></RowIcon>
      </div>
      <div className="titem-body">
        <div className="titem-line">
          <RunningTitle label={displayName} />
          <span className="truncate" style={{ fontSize: '0.75rem', opacity: 0.55 }}>{prepText}</span>
        </div>
      </div>
    </div>
  );
}

interface ToolCallRowProps {
  item: ToolActivityItem;
  onClick?: () => void;
  /** The call has not returned yet: the row shows its running verb instead
   *  of a settled title and pill. Lean timeline only. */
  running?: boolean;
}

export const ToolCallRow = memo(function ToolCallRow({ item, onClick, running = false }: ToolCallRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = item.toolName || '';
  const args = item.toolCall?.args;
  const artifact = item.toolCallResult?.artifact;
  const title = completedToolTitle(item, t);
  const summary = completedToolSummary(item, t);
  const isFailed = item.isFailed === true || item._liveState === 'failed';
  const failedLabel = t('toolArtifact.a11y.toolCallFailed');

  // Decide the destination tab label for the pill tooltip.
  const cat = categorizeTool(toolName, item.toolCall, item.root);
  const isMemory = cat === 'memoryRead' || cat === 'memoryWrite';
  const tabLabel = isMemory
    ? t('filePanel.tabs.memory')
    : cat === 'memo' || cat === 'memoWrite'
      ? t('filePanel.tabs.memo')
      : t('filePanel.tabs.files');

  // No pill → the row title becomes the click target so the affordance isn't
  // lost (e.g., memory/memo index rows where the verb already names the file).
  const openInTabLabel = t('toolArtifact.a11y.openInTab', { tab: tabLabel });
  const titleNode = summary ? (
    <span className="titem-title">{title}</span>
  ) : (
    <button
      type="button"
      onClick={onClick}
      className="titem-title titem-title-button"
      title={openInTabLabel}
      aria-label={openInTabLabel}
    >
      {title}
    </button>
  );

  return (
    <div className={`titem${isFailed ? ' failed' : ''}${running ? ' running' : ''}`}>
      <div className="titem-icon" title={isFailed ? failedLabel : undefined}>
        <RowIcon running={running}>
          <ToolIcon toolName={toolName} args={args} artifact={artifact} className="h-4 w-4" style={{ color: 'var(--Labels-Secondary)' }} />
        </RowIcon>
        <FailedIconBadge failed={isFailed} label={failedLabel} />
      </div>
      <div className="titem-body">
        {/* The settle crossfades in place: the running verb out, the past
            tense and its pill in, in one cell, so the row holds still. */}
        <SwapCell swapKey={running ? 'running' : 'settled'} layerClassName="titem-line">
          {running ? (
            <RunningLine item={item} onOpenPill={onClick} />
          ) : (
            <>
              {titleNode}
              {summary && (
                <span className="titem-pill-wrap">
                  <button
                    type="button"
                    onClick={onClick}
                    className="obj"
                    title={t('toolArtifact.a11y.openInTab', { tab: tabLabel })}
                  >
                    {summary}
                  </button>
                </span>
              )}
              {/* A call that moved to the background returned before its
                  command ended; the row follows the command from there. */}
              {isCommandTool(toolName) && <ToolCallCommandState toolCallId={item.toolCallId} />}
            </>
          )}
        </SwapCell>
      </div>
    </div>
  );
});
