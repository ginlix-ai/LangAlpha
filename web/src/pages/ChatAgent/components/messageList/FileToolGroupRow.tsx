import React, { memo, useState } from 'react';
import { SPRING_FOLD } from '@/lib/motion';
import { motion } from '@/lib/framer';
import { ChevronDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { categorizeTool, getToolIcon, getCompletedRowTitle, getCompletedSummary } from '../toolDisplayConfig';
import { classifyAgentPath, ranElsewhere } from '../../utils/agentPaths';
import { RunningLine } from './ToolCallRow';
import { RowIcon, FailedIconBadge } from './ActivityRowParts';
import { FoldPanel } from './FoldPanel';
import { SwapCell } from '@/components/ui/crossfade';
import { announceAnchoredToggle } from '../../utils/anchoredToggle';
import { EditDiff } from './EditDiff';
import { editStrings, filePathOf } from './groupFileToolRuns';
import type { ActivityItem, ToolActivityItem } from './activityTypes';

interface FileToolGroupRowProps {
  /** A single file call, or compatible settled calls of one verb. */
  items: ToolActivityItem[];
  onToolCallClick?: (item: ActivityItem) => void;
  onOpenFile?: (path: string, workspaceId?: string) => void;
  /** The run's last call is still out: the title shimmers until it lands. */
  isStreaming?: boolean;
}

/**
 * One row for a run of Read, Edit or Write calls: the verb with a count,
 * then a pill per distinct file. A run that keeps hitting one file (a report
 * edited eight times) names the file once and counts the calls beside it.
 * An Edit run keeps the single chevron, which opens every diff in order.
 */
export const FileToolGroupRow = memo(function FileToolGroupRow({ items, onOpenFile, onToolCallClick, isStreaming = false }: FileToolGroupRowProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const { root } = items[0];
  const toolName = items[0].toolName || 'Read';
  const IconComponent = getToolIcon(toolName, items[0].toolCall?.args, root);
  const isFailed = items.some((item) => item.isFailed === true || item._liveState === 'failed');
  const single = items.length === 1;
  const item = items[0];
  const failedLabel = t('toolArtifact.a11y.toolCallFailed');

  const paths: string[] = [];
  for (const item of items) {
    const p = filePathOf(item);
    if (p && !paths.includes(p)) paths.push(p);
  }
  const verbKey = toolName === 'Edit' ? 'edit' : toolName === 'Write' ? 'write' : 'read';
  const title = single ? getCompletedRowTitle(toolName, item.toolCall, t, { root }) : paths.length > 1
    ? t(`toolArtifact.fileGroup.${verbKey}`, { count: paths.length })
    : t(`toolArtifact.fileGroup.${verbKey}One`);
  const repeat = !single && paths.length <= 1
    ? (toolName === 'Edit' ? t('toolArtifact.fileGroup.edits', { count: items.length }) : `${items.length}×`)
    : null;

  const diffs = toolName === 'Edit'
    ? items.map((item) => ({ item, path: filePathOf(item), ...editStrings(item) })).filter((d) => d.oldStr || d.newStr)
    : [];
  const hasDiff = diffs.length > 0;
  const category = categorizeTool(toolName, item.toolCall, root);
  const tab = category === 'memoryRead' || category === 'memoryWrite' ? 'memory'
    : category === 'memo' || category === 'memoWrite' ? 'memo' : 'files';
  const openLabel = t('toolArtifact.a11y.openInTab', { tab: t(`filePanel.tabs.${tab}`) });
  const singlePath = filePathOf(item);
  const singleLabel = getCompletedSummary(toolName, item.toolCall, t, root)
    || (singlePath && classifyAgentPath(singlePath, root).kind === 'file' ? singlePath.split('/').pop() : null);
  // A run keeps to one computer. On another one the path is that machine's,
  // and the file panel would read it here, so the pill opens the call.
  const elsewhere = ranElsewhere(item.toolCall?.args);
  const openPath = (path: string) => {
    if (onOpenFile && !elsewhere) onOpenFile(path);
    else {
      const call = items.find((entry) => filePathOf(entry) === path);
      if (call) onToolCallClick?.(call);
    }
  };
  const openSingle = () => singlePath ? openPath(singlePath) : onToolCallClick?.(item);
  const pills = single ? (singlePath && singleLabel ? [{ path: singlePath, label: singleLabel }] : [])
    : paths.map((path) => ({ path, label: path.split('/').pop() }));
  // Only a lone call has a running line of its own; a run's title holds still
  // while its newest call is out.
  const runningSingle = isStreaming && single;

  return (
    <div className={`titem${isFailed ? ' failed' : ''}${isStreaming ? ' running' : ''}`} data-file-group={single ? undefined : toolName}>
      <div className="titem-icon" title={isFailed ? failedLabel : undefined}>
        <RowIcon running={isStreaming}><IconComponent className="h-4 w-4" /></RowIcon>
        <FailedIconBadge failed={isFailed} label={failedLabel} />
      </div>
      {/* No flex gap: the diffs' spacing rides inside their fold, so it opens
          and closes with them instead of landing whole on the first frame. */}
      <div className="titem-body" style={{ gap: 0 }}>
        <SwapCell swapKey={runningSingle ? 'running' : 'settled'} layerClassName="titem-line flex-wrap gap-y-[4px]">
          {runningSingle ? <RunningLine item={item} onOpenPill={openSingle} /> : <>
            {pills.length > 0 ? <span className="titem-title">{title}</span>
                : <button type="button" onClick={openSingle} className="titem-title titem-title-button" title={openLabel}>{title}</button>}
            {pills.map(({ path, label }) => <span key={path} className="titem-pill-wrap">
              <button type="button" onClick={() => openPath(path)} className="obj" title={openLabel}>{label}</button>
            </span>)}
            {repeat && <span style={{ opacity: 0.55, fontSize: '0.8125rem' }}>{repeat}</span>}
            {hasDiff && !isStreaming && (
              <button
                type="button"
                onClick={(e) => { announceAnchoredToggle(e.currentTarget); setExpanded(!expanded); }}
                className="inline-flex items-center shrink-0 bg-transparent border-0 p-0 cursor-pointer"
                style={{ color: 'inherit', alignSelf: 'center' }}
                aria-expanded={expanded}
                aria-label={expanded ? t('toolArtifact.a11y.collapseDiff') : t('toolArtifact.a11y.expandDiff')}
              >
                <motion.span className="inline-flex" animate={{ rotate: expanded ? 90 : 0 }} transition={SPRING_FOLD}>
                  <ChevronDown className="h-3 w-3 -rotate-90" style={{ opacity: 0.5 }} />
                </motion.span>
              </button>
            )}
          </>}
        </SwapCell>

        <FoldPanel open={expanded && hasDiff && !isStreaming}>
          {/* The body's former 6px gap plus the 8px the diffs kept above them. */}
          <div className="pt-3.5 flex flex-col gap-2">
            {diffs.map((d, i) => (
              <div key={d.item.id || i}>
                {paths.length > 1 && d.path && (
                  <div className="font-mono mb-1" style={{ fontSize: '0.6875rem', color: 'var(--Labels-Tertiary)' }}>
                    {d.path.split('/').pop()}
                  </div>
                )}
                <EditDiff oldStr={d.oldStr} newStr={d.newStr} />
              </div>
            ))}
          </div>
        </FoldPanel>
      </div>
    </div>
  );
});
