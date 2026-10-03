import React, { memo, useEffect, useId, useMemo, useState } from 'react';
import { AnimatePresence, motion } from '@/lib/framer';
import { ChevronDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { INLINE_ARTIFACT_MAP, isInlineArtifactReady, openCardTarget } from './charts/InlineArtifactCards';
import { isUserDataReadmePath } from '../utils/agentPaths';
import { announceAnchoredToggle } from '../utils/anchoredToggle';
import { summarizeCompletedItems } from './messageList/activitySummary';
import { type ActivityItem, type PreparingToolCallData, isRunning } from './messageList/activityTypes';
import { filePathOf, groupFileToolRuns } from './messageList/groupFileToolRuns';
import { FileToolGroupRow } from './messageList/FileToolGroupRow';
import { ReasoningRow } from './messageList/ReasoningRow';
import { ToolCallRow, PreparingTimelineRow } from './messageList/ToolCallRow';
import { PreparingToolCallRow, ToolCallLiveRow } from './messageList/InlineActivityRows';
import { LiveRow } from './messageList/LiveRow';
import { SettlingLabel } from './messageList/SettlingLabel';
import { SPRING_FOLD, EXIT_TWEEN } from './messageList/liveZoneTiming';
import { useActivityRowKeys } from './messageList/useActivityRowKeys';
import type { ChartTabSpec } from './filePanel/types';
import './ActivityBlock.css';

interface ActivityBlockProps {
  items: ActivityItem[];
  preparingToolCall?: PreparingToolCallData | null;
  isStreaming: boolean;
  /** Inline is the compatibility layout for a transcript without a turn fold. */
  presentation?: 'inline' | 'folded' | 'expanded';
  liveReasoningOpen?: boolean;
  onToolCallClick?: (item: ActivityItem) => void;
  onOpenFile?: (path: string, workspaceId?: string) => void;
  onOpenChart?: (spec: ChartTabSpec) => void;
}

const ActivityBlock = memo(function ActivityBlock({
  items, preparingToolCall, isStreaming, presentation = 'inline', liveReasoningOpen = false,
  onToolCallClick, onOpenFile, onOpenChart,
}: ActivityBlockProps): React.ReactElement | null {
  const { t } = useTranslation();
  const inline = presentation === 'inline';
  const [expanded, setExpanded] = useState(presentation === 'expanded');
  useEffect(() => { setExpanded(presentation === 'expanded'); }, [presentation]);
  const id = useId();
  const summaryId = `activity-summary-${id}`;
  const panelId = `activity-timeline-${id}`;

  const { timeline, charts } = useMemo(() => {
    const timeline: ActivityItem[] = [];
    const charts: ActivityItem[] = [];
    for (const item of items) {
      if (item.type === 'tool_call') {
        const path = filePathOf(item);
        if (item.toolName === 'Read' && path && isUserDataReadmePath(path)) continue;
        if (item._liveState === 'completed' && !item._annotationStep
          && isInlineArtifactReady(item.toolName, item.toolCallResult?.artifact)) {
          charts.push(item);
          continue;
        }
      }
      timeline.push(item);
    }
    return { timeline, charts };
  }, [items]);
  const completed = useMemo(() => timeline.filter((item) => item._liveState === 'completed'), [timeline]);
  const hasLive = timeline.some((item) => item._liveState !== 'completed');
  const bareReasoning = !inline && completed.length > 0 && !hasLive && !preparingToolCall
    && completed.every((item) => item.type === 'reasoning');
  const showCompleted = expanded || bareReasoning;
  const showSummary = completed.length > 0 && !bareReasoning;
  const summary = useMemo(() => summarizeCompletedItems(completed, t, { expanded }), [completed, expanded, t]);
  const keys = useActivityRowKeys(timeline, !!preparingToolCall);

  // One list owns both live and settled rows. Opening the accordion changes
  // visibility, never the live rows' parent, keys, or reasoning policy.
  const runs = groupFileToolRuns(timeline);
  const rows: Array<{ key: string; content: React.ReactNode; live: boolean }> = [];
  for (const run of runs) {
    const item = run[0];
    const live = run.some((entry) => entry._liveState !== 'completed');
    if (!live && !showCompleted) continue;
    let content: React.ReactNode;
    if (inline && live && item.type === 'tool_call') {
      content = <ToolCallLiveRow tc={item} liveState={item._liveState} />;
    } else if (item.type === 'reasoning') {
      content = <ReasoningRow item={item} isStreaming={isRunning(item)}
        defaultExpanded={isRunning(item) ? liveReasoningOpen : inline} />;
    } else if (item.toolName === 'Read' || item.toolName === 'Write' || item.toolName === 'Edit') {
      content = <FileToolGroupRow items={run.filter((entry) => entry.type === 'tool_call')}
        onOpenFile={onOpenFile} onToolCallClick={onToolCallClick} isStreaming={run.some(isRunning)} />;
    } else {
      content = <ToolCallRow item={item} running={isRunning(item)} onClick={() => onToolCallClick?.(item)} />;
    }
    rows.push({ key: keys.forItem(item), content, live });
  }
  if (preparingToolCall) {
    rows.push({ key: keys.preparing, live: true, content: inline
      ? <PreparingToolCallRow tc={preparingToolCall} />
      : <PreparingTimelineRow tc={preparingToolCall} /> });
  }

  if (timeline.length === 0 && charts.length === 0 && !preparingToolCall) return null;
  return <div>
    {charts.map((item) => {
      if (item.type !== 'tool_call') return null;
      const artifact = item.toolCallResult?.artifact;
      const Chart = artifact && INLINE_ARTIFACT_MAP[artifact.type as string];
      return Chart ? <div key={item.id} className="mb-1.5"><Chart artifact={artifact} toolArgs={item.toolCall?.args} onClick={() => openCardTarget(artifact, onOpenChart, () => onToolCallClick?.(item))} /></div> : null;
    })}
    <AnimatePresence initial={false}>
      {showSummary && <motion.div key="summary" className="clips-focus-ring"
        initial={{ opacity: 0, height: 0 }} animate={{ opacity: 1, height: 'auto' }}
        exit={{ opacity: 0, height: 0 }} transition={EXIT_TWEEN} style={{ overflow: 'hidden' }}>
        <button id={summaryId} type="button" aria-expanded={expanded} aria-controls={panelId}
          onClick={(e) => { announceAnchoredToggle(e.currentTarget); setExpanded(!expanded); }}
          className="inline-flex items-center gap-2 text-left bg-transparent border-0 p-0 cursor-pointer transition-colors hover:text-foreground"
          style={{ paddingTop: '5px', paddingBottom: '5px', fontSize: '0.8125rem', color: 'var(--Labels-Tertiary)' }}>
          <SettlingLabel text={summary ?? ''} />
          <motion.span animate={{ rotate: expanded ? 90 : 0 }} transition={SPRING_FOLD} className="shrink-0" style={{ opacity: 0.6 }}>
            <ChevronDown className="h-3.5 w-3.5 -rotate-90" />
          </motion.span>
        </button>
      </motion.div>}
    </AnimatePresence>
    <div id={panelId} role="region" aria-labelledby={showSummary ? summaryId : undefined}>
      <div role="list" className="timeline live-zone" data-testid="activity-live-zone">
        <AnimatePresence initial={isStreaming}>
          {/* Segment spacing owns the outside gap. Only a summary above the
              timeline needs an inset; standalone rows already have padding. */}
          {rows.map((row, index) => <LiveRow key={row.key}
            gap={index === 0 && showSummary ? '8px' : '0px'}
            gapBottom="0px">
            <div role="listitem" data-activity-state={row.live ? 'live' : 'settled'}>{row.content}</div>
          </LiveRow>)}
        </AnimatePresence>
      </div>
    </div>
  </div>;
});

export default ActivityBlock;
