import React, { memo, useEffect, useId, useMemo, useState } from 'react';
import { AnimatePresence, motion } from '@/lib/framer';
import { ChevronDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { SPRING_FOLD } from '@/lib/motion';
import { useTranscriptDisplay } from '@/lib/transcriptDisplay';
import { INLINE_ARTIFACT_MAP, isInlineArtifactReady, openCardTarget } from './charts/InlineArtifactCards';
import { PILL_ROW_CLASS, isPillArtifactType } from './charts/pillArtifacts';
import { announceAnchoredToggle } from '../utils/anchoredToggle';
import { summarizeCompletedItems } from './messageList/activitySummary';
import { type ActivityItem, type PreparingCall, type PreparingToolCallData, arrivedAt, isRunning } from './messageList/activityTypes';
import { layoutRows, layoutStatusLine, type Row, type Step } from './messageList/activityLayout';
import { useStatusLineStep } from './messageList/useStatusLineStep';
import { GROUPABLE_FILE_TOOLS } from './messageList/groupFileToolRuns';
import { FileToolGroupRow } from './messageList/FileToolGroupRow';
import { ReasoningRow } from './messageList/ReasoningRow';
import { ToolCallRow, PreparingTimelineRow } from './messageList/ToolCallRow';
import { PreparingToolCallRow, ToolCallLiveRow } from './messageList/InlineActivityRows';
import { LiveRow } from './messageList/LiveRow';
import { SettlingLabel } from './messageList/SettlingLabel';
import { SwapCell } from '@/components/ui/crossfade';
import { RunningGlyphContext } from './messageList/runningGlyph';
import { useActivityRowKeys } from './messageList/useActivityRowKeys';
import { isFreshArrival } from './messageList/transcriptMotion';
import type { ChartTabSpec } from './filePanel/types';
import './ActivityBlock.css';

interface ActivityBlockProps {
  items: ActivityItem[];
  preparingToolCall?: PreparingToolCallData | null;
  isStreaming: boolean;
  /** The running turn's newest activity, which keeps the status line up
   *  between steps, on the step that last finished, until the turn folds. */
  holdsLine?: boolean;
  /** Inline is the compatibility layout for a transcript without a turn fold. */
  presentation?: 'inline' | 'folded' | 'expanded';
  liveReasoningOpen?: boolean;
  onToolCallClick?: (item: ActivityItem) => void;
  onOpenFile?: (path: string, workspaceId?: string) => void;
  onOpenChart?: (spec: ChartTabSpec) => void;
}

const NO_CALLS: readonly PreparingCall[] = [];

const SUMMARY_TEXT: React.CSSProperties = { paddingTop: '5px', paddingBottom: '5px', fontSize: '0.8125rem', color: 'var(--Labels-Tertiary)' };

const ActivityBlock = memo(function ActivityBlock({
  items, preparingToolCall, isStreaming, holdsLine = false, presentation = 'inline', liveReasoningOpen = false,
  onToolCallClick, onOpenFile, onOpenChart,
}: ActivityBlockProps): React.ReactElement | null {
  const { t } = useTranslation();
  const inline = presentation === 'inline';
  // The status line is a way to fold a live turn, so a transcript without a
  // turn fold keeps its rows.
  const status = useTranscriptDisplay().toolCallLayout === 'status' && !inline;
  const [expanded, setExpanded] = useState(presentation === 'expanded');
  useEffect(() => { setExpanded(presentation === 'expanded'); }, [presentation]);
  const id = useId();
  const summaryId = `activity-summary-${id}`;
  const panelId = `activity-timeline-${id}`;

  const { timeline, charts } = useMemo(() => {
    const timeline: ActivityItem[] = [];
    const charts: ActivityItem[] = [];
    // A user-data README read never arrives here: the projection that built
    // these items drops it, under the root it stamped on them.
    for (const item of items) {
      if (item.type === 'tool_call' && item._liveState === 'completed' && !item._annotationStep
        && isInlineArtifactReady(item.toolName, item.toolCallResult?.artifact)) {
        charts.push(item);
      } else {
        timeline.push(item);
      }
    }
    return { timeline, charts };
  }, [items]);
  const calls = preparingToolCall?.calls ?? NO_CALLS;
  const keys = useActivityRowKeys(timeline, calls);
  // What the block holds when it mounts unfolds only if the block is new. One
  // mounting again mid-run (a reload, the main view back from a subagent tab,
  // another subagent's tab) draws the rows it already had, settled. Fixed at
  // mount, which is the only time presence reads it.
  const [fresh] = useState(() => isStreaming
    && isFreshArrival([...timeline.map(arrivedAt), ...calls.map((call) => call.firstSeenAt)]));

  const { line, inFlight } = useStatusLineStep(timeline, calls, keys, liveReasoningOpen, status);
  const { slot, rows } = status
    ? layoutStatusLine({ timeline, calls, keys, expanded, liveReasoningOpen, line, up: inFlight || holdsLine })
    : layoutRows({ timeline, calls, keys, expanded, inline });
  const summary = slot?.kind === 'summary'
    ? summarizeCompletedItems(slot.counted, t, { expanded, countFailures: status }) ?? ''
    : '';

  const stepRow = (step: Step): React.ReactNode => {
    if (step.kind === 'call') return <PreparingTimelineRow tc={step.call} />;
    const { item } = step;
    if (item.type === 'reasoning') {
      return <ReasoningRow item={item} isStreaming={isRunning(item)} defaultExpanded={false} />;
    }
    if (GROUPABLE_FILE_TOOLS.has(item.toolName)) {
      return <FileToolGroupRow items={[item]} onOpenFile={onOpenFile} onToolCallClick={onToolCallClick} isStreaming={isRunning(item)} />;
    }
    return <ToolCallRow item={item} running={isRunning(item)} onClick={() => onToolCallClick?.(item)} />;
  };

  const lineNode = (key: string, step: Step): React.ReactNode => (
    <RunningGlyphContext value>
      {/* A call that lands under its preparing row's key crossfades from
          the one form to the other; a new step rises in in its place. */}
      <SwapCell swapKey={`${key}:${step.kind}`} riseKey={key}>{stepRow(step)}</SwapCell>
    </RunningGlyphContext>
  );

  // One list owns both live and settled rows. Opening the accordion changes
  // visibility, never the live rows' parent, keys, or reasoning policy.
  const rowNode = (row: Row): React.ReactNode => {
    if (row.kind === 'line') return lineNode(row.line.key, row.line.step);
    if (row.kind === 'call') return inline ? <PreparingToolCallRow tc={row.call} /> : <PreparingTimelineRow tc={row.call} />;
    const [item] = row.run;
    if (inline && row.live && item.type === 'tool_call') return <ToolCallLiveRow tc={item} liveState={item._liveState} />;
    if (item.type === 'reasoning') {
      return <ReasoningRow item={item} isStreaming={isRunning(item)}
        defaultExpanded={isRunning(item) ? liveReasoningOpen : inline} />;
    }
    if (GROUPABLE_FILE_TOOLS.has(item.toolName)) {
      return <FileToolGroupRow items={row.run.filter((entry) => entry.type === 'tool_call')}
        onOpenFile={onOpenFile} onToolCallClick={onToolCallClick} isStreaming={row.run.some(isRunning)} />;
    }
    return <ToolCallRow item={item} running={isRunning(item)} onClick={() => onToolCallClick?.(item)} />;
  };

  // Consecutive pill cards share one wrapping row; any other card stands alone.
  const chartGroups: Array<{ key: string; pill: boolean; cards: Array<{ key: string; node: React.ReactNode }> }> = [];
  for (const item of charts) {
    if (item.type !== 'tool_call') continue;
    const artifact = item.toolCallResult?.artifact;
    const Chart = artifact && INLINE_ARTIFACT_MAP[artifact.type as string];
    if (!Chart) continue;
    const pill = isPillArtifactType(artifact.type);
    const node = <Chart artifact={artifact} toolArgs={item.toolCall?.args} onClick={() => openCardTarget(artifact, onOpenChart, () => onToolCallClick?.(item))} />;
    const card = { key: item.id, node };
    const last = chartGroups[chartGroups.length - 1];
    if (pill && last?.pill) last.cards.push(card);
    else chartGroups.push({ key: `pills-${item.id}`, pill, cards: [card] });
  }

  if (timeline.length === 0 && charts.length === 0 && calls.length === 0) return null;
  return <div data-activity-block="">
    {chartGroups.map((group) => group.pill && group.cards.length > 1
      ? <div key={group.key} data-pill-row className={`mb-1.5 ${PILL_ROW_CLASS}`}>{group.cards.map((c) => c.node)}</div>
      : group.cards.map((c) => <div key={c.key} className="mb-1.5">{c.node}</div>))}
    {/* The slot over the rows, where the status line's first step runs and
        the count then takes its place. The inset under it is its own, not the
        first row's: a first row folding away handed it to the next, which grew
        by it and then folded in turn, so every fold in a burst moved the page
        twice. */}
    <AnimatePresence initial={status && fresh}>
      {slot && <LiveRow key="summary" gap="0px" gapBottom={rows.length > 0 ? '8px' : '0px'} calls={slot.calls}>
        {/* The step the block opened on lifts out as the count rises in. */}
        <SwapCell swapKey={slot.kind} riseKey={slot.kind} layerClassName="clips-focus-ring">
          {/* One tree whether the step is running or settled, so a reply
              arriving leaves the row as it is. */}
          {slot.kind === 'lead'
            ? <div className="activity-lead" data-status-line={slot.isLine ? '' : undefined}
              data-activity-state={!slot.isLine ? undefined : slot.step.kind === 'call' || isRunning(slot.step.item) ? 'live' : 'settled'}>
              {lineNode(slot.key, slot.step)}
            </div>
            : <button id={summaryId} type="button" aria-expanded={expanded} aria-controls={panelId}
              onClick={(e) => { announceAnchoredToggle(e.currentTarget); setExpanded(!expanded); }}
              className="inline-flex max-w-full items-center gap-2 text-left bg-transparent border-0 p-0 cursor-pointer transition-colors hover:text-foreground"
              style={SUMMARY_TEXT}>
              <SettlingLabel text={summary} />
              <motion.span animate={{ rotate: expanded ? 90 : 0 }} transition={SPRING_FOLD} className="shrink-0" style={{ opacity: 0.6 }}>
                <ChevronDown className="h-3.5 w-3.5 -rotate-90" />
              </motion.span>
            </button>}
        </SwapCell>
      </LiveRow>}
    </AnimatePresence>
    <div id={panelId} role="region" aria-labelledby={slot?.kind === 'summary' ? summaryId : undefined}>
      <div role="list" className="timeline live-zone" data-testid="activity-live-zone">
        <AnimatePresence initial={fresh}>
          {/* Segment spacing owns the outside gap, and standalone rows already
              have padding. */}
          {/* A row gives its place to the card its call lands as. */}
          {rows.map((row) => <LiveRow key={row.key} gap="0px" calls={row.calls}>
            <div role="listitem" data-activity-state={row.live ? 'live' : 'settled'}
              data-status-line={row.kind === 'line' ? '' : undefined}>{rowNode(row)}</div>
          </LiveRow>)}
        </AnimatePresence>
      </div>
    </div>
  </div>;
});

export default ActivityBlock;
