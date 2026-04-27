import React, { useState, useRef, useMemo, useId, memo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Brain, ChevronDown, Wrench, X as XIcon } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import {
  getDisplayName,
  getToolIcon,
  getPreparingText,
  getCompletedSummary,
  getActiveLabel,
  getCompletedRowTitle,
  categorizeTool,
  type ToolCategory,
} from './toolDisplayConfig';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { DotLoader } from '@/components/ui/dot-loader';
import { useAnimatedText } from '@/components/ui/animated-text';
import Markdown from './Markdown';
import {
  INLINE_ARTIFACT_TOOLS,
  InlineStockPriceCard,
  InlineCompanyOverviewCard,
  InlineMarketIndicesCard,
  InlineSectorPerformanceCard,
  InlineSecFilingCard,
  InlineStockScreenerCard,
  InlineWebSearchCard,
} from './charts/InlineArtifactCards';
import { InlineAutomationCard } from './charts/InlineAutomationCards';
import { InlinePreviewCard } from './charts/InlinePreviewCard';
import { useTranslation } from 'react-i18next';
import './ActivityBlock.css';

/** Tool names where clicking should open the file in the FilePanel */
const FILE_NAV_TOOLS = new Set(['Read', 'Write']);

function getFilePathFromArgs(args: Record<string, unknown> | undefined): string | null {
  if (!args) return null;
  return (args.file_path || args.filePath || args.path || args.filename || null) as string | null;
}

/** Map artifact type to inline chart component */
const INLINE_ARTIFACT_MAP: Record<string, React.ComponentType<{ artifact: Record<string, unknown>; onClick?: () => void }>> = {
  stock_prices: InlineStockPriceCard,
  company_overview: InlineCompanyOverviewCard,
  market_indices: InlineMarketIndicesCard,
  sector_performance: InlineSectorPerformanceCard,
  sec_filing: InlineSecFilingCard,
  stock_screener: InlineStockScreenerCard,
  automations: InlineAutomationCard,
  preview_url: InlinePreviewCard,
  web_search: InlineWebSearchCard,
};

/** Spring config matching radix-accordion feel */
const SPRING = { type: 'spring' as const, stiffness: 150, damping: 17 };
const SPRING_SNAPPY = { type: 'spring' as const, stiffness: 200, damping: 22 };

type LiveState = 'active' | 'completing' | 'completed' | 'failed';

interface ToolCallData {
  args?: Record<string, unknown>;
  [key: string]: unknown;
}

interface ToolCallResultData {
  content?: unknown;
  artifact?: Record<string, unknown>;
  [key: string]: unknown;
}

interface ActivityItem {
  id?: string;
  toolCallId?: string;
  type: 'reasoning' | 'tool_call';
  toolName?: string;
  toolCall?: ToolCallData;
  toolCallResult?: ToolCallResultData;
  isComplete?: boolean;
  /** Set in MessageList from `proc.isFailed`. Persists across the live→completed
   *  transition so the accordion timeline can render a failure indicator. */
  isFailed?: boolean;
  _recentlyCompleted?: boolean;
  _liveState?: LiveState;
  content?: string;
  reasoningTitle?: string;
  [key: string]: unknown;
}

interface PreparingToolCallData {
  toolName?: string;
  argsLength: number;
  [key: string]: unknown;
}

interface ActivityBlockProps {
  items: ActivityItem[];
  preparingToolCall?: PreparingToolCallData | null;
  isStreaming: boolean;
  onToolCallClick?: (item: ActivityItem) => void;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

/**
 * ActivityBlock -- unified component for completed + live activity items.
 *
 * Items move from the live zone to the accordion zone in the same React render,
 * eliminating the visible gap that separate components caused between
 * fade-out and reappear across render cycles.
 */
const ActivityBlock = memo(function ActivityBlock({ items, preparingToolCall, isStreaming, onToolCallClick, onOpenFile }: ActivityBlockProps): React.ReactElement | null {
  const { t } = useTranslation();
  const [isExpanded, setIsExpanded] = useState(false);
  const prevCompletedIdsRef = useRef<Set<string | undefined>>(new Set());
  // Stable per-instance id pair for the toggle button + the timeline panel it
  // controls — assistive tech needs both `aria-expanded`/`aria-controls` and
  // a labelled region to announce the accordion correctly.
  const reactId = useId();
  const summaryButtonId = `activity-summary-${reactId}`;
  const timelinePanelId = `activity-timeline-${reactId}`;

  // Memoize partition of items into zones
  const { completedItems, liveItems, inlineChartItems } = useMemo(() => {
    const completed: ActivityItem[] = [];
    const live: ActivityItem[] = [];
    const inlineCharts: ActivityItem[] = [];

    for (const item of items) {
      if (item._liveState === 'completed') {
        if (
          item.type === 'tool_call' &&
          INLINE_ARTIFACT_TOOLS.has(item.toolName || '') &&
          item.toolCallResult?.artifact
        ) {
          inlineCharts.push(item);
        } else {
          completed.push(item);
        }
      } else {
        live.push(item);
      }
    }
    return { completedItems: completed, liveItems: live, inlineChartItems: inlineCharts };
  }, [items]);

  // Detect newly completed items for entrance animation
  const currentCompletedIds = new Set(completedItems.map(i => i.id || i.toolCallId));
  const newlyCompletedIds = new Set<string | undefined>();
  if (isStreaming) {
    for (const id of currentCompletedIds) {
      if (!prevCompletedIdsRef.current.has(id)) {
        newlyCompletedIds.add(id);
      }
    }
  }
  prevCompletedIdsRef.current = currentCompletedIds;

  // Content-aware accordion header \u2014 group completed items by category and
  // emit `<count> <label>` fragments. Computed BEFORE any early-return so
  // hook order stays stable.
  //
  // Fingerprint includes the file_path arg AND `isFailed` per item so the
  // breakdown re-walks when an item's args arrive late (a common SSE pattern:
  // row created with empty args, file_path patched in a later chunk) or when
  // a failure flag flips after the initial completed render. The earlier
  // `[length, lastId]` key silently froze the category counts in both cases.
  const summaryFingerprint = completedItems
    .map((i) => {
      const args = i.toolCall?.args as Record<string, unknown> | undefined;
      const fp = (args?.file_path || args?.filePath || '') as string;
      const failed = i.isFailed ? '1' : '0';
      return `${i.id || i.toolCallId || ''}:${i.type}:${i.toolName || ''}:${fp}:${failed}`;
    })
    .join('|');
  // Slot = what we emit in the header. `memory` collapses read+write into one
  // fragment whose verb flips on any write (its store is conceptually one
  // surface). `fileRead`/`fileEdit` and `memo`/`memoWrite` stay separate —
  // distinct file/memo paths shouldn't collide under one label, and any
  // memo modification is surfaced distinctly so a future regression letting
  // the agent mutate a memo is visible. `failed` is orthogonal to the
  // category axis but is its own fragment so the user sees that the
  // accordion contains failures even before expanding (otherwise a failed
  // read counts identically to a successful one once `_liveState` flips to
  // 'completed').
  type SummarySlot = 'skill' | 'memory' | 'memo' | 'memoWrite' | 'code' | 'web' | 'search' | 'fileRead' | 'fileEdit' | 'reasoning' | 'generic' | 'failed';
  const summaryFragments = useMemo<{ slot: SummarySlot; count: number; modified?: boolean }[]>(() => {
    const counts = new Map<ToolCategory | 'reasoning', number>();
    let failedCount = 0;
    for (const item of completedItems) {
      const key: ToolCategory | 'reasoning' = item.type === 'reasoning'
        ? 'reasoning'
        : categorizeTool(item.toolName || '', item.toolCall);
      counts.set(key, (counts.get(key) ?? 0) + 1);
      if (item.isFailed) failedCount += 1;
    }
    const memReads = counts.get('memoryRead') ?? 0;
    const memWrites = counts.get('memoryWrite') ?? 0;
    const memTotal = memReads + memWrites;
    const order: SummarySlot[] = ['skill', 'memory', 'memo', 'memoWrite', 'code', 'web', 'search', 'fileRead', 'fileEdit', 'reasoning', 'generic'];
    const out: { slot: SummarySlot; count: number; modified?: boolean }[] = [];
    for (const slot of order) {
      if (slot === 'memory') {
        if (memTotal > 0) out.push({ slot, count: memTotal, modified: memWrites > 0 });
      } else if (counts.has(slot as ToolCategory | 'reasoning')) {
        out.push({ slot, count: counts.get(slot as ToolCategory | 'reasoning')! });
      }
    }
    if (failedCount > 0) out.push({ slot: 'failed', count: failedCount });
    return out;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [summaryFingerprint]);

  const hasInlineCharts = inlineChartItems.length > 0;
  const hasCompleted = completedItems.length > 0;
  const hasLive = liveItems.length > 0;
  const hasPreparingTools = !!preparingToolCall;

  if (!hasInlineCharts && !hasCompleted && !hasLive && !hasPreparingTools) return null;

  let summaryLabel: string | undefined;
  if (summaryFragments.length > 0 && summaryFragments.some((f) => f.slot !== 'reasoning' && f.slot !== 'generic')) {
    // When folded, cap the breakdown at 3 fragments + "…" so the header stays
    // scannable on turns with many tool categories. Expanding the accordion
    // reveals the full breakdown alongside the per-step rows.
    // High-signal fragments (memory writes, memo writes, failures) get
    // priority: they're information the user specifically needs to see and
    // shouldn't drop into the "and more …" tail when 4+ categories are
    // present. We hoist them to the front of the visible slice while
    // preserving the relative order of everything else.
    const FOLDED_MAX = 3;
    const isPriority = (f: { slot: SummarySlot; modified?: boolean }) =>
      f.slot === 'failed' || f.slot === 'memoWrite' || (f.slot === 'memory' && f.modified === true);
    const priority = summaryFragments.filter(isPriority);
    const rest = summaryFragments.filter((f) => !isPriority(f));
    const ordered = [...priority, ...rest];
    const overflowing = !isExpanded && ordered.length > FOLDED_MAX;
    const visible = overflowing ? ordered.slice(0, FOLDED_MAX) : ordered;
    const labels = visible
      .map((f) => {
        if (f.slot === 'reasoning') return t('toolArtifact.nReasoning', { count: f.count });
        if (f.slot === 'skill') return t('toolArtifact.categoryCount.skill', { count: f.count });
        if (f.slot === 'memory') {
          // No count for memory — any write/edit overrules pure-read framing.
          return t(f.modified ? 'toolArtifact.categoryCount.memoryUpdated' : 'toolArtifact.categoryCount.memoryRead');
        }
        if (f.slot === 'fileRead') return t('toolArtifact.categoryCount.fileRead', { count: f.count });
        if (f.slot === 'fileEdit') return t('toolArtifact.categoryCount.fileEdit', { count: f.count });
        if (f.slot === 'memo') return t('toolArtifact.categoryCount.memo', { count: f.count });
        if (f.slot === 'memoWrite') return t('toolArtifact.categoryCount.memoWrite', { count: f.count });
        if (f.slot === 'code') return t('toolArtifact.categoryCount.code', { count: f.count });
        if (f.slot === 'web') return t('toolArtifact.categoryCount.web', { count: f.count });
        if (f.slot === 'search') return t('toolArtifact.categoryCount.search', { count: f.count });
        if (f.slot === 'failed') return t('toolArtifact.categoryCount.failed', { count: f.count });
        return t('toolArtifact.categoryCount.generic', { count: f.count });
      });
    summaryLabel = labels.join(' \u00b7 ');
    if (overflowing) summaryLabel = `${summaryLabel} ${t('toolArtifact.andMore')}`;
  } else if (completedItems.length > 0) {
    summaryLabel = t('toolArtifact.nStepsCompleted', { count: completedItems.length });
  }
  // Capitalize first character only \u2014 leave the rest untouched so embedded
  // proper nouns / casing stay intact. Applies to both the fragment-based label
  // and the fallback "completed N steps" label.
  if (summaryLabel && summaryLabel.length > 0) {
    summaryLabel = summaryLabel.charAt(0).toUpperCase() + summaryLabel.slice(1);
  }

  return (
    <div className="mb-1">
      {/* Inline chart cards -- always visible, above accordion */}
      {hasInlineCharts && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: hasCompleted || hasLive || hasPreparingTools ? 6 : 0 }}>
          {inlineChartItems.map((item, idx) => {
            const artifact = item.toolCallResult!.artifact!;
            const ChartComponent = INLINE_ARTIFACT_MAP[artifact.type as string];
            if (!ChartComponent) return null;
            return (
              <div key={`chart-${item.id || idx}`}>
                <ChartComponent
                  artifact={artifact}
                  onClick={() => onToolCallClick?.(item)}
                />
              </div>
            );
          })}
        </div>
      )}

      {/* Accordion zone (top) -- completed items, animates in smoothly */}
      <AnimatePresence initial={false}>
        {hasCompleted && (
          <motion.div
            key="accordion-zone"
            className="-mt-2"
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            transition={SPRING_SNAPPY}
            style={{ overflow: 'hidden' }}
          >
            <button
              id={summaryButtonId}
              type="button"
              aria-expanded={isExpanded}
              aria-controls={timelinePanelId}
              onClick={() => setIsExpanded(!isExpanded)}
              className="inline-flex items-center gap-2 text-left bg-transparent border-0 p-0 cursor-pointer transition-colors hover:text-foreground"
              style={{
                paddingTop: '5px',
                paddingBottom: '5px',
                fontSize: '13px',
                color: 'var(--Labels-Tertiary)',
              }}
            >
              <span className="truncate">{summaryLabel}</span>
              <motion.div
                animate={{ rotate: isExpanded ? 90 : 0 }}
                transition={SPRING}
                className="flex-shrink-0"
                style={{ opacity: 0.6 }}
              >
                <ChevronDown className="h-3.5 w-3.5 -rotate-90" />
              </motion.div>
            </button>

            <AnimatePresence initial={false}>
              {isExpanded && (
                <motion.div
                  key="accordion-body"
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: 'auto', opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={SPRING}
                  style={{ overflow: 'hidden' }}
                >
                  <div
                    id={timelinePanelId}
                    role="region"
                    aria-labelledby={summaryButtonId}
                  >
                  <ol className="timeline mt-1">
                    {completedItems.map((item, idx) => {
                      const itemId = item.id || item.toolCallId;
                      const isNew = newlyCompletedIds.has(itemId);
                      const itemKey = item.type === 'reasoning' ? `r-${itemId || idx}` : `t-${itemId || idx}`;

                      const content = renderCompletedItem(item, idx, onToolCallClick, onOpenFile);
                      if (!content) return null;

                      if (isNew) {
                        return (
                          <motion.li
                            key={itemKey}
                            initial={{ opacity: 0, height: 0 }}
                            animate={{ opacity: 1, height: 'auto' }}
                            transition={SPRING_SNAPPY}
                            style={{ overflow: 'hidden', listStyle: 'none' }}
                          >
                            {content}
                          </motion.li>
                        );
                      }

                      return <li key={itemKey} style={{ listStyle: 'none' }}>{content}</li>;
                    })}
                  </ol>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Live zone (bottom) -- active/completing items + preparing */}
      <AnimatePresence initial={false}>
        {(hasLive || hasPreparingTools) && (
          <motion.div
            key="live-zone"
            className={`${hasCompleted ? 'mt-2 ' : '-mt-1 '}space-y-2`}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0, height: 0, marginTop: 0 }}
            transition={SPRING_SNAPPY}
            style={{ overflow: 'hidden' }}
          >
            {/* Live items in chronological order */}
            <AnimatePresence initial={false}>
              {liveItems.map(item => {
                if (item.type === 'reasoning') {
                  const { title: extractedTitle, body: extractedBody } = extractLeadingBoldHeader(item.content || '');
                  const effectiveTitle = item.reasoningTitle || extractedTitle;
                  const liveBody = extractedTitle ? extractedBody : item.content;
                  return (
                    <motion.div
                      key={`live-r-${item.id}`}
                      initial={{ opacity: 0, height: 0 }}
                      animate={{ opacity: item._liveState === 'completing' ? 0.6 : 1, height: 'auto' }}
                      exit={{ opacity: 0, height: 0, paddingTop: 0, paddingBottom: 0 }}
                      transition={SPRING_SNAPPY}
                      style={{ overflow: 'hidden', paddingTop: '8px', paddingBottom: '8px' }}
                      className="px-3"
                    >
                      <div
                        className="flex items-center gap-2 mb-1"
                        style={{ fontSize: '13px', color: 'var(--Labels-Secondary)' }}
                      >
                        <Brain className="h-4 w-4 flex-shrink-0" />
                        {item._liveState === 'active' ? (
                          <TextShimmer
                            as="span"
                            className="font-medium truncate text-[13px] [--base-color:var(--Labels-Secondary)] [--base-gradient-color:var(--color-text-primary)]"
                            duration={1.5}
                          >
                            {effectiveTitle || t('toolArtifact.reasoningPending')}
                          </TextShimmer>
                        ) : (
                          <span className="font-medium truncate">{effectiveTitle || t('toolArtifact.reasoningComplete')}</span>
                        )}
                      </div>

                      {liveBody && (
                        <AnimatedReasoningContent
                          content={liveBody}
                          isStreaming={item._liveState === 'active'}
                        />
                      )}
                    </motion.div>
                  );
                }
                if (item.type === 'tool_call') {
                  return (
                    <motion.div
                      key={`live-t-${item.id || item.toolCallId}`}
                      initial={{ opacity: 0, height: 0 }}
                      animate={{ opacity: 1, height: 'auto' }}
                      exit={{ opacity: 0, height: 0 }}
                      transition={SPRING_SNAPPY}
                      style={{ overflow: 'hidden' }}
                    >
                      <ToolCallLiveRow tc={item} liveState={item._liveState} />
                    </motion.div>
                  );
                }
                return null;
              })}
            </AnimatePresence>

            {/* Preparing tool call -- always at the bottom */}
            <AnimatePresence initial={false}>
              {hasPreparingTools && (
                <motion.div
                  key="preparing"
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  transition={SPRING_SNAPPY}
                  style={{ overflow: 'hidden' }}
                >
                  <PreparingToolCallRow tc={preparingToolCall!} />
                </motion.div>
              )}
            </AnimatePresence>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
});

function renderCompletedItem(
  item: ActivityItem,
  idx: number,
  onToolCallClick?: (item: ActivityItem) => void,
  onOpenFile?: (path: string, workspaceId?: string) => void,
): React.ReactElement | null {
  if (item.type === 'reasoning') {
    return <ReasoningRow item={item} />;
  }
  if (item.type === 'tool_call') {
    const toolName = item.toolName || '';

    if (toolName === 'Edit') {
      return <EditToolRow item={item} onOpenFile={onOpenFile} />;
    }

    if (FILE_NAV_TOOLS.has(toolName)) {
      const filePath = getFilePathFromArgs(item.toolCall?.args);
      return (
        <ToolCallRow
          item={item}
          onClick={() => {
            if (filePath && onOpenFile) {
              onOpenFile(filePath);
            } else {
              onToolCallClick?.(item);
            }
          }}
        />
      );
    }

    return (
      <ToolCallRow
        item={item}
        onClick={() => onToolCallClick?.(item)}
      />
    );
  }
  return null;
}

interface AnimatedReasoningContentProps {
  content: string;
  isStreaming: boolean;
}

function AnimatedReasoningContent({ content, isStreaming }: AnimatedReasoningContentProps): React.ReactElement {
  const displayText = useAnimatedText(content || '', { enabled: isStreaming });
  return (
    <Markdown
      variant="compact"
      content={displayText}
      className="text-xs"
      style={{ opacity: 0.8 }}
    />
  );
}

interface ToolCallLiveRowProps {
  tc: ActivityItem;
  liveState?: LiveState;
}

/** Live tool call row -- monochrome state visuals.
 *
 * Active state:    2px left rule (.nrow.state-active::before) + label shimmer
 *                  + gentle icon pulse.
 * Completing state: no badge — row dims via opacity 0.7 and the title flips
 *                  to past tense. (We deliberately dropped the green ✓ to
 *                  avoid the SaaS-default badge look.)
 * Failed state:    gray ✕ badge overlaid on the tool icon + past-tense title.
 */
const ToolCallLiveRow = memo(function ToolCallLiveRow({ tc, liveState }: ToolCallLiveRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const args = tc.toolCall?.args;
  const IconComponent = getToolIcon(toolName, args);
  const isInProgress = liveState === 'active' && !tc.isComplete && !tc._recentlyCompleted;
  // Only `state-active` has a CSS treatment (left-rule shimmer in
  // ActivityBlock.css). Completing and failed states get their visual cue
  // from the inline icon badges below; keeping unused class hooks would
  // mislead the next CSS author.
  const stateClass = isInProgress ? 'state-active' : '';

  const activeLabel = isInProgress ? getActiveLabel(toolName, tc.toolCall, t) : null;
  const completedTitle = !isInProgress ? getCompletedRowTitle(toolName, tc.toolCall, t) : null;
  const summary = !isInProgress ? getCompletedSummary(toolName, tc.toolCall, t) : null;

  return (
    <motion.div
      className={`nrow ${stateClass} flex items-center gap-2 pl-3 pr-3 py-1.5`}
      animate={{ opacity: isInProgress ? 1 : 0.7 }}
      transition={{ duration: 0.3, ease: 'easeOut' }}
      style={{ fontSize: '13px', color: 'var(--Labels-Secondary)' }}
    >
      <div className="relative flex-shrink-0 flex items-center justify-center h-5 w-5">
        <motion.span
          animate={isInProgress ? { opacity: [0.7, 1, 0.7] } : { opacity: 1 }}
          transition={isInProgress ? { duration: 1.4, repeat: Infinity, ease: 'easeInOut' } : { duration: 0.2 }}
          style={{ display: 'inline-flex' }}
        >
          <IconComponent className="h-4 w-4" />
        </motion.span>
        <AnimatePresence>
          {liveState === 'failed' && (
            <motion.span
              key="fail"
              className="nrow-badge"
              initial={{ scale: 0, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={SPRING_SNAPPY}
              aria-label={t('toolArtifact.a11y.toolCallFailed')}
            >
              <XIcon className="h-3 w-3" />
            </motion.span>
          )}
        </AnimatePresence>
      </div>
      {isInProgress ? (
        <TextShimmer
          as="span"
          className="font-medium text-[13px] [--base-color:var(--Labels-Secondary)] [--base-gradient-color:var(--color-text-primary)] truncate"
          duration={1.5}
        >
          {activeLabel || ''}
        </TextShimmer>
      ) : (
        <>
          <span className="font-medium flex-shrink-0 whitespace-nowrap">{completedTitle}</span>
          {summary
            ? <span className="truncate min-w-0" style={{ opacity: 0.55 }}>&mdash; {summary}</span>
            : <span style={{ opacity: 0.55 }}>{t('toolArtifact.done')}</span>}
        </>
      )}
    </motion.div>
  );
});

interface PreparingToolCallRowProps {
  tc: PreparingToolCallData;
}

/** Preparing row -- shown while tool_call_chunks are still streaming.
 *  No left rule; just DotLoader + icon + label. Args aren't yet available
 *  to classify, so we fall back to the generic display name. */
function PreparingToolCallRow({ tc }: PreparingToolCallRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const displayName = toolName ? getDisplayName(toolName, t) : t('toolArtifact.toolCall');
  const IconComponent: LucideIcon = toolName ? getToolIcon(toolName) : Wrench;
  const prepText = getPreparingText(toolName, tc.argsLength, t);

  return (
    <div
      className="nrow flex items-center gap-2 pl-3 pr-3"
      style={{
        fontSize: '13px',
        color: 'var(--Labels-Secondary)',
        padding: '6px 12px',
        opacity: 0.85,
      }}
    >
      <DotLoader
        className="flex-shrink-0 gap-px"
        dotClassName="bg-foreground/15 [&.active]:bg-foreground size-[1.5px]"
      />
      <span className="flex-shrink-0 flex items-center justify-center h-5 w-5">
        <IconComponent className="h-4 w-4" />
      </span>
      <span className="font-medium">{displayName}</span>
      <span style={{ opacity: 0.55 }}>{prepText}</span>
    </div>
  );
}

/* --- Accordion sub-components --- */

interface ReasoningRowProps {
  item: ActivityItem;
}

/** Extract a leading `**subtitle**` line from reasoning content so we can
 * promote it into the row title and strip it from the body.
 *
 * Conservative on purpose — leaves content alone unless we see the o1-style
 * "header line, blank line, body" pattern:
 *   - bold appears at the very start (after optional whitespace)
 *   - bold spans a single line, no inner newlines or asterisks
 *   - bold is short enough to read as a heading (≤ 80 chars after trim)
 *   - bold is followed by ≥ 1 newline AND a non-empty body
 *
 * If any check fails, returns { title: null, body: original } and the row
 * keeps the generic "Reasoning" label. */
// Exported for unit testing alongside the components below; the only export
// in this file other than the default `ActivityBlock`. The HMR fast-refresh
// rule complains about mixed exports, but extracting this 8-line pure helper
// to its own module is more churn than it's worth.
// eslint-disable-next-line react-refresh/only-export-components
export function extractLeadingBoldHeader(content: string): { title: string | null; body: string } {
  if (!content) return { title: null, body: content };
  const match = content.match(/^\s*\*\*([^*\n]+?)\*\*\s*\n+([\s\S]+)$/);
  if (!match) return { title: null, body: content };
  const candidate = match[1].trim();
  const body = match[2];
  if (!candidate || candidate.length > 80 || !body.trim()) {
    return { title: null, body: content };
  }
  return { title: candidate, body };
}

const ReasoningRow = memo(function ReasoningRow({ item }: ReasoningRowProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(true);
  const { title: extractedTitle, body: extractedBody } = useMemo(
    () => extractLeadingBoldHeader(item.content || ''),
    [item.content],
  );
  const effectiveTitle = item.reasoningTitle || extractedTitle;
  const title = effectiveTitle || t('toolArtifact.reasoning');
  const displayContent = extractedTitle ? extractedBody : item.content;
  const hasContent = !!displayContent;

  return (
    <div className="titem">
      <div className="titem-icon">
        <Brain className="h-4 w-4" />
      </div>
      <div className="titem-body">
        <button
          type="button"
          onClick={() => hasContent && setExpanded(!expanded)}
          className={`titem-line text-left bg-transparent border-0 p-0 ${hasContent ? 'cursor-pointer' : 'cursor-default'}`}
          style={{ color: 'inherit' }}
        >
          <span className="titem-title truncate">{title}</span>
          {hasContent && (
            <motion.div
              animate={{ rotate: expanded ? 90 : 0 }}
              transition={SPRING}
              className="flex-shrink-0 inline-flex items-center"
              style={{ opacity: 0.6, alignSelf: 'center' }}
            >
              <ChevronDown className="h-3 w-3 -rotate-90" />
            </motion.div>
          )}
        </button>
        <AnimatePresence initial={false}>
          {expanded && displayContent && (
            <motion.div
              key="reasoning-content"
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={SPRING}
              style={{ overflow: 'hidden' }}
            >
              <div className="titem-reasoning-card">
                <Markdown
                  variant="compact"
                  content={displayContent}
                />
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
});

interface ToolCallRowProps {
  item: ActivityItem;
  onClick?: () => void;
}

/** Gray ✕ overlay marking a completed-but-failed tool call in the accordion
 * timeline. Reuses `.nrow-badge` from the live zone for visual parity. */
function FailedIconBadge({ label }: { label: string }): React.ReactElement {
  return (
    <span className="nrow-badge" aria-label={label}>
      <XIcon className="h-3 w-3" />
    </span>
  );
}

const ToolCallRow = memo(function ToolCallRow({ item, onClick }: ToolCallRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = item.toolName || '';
  const args = item.toolCall?.args;
  const title = getCompletedRowTitle(toolName, item.toolCall, t);
  const IconComponent = getToolIcon(toolName, args);
  const summary = getCompletedSummary(toolName, item.toolCall, t);
  const isFailed = item.isFailed === true;
  const failedLabel = t('toolArtifact.a11y.toolCallFailed');

  // Decide the destination tab label for the pill tooltip.
  const cat = categorizeTool(toolName, item.toolCall);
  const isMemory = cat === 'memoryRead' || cat === 'memoryWrite';
  const tabLabel = isMemory
    ? t('rightPanel.tabs.memory')
    : cat === 'memo' || cat === 'memoWrite'
      ? t('rightPanel.tabs.memo')
      : t('rightPanel.tabs.files');

  // No pill → the row title becomes the click target so the affordance isn't
  // lost (e.g., memory/memo index rows where the verb already names the file).
  const titleNode = summary ? (
    <span className="titem-title">{title}</span>
  ) : (
    <button
      type="button"
      onClick={onClick}
      className="titem-title titem-title-button"
      title={t('toolArtifact.a11y.openInTab', { tab: tabLabel })}
    >
      {title}
    </button>
  );

  return (
    <div className={`titem${isFailed ? ' failed' : ''}`}>
      <div className="titem-icon" title={isFailed ? failedLabel : undefined}>
        <IconComponent className="h-4 w-4" style={{ color: 'var(--Labels-Secondary)' }} />
        {isFailed && <FailedIconBadge label={failedLabel} />}
      </div>
      <div className="titem-body">
        <div className="titem-line">
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
        </div>
      </div>
    </div>
  );
});

interface EditToolRowProps {
  item: ActivityItem;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

const EditToolRow = memo(function EditToolRow({ item, onOpenFile }: EditToolRowProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const args = (item.toolCall?.args || {}) as Record<string, unknown>;
  const title = getCompletedRowTitle(item.toolName || 'Edit', item.toolCall, t);
  const IconComponent = getToolIcon(item.toolName || 'Edit', args);
  const isFailed = item.isFailed === true;
  const failedLabel = t('toolArtifact.a11y.toolCallFailed');

  const filePath = getFilePathFromArgs(args);
  const fileName = filePath ? filePath.split('/').pop() : '';
  const oldStr = (args.old_string || args.oldString || '') as string;
  const newStr = (args.new_string || args.newString || '') as string;
  const hasDiff = !!(oldStr || newStr);
  const summary = getCompletedSummary(item.toolName || 'Edit', item.toolCall, t) || fileName;

  // Match the destination tab label to the actual classification of the
  // path so the tooltip doesn't lie when the click routes to Memory.
  const editCat = categorizeTool(item.toolName || 'Edit', item.toolCall);
  const editTabLabel =
    editCat === 'memoryWrite' || editCat === 'memoryRead'
      ? t('rightPanel.tabs.memory')
      : editCat === 'memo' || editCat === 'memoWrite'
        ? t('rightPanel.tabs.memo')
        : t('rightPanel.tabs.files');

  return (
    <div className={`titem${isFailed ? ' failed' : ''}`}>
      <div className="titem-icon" title={isFailed ? failedLabel : undefined}>
        <IconComponent className="h-4 w-4" />
        {isFailed && <FailedIconBadge label={failedLabel} />}
      </div>
      <div className="titem-body">
        <div className="titem-line">
          <span className="titem-title">{title}</span>
          {summary && (
            <span className="titem-pill-wrap">
              <button
                type="button"
                onClick={() => filePath && onOpenFile?.(filePath)}
                className="obj"
                title={t('toolArtifact.a11y.openInTab', { tab: editTabLabel })}
              >
                {summary}
              </button>
            </span>
          )}
          {hasDiff && (
            <button
              type="button"
              onClick={() => setExpanded(!expanded)}
              className="titem-trail bg-transparent border-0 p-0 cursor-pointer"
              style={{ color: 'inherit' }}
              aria-label={expanded ? t('toolArtifact.a11y.collapseDiff') : t('toolArtifact.a11y.expandDiff')}
            >
              <motion.div
                animate={{ rotate: expanded ? 90 : 0 }}
                transition={SPRING}
              >
                <ChevronDown className="h-3 w-3 -rotate-90" style={{ opacity: 0.5 }} />
              </motion.div>
            </button>
          )}
        </div>

        <AnimatePresence initial={false}>
          {expanded && hasDiff && (
            <motion.div
              key="diff-content"
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={SPRING}
              style={{ overflow: 'hidden' }}
            >
              <div className="mt-2 rounded overflow-hidden" style={{ fontSize: '12px', border: '1px solid var(--color-border-muted)' }}>
                {oldStr && (
                  <div style={{ backgroundColor: 'var(--color-loss-soft)' }}>
                    {oldStr.split('\n').map((line, i) => (
                      <div key={`old-${i}`} className="flex" style={{ minHeight: '20px' }}>
                        <span
                          className="flex-shrink-0 select-none text-right px-2"
                          style={{ color: 'var(--color-loss-muted)', width: '20px', userSelect: 'none' }}
                        >&minus;</span>
                        <pre className="flex-1 font-mono whitespace-pre-wrap break-all m-0 pr-2" style={{ color: 'var(--color-loss)' }}>
                          {line}
                        </pre>
                      </div>
                    ))}
                  </div>
                )}
                {newStr && (
                  <div style={{ backgroundColor: 'var(--color-profit-soft)' }}>
                    {newStr.split('\n').map((line, i) => (
                      <div key={`new-${i}`} className="flex" style={{ minHeight: '20px' }}>
                        <span
                          className="flex-shrink-0 select-none text-right px-2"
                          style={{ color: 'var(--color-profit-muted)', width: '20px', userSelect: 'none' }}
                        >+</span>
                        <pre className="flex-1 font-mono whitespace-pre-wrap break-all m-0 pr-2" style={{ color: 'var(--color-profit)' }}>
                          {line}
                        </pre>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
});

export default ActivityBlock;
