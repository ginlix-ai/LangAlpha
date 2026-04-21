import React, { useState, useRef, useMemo, memo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Brain, ChevronDown, Wrench } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { getDisplayName, getToolIcon, getInProgressText, getPreparingText, getCompletedSummary } from './toolDisplayConfig';
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

/** Tool names where clicking should open the file in the FilePanel */
const FILE_NAV_TOOLS = new Set(['Read', 'Write', 'Save', 'read_file', 'write_file', 'save_file']);

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

type LiveState = 'active' | 'completing' | 'completed';

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
 * Replaces the old ActivityAccordion + LiveActivity two-component architecture.
 * Items move from the live zone to the accordion zone in the same React render,
 * eliminating the visible gap caused by fade-out/reappear across render cycles.
 *
 * Uses framer-motion spring animations for smooth accordion expand/collapse,
 * item entrance/exit, and chevron rotation.
 */
const ActivityBlock = memo(function ActivityBlock({ items, preparingToolCall, isStreaming, onToolCallClick, onOpenFile }: ActivityBlockProps): React.ReactElement | null {
  const { t } = useTranslation();
  const [isExpanded, setIsExpanded] = useState(false);
  const prevCompletedIdsRef = useRef<Set<string | undefined>>(new Set());

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

  const hasInlineCharts = inlineChartItems.length > 0;
  const hasCompleted = completedItems.length > 0;
  const hasLive = liveItems.length > 0;
  const hasPreparingTools = !!preparingToolCall;

  if (!hasInlineCharts && !hasCompleted && !hasLive && !hasPreparingTools) return null;

  // Build accordion summary label
  const reasoningCount = completedItems.filter(i => i.type === 'reasoning').length;
  const toolCallCount = completedItems.filter(i => i.type === 'tool_call').length;
  let summaryLabel: string | undefined;
  if (reasoningCount > 0 && toolCallCount > 0) {
    const parts: string[] = [];
    if (reasoningCount > 0) parts.push(t('toolArtifact.nReasoning', { count: reasoningCount }));
    if (toolCallCount > 0) parts.push(t('toolArtifact.nToolCalls', { count: toolCallCount }));
    summaryLabel = parts.join(' \u00b7 ');
  } else if (completedItems.length > 0) {
    summaryLabel = t('toolArtifact.nStepsCompleted', { count: completedItems.length });
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
              onClick={() => setIsExpanded(!isExpanded)}
              className="flex items-center gap-2 transition-colors hover:bg-foreground/5 w-full rounded-md"
              style={{
                padding: '5px 10px',
                fontSize: '13px',
                color: 'var(--Labels-Tertiary)',
              }}
            >
              <motion.div
                animate={{ rotate: isExpanded ? 90 : 0 }}
                transition={SPRING}
                className="flex-shrink-0"
              >
                <ChevronDown className="h-3.5 w-3.5 -rotate-90" />
              </motion.div>
              <span>{summaryLabel}</span>
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
                    className="mt-1 ml-2 space-y-0.5 rounded-md"
                    style={{
                      borderLeft: '2px solid var(--color-border-muted)',
                      padding: '4px 0',
                    }}
                  >
                    {completedItems.map((item, idx) => {
                      const itemId = item.id || item.toolCallId;
                      const isNew = newlyCompletedIds.has(itemId);
                      const itemKey = item.type === 'reasoning' ? `r-${itemId || idx}` : `t-${itemId || idx}`;

                      const content = renderCompletedItem(item, idx, onToolCallClick, onOpenFile);
                      if (!content) return null;

                      if (isNew) {
                        return (
                          <motion.div
                            key={itemKey}
                            initial={{ opacity: 0, height: 0 }}
                            animate={{ opacity: 1, height: 'auto' }}
                            transition={SPRING_SNAPPY}
                            style={{ overflow: 'hidden' }}
                          >
                            {content}
                          </motion.div>
                        );
                      }

                      return <div key={itemKey}>{content}</div>;
                    })}
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
                            {item.reasoningTitle
                              ? t('toolArtifact.reasoningLabel', { title: item.reasoningTitle })
                              : t('toolArtifact.reasoningPending')}
                          </TextShimmer>
                        ) : (
                          <span className="font-medium truncate">{t('toolArtifact.reasoningComplete')}</span>
                        )}
                      </div>

                      {item.content && (
                        <AnimatedReasoningContent
                          content={item.content}
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

/** Renders a single completed item (reasoning or tool_call) for the accordion body */
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

    if (toolName === 'Edit' || toolName === 'edit_file') {
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

/** Animated reasoning content -- smoothly reveals text during streaming */
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

/** Live tool call row -- shows active or completing state */
const ToolCallLiveRow = memo(function ToolCallLiveRow({ tc, liveState }: ToolCallLiveRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const displayName = getDisplayName(toolName, t);
  const IconComponent = getToolIcon(toolName);
  const isInProgress = liveState === 'active' && !tc.isComplete && !tc._recentlyCompleted;
  const progressText = isInProgress ? getInProgressText(toolName, tc.toolCall, t) : null;

  return (
    <motion.div
      className="flex items-center gap-2 px-3 rounded-md"
      animate={{
        backgroundColor: isInProgress ? 'var(--color-accent-soft)' : 'var(--color-border-muted)',
        opacity: isInProgress ? 1 : 0.6,
      }}
      transition={{ duration: 0.3, ease: 'easeOut' }}
      style={{
        border: '1px solid var(--color-border-muted)',
        fontSize: '13px',
        color: 'var(--Labels-Secondary)',
        paddingTop: '6px',
        paddingBottom: '6px',
      }}
    >
      <div className="relative flex-shrink-0">
        <IconComponent className="h-4 w-4" />
        <AnimatePresence>
          {!isInProgress && (
            <motion.span
              initial={{ scale: 0, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={SPRING_SNAPPY}
              className="h-3 w-3 absolute -top-0.5 -right-0.5 flex items-center justify-center"
              style={{ color: 'var(--color-profit-muted)' }}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="h-3 w-3">
                <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14" />
                <polyline points="22 4 12 14.01 9 11.01" />
              </svg>
            </motion.span>
          )}
        </AnimatePresence>
      </div>
      {isInProgress ? (
        <TextShimmer
          as="span"
          className="font-medium text-[13px] [--base-color:var(--Labels-Secondary)] [--base-gradient-color:var(--color-text-primary)]"
          duration={1.5}
        >
          {`${displayName} ${progressText || ''}`}
        </TextShimmer>
      ) : (
        <>
          <span className="font-medium flex-shrink-0 whitespace-nowrap">{displayName}</span>
          {(() => {
            const summary = getCompletedSummary(toolName, tc.toolCall);
            return summary
              ? <span className="truncate min-w-0" style={{ opacity: 0.55 }}>&mdash; {summary}</span>
              : <span style={{ opacity: 0.55 }}>{t('toolArtifact.done')}</span>;
          })()}
        </>
      )}
    </motion.div>
  );
});

interface PreparingToolCallRowProps {
  tc: PreparingToolCallData;
}

/** Preparing row -- shown while tool_call_chunks are still streaming */
function PreparingToolCallRow({ tc }: PreparingToolCallRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = tc.toolName || '';
  const displayName = toolName ? getDisplayName(toolName, t) : t('toolArtifact.toolCall');
  const IconComponent: LucideIcon = toolName ? getToolIcon(toolName) : Wrench;
  const prepText = getPreparingText(toolName, tc.argsLength, t);

  return (
    <div
      className="flex items-center gap-2 px-3"
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
      <IconComponent className="h-4 w-4 flex-shrink-0" />
      <span className="font-medium">{displayName}</span>
      <span style={{ opacity: 0.55 }}>{prepText}</span>
    </div>
  );
}

/* --- Accordion sub-components --- */

interface ReasoningRowProps {
  item: ActivityItem;
}

const ReasoningRow = memo(function ReasoningRow({ item }: ReasoningRowProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(true);
  const title = item.reasoningTitle ? t('toolArtifact.reasoningLabel', { title: item.reasoningTitle }) : t('toolArtifact.reasoning');
  const hasContent = !!item.content;

  return (
    <div>
      <button
        onClick={() => hasContent && setExpanded(!expanded)}
        className={`flex items-center gap-2 px-3 py-1 w-full text-left rounded ${hasContent ? 'transition-colors hover:bg-foreground/5 cursor-pointer' : ''}`}
        style={{ fontSize: '13px', color: 'var(--Labels-Tertiary)' }}
      >
        <Brain className="h-3.5 w-3.5 flex-shrink-0" style={{ opacity: 0.7 }} />
        <span className="truncate">{title}</span>
        {hasContent && (
          <motion.div
            animate={{ rotate: expanded ? 180 : 0 }}
            transition={SPRING}
            className="ml-auto flex-shrink-0"
          >
            <ChevronDown className="h-3 w-3" style={{ opacity: 0.5 }} />
          </motion.div>
        )}
      </button>
      <AnimatePresence initial={false}>
        {expanded && item.content && (
          <motion.div
            key="reasoning-content"
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={SPRING}
            style={{ overflow: 'hidden' }}
          >
            <Markdown
              variant="compact"
              content={item.content}
              className="ml-3 pl-3 pr-2 py-1 text-xs"
              style={{ borderLeft: '2px solid var(--color-accent-overlay)' }}
            />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
});

interface ToolCallRowProps {
  item: ActivityItem;
  onClick?: () => void;
}

const ToolCallRow = memo(function ToolCallRow({ item, onClick }: ToolCallRowProps): React.ReactElement {
  const { t } = useTranslation();
  const toolName = item.toolName || '';
  const displayName = getDisplayName(toolName, t);
  const IconComponent = getToolIcon(toolName);

  const summary = getCompletedSummary(toolName, item.toolCall) || '';

  return (
    <button
      onClick={onClick}
      className="flex items-center gap-2 px-3 py-1 w-full text-left transition-colors hover:bg-foreground/5 rounded"
      style={{ fontSize: '13px', color: 'var(--Labels-Tertiary)' }}
    >
      <IconComponent className="h-3.5 w-3.5 flex-shrink-0" style={{ opacity: 0.7 }} />
      <span className="font-medium flex-shrink-0 whitespace-nowrap" style={{ color: 'var(--Labels-Secondary)' }}>
        {displayName}
      </span>
      {summary && (
        <span className="truncate min-w-0" style={{ opacity: 0.6 }}>
          &mdash; {summary}
        </span>
      )}
    </button>
  );
});

interface EditToolRowProps {
  item: ActivityItem;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

const EditToolRow = memo(function EditToolRow({ item, onOpenFile }: EditToolRowProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const displayName = getDisplayName(item.toolName || 'Edit', t);
  const IconComponent = getToolIcon(item.toolName || 'Edit');

  const args = (item.toolCall?.args || {}) as Record<string, unknown>;
  const filePath = getFilePathFromArgs(args);
  const fileName = filePath ? filePath.split('/').pop() : '';
  const oldStr = (args.old_string || args.oldString || '') as string;
  const newStr = (args.new_string || args.newString || '') as string;
  const hasDiff = !!(oldStr || newStr);

  return (
    <div>
      <div
        className="flex items-center gap-2 px-3 py-1 w-full text-left rounded"
        style={{ fontSize: '13px', color: 'var(--Labels-Tertiary)' }}
      >
        <IconComponent className="h-3.5 w-3.5 flex-shrink-0" style={{ opacity: 0.7 }} />
        <span className="font-medium" style={{ color: 'var(--Labels-Secondary)' }}>
          {displayName}
        </span>
        {fileName && (
          <button
            onClick={() => filePath && onOpenFile?.(filePath)}
            className="truncate transition-colors hover:underline"
            style={{ opacity: 0.6, color: 'var(--color-accent-primary)', background: 'none', border: 'none', cursor: 'pointer', padding: 0, fontSize: 'inherit' }}
          >
            &mdash; {fileName}
          </button>
        )}
        {hasDiff && (
          <button
            onClick={() => setExpanded(!expanded)}
            className="ml-auto flex-shrink-0"
            style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 0, color: 'inherit' }}
          >
            <motion.div
              animate={{ rotate: expanded ? 180 : 0 }}
              transition={SPRING}
            >
              <ChevronDown className="h-3 w-3" style={{ opacity: 0.5 }} />
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
            <div className="ml-6 mr-2 mt-1 mb-1 rounded overflow-hidden" style={{ fontSize: '12px', border: '1px solid var(--color-border-muted)' }}>
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
  );
});

export default ActivityBlock;
