import { useState } from 'react';
import { Brain, ChevronDown, ChevronRight } from 'lucide-react';
import { getDisplayName, getToolIcon } from './toolDisplayConfig';
import Markdown from './Markdown';
import {
  INLINE_CHART_TOOLS,
  InlineStockPriceCard,
  InlineCompanyOverviewCard,
  InlineMarketIndicesCard,
  InlineSectorPerformanceCard,
  InlineSecFilingCard,
} from './charts/InlineMarketCharts';

/** Tool names where clicking should open the file in the FilePanel */
const FILE_NAV_TOOLS = new Set(['Read', 'Write', 'Save', 'read_file', 'write_file', 'save_file']);

function getFilePathFromArgs(args) {
  if (!args) return null;
  return args.file_path || args.filePath || args.path || args.filename || null;
}

/**
 * ActivityAccordion Component
 *
 * A single collapsible row showing completed reasoning + tool call steps.
 * Collapsed: shows count + icons (e.g., "5 steps completed")
 * Expanded: lists all items vertically with clickable tool call rows.
 *
 * @param {Array} completedItems - chronologically ordered array of {type, ...data}
 * @param {Function} onToolCallClick - (toolCallProcess) => void, opens detail panel
 * @param {Function} onOpenFile - (filePath) => void, opens file in FilePanel
 */
/** Map artifact type → inline chart component */
const INLINE_CHART_MAP = {
  stock_prices: InlineStockPriceCard,
  company_overview: InlineCompanyOverviewCard,
  market_indices: InlineMarketIndicesCard,
  sector_performance: InlineSectorPerformanceCard,
  sec_filing: InlineSecFilingCard,
};

function ActivityAccordion({ completedItems, onToolCallClick, onOpenFile }) {
  const [isExpanded, setIsExpanded] = useState(false);

  if (!completedItems || completedItems.length === 0) return null;

  // Split items: inline chart cards vs regular accordion items
  const inlineChartItems = [];
  const accordionItems = [];

  for (const item of completedItems) {
    if (
      item.type === 'tool_call' &&
      INLINE_CHART_TOOLS.has(item.toolName || '') &&
      item.toolCallResult?.artifact
    ) {
      inlineChartItems.push(item);
    } else {
      accordionItems.push(item);
    }
  }

  const reasoningCount = accordionItems.filter((i) => i.type === 'reasoning').length;
  const toolCallCount = accordionItems.filter((i) => i.type === 'tool_call').length;

  // Build summary label (only for accordion items)
  let summaryLabel;
  if (reasoningCount > 0 && toolCallCount > 0) {
    const parts = [];
    if (reasoningCount > 0) parts.push(`${reasoningCount} reasoning`);
    if (toolCallCount > 0) parts.push(`${toolCallCount} tool call${toolCallCount > 1 ? 's' : ''}`);
    summaryLabel = parts.join(' · ');
  } else if (accordionItems.length > 0) {
    summaryLabel = `${accordionItems.length} step${accordionItems.length > 1 ? 's' : ''} completed`;
  }

  return (
    <div className="mt-1 mb-1">
      {/* Inline chart cards — always visible, above the accordion */}
      {inlineChartItems.length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginBottom: accordionItems.length > 0 ? 6 : 0 }}>
          {inlineChartItems.map((item, idx) => {
            const artifact = item.toolCallResult.artifact;
            const ChartComponent = INLINE_CHART_MAP[artifact.type];
            if (!ChartComponent) return null;
            const toolName = item.toolName || '';
            const displayName = getDisplayName(toolName);
            const IconComponent = getToolIcon(toolName);
            return (
              <div key={`chart-${item.id || idx}`}>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '4px 10px',
                    fontSize: 12,
                    color: 'var(--Labels-Tertiary)',
                  }}
                >
                  <IconComponent style={{ width: 13, height: 13, opacity: 0.7 }} />
                  <span>{displayName}</span>
                </div>
                <ChartComponent
                  artifact={artifact}
                  onClick={() => onToolCallClick?.(item)}
                />
              </div>
            );
          })}
        </div>
      )}

      {/* Collapsible accordion for remaining items */}
      {accordionItems.length > 0 && (
        <>
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="flex items-center gap-2 transition-colors hover:bg-white/5 w-full rounded-md"
            style={{
              padding: '5px 10px',
              fontSize: '13px',
              color: 'var(--Labels-Tertiary)',
            }}
          >
            {isExpanded ? (
              <ChevronDown className="h-3.5 w-3.5 flex-shrink-0" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5 flex-shrink-0" />
            )}
            <span>{summaryLabel}</span>
          </button>

          {isExpanded && (
            <div
              className="mt-1 ml-2 space-y-0.5 rounded-md"
              style={{
                backgroundColor: 'rgba(255, 255, 255, 0.02)',
                padding: '4px 0',
              }}
            >
              {accordionItems.map((item, idx) => {
                if (item.type === 'reasoning') {
                  return (
                    <ReasoningRow key={`r-${item.id || idx}`} item={item} />
                  );
                }
                if (item.type === 'tool_call') {
                  const toolName = item.toolName || '';

                  // Edit tool — show inline diff
                  if (toolName === 'Edit' || toolName === 'edit_file') {
                    return (
                      <EditToolRow
                        key={`t-${item.id || idx}`}
                        item={item}
                        onOpenFile={onOpenFile}
                      />
                    );
                  }

                  // Read/Write — navigate to file
                  if (FILE_NAV_TOOLS.has(toolName)) {
                    const filePath = getFilePathFromArgs(item.toolCall?.args);
                    return (
                      <ToolCallRow
                        key={`t-${item.id || idx}`}
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

                  // All other tools — open detail panel
                  return (
                    <ToolCallRow
                      key={`t-${item.id || idx}`}
                      item={item}
                      onClick={() => onToolCallClick?.(item)}
                    />
                  );
                }
                return null;
              })}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function ReasoningRow({ item }) {
  const [expanded, setExpanded] = useState(false);
  const title = item.reasoningTitle || 'Reasoning';
  const hasContent = !!item.content;

  return (
    <div>
      <button
        onClick={() => hasContent && setExpanded(!expanded)}
        className={`flex items-center gap-2 px-3 py-1 w-full text-left rounded ${hasContent ? 'transition-colors hover:bg-white/5 cursor-pointer' : ''}`}
        style={{ fontSize: '13px', color: 'var(--Labels-Tertiary)' }}
      >
        <Brain className="h-3.5 w-3.5 flex-shrink-0" style={{ opacity: 0.7 }} />
        <span className="truncate">{title}</span>
        {hasContent && (
          <ChevronDown
            className="h-3 w-3 flex-shrink-0 ml-auto transition-transform"
            style={{ opacity: 0.5, transform: expanded ? 'rotate(180deg)' : undefined }}
          />
        )}
      </button>
      {expanded && item.content && (
        <Markdown
          variant="compact"
          content={item.content}
          className="ml-3 pl-3 pr-2 py-1 text-xs"
          style={{ borderLeft: '2px solid rgba(97, 85, 245, 0.3)' }}
        />
      )}
    </div>
  );
}

function ToolCallRow({ item, onClick }) {
  const toolName = item.toolName || '';
  const displayName = getDisplayName(toolName);
  const IconComponent = getToolIcon(toolName);

  // Build a short summary from args
  let summary = '';
  const args = item.toolCall?.args;
  if (args?.symbol) summary = args.symbol;
  else if (args?.query) summary = args.query;
  else if (args?.file_path || args?.filePath) {
    const fp = args.file_path || args.filePath;
    summary = fp.split('/').pop() || '';
  }

  return (
    <button
      onClick={onClick}
      className="flex items-center gap-2 px-3 py-1 w-full text-left transition-colors hover:bg-white/5 rounded"
      style={{ fontSize: '13px', color: 'var(--Labels-Tertiary)' }}
    >
      <IconComponent className="h-3.5 w-3.5 flex-shrink-0" style={{ opacity: 0.7 }} />
      <span className="font-medium" style={{ color: 'var(--Labels-Secondary)' }}>
        {displayName}
      </span>
      {summary && (
        <span className="truncate" style={{ opacity: 0.6 }}>
          — {summary}
        </span>
      )}
    </button>
  );
}

/**
 * EditToolRow — shows file name, expandable inline diff of old_string vs new_string.
 * Clicking the file name navigates to the file in FilePanel.
 */
function EditToolRow({ item, onOpenFile }) {
  const [expanded, setExpanded] = useState(false);
  const displayName = getDisplayName(item.toolName || 'Edit');
  const IconComponent = getToolIcon(item.toolName || 'Edit');

  const args = item.toolCall?.args || {};
  const filePath = getFilePathFromArgs(args);
  const fileName = filePath ? filePath.split('/').pop() : '';
  const oldStr = args.old_string || args.oldString || '';
  const newStr = args.new_string || args.newString || '';
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
            style={{ opacity: 0.6, color: '#6155F5', background: 'none', border: 'none', cursor: 'pointer', padding: 0, fontSize: 'inherit' }}
          >
            — {fileName}
          </button>
        )}
        {hasDiff && (
          <button
            onClick={() => setExpanded(!expanded)}
            className="ml-auto flex-shrink-0"
            style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 0, color: 'inherit' }}
          >
            <ChevronDown
              className="h-3 w-3 transition-transform"
              style={{ opacity: 0.5, transform: expanded ? 'rotate(180deg)' : undefined }}
            />
          </button>
        )}
      </div>

      {expanded && hasDiff && (
        <div className="ml-6 mr-2 mt-1 mb-1 rounded overflow-hidden" style={{ fontSize: '12px', border: '1px solid rgba(255,255,255,0.08)' }}>
          {oldStr && (
            <div style={{ backgroundColor: 'rgba(220, 38, 38, 0.1)' }}>
              {oldStr.split('\n').map((line, i) => (
                <div key={`old-${i}`} className="flex" style={{ minHeight: '20px' }}>
                  <span
                    className="flex-shrink-0 select-none text-right px-2"
                    style={{ color: 'rgba(220, 38, 38, 0.6)', width: '20px', userSelect: 'none' }}
                  >−</span>
                  <pre className="flex-1 font-mono whitespace-pre-wrap break-all m-0 pr-2" style={{ color: 'rgba(255, 150, 150, 0.85)' }}>
                    {line}
                  </pre>
                </div>
              ))}
            </div>
          )}
          {newStr && (
            <div style={{ backgroundColor: 'rgba(34, 197, 94, 0.08)' }}>
              {newStr.split('\n').map((line, i) => (
                <div key={`new-${i}`} className="flex" style={{ minHeight: '20px' }}>
                  <span
                    className="flex-shrink-0 select-none text-right px-2"
                    style={{ color: 'rgba(34, 197, 94, 0.6)', width: '20px', userSelect: 'none' }}
                  >+</span>
                  <pre className="flex-1 font-mono whitespace-pre-wrap break-all m-0 pr-2" style={{ color: 'rgba(150, 255, 150, 0.85)' }}>
                    {line}
                  </pre>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default ActivityAccordion;
