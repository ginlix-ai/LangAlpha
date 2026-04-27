import React, { useState, useRef } from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';
import { X, FileText, ArrowRight, Zap, Loader2, ExternalLink, ChevronRight } from 'lucide-react';
import { getDisplayName, getToolIcon, stripLineNumbers, parseTruncatedResult } from './toolDisplayConfig';
import {
  StockPriceChart,
  CompanyOverviewCard,
  MarketIndicesChart,
  SectorPerformanceChart,
  StockScreenerTable,
} from './charts/MarketDataCharts';
import SecFilingViewer from './charts/SecFilingViewer';
import { FaviconImg, googleFaviconUrl } from './charts/InlineArtifactCards';
import AutomationDetailPanel from './charts/AutomationDetailPanel';
import Markdown, { CodeBlock } from './Markdown';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { parseDisplayableResults, buildRichResultMap, resolveSnippet } from './webSearchUtils';

// --- Types ---

/** Loose record for artifact data from SSE */
type ArtifactRecord = Record<string, unknown> & { type?: string };

/** Loose record for tool call args */
type ToolCallArgs = Record<string, unknown>;

interface ToolCallData {
  id?: string;
  name?: string;
  args?: ToolCallArgs;
}

interface ToolCallResultData {
  content?: string | unknown;
  artifact?: ArtifactRecord;
  [key: string]: unknown;
}

interface ToolCallProcessRecord {
  toolName?: string;
  toolCall?: ToolCallData | null;
  toolCallResult?: ToolCallResultData | null;
  isInProgress?: boolean;
  isComplete?: boolean;
  isFailed?: boolean;
  _subagentResult?: string | null;
  _subagentStatus?: string | null;
  [key: string]: unknown;
}

interface SubagentInfo {
  subagentId: string;
  description?: string;
  type?: string;
  status?: string;
}

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
  const scrollContainerRef = useRef<HTMLDivElement>(null);

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

  // Extract subagent info from Task tool args
  const subagentType = isTaskTool ? ((toolCallProcess.toolCall?.args?.subagent_type as string) || 'general-purpose') : '';
  const subagentDescription = isTaskTool ? ((toolCallProcess.toolCall?.args?.description as string) || '') : '';
  const subagentId = isTaskTool ? toolCallProcess.toolCall?.id ?? null : null;
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

      {/* URL bar for WebFetch */}
      {(toolName === 'WebFetch' || toolName === 'web_fetch') && (toolCallProcess.toolCall?.args?.url as string | undefined) && (
        <a
          href={toolCallProcess.toolCall!.args!.url as string}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-2 px-4 py-2 text-xs truncate flex-shrink-0 transition-colors hover:bg-foreground/5"
          style={{
            color: 'var(--color-accent-primary)',
            borderBottom: '1px solid var(--color-border-muted)',
          }}
        >
          <ExternalLink className="h-3 w-3 flex-shrink-0" />
          <span className="truncate">{toolCallProcess.toolCall!.args!.url as string}</span>
        </a>
      )}

      {/* Content — on mobile, no overflow-y-auto so MobileBottomSheet's scroll container handles it.
           Exception: sec_filing needs flex layout because the iframe fills available height and scrolls internally. */}
      <div
        ref={scrollContainerRef}
        className={`${isMobile && artifact?.type !== 'sec_filing' ? '' : 'flex-1'} px-4 py-4 overflow-x-hidden ${artifact?.type === 'sec_filing' ? 'flex flex-col overflow-hidden' : (isMobile ? '' : 'overflow-y-auto')}`}
        style={isMobile && artifact?.type !== 'sec_filing' ? undefined : { minHeight: 0 }}
      >
        {isTaskTool ? (
          <TaskToolContent
            content={content}
            description={subagentDescription}
            type={subagentType}
            subagentId={subagentId}
            subagentResult={toolCallProcess._subagentResult || null}
            subagentStatus={toolCallProcess._subagentStatus || null}
            onOpenSubagentTask={onOpenSubagentTask}
          />
        ) : (
          <ArtifactOrMarkdown
            artifact={artifact}
            content={content}
            toolName={toolName}
            toolCallProcess={toolCallProcess}
            onOpenFile={onOpenFile}
            scrollContainerRef={scrollContainerRef}
          />
        )}
      </div>
    </div>
  );
}

// --- TaskToolContent ---

interface TaskToolContentProps {
  content?: string | unknown;
  description: string;
  type: string;
  subagentId: string | null;
  subagentResult: string | null;
  subagentStatus: string | null;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
}

function TaskToolContent({ description, type, subagentId, subagentResult, subagentStatus, onOpenSubagentTask }: TaskToolContentProps): React.ReactElement {
  const { t } = useTranslation();

  const handleGoToSubagent = () => {
    if (onOpenSubagentTask && subagentId) {
      onOpenSubagentTask({
        subagentId,
        description,
        type,
        status: subagentStatus || 'completed',
      });
    }
  };

  const isRunning = subagentStatus && subagentStatus !== 'completed';

  return (
    <div className="space-y-4">
      {/* Instructions section */}
      {description && (
        <div>
          <div
            className="text-xs font-medium uppercase tracking-wider mb-2 px-1"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('toolArtifact.instructions')}
          </div>
          <div
            className="rounded-lg px-3 py-3"
            style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)' }}
          >
            <Markdown variant="panel" content={description} className="text-sm" />
          </div>
        </div>
      )}

      {/* Result section */}
      <div>
        <div
          className="text-xs font-medium uppercase tracking-wider mb-2 px-1"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('toolArtifact.result')}
        </div>
        {subagentResult ? (
          <div
            className="rounded-lg px-3 py-3"
            style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)' }}
          >
            <Markdown variant="panel" content={subagentResult} className="text-sm" />
          </div>
        ) : isRunning ? (
          <div
            className="flex items-center gap-2 px-3 py-3 rounded-lg"
            style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)' }}
          >
            <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
            <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('toolArtifact.subagentStillRunning')}
            </span>
          </div>
        ) : (
          <div
            className="px-3 py-3 rounded-lg text-sm"
            style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
          >
            {t('toolArtifact.noResultAvailable')}
          </div>
        )}
      </div>

      {/* Footer link to subagent tab */}
      {onOpenSubagentTask && subagentId && (
        <button
          onClick={handleGoToSubagent}
          className="flex items-center gap-3 w-full px-3 py-2.5 rounded-lg transition-colors hover:brightness-110"
          style={{
            backgroundColor: 'var(--color-accent-soft)',
            border: '1px solid var(--color-accent-soft)',
          }}
        >
          <img src={isRunning ? iconRoboSing : iconRobo} alt="Subagent" className="w-5 h-5 flex-shrink-0" />
          <div className="flex flex-col gap-0.5 min-w-0 flex-1 text-left">
            <span className="text-xs font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('toolArtifact.goToSubagentTab')}
            </span>
            {description && (
              <span className="text-xs truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                {description}
              </span>
            )}
          </div>
          <ArrowRight className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
        </button>
      )}
    </div>
  );
}

// --- formatToolInput ---

interface FormattedToolInput {
  code: string;
  language: string;
}

function formatToolInput(toolName: string, args: ToolCallArgs | undefined): FormattedToolInput | null {
  if (!args) return null;
  if (toolName === 'ExecuteCode') {
    return args.code ? { code: args.code as string, language: (args.language as string) || 'python' } : null;
  }
  if (toolName === 'Bash') {
    return args.command ? { code: args.command as string, language: 'bash' } : null;
  }
  if (toolName === 'Grep') {
    const parts = ['grep'];
    if (args.pattern) parts.push(`"${args.pattern}"`);
    if (args.path) parts.push(args.path as string);
    if (args.glob) parts.push(`--glob="${args.glob}"`);
    if (args.type) parts.push(`--type=${args.type}`);
    if (args.output_mode) parts.push(`--output=${args.output_mode}`);
    if (args.context) parts.push(`-C ${args.context}`);
    if (args['-A']) parts.push(`-A ${args['-A']}`);
    if (args['-B']) parts.push(`-B ${args['-B']}`);
    if (args['-C']) parts.push(`-C ${args['-C']}`);
    if (args['-i']) parts.push('-i');
    if (args.head_limit) parts.push(`--head=${args.head_limit}`);
    return parts.length > 1 ? { code: parts.join(' '), language: 'bash' } : null;
  }
  if (toolName === 'Glob') {
    const parts = ['glob'];
    if (args.pattern) parts.push(`"${args.pattern}"`);
    if (args.path) parts.push(args.path as string);
    return parts.length > 1 ? { code: parts.join(' '), language: 'bash' } : null;
  }
  return null;
}

// --- CodeToolDisplay ---

interface CodeToolDisplayProps {
  toolName: string;
  toolCallProcess: ToolCallProcessRecord;
  rawContent: string;
}

function CodeToolDisplay({ toolName, toolCallProcess, rawContent }: CodeToolDisplayProps): React.ReactElement {
  const { t } = useTranslation();
  const [inputExpanded, setInputExpanded] = useState(false);
  const input = formatToolInput(toolName, toolCallProcess.toolCall?.args);

  let displayContent = rawContent || t('toolArtifact.noResultContent');
  if (toolName === 'ExecuteCode') displayContent = displayContent.replace(/^SUCCESS\n?/, '');

  return (
    <div className="space-y-4">
      {/* Input section */}
      {input && (
        <div>
          <button
            onClick={() => setInputExpanded(!inputExpanded)}
            className="flex items-center gap-1.5 mb-2 px-1 group"
          >
            <ChevronRight
              className="h-3 w-3 flex-shrink-0 transition-transform duration-200"
              style={{
                color: 'var(--color-text-tertiary)',
                transform: inputExpanded ? 'rotate(90deg)' : 'rotate(0deg)',
              }}
            />
            <span
              className="text-xs font-medium uppercase tracking-wider"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('toolArtifact.input')}
            </span>
          </button>
          {inputExpanded && (
            <CodeBlock language={input.language} code={input.code} />
          )}
        </div>
      )}

      {/* Output section */}
      <div>
        <div
          className="text-xs font-medium uppercase tracking-wider mb-2 px-1"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('toolArtifact.output')}
        </div>
        <Markdown variant="panel" content={`\`\`\`\n${displayContent}\n\`\`\``} className="text-sm" />
      </div>
    </div>
  );
}

// --- ArtifactOrMarkdown ---

interface ArtifactOrMarkdownProps {
  artifact: ArtifactRecord | undefined;
  content: string | unknown;
  toolName: string;
  toolCallProcess: ToolCallProcessRecord;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  scrollContainerRef?: React.RefObject<HTMLDivElement | null>;
}

function ArtifactOrMarkdown({ artifact, content, toolName, toolCallProcess, onOpenFile, scrollContainerRef }: ArtifactOrMarkdownProps): React.ReactElement {
  const { t } = useTranslation();
  // Route by artifact type first (takes priority over truncation display)
  if (artifact?.type) {
    switch (artifact.type) {
      case 'stock_prices':
        return <StockPriceChart data={artifact} />;
      case 'company_overview':
        return <CompanyOverviewCard data={artifact} scrollContainerRef={scrollContainerRef} />;
      case 'market_indices':
        return <MarketIndicesChart data={artifact} />;
      case 'sector_performance':
        return <SectorPerformanceChart data={artifact} />;
      case 'stock_screener':
        return <StockScreenerTable data={artifact} />;
      case 'sec_filing':
        return <SecFilingViewer data={artifact} />;
      case 'automations':
        return <AutomationDetailPanel data={artifact} />;
    }
  }

  // Check for truncated results
  const rawContent = typeof content === 'string' ? content : content ? String(content) : '';
  const truncated = parseTruncatedResult(rawContent);
  if (truncated.isTruncated) {
    return (
      <TruncatedResultMessage
        filePath={truncated.filePath}
        preview={truncated.preview}
        onOpenFile={onOpenFile}
      />
    );
  }

  // WebSearch: bubble card display
  if (toolName === 'WebSearch' || toolName === 'web_search') {
    const parsed = parseWebSearchResults(toolCallProcess, t);
    if (parsed) {
      return <WebSearchCards data={parsed} />;
    }
  }

  // ExecuteCode / Bash / Glob / Grep: input + output display
  if (toolName === 'ExecuteCode' || toolName === 'Bash' || toolName === 'Glob' || toolName === 'Grep') {
    return (
      <CodeToolDisplay
        toolName={toolName}
        toolCallProcess={toolCallProcess}
        rawContent={rawContent}
      />
    );
  }

  // Fallback: render content as markdown (strip line numbers from Read/SEC filing results)
  const displayContent = stripLineNumbers(rawContent || t('toolArtifact.noResultContent'));

  return <Markdown variant="panel" content={displayContent || ''} className="text-sm" />;
}

// --- parseWebSearchResults ---

interface WebSearchResultItem {
  title: string;
  url: string;
  snippet: string;
  date: string;
  domain: string;
  source?: string;
  favicon?: string;
}

interface WebSearchData {
  answer: string | null;
  query: string;
  results: WebSearchResultItem[];
}

function parseWebSearchResults(proc: ToolCallProcessRecord, t: TFunction): WebSearchData | null {
  const raw = proc.toolCallResult?.content;
  if (!raw) return null;

  const displayableResults = parseDisplayableResults(raw);
  if (!displayableResults) return null;

  const artifact = proc.toolCallResult?.artifact as Record<string, unknown> | undefined;
  const richResultByUrl = buildRichResultMap(artifact);
  const answerBox = artifact?.answer_box as Record<string, unknown> | undefined;
  const knowledgeGraph = artifact?.knowledge_graph as Record<string, unknown> | undefined;

  return {
    answer: (artifact?.answer as string) || answerBox?.answer as string || answerBox?.snippet as string || knowledgeGraph?.description as string || null,
    query: (artifact?.query as string) || (proc.toolCall?.args?.query as string) || '',
    results: displayableResults.map((item) => {
      const itemUrl = (item.url as string) || '';
      const rich = richResultByUrl.get(itemUrl);
      return {
        title: (item.title as string) || t('toolArtifact.untitled'),
        url: itemUrl,
        favicon: (rich?.favicon as string) || '',
        snippet: resolveSnippet(item, rich),
        date: (item.date as string) || (item.publish_time as string) || '',
        domain: (() => {
          try { return new URL(itemUrl).hostname.replace(/^www\./, ''); } catch { return ''; }
        })(),
        source: (item.source as string) || (item.site_name as string) || undefined,
      };
    }),
  };
}

// --- WebSearchCards ---

interface WebSearchCardsProps {
  data: WebSearchData;
}

function WebSearchCards({ data }: WebSearchCardsProps): React.ReactElement {
  const { t } = useTranslation();
  const { answer, query, results } = data;

  return (
    <div className="space-y-3">
      {/* Answer box */}
      {answer && (
        <div
          className="rounded-lg px-4 py-3"
          style={{
            backgroundColor: 'var(--color-accent-soft)',
            border: '1px solid var(--color-accent-soft)',
          }}
        >
          <p className="text-sm" style={{ color: 'var(--color-text-primary)', lineHeight: 1.6 }}>
            {answer}
          </p>
        </div>
      )}

      {/* Query label */}
      {query && (
        <div
          className="text-xs font-medium uppercase tracking-wider px-1"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('toolArtifact.nSearchResults', { count: results.length, query })}
        </div>
      )}

      {/* Result cards */}
      {results.map((item, i) => (
        <a
          key={i}
          href={item.url}
          target="_blank"
          rel="noopener noreferrer"
          className="block rounded-lg px-4 py-3 group"
          style={{
            backgroundColor: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-muted)',
            textDecoration: 'none',
            transition: 'border-color 0.15s, background-color 0.15s',
          }}
          onMouseEnter={(e: React.MouseEvent<HTMLAnchorElement>) => {
            e.currentTarget.style.borderColor = 'var(--color-accent-overlay)';
            e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
          }}
          onMouseLeave={(e: React.MouseEvent<HTMLAnchorElement>) => {
            e.currentTarget.style.borderColor = 'var(--color-border-muted)';
            e.currentTarget.style.backgroundColor = 'var(--color-bg-surface)';
          }}
        >
          {/* Domain + external link icon */}
          <div className="flex items-center justify-between mb-1.5">
            <div className="flex items-center gap-1.5 min-w-0">
              {(item.favicon || item.domain) && (
                <FaviconImg
                  src={item.favicon || googleFaviconUrl(item.domain)}
                  domain={item.domain}
                />
              )}
              <span
                className="text-xs truncate"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {item.source || item.domain}
              </span>
            </div>
            <ExternalLink
              className="h-3 w-3 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity"
              style={{ color: 'var(--color-text-tertiary)' }}
            />
          </div>

          {/* Title */}
          <div
            className="text-sm font-medium mb-1 leading-snug"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {item.title}
          </div>

          {/* Snippet */}
          {item.snippet && (
            <div
              className="text-xs leading-relaxed"
              style={{
                color: 'var(--color-text-tertiary)',
                display: '-webkit-box',
                WebkitLineClamp: 3,
                WebkitBoxOrient: 'vertical',
                overflow: 'hidden',
              }}
            >
              {item.snippet}
            </div>
          )}

          {/* Date */}
          {item.date && (
            <div
              className="text-xs mt-1.5"
              style={{ color: 'var(--color-icon-muted)' }}
            >
              {item.date}
            </div>
          )}
        </a>
      ))}
    </div>
  );
}

// --- TruncatedResultMessage ---

interface TruncatedResultMessageProps {
  filePath: string | null;
  preview: string | null;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
}

function TruncatedResultMessage({ filePath, preview, onOpenFile }: TruncatedResultMessageProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="space-y-4">
      {/* Info card */}
      <div
        className="rounded-lg px-4 py-3"
        style={{
          backgroundColor: 'var(--color-accent-soft)',
          border: '1px solid var(--color-accent-overlay)',
        }}
      >
        <div className="flex items-start gap-3">
          <FileText className="h-5 w-5 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-accent-primary)' }} />
          <div className="space-y-2 min-w-0">
            <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('toolArtifact.resultTooLarge')}
            </p>
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('toolArtifact.resultSavedToFilesystem')}
            </p>
            {filePath && onOpenFile && (
              <button
                onClick={() => onOpenFile(filePath)}
                className="flex items-center gap-2 px-3 py-1.5 rounded-md text-sm transition-colors hover:bg-foreground/10"
                style={{
                  color: 'var(--color-accent-primary)',
                  border: '1px solid var(--color-accent-overlay)',
                }}
              >
                <FileText className="h-3.5 w-3.5" />
                {t('toolArtifact.openFullResult')}
              </button>
            )}
            {filePath && (
              <p className="text-xs font-mono truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                {filePath}
              </p>
            )}
          </div>
        </div>
      </div>

      {/* Preview */}
      {preview && (
        <div className="space-y-2">
          <p className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('toolArtifact.preview')}
          </p>
          <Markdown variant="panel" content={stripLineNumbers(preview) || ''} className="text-sm" />
        </div>
      )}
    </div>
  );
}

export default DetailPanel;
