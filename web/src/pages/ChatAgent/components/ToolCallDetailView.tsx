import React, { useState, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { cn } from '@/lib/utils';
import { ArrowRight, ArrowUpRight, ChevronRight, FileText } from 'lucide-react';
import { isTaskTool, parseTruncatedResult, stripLineNumbers } from './toolDisplayConfig';
import { FaviconImg } from './charts/FaviconImg';
import { TermCard } from './charts/TermCard';
import { TermPanel } from './charts/TermPanel';
import { hostOf, readWebSearch, text } from './charts/cardReaders';
import { artifactPanel } from './charts/panelArtifacts';
import Markdown, { CodeBlock } from './Markdown';
import { TaskStatusChip, taskCardStatusKind } from './taskStatusUi';
import iconRobo from '../../../assets/img/icon-robo.png';
import iconRoboSing from '../../../assets/img/icon-robo-sing.png';
import { parseDisplayableResults, buildRichResultMap, resolveSnippet } from './webSearchUtils';
import { isDirectToolName } from '../utils/directTools';
import { DirectToolDetail } from './mcp/DirectToolDetail';
import { ToolCallCommandState } from './commands/CommandState';
import { isCommandTool } from './commands/commandWords';

// --- Public types ---

export type ArtifactRecord = Record<string, unknown> & { type?: string };
export type ToolCallArgs = Record<string, unknown>;

export interface ToolCallData {
  id?: string;
  name?: string;
  args?: ToolCallArgs;
}

export interface ToolCallResultData {
  content?: string | unknown;
  artifact?: ArtifactRecord;
  [key: string]: unknown;
}

export interface ToolCallProcessRecord {
  toolName?: string;
  toolCall?: ToolCallData | null;
  toolCallResult?: ToolCallResultData | null;
  isInProgress?: boolean;
  isComplete?: boolean;
  isFailed?: boolean;
  _subagentStatus?: string | null;
  [key: string]: unknown;
}

export interface SubagentInfo {
  subagentId: string;
  description?: string;
  type?: string;
  status?: string;
}

interface ToolCallDetailViewProps {
  toolCallProcess: ToolCallProcessRecord;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
}

/**
 * Presentational view of a tool call's URL bar + body content.
 * Caller (DetailPanel or MarketDetailDialog) supplies the surrounding
 * chrome (header, close button, panel borders).
 */
export default function ToolCallDetailView({
  toolCallProcess,
  onOpenFile,
  onOpenSubagentTask,
}: ToolCallDetailViewProps): React.ReactElement {
  const isMobile = useIsMobile();
  const scrollContainerRef = useRef<HTMLDivElement>(null);

  const toolName = toolCallProcess.toolName || '';
  const isTask = isTaskTool(toolName);
  const artifact = toolCallProcess.toolCallResult?.artifact;
  const content = toolCallProcess.toolCallResult?.content;

  const subagentType = isTask ? ((toolCallProcess.toolCall?.args?.subagent_type as string) || 'general-purpose') : '';
  const subagentDescription = isTask ? ((toolCallProcess.toolCall?.args?.description as string) || '') : '';
  const subagentId = isTask ? toolCallProcess.toolCall?.id ?? null : null;
  const fetchedUrl = toolName === 'WebFetch' || toolName === 'web_fetch'
    ? (toolCallProcess.toolCall?.args?.url as string | undefined)
    : undefined;

  return (
    <>
      {fetchedUrl && <FetchedUrlBar url={fetchedUrl} />}

      {/* On a phone the sheet scrolls; a panel that fills (termPanel.css) lays out in this height either way. */}
      <div
        ref={scrollContainerRef}
        className={cn('term-panel-scroller px-4 py-4 overflow-x-hidden', !isMobile && 'flex-1 min-h-0 overflow-y-auto')}
      >
        {isTask ? (
          <TaskToolContent
            description={subagentDescription}
            type={subagentType}
            subagentId={subagentId}
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
    </>
  );
}

// --- FetchedUrlBar ---

/** `https://` / `www.reuters.com` / `/technology/…`: the host is what a reader checks. */
function splitUrl(url: string): { scheme: string; host: string; rest: string } {
  const m = /^([a-z][a-z0-9+.-]*:\/\/)([^/?#]*)(.*)$/i.exec(url);
  return m ? { scheme: m[1], host: m[2], rest: m[3] } : { scheme: '', host: '', rest: url };
}

/** The page a fetch read, held above what it read, and the way out to it. */
function FetchedUrlBar({ url }: { url: string }): React.ReactElement {
  const { scheme, host, rest } = splitUrl(url);
  const domain = hostOf(url);
  return (
    <a href={url} target="_blank" rel="noopener noreferrer" className="term-fetch-url" title={url}>
      {domain && <FaviconImg domain={domain} size={12} />}
      <span className="term-fetch-url-text">
        <span className="term-t3">{scheme}</span>
        <span>{host}</span>
        <span className="term-t2">{rest}</span>
      </span>
      <ArrowUpRight className="term-link-mark" aria-hidden="true" />
    </a>
  );
}

// --- TaskToolContent ---

interface TaskToolContentProps {
  description: string;
  type: string;
  subagentId: string | null;
  subagentStatus: string | null;
  onOpenSubagentTask?: (info: SubagentInfo) => void;
}

/**
 * A spawned task's detail panel: what it was asked to do, how it is going, and
 * the way through to what it actually produced.
 *
 * It deliberately shows no result of its own. The `Task` tool returns the
 * moment the subagent is dispatched, and nothing ever overwrites that reply —
 * so the only "result" this panel could reach is the dispatch boilerplate,
 * which used to render here under a heading reading "Result", telling the
 * reader to call `TaskOutput(...)`. The output lives in the subagent's tab.
 */
function TaskToolContent({ description, type, subagentId, subagentStatus, onOpenSubagentTask }: TaskToolContentProps): React.ReactElement {
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

  // The same mapping the chip above renders, so the panel cannot say "Failed"
  // in one line and show the still-working glyph in the next.
  const isRunning = taskCardStatusKind(subagentStatus) === 'running';

  return (
    // sibling-space-y, not space-y: the chip is inline, so space-y's margin
    // would sit inside its line box instead of below it.
    <div className="sibling-space-y-4">
      <TaskStatusChip
        kind={taskCardStatusKind(subagentStatus)}
        rawStatus={subagentStatus ?? undefined}
        style={{ paddingLeft: 4 }}
      />

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

      {onOpenSubagentTask && subagentId && (
        <button
          onClick={handleGoToSubagent}
          className="flex items-center gap-3 w-full px-3 py-2.5 rounded-lg transition-colors hover:brightness-110"
          style={{
            backgroundColor: 'var(--color-bg-elevated)',
            border: '1px solid var(--color-border-elevated)',
          }}
        >
          <img src={isRunning ? iconRoboSing : iconRobo} alt="Subagent" className="w-5 h-5 shrink-0" />
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
          <ArrowRight className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
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
  const toolCallId = toolCallProcess.toolCall?.id;

  let displayContent = rawContent || t('toolArtifact.noResultContent');
  if (toolName === 'ExecuteCode') displayContent = displayContent.replace(/^SUCCESS\n?/, '');

  return (
    <div className="space-y-4">
      {/* The output of a call that moved to the background says only that
          it did; where its command stands now leads the card. */}
      {isCommandTool(toolName) && toolCallId && <ToolCallCommandState toolCallId={toolCallId} className="px-1" />}
      {input && (
        <div>
          <button
            onClick={() => setInputExpanded(!inputExpanded)}
            className="flex items-center gap-1.5 mb-2 px-1 group"
          >
            <ChevronRight
              className="h-3 w-3 shrink-0 transition-transform duration-fold"
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

function ArtifactOrMarkdown(props: ArtifactOrMarkdownProps): React.ReactElement {
  const { content, toolName, toolCallProcess, onOpenFile } = props;
  const { t } = useTranslation();
  const panel = panelBody(props, t);
  if (panel) return <TermPanel>{panel}</TermPanel>;

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

  if (toolName === 'ExecuteCode' || toolName === 'Bash' || toolName === 'Glob' || toolName === 'Grep') {
    return (
      <CodeToolDisplay
        toolName={toolName}
        toolCallProcess={toolCallProcess}
        rawContent={rawContent}
      />
    );
  }

  const displayContent = stripLineNumbers(rawContent || t('toolArtifact.noResultContent'));
  return <Markdown variant="panel" content={displayContent || ''} className="text-sm" />;
}

/**
 * What the panel frames, when the call has one: its artifact's view, a direct
 * tool's call and answer, or a search's results (whose text a cut-off result
 * never parses as).
 */
function panelBody(
  { artifact, content, toolName, toolCallProcess, onOpenFile, scrollContainerRef }: ArtifactOrMarkdownProps,
  t: TFunction,
): React.ReactNode {
  const args = toolCallProcess.toolCall?.args;
  const Panel = artifactPanel(artifact, args);
  if (Panel) return <Panel data={artifact!} toolArgs={args} onOpenFile={onOpenFile} scrollContainerRef={scrollContainerRef} />;

  // An order call lands here too, with no receipt above it: the receipt is the
  // card this panel was opened from, still in the thread beside it, and a
  // second copy of it reads as a second order.
  if (isDirectToolName(toolName)) {
    return <DirectToolDetail toolName={toolName} args={args} content={content} artifact={artifact} isFailed={toolCallProcess.isFailed} />;
  }

  if (toolName === 'WebSearch' || toolName === 'web_search') {
    const parsed = parseWebSearchResults(toolCallProcess, t);
    if (parsed) return <WebSearchPanel data={parsed} />;
  }
  return null;
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
  answer?: string;
  query: string;
  results: WebSearchResultItem[];
}

function parseWebSearchResults(proc: ToolCallProcessRecord, t: TFunction): WebSearchData | null {
  const raw = proc.toolCallResult?.content;
  if (!raw) return null;

  const displayableResults = parseDisplayableResults(raw);
  if (!displayableResults) return null;

  const artifact = proc.toolCallResult?.artifact;
  const richResultByUrl = buildRichResultMap(artifact);
  const { query, answer } = readWebSearch(artifact);

  return {
    answer,
    query: query ?? text(proc.toolCall?.args?.query) ?? '',
    results: displayableResults.map((item) => {
      const itemUrl = (item.url as string) || '';
      const rich = richResultByUrl.get(itemUrl);
      return {
        title: (item.title as string) || t('toolArtifact.untitled'),
        url: itemUrl,
        favicon: (rich?.favicon as string) || '',
        snippet: resolveSnippet(item, rich),
        date: (item.date as string) || (item.publish_time as string) || '',
        domain: hostOf(itemUrl),
        source: (item.source as string) || (item.site_name as string) || undefined,
      };
    }),
  };
}

// --- WebSearchPanel ---

/**
 * The search its card shows, a size up: the query, then each result with its
 * site and date in the card's type, and its title and snippet set as prose.
 * The title is the way out to the page.
 */
function WebSearchPanel({ data }: { data: WebSearchData }): React.ReactElement {
  const { t } = useTranslation();
  const { answer, query, results } = data;

  return (
    <TermCard
      title={t('toolArtifact.tool.webSearch')}
      meta={t('toolArtifact.nResults', { count: results.length })}
    >
      {query && (
        <div className="term-web-query term-t2">
          <span className="term-t3">{'> '}</span>&quot;<span>{query}</span>&quot;
        </div>
      )}
      {answer && <p className="term-prose term-web-answer">{answer}</p>}
      <ol className="term-web-results">
        {results.map((item, i) => (
          <li key={i}>
            <span className="term-web-index">{String(i + 1).padStart(2, '0')}</span>
            <div className="term-web-result">
              <div className="term-web-site">
                {(item.favicon || item.domain) && (
                  <span className="term-web-favicon">
                    <FaviconImg domain={item.domain} icon={item.favicon} size={12} />
                  </span>
                )}
                <span className="term-web-host">{item.source || item.domain}</span>
                {item.date && (
                  <>
                    <span className="term-kv-leader" aria-hidden="true" />
                    <span className="term-web-date">{item.date}</span>
                  </>
                )}
              </div>
              <a className="term-prose term-web-title" href={item.url} target="_blank" rel="noopener noreferrer">
                {item.title}
                <ArrowUpRight className="term-link-mark" aria-hidden="true" />
              </a>
              {item.snippet && <p className="term-prose term-web-snippet">{item.snippet}</p>}
            </div>
          </li>
        ))}
      </ol>
    </TermCard>
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
      <div
        className="rounded-lg px-4 py-3"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-default)',
        }}
      >
        <div className="flex items-start gap-3">
          <FileText className="h-5 w-5 shrink-0 mt-0.5" style={{ color: 'var(--color-accent-primary)' }} />
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
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-elevated)',
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
