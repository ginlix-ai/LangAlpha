import type { LucideIcon } from 'lucide-react';
import {
  TrendingUp, Building2, BarChart3, PieChart, Search, Globe,
  FilePlus, FileText, FilePen, FolderSearch, SquareChevronRight, Wrench,
  Newspaper, Brain, User, FileBarChart, Clock, ClipboardList, Zap, Settings, Terminal,
  Sparkles, BookText, BookMarked, BookPlus,
} from 'lucide-react';
import { classifyAgentPath, topicFromMemoryKey, type AgentPathInfo } from '../utils/agentPaths';

/** Translation function signature compatible with i18next's t() */
type TFn = (key: string, opts?: Record<string, unknown>) => string;

interface ToolDisplayEntry {
  displayName: string;
  i18nKey: string;
  icon: LucideIcon;
}

interface ToolCallArgs {
  symbol?: string;
  query?: string;
  pattern?: string;
  url?: string;
  file_path?: string;
  filePath?: string;
  description?: string;
  name?: string;
  action?: string;
  [key: string]: unknown;
}

interface ToolCall {
  args?: ToolCallArgs;
  [key: string]: unknown;
}

interface TruncatedResultParsed {
  isTruncated: true;
  filePath: string | null;
  preview: string;
}

interface NotTruncatedResult {
  isTruncated: false;
  filePath?: undefined;
  preview?: undefined;
}

type ParseTruncatedResult = TruncatedResultParsed | NotTruncatedResult;

export const TOOL_DISPLAY_CONFIG: Record<string, ToolDisplayEntry> = {
  // Market Data
  get_stock_daily_prices:   { displayName: 'Stock Prices',         i18nKey: 'stockPrices',         icon: TrendingUp },
  get_company_overview:     { displayName: 'Company Overview',     i18nKey: 'companyOverview',     icon: Building2 },
  get_market_indices:       { displayName: 'Market Indices',       i18nKey: 'marketIndices',       icon: BarChart3 },
  get_sector_performance:   { displayName: 'Sector Performance',   i18nKey: 'sectorPerformance',   icon: PieChart },
  screen_stocks:            { displayName: 'Stock Screener',       i18nKey: 'stockScreener',       icon: Search },
  // SEC
  get_sec_filing:           { displayName: 'SEC Filing',           i18nKey: 'secFiling',           icon: FileBarChart },
  // Web search / news
  get_entity_news:          { displayName: 'Entity News',          i18nKey: 'entityNews',          icon: Newspaper },
  search_tickers:           { displayName: 'Ticker Search',        i18nKey: 'tickerSearch',        icon: Search },
  // Fundamentals (MCP)
  get_financial_statements: { displayName: 'Financial Statements', i18nKey: 'financialStatements', icon: FileBarChart },
  get_financial_ratios:     { displayName: 'Financial Ratios',     i18nKey: 'financialRatios',     icon: FileBarChart },
  get_growth_metrics:       { displayName: 'Growth Metrics',       i18nKey: 'growthMetrics',       icon: TrendingUp },
  get_historical_valuation: { displayName: 'Valuation History',    i18nKey: 'valuationHistory',    icon: BarChart3 },
  // Price Data (MCP)
  get_stock_data:           { displayName: 'Stock Data',           i18nKey: 'stockData',           icon: TrendingUp },
  get_asset_data:           { displayName: 'Asset Data',           i18nKey: 'assetData',           icon: TrendingUp },
  // User Data
  get_user_data:            { displayName: 'User Data',            i18nKey: 'userData',            icon: User },
  update_user_data:         { displayName: 'Update Data',          i18nKey: 'updateData',          icon: User },
  remove_user_data:         { displayName: 'Remove Data',          i18nKey: 'removeData',          icon: User },
  // Core tools
  Bash:                     { displayName: 'Bash',                 i18nKey: 'bash',                icon: Terminal },
  Glob:                     { displayName: 'Glob',                 i18nKey: 'glob',                icon: FolderSearch },
  Grep:                     { displayName: 'Grep',                 i18nKey: 'grep',                icon: Search },
  WebSearch:                { displayName: 'Web Search',           i18nKey: 'webSearch',           icon: Globe },
  WebFetch:                 { displayName: 'Web Fetch',            i18nKey: 'webFetch',            icon: Newspaper },
  Write:                    { displayName: 'Write',                i18nKey: 'write',               icon: FilePlus },
  Read:                     { displayName: 'Read',                 i18nKey: 'read',                icon: FileText },
  Edit:                     { displayName: 'Edit',                 i18nKey: 'edit',                icon: FilePen },
  ExecuteCode:              { displayName: 'Execute Code',         i18nKey: 'executeCode',         icon: SquareChevronRight },
  think_tool:               { displayName: 'Thinking',             i18nKey: 'thinking',            icon: Brain },
  // Background subagent management
  TaskOutput:               { displayName: 'Task Output',          i18nKey: 'taskOutput',          icon: ClipboardList },
  // Automations
  check_automations:        { displayName: 'Automations',          i18nKey: 'automations',         icon: Clock },
  create_automation:        { displayName: 'Create Automation',    i18nKey: 'createAutomation',    icon: Zap },
  manage_automation:        { displayName: 'Manage Automation',    i18nKey: 'manageAutomation',    icon: Settings },
};

// Single source of truth for "what tool name indicates a file
// write/edit/read?" — these are the only tool decorators registered in the
// agent (see src/ptc_agent/agent/tools/file_ops.py: @tool("Read"),
// @tool("Write"), @tool("Edit")). Used by classifyFromArgs and categorizeTool.
const FILE_WRITE_TOOLS = new Set(['Write', 'Edit']);
const FILE_READ_TOOLS = new Set(['Read']);
const FILE_PATH_TOOLS = new Set([...FILE_WRITE_TOOLS, ...FILE_READ_TOOLS]);

/** Classifies the file_path arg for Read/Write/Edit. Returns null for all other tools. */
function classifyFromArgs(toolName: string, args: ToolCallArgs | undefined): AgentPathInfo | null {
  if (!args) return null;
  if (!FILE_PATH_TOOLS.has(toolName)) return null;
  const fp = (args.file_path || args.filePath) as string | undefined;
  if (!fp) return null;
  return classifyAgentPath(fp);
}

export function getDisplayName(rawToolName: string, t?: TFn, args?: ToolCallArgs): string {
  const info = classifyFromArgs(rawToolName, args);
  if (info) {
    if (info.kind === 'skill') {
      return t ? t('toolArtifact.tool.skill') : 'Skill';
    }
    if (info.kind === 'memory') {
      const isWorkspaceTier = info.tier === 'workspace';
      if (isWorkspaceTier) return t ? t('toolArtifact.tool.workspaceMemory') : 'Workspace Memory';
      return t ? t('toolArtifact.tool.memory') : 'Memory';
    }
    if (info.kind === 'memo') {
      return t ? t('toolArtifact.tool.memo') : 'Memo';
    }
  }
  const config = TOOL_DISPLAY_CONFIG[rawToolName];
  if (t && config?.i18nKey) return t(`toolArtifact.tool.${config.i18nKey}`);
  return config?.displayName || rawToolName;
}

export function getToolIcon(rawToolName: string, args?: ToolCallArgs): LucideIcon {
  const info = classifyFromArgs(rawToolName, args);
  if (info) {
    if (info.kind === 'skill') return Sparkles;
    if (info.kind === 'memory') return BookMarked;
    if (info.kind === 'memo') {
      // Distinguish memo writes/edits from reads with a "book + plus" glyph
      // so the timeline row hints that the agent modified the memo (even
      // though current backend rules make this read-only by design).
      return FILE_WRITE_TOOLS.has(rawToolName) ? BookPlus : BookText;
    }
  }
  return TOOL_DISPLAY_CONFIG[rawToolName]?.icon || Wrench;
}

export function getInProgressText(rawToolName: string, toolCall: ToolCall | undefined, t?: TFn): string {
  const args = toolCall?.args;
  const tr = t ? (key: string, opts?: Record<string, unknown>) => t(`toolArtifact.inProgress.${key}`, opts) : null;

  // Path-aware variants for store-backed paths. These short-circuit the
  // generic Read/Write/Edit branches below.
  const info = classifyFromArgs(rawToolName, args);
  if (info) {
    if (info.kind === 'skill' && rawToolName === 'Read') {
      return info.name
        ? (tr?.('activatingSkillName', { name: info.name }) ?? `activating skill ${info.name}...`)
        : (tr?.('activatingSkill') ?? 'activating skill...');
    }
    if (info.kind === 'memory') {
      const topic = topicFromMemoryKey(info.key);
      const ws = info.tier === 'workspace';
      if (rawToolName === 'Read') {
        if (info.isIndex) {
          return ws
            ? (tr?.('readingWorkspaceMemory') ?? 'reading workspace memory...')
            : (tr?.('readingMemory') ?? 'reading memory...');
        }
        return ws
          ? (tr?.('readingWorkspaceMemoryTopic', { topic }) ?? `reading workspace memory about ${topic}...`)
          : (tr?.('readingMemoryTopic', { topic }) ?? `reading memory about ${topic}...`);
      }
      if (rawToolName === 'Write') {
        // Index writes shouldn't render the topic — `topicFromMemoryKey('memory.md')`
        // returns 'memory', which would print "adding memory memory...".
        if (info.isIndex) {
          return ws
            ? (tr?.('addingWorkspaceMemory') ?? 'adding workspace memory...')
            : (tr?.('addingMemory') ?? 'adding memory...');
        }
        return ws
          ? (tr?.('addingWorkspaceMemoryTopic', { topic }) ?? `adding workspace memory ${topic}...`)
          : (tr?.('addingMemoryTopic', { topic }) ?? `adding memory ${topic}...`);
      }
      if (rawToolName === 'Edit') {
        if (info.isIndex) {
          return ws
            ? (tr?.('updatingWorkspaceMemory') ?? 'updating workspace memory...')
            : (tr?.('updatingMemory') ?? 'updating memory...');
        }
        return ws
          ? (tr?.('updatingWorkspaceMemoryTopic', { topic }) ?? `updating workspace memory ${topic}...`)
          : (tr?.('updatingMemoryTopic', { topic }) ?? `updating memory ${topic}...`);
      }
    }
    if (info.kind === 'memo') {
      if (rawToolName === 'Read') {
        if (info.isIndex) {
          return tr?.('readingMemoIndex') ?? 'reading memo index...';
        }
        return tr?.('readingMemoSlug', { slug: info.key }) ?? `reading memo ${info.key}...`;
      }
      if (rawToolName === 'Write') {
        return tr?.('writingMemoSlug', { slug: info.key }) ?? `writing memo ${info.key}...`;
      }
      if (rawToolName === 'Edit') {
        return tr?.('updatingMemoSlug', { slug: info.key }) ?? `updating memo ${info.key}...`;
      }
    }
  }

  switch (rawToolName) {
    case 'get_stock_daily_prices':
      return args?.symbol
        ? (tr?.('fetchingSymbolPrices', { symbol: args.symbol }) ?? `fetching ${args.symbol} prices...`)
        : (tr?.('fetchingPrices') ?? 'fetching prices...');
    case 'get_company_overview':
      return args?.symbol
        ? (tr?.('analyzingSymbol', { symbol: args.symbol }) ?? `analyzing ${args.symbol}...`)
        : (tr?.('analyzing') ?? 'analyzing...');
    case 'get_market_indices':
      return tr?.('fetchingMarketIndices') ?? 'fetching market indices...';
    case 'get_sector_performance':
      return tr?.('fetchingSectorData') ?? 'fetching sector data...';
    case 'screen_stocks':
      return tr?.('screeningStocks') ?? 'screening stocks...';
    case 'get_sec_filing':
      return args?.symbol
        ? (tr?.('fetchingSymbolFiling', { symbol: args.symbol }) ?? `fetching ${args.symbol} filing...`)
        : (tr?.('fetchingFiling') ?? 'fetching filing...');
    case 'Glob': {
      const pattern = args?.pattern;
      return pattern
        ? (tr?.('searchingPattern', { pattern }) ?? `searching '${pattern}'...`)
        : (tr?.('searching') ?? 'searching...');
    }
    case 'Grep': {
      const pattern = args?.pattern;
      return pattern
        ? (tr?.('searchingForPattern', { pattern }) ?? `searching for '${pattern}'...`)
        : (tr?.('searching') ?? 'searching...');
    }
    case 'WebSearch': {
      const query = args?.query;
      return query
        ? (tr?.('searchingQuery', { query }) ?? `searching '${query}'...`)
        : (tr?.('searching') ?? 'searching...');
    }
    case 'WebFetch': {
      try {
        const domain = args?.url ? new URL(args.url).hostname : null;
        return domain
          ? (tr?.('fetchingDomain', { domain }) ?? `fetching ${domain}...`)
          : (tr?.('fetching') ?? 'fetching...');
      } catch {
        return tr?.('fetching') ?? 'fetching...';
      }
    }
    case 'Write': {
      const fp = args?.file_path || args?.filePath || '';
      const name = fp.split('/').pop();
      return name
        ? (tr?.('writingFile', { name }) ?? `writing ${name}...`)
        : (tr?.('writing') ?? 'writing...');
    }
    case 'Read': {
      const fp = args?.file_path || args?.filePath || '';
      const name = fp.split('/').pop();
      return name
        ? (tr?.('readingFile', { name }) ?? `reading ${name}...`)
        : (tr?.('reading') ?? 'reading...');
    }
    case 'Edit': {
      const fp = args?.file_path || args?.filePath || '';
      const name = fp.split('/').pop();
      return name
        ? (tr?.('editingFile', { name }) ?? `editing ${name}...`)
        : (tr?.('editing') ?? 'editing...');
    }
    case 'Bash':
      return args?.description ? `${args.description}...` : (tr?.('runningCommand') ?? 'running command...');
    case 'ExecuteCode':
      return args?.description ? `${args.description}...` : (tr?.('executing') ?? 'executing...');
    case 'TaskOutput':
      return args?.timeout
        ? (tr?.('waitingForSubagent') ?? 'waiting for subagent...')
        : (tr?.('fetchingTaskOutput') ?? 'fetching task output...');
    case 'check_automations':
      return tr?.('checkingAutomations') ?? 'checking automations...';
    case 'create_automation':
      return args?.name
        ? (tr?.('creatingName', { name: args.name }) ?? `creating ${args.name}...`)
        : (tr?.('creatingAutomation') ?? 'creating automation...');
    case 'manage_automation':
      return args?.action
        ? (tr?.('actionAutomation', { action: args.action }) ?? `${args.action} automation...`)
        : (tr?.('managingAutomation') ?? 'managing automation...');
    default:
      return tr?.('processing') ?? 'processing...';
  }
}

/**
 * `_t` (optional, currently unused) is accepted for parity with sibling
 * helpers — the summary is a bare noun (topic / slug / skill name) that
 * doesn't need localization today, but the param keeps call sites symmetric
 * and reserves space for future translated variants.
 */
export function getCompletedSummary(toolName: string, toolCall: ToolCall | undefined, _t?: TFn): string | null {
  const args = toolCall?.args;
  if (!args) return null;
  // Path-aware summaries: emit just the meaningful object (skill name, topic,
  // memo slug). The verb is carried by getDisplayName / row label.
  const info = classifyFromArgs(toolName, args);
  if (info) {
    if (info.kind === 'skill') return info.name || null;
    if (info.kind === 'memory') {
      // Index files: no pill — the row title already says "Read memory" /
      // "Added memory" etc., and the row itself becomes the click target.
      if (info.isIndex) return null;
      return topicFromMemoryKey(info.key) || null;
    }
    if (info.kind === 'memo') {
      if (info.isIndex) return null;
      return info.key || null;
    }
  }
  if (args.description) return args.description;
  if (args.symbol) return args.symbol;
  if (args.query) return args.query;
  if (args.pattern) return args.pattern;
  if (args.url) {
    try {
      return new URL(args.url).hostname;
    } catch {
      return args.url;
    }
  }
  if (args.file_path || args.filePath) {
    const fp = args.file_path || args.filePath;
    return fp!.split('/').pop() || null;
  }
  return null;
}

/**
 * Full live-row label for the Active state. For path-aware tools this is a
 * single coherent phrase ("Reading memory about <topic>") with no leading
 * noun; for generic tools we fall back to the historic
 * `${displayName} ${progressText}` concat.
 */
export function getActiveLabel(toolName: string, toolCall: ToolCall | undefined, t?: TFn): string {
  const args = toolCall?.args;
  const info = classifyFromArgs(toolName, args);
  if (info && info.kind !== 'file') {
    // getInProgressText already produces the full phrase for path-aware paths.
    return getInProgressText(toolName, toolCall, t);
  }
  const dn = getDisplayName(toolName, t, args);
  const pt = getInProgressText(toolName, toolCall, t);
  return pt ? `${dn} ${pt}` : dn;
}

/**
 * Title for completed timeline rows. Returns the past-tense verb phrase for
 * path-aware tools ("Read memory", "Added memory", "Activated skill") and the
 * generic displayName otherwise.
 */
export function getCompletedRowTitle(toolName: string, toolCall: ToolCall | undefined, t?: TFn): string {
  const args = toolCall?.args;
  const info = classifyFromArgs(toolName, args);
  if (info) {
    if (info.kind === 'skill' && toolName === 'Read') {
      return t ? t('toolArtifact.completed.activatedSkill') : 'Activated skill';
    }
    if (info.kind === 'memory') {
      const ws = info.tier === 'workspace';
      if (toolName === 'Write') {
        return t ? t('toolArtifact.completed.addedMemory') : 'Added memory';
      }
      if (toolName === 'Edit') {
        return t ? t('toolArtifact.completed.updatedMemory') : 'Updated memory';
      }
      if (toolName === 'Read') {
        if (ws) return t ? t('toolArtifact.completed.readWorkspaceMemory') : 'Read workspace memory';
        return t ? t('toolArtifact.completed.readMemory') : 'Read memory';
      }
    }
    if (info.kind === 'memo') {
      if (toolName === 'Read') {
        if (info.isIndex) return t ? t('toolArtifact.completed.readMemoIndex') : 'Read memo index';
        return t ? t('toolArtifact.completed.readMemo') : 'Read memo';
      }
      // Distinguish Write vs Edit verbs so a regression that lets the agent
      // mutate a memo surfaces with the correct framing instead of "Read memo".
      if (toolName === 'Write') {
        return t ? t('toolArtifact.completed.wroteMemo') : 'Wrote memo';
      }
      if (toolName === 'Edit') {
        return t ? t('toolArtifact.completed.updatedMemo') : 'Updated memo';
      }
    }
  }
  return getDisplayName(toolName, t, args);
}

/**
 * Discriminator used by the content-aware accordion header. Maps a completed
 * tool call to a coarse category for counting. Memory is split into read vs
 * write so the header can collapse a mixed sequence into a single fragment
 * whose verb reflects whether anything was modified.
 */
export type ToolCategory =
  | 'skill'
  | 'memoryRead'
  | 'memoryWrite'
  | 'memo'        // Read on a memo path
  | 'memoWrite'   // Write/Edit/Save on a memo path (rare — backend currently
                  // restricts agents to read-only memo access — but we
                  // surface modifications distinctly so any future regression
                  // is visible to the user instead of being hidden behind the
                  // generic "read N memos" framing.)
  | 'code'
  | 'web'
  | 'search'      // Glob, Grep
  | 'fileRead'    // Read on a non-store path
  | 'fileEdit'    // Write/Edit/Save on a non-store path
  | 'generic';    // anything else (TaskOutput, MCP tools without artifacts, ...)

export function categorizeTool(toolName: string, toolCall: ToolCall | undefined): ToolCategory {
  const args = toolCall?.args;
  const info = classifyFromArgs(toolName, args);
  if (info) {
    if (info.kind === 'skill') return 'skill';
    if (info.kind === 'memory') {
      return FILE_WRITE_TOOLS.has(toolName) ? 'memoryWrite' : 'memoryRead';
    }
    if (info.kind === 'memo') {
      return FILE_WRITE_TOOLS.has(toolName) ? 'memoWrite' : 'memo';
    }
    // info.kind === 'file' (a known file tool but path is generic) falls
    // through to the per-tool bucketing below.
  }
  if (toolName === 'ExecuteCode' || toolName === 'Bash') return 'code';
  if (toolName === 'WebSearch' || toolName === 'WebFetch') return 'web';
  if (toolName === 'Glob' || toolName === 'Grep') return 'search';
  if (FILE_READ_TOOLS.has(toolName)) return 'fileRead';
  if (FILE_WRITE_TOOLS.has(toolName)) return 'fileEdit';
  return 'generic';
}

function formatByteSize(bytes: number, t?: TFn): string | null {
  if (bytes < 100) return null;
  if (bytes < 1000) {
    return t ? t('toolArtifact.nChars', { count: bytes }) : `~${bytes} chars`;
  }
  const size = (bytes / 1000).toFixed(1);
  return t ? t('toolArtifact.nKB', { size }) : `~${size} KB`;
}

export function getPreparingText(toolName: string, argsLength: number, t?: TFn): string {
  const size = formatByteSize(argsLength, t);
  const sizeLabel = size ? ` (${size})` : '';
  const tr = t ? (key: string, opts?: Record<string, unknown>) => t(`toolArtifact.preparing.${key}`, opts) : null;

  switch (toolName) {
    case 'Write':
      return tr?.('writingCode', { size: sizeLabel }) ?? `writing code${sizeLabel}...`;
    case 'Edit':
      return tr?.('composingEdits', { size: sizeLabel }) ?? `composing edits${sizeLabel}...`;
    case 'Bash':
      return tr?.('preparingCommand', { size: sizeLabel }) ?? `preparing command${sizeLabel}...`;
    case 'ExecuteCode':
      return tr?.('composingCode', { size: sizeLabel }) ?? `composing code${sizeLabel}...`;
    case 'WebSearch':
      return tr?.('craftingSearch') ?? 'crafting search...';
    case 'WebFetch':
      return tr?.('preparingRequest') ?? 'preparing request...';
    case 'Grep':
      return tr?.('buildingSearchPattern') ?? 'building search pattern...';
    default:
      return tr?.('generating', { size: sizeLabel }) ?? `generating${sizeLabel}...`;
  }
}

/**
 * Detects if a tool result was truncated due to size and saved to filesystem.
 * Returns { isTruncated, filePath, preview } or { isTruncated: false }.
 */
export function parseTruncatedResult(content: string | null | undefined): ParseTruncatedResult {
  if (!content || typeof content !== 'string') return { isTruncated: false };

  if (!content.startsWith('Tool result too large')) return { isTruncated: false };

  // Extract the filesystem path
  const pathMatch = content.match(/saved in the filesystem at this path:\s*(\/large_tool_results\/\S+)/);
  const filePath = pathMatch?.[1] || null;

  // Extract the preview (everything after the "head and tail" intro line)
  const previewMatch = content.match(/indicate omitted lines in the middle of the content\):\n\n([\s\S]*)$/);
  const rawPreview = previewMatch?.[1]?.trim() || '';

  return { isTruncated: true, filePath, preview: rawPreview };
}

/**
 * Strips `cat -n` style line number prefixes from content.
 * Matches lines like "     1\t..." or "  123\t..." and removes the prefix.
 * Only strips if the majority of lines match the pattern (to avoid false positives).
 */
export function stripLineNumbers(content: string | null | undefined): string | null | undefined {
  if (!content || typeof content !== 'string') return content;

  const lines = content.split('\n');
  // Check if content has line number prefixes: spaces + digits + tab
  const lineNumPattern = /^\s*\d+\t/;
  const matchCount = lines.filter((l) => lineNumPattern.test(l)).length;

  // Only strip if >50% of non-empty lines match the pattern
  const nonEmptyLines = lines.filter((l) => l.trim().length > 0).length;
  if (nonEmptyLines === 0 || matchCount / nonEmptyLines < 0.5) return content;

  return lines
    .map((line) => line.replace(lineNumPattern, ''))
    .join('\n');
}
