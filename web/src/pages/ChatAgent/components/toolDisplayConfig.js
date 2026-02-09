import {
  TrendingUp, Building2, BarChart3, PieChart, Search, Globe,
  FilePlus, FileText, FilePen, FolderSearch, Play, Wrench,
  Newspaper, Brain, User, FileBarChart, Clock, ClipboardList,
} from 'lucide-react';

export const TOOL_DISPLAY_CONFIG = {
  // Market Data
  get_stock_daily_prices:   { displayName: 'Stock Prices',         icon: TrendingUp },
  get_company_overview:     { displayName: 'Company Overview',     icon: Building2 },
  get_market_indices:       { displayName: 'Market Indices',       icon: BarChart3 },
  get_sector_performance:   { displayName: 'Sector Performance',   icon: PieChart },
  // SEC
  get_sec_filing:           { displayName: 'SEC Filing',           icon: FileBarChart },
  // News (MCP tickertick)
  get_ticker_news:          { displayName: 'Ticker News',          icon: Newspaper },
  get_broad_ticker_news:    { displayName: 'Market News',          icon: Newspaper },
  get_curated_news:         { displayName: 'Curated News',         icon: Newspaper },
  get_news_from_source:     { displayName: 'News',                 icon: Newspaper },
  get_news_for_multiple_tickers: { displayName: 'Multi-Ticker News', icon: Newspaper },
  get_entity_news:          { displayName: 'Entity News',          icon: Newspaper },
  search_tickers:           { displayName: 'Ticker Search',        icon: Search },
  // Fundamentals (MCP)
  get_financial_statements: { displayName: 'Financial Statements', icon: FileBarChart },
  get_financial_ratios:     { displayName: 'Financial Ratios',     icon: FileBarChart },
  get_growth_metrics:       { displayName: 'Growth Metrics',       icon: TrendingUp },
  get_historical_valuation: { displayName: 'Valuation History',    icon: BarChart3 },
  // Price Data (MCP)
  get_stock_data:           { displayName: 'Stock Data',           icon: TrendingUp },
  get_asset_data:           { displayName: 'Asset Data',           icon: TrendingUp },
  // User Data
  get_user_data:            { displayName: 'User Data',            icon: User },
  update_user_data:         { displayName: 'Update Data',          icon: User },
  remove_user_data:         { displayName: 'Remove Data',          icon: User },
  // Core tools
  Glob:                     { displayName: 'Glob',                 icon: FolderSearch },
  Grep:                     { displayName: 'Grep',                 icon: Search },
  WebSearch:                { displayName: 'Web Search',           icon: Globe },
  WebFetch:                 { displayName: 'Web Fetch',            icon: Globe },
  Write:                    { displayName: 'Write',                icon: FilePlus },
  Read:                     { displayName: 'Read',                 icon: FileText },
  Edit:                     { displayName: 'Edit',                 icon: FilePen },
  ExecuteCode:              { displayName: 'Execute Code',         icon: Play },
  think_tool:               { displayName: 'Thinking',             icon: Brain },
  // Background subagent management
  Wait:                     { displayName: 'Waiting for Subagent', icon: Clock },
  TaskOutput:               { displayName: 'Task Output',          icon: ClipboardList },
};

export function getDisplayName(rawToolName) {
  return TOOL_DISPLAY_CONFIG[rawToolName]?.displayName || rawToolName;
}

export function getToolIcon(rawToolName) {
  return TOOL_DISPLAY_CONFIG[rawToolName]?.icon || Wrench;
}

export function getInProgressText(rawToolName, toolCall) {
  const args = toolCall?.args;
  switch (rawToolName) {
    case 'get_stock_daily_prices':
      return args?.symbol ? `fetching ${args.symbol} prices...` : 'fetching prices...';
    case 'get_company_overview':
      return args?.symbol ? `analyzing ${args.symbol}...` : 'analyzing...';
    case 'get_market_indices':
      return 'fetching market indices...';
    case 'get_sector_performance':
      return 'fetching sector data...';
    case 'get_sec_filing':
      return args?.symbol ? `fetching ${args.symbol} filing...` : 'fetching filing...';
    case 'Grep': {
      const pattern = args?.pattern;
      return pattern ? `searching for '${pattern}'...` : 'searching...';
    }
    case 'WebSearch': {
      const query = args?.query;
      return query ? `searching '${query}'...` : 'searching...';
    }
    case 'WebFetch': {
      try {
        const domain = args?.url ? new URL(args.url).hostname : null;
        return domain ? `fetching ${domain}...` : 'fetching...';
      } catch {
        return 'fetching...';
      }
    }
    case 'Write': {
      const fp = args?.file_path || args?.filePath || '';
      const name = fp.split('/').pop();
      return name ? `writing ${name}...` : 'writing...';
    }
    case 'Read': {
      const fp = args?.file_path || args?.filePath || '';
      const name = fp.split('/').pop();
      return name ? `reading ${name}...` : 'reading...';
    }
    case 'Edit': {
      const fp = args?.file_path || args?.filePath || '';
      const name = fp.split('/').pop();
      return name ? `editing ${name}...` : 'editing...';
    }
    case 'ExecuteCode':
      return 'executing...';
    case 'Wait':
      return 'waiting for subagent...';
    case 'TaskOutput':
      return 'fetching task output...';
    default:
      return 'processing...';
  }
}

/**
 * Detects if a tool result was truncated due to size and saved to filesystem.
 * Returns { isTruncated, filePath, preview } or { isTruncated: false }.
 */
export function parseTruncatedResult(content) {
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
export function stripLineNumbers(content) {
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
