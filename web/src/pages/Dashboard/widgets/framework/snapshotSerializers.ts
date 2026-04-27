/**
 * Markdown serializers for widget context snapshots.
 *
 * Each helper produces a string that becomes the body of a
 * `<widget-context>...</widget-context>` block. Keep them small and pure —
 * the per-widget render components compose them inline.
 */


/** Wrap a body in a <widget-context> envelope with type and arbitrary key/value attributes. */
export function wrapWidgetContext(
  type: string,
  attrs: Record<string, string | number | undefined | null>,
  body: string,
): string {
  const attrStr = Object.entries(attrs)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${k}='${String(v).replace(/'/g, '&#39;')}'`)
    .join(' ');
  const head = attrStr ? `<widget-context type='${type}' ${attrStr}>` : `<widget-context type='${type}'>`;
  return `${head}\n${body}\n</widget-context>`;
}

// ---------------------------------------------------------------------------
// OHLCV (chart widgets)
// ---------------------------------------------------------------------------

export interface OhlcvBar {
  time: string | number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

/** Render OHLCV bars as a markdown table. Truncates long series with head/tail. */
export function serializeOhlcvToMarkdown(bars: OhlcvBar[], opts: { headRows?: number; tailRows?: number } = {}): string {
  if (!bars.length) return '_no bars_';
  const headRows = opts.headRows ?? 8;
  const tailRows = opts.tailRows ?? 8;
  const includeVolume = bars.some((b) => b.volume !== undefined);
  const headers = ['time', 'open', 'high', 'low', 'close'];
  if (includeVolume) headers.push('volume');
  const lines: string[] = [];
  lines.push(`| ${headers.join(' | ')} |`);
  lines.push(`| ${headers.map(() => '---').join(' | ')} |`);
  const fmt = (b: OhlcvBar) => {
    const cells = [
      String(b.time),
      b.open.toFixed(2),
      b.high.toFixed(2),
      b.low.toFixed(2),
      b.close.toFixed(2),
    ];
    if (includeVolume) cells.push(b.volume !== undefined ? String(b.volume) : '');
    return `| ${cells.join(' | ')} |`;
  };
  if (bars.length <= headRows + tailRows + 2) {
    bars.forEach((b) => lines.push(fmt(b)));
  } else {
    bars.slice(0, headRows).forEach((b) => lines.push(fmt(b)));
    lines.push(`| _... ${bars.length - headRows - tailRows} bars omitted ..._ |`);
    bars.slice(-tailRows).forEach((b) => lines.push(fmt(b)));
  }
  return lines.join('\n');
}

export function summarizeOhlcv(bars: OhlcvBar[]): string {
  if (!bars.length) return '';
  const last = bars[bars.length - 1];
  const first = bars[0];
  const pctChange = ((last.close - first.close) / first.close) * 100;
  const lows = bars.map((b) => b.low);
  const highs = bars.map((b) => b.high);
  const minLow = Math.min(...lows);
  const maxHigh = Math.max(...highs);
  const sign = pctChange >= 0 ? '+' : '';
  return `last close ${last.close.toFixed(2)} (${sign}${pctChange.toFixed(2)}%), range ${minLow.toFixed(2)}–${maxHigh.toFixed(2)} over ${bars.length} bars`;
}

// ---------------------------------------------------------------------------
// News
// ---------------------------------------------------------------------------

export interface NewsItem {
  title: string;
  source?: string;
  publishedAt?: string;
  url?: string;
  tickers?: string[];
  summary?: string;
}

export function serializeNewsItemToMarkdown(item: NewsItem): string {
  const lines: string[] = [`**${item.title}**`];
  const meta: string[] = [];
  if (item.source) meta.push(item.source);
  if (item.publishedAt) meta.push(item.publishedAt);
  if (item.tickers && item.tickers.length) meta.push(item.tickers.join(', '));
  if (meta.length) lines.push(meta.join(' · '));
  if (item.url) lines.push(`URL: ${item.url}`);
  if (item.summary) lines.push('', item.summary);
  return lines.join('\n');
}

export interface NewsArticleDetail {
  title: string;
  description?: string;
  author?: string;
  publishedAt?: string;
  url?: string;
  source?: string;
  tickers?: string[];
  keywords?: string[];
  sentiments?: Array<{ ticker: string; sentiment?: string; reasoning?: string }>;
}

/**
 * Render a fully-fetched news article to markdown. Includes summary,
 * keywords, and per-ticker sentiment so the agent gets the same context
 * the user sees in the news detail modal.
 */
export function serializeNewsArticleToMarkdown(article: NewsArticleDetail): string {
  const lines: string[] = [`**${article.title}**`];
  const meta: string[] = [];
  if (article.source) meta.push(article.source);
  if (article.author) meta.push(`by ${article.author}`);
  if (article.publishedAt) meta.push(article.publishedAt);
  if (article.tickers?.length) meta.push(article.tickers.join(', '));
  if (meta.length) lines.push(meta.join(' · '));
  if (article.url) lines.push(`URL: ${article.url}`);
  if (article.description) lines.push('', '**Summary**', article.description);
  if (article.keywords?.length) {
    lines.push('', `**Topics:** ${article.keywords.map((k) => `#${k}`).join(' ')}`);
  }
  if (article.sentiments?.length) {
    lines.push('', '**Ticker Sentiment**');
    article.sentiments.forEach((s) => {
      const tag = s.sentiment ? ` (${s.sentiment})` : '';
      const why = s.reasoning ? ` — ${s.reasoning}` : '';
      lines.push(`- ${s.ticker}${tag}${why}`);
    });
  }
  return lines.join('\n');
}

export function serializeNewsItemsToMarkdown(items: NewsItem[]): string {
  if (!items.length) return '_no headlines_';
  const lines: string[] = [`| # | title | source | when | tickers |`, `|---|---|---|---|---|`];
  items.forEach((it, i) => {
    const tickers = it.tickers && it.tickers.length ? it.tickers.join(', ') : '';
    lines.push(
      `| ${i + 1} | ${it.title.replace(/\|/g, '\\|')} | ${it.source ?? ''} | ${it.publishedAt ?? ''} | ${tickers} |`,
    );
  });
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// Watchlist / portfolio rows
// ---------------------------------------------------------------------------

export interface QuoteRow {
  symbol: string;
  price?: number;
  change?: number;
  changePercent?: number;
  preMarket?: number;
  postMarket?: number;
  volume?: number;
  shares?: number;
  marketValue?: number;
}

export function serializeQuoteRowToMarkdown(row: QuoteRow): string {
  const lines: string[] = [`**${row.symbol}**`];
  const bits: string[] = [];
  if (row.price !== undefined) bits.push(`$${row.price.toFixed(2)}`);
  if (row.change !== undefined && row.changePercent !== undefined) {
    const sign = row.change >= 0 ? '+' : '';
    bits.push(`${sign}${row.change.toFixed(2)} (${sign}${row.changePercent.toFixed(2)}%)`);
  }
  if (bits.length) lines.push(bits.join(' '));
  if (row.preMarket !== undefined) lines.push(`pre-market $${row.preMarket.toFixed(2)}`);
  if (row.postMarket !== undefined) lines.push(`post-market $${row.postMarket.toFixed(2)}`);
  if (row.volume !== undefined) lines.push(`vol ${row.volume.toLocaleString()}`);
  if (row.shares !== undefined) lines.push(`shares ${row.shares}`);
  if (row.marketValue !== undefined) lines.push(`mkt val $${row.marketValue.toLocaleString()}`);
  return lines.join('\n');
}

export function serializeQuoteRowsToMarkdown(rows: QuoteRow[]): string {
  if (!rows.length) return '_no symbols_';
  const includeVol = rows.some((r) => r.volume !== undefined);
  const headers = ['symbol', 'price', 'change', 'change%'];
  if (includeVol) headers.push('volume');
  const lines: string[] = [`| ${headers.join(' | ')} |`, `| ${headers.map(() => '---').join(' | ')} |`];
  rows.forEach((r) => {
    const cells = [
      r.symbol,
      r.price !== undefined ? r.price.toFixed(2) : '',
      r.change !== undefined ? r.change.toFixed(2) : '',
      r.changePercent !== undefined ? `${r.changePercent.toFixed(2)}%` : '',
    ];
    if (includeVol) cells.push(r.volume !== undefined ? String(r.volume) : '');
    lines.push(`| ${cells.join(' | ')} |`);
  });
  return lines.join('\n');
}

// ---------------------------------------------------------------------------
// AI Insight briefs
// ---------------------------------------------------------------------------

export interface InsightTopic {
  text: string;
  trend?: 'up' | 'down' | 'neutral';
}

export interface InsightContentItem {
  title: string;
  body: string;
  url?: string;
}

export interface InsightSource {
  url: string;
  title?: string;
}

export interface InsightDetail {
  market_insight_id: string;
  type: string;
  headline: string;
  summary?: string;
  completed_at?: string;
  topics?: InsightTopic[];
  content?: InsightContentItem[];
  sources?: InsightSource[];
  model?: string;
}

/** Render a single AI brief as a markdown sub-section. Caller wraps in <widget-context>. */
export function serializeInsightToMarkdown(item: InsightDetail, opts: { heading?: string } = {}): string {
  const lines: string[] = [];
  lines.push(opts.heading ?? `## ${item.headline}`);
  const meta: string[] = [`type: ${item.type}`];
  if (item.completed_at) meta.push(`completed: ${item.completed_at}`);
  if (item.model) meta.push(`model: ${item.model}`);
  lines.push(`_${meta.join(' · ')}_`);
  if (item.summary) lines.push('', item.summary);
  if (item.topics?.length) {
    const topicStr = item.topics.map((tp) => (tp.trend ? `${tp.text} (${tp.trend})` : tp.text)).join(', ');
    lines.push('', `**Topics:** ${topicStr}`);
  }
  if (item.content?.length) {
    lines.push('', '**Items**');
    item.content.forEach((c, i) => {
      lines.push(`${i + 1}. **${c.title}**${c.url ? ` — ${c.url}` : ''}`);
      if (c.body) lines.push(`   ${c.body.replace(/\n/g, '\n   ')}`);
    });
  }
  if (item.sources?.length) {
    lines.push('', '**Sources**');
    item.sources.forEach((s) => {
      lines.push(`- ${s.title ? `${s.title} — ` : ''}${s.url}`);
    });
  }
  return lines.join('\n');
}

/** Render a whole-day brief stack (latest + earlier + personalized) as one markdown body. */
export function serializeInsightDayToMarkdown(items: InsightDetail[]): string {
  if (!items.length) return '_no briefs_';
  const sections = items.map((it, i) => {
    const heading = i === 0 ? `## ${it.headline} (latest)` : `## ${it.headline}`;
    return serializeInsightToMarkdown(it, { heading });
  });
  return [`_${items.length} brief${items.length === 1 ? '' : 's'} on this date_`, '', sections.join('\n\n---\n\n')].join('\n');
}

// ---------------------------------------------------------------------------
// Generic table
// ---------------------------------------------------------------------------

export function serializeRowsToMarkdown<T extends Record<string, unknown>>(
  rows: T[],
  columns: Array<{ key: keyof T; label: string; format?: (v: unknown) => string }>,
): string {
  if (!rows.length) return '_no rows_';
  const headers = columns.map((c) => c.label);
  const lines: string[] = [`| ${headers.join(' | ')} |`, `| ${headers.map(() => '---').join(' | ')} |`];
  rows.forEach((r) => {
    const cells = columns.map((c) => {
      const v = r[c.key];
      if (v === null || v === undefined) return '';
      if (c.format) return c.format(v);
      return String(v).replace(/\|/g, '\\|');
    });
    lines.push(`| ${cells.join(' | ')} |`);
  });
  return lines.join('\n');
}

