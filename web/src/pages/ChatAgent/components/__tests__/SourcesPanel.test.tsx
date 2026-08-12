/**
 * SourcesPanel: groups a turn's provenance by source_type with per-group
 * counts, dedups by (source_type, identifier), and tags subagent records.
 * Every source is a card; clicking one opens a detail dialog exposing the
 * content fingerprint. URL/file sources expose an "Open link"/"Open file"
 * action inside that dialog.
 */
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import SourcesPanel from '../SourcesPanel';
import type { ProvenanceRecord } from '@/types/chat';

// Real i18n is initialized by the test setup, so t() returns English strings.

// Radix Popover's DismissableLayer touches pointer-capture APIs jsdom omits.
beforeAll(() => {
  if (!Element.prototype.hasPointerCapture) {
    Element.prototype.hasPointerCapture = () => false;
    Element.prototype.setPointerCapture = () => {};
    Element.prototype.releasePointerCapture = () => {};
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = () => {};
  }
});

function rec(partial: Partial<ProvenanceRecord> & Pick<ProvenanceRecord, 'record_id' | 'source_type' | 'identifier'>): ProvenanceRecord {
  return {
    timestamp: '2026-01-01T00:00:00Z',
    title: undefined,
    ...partial,
  } as ProvenanceRecord;
}

function asMap(records: ProvenanceRecord[]): Record<string, ProvenanceRecord> {
  const out: Record<string, ProvenanceRecord> = {};
  for (const r of records) out[r.record_id] = r;
  return out;
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe('SourcesPanel', () => {
  it('renders an empty state when there are no records', () => {
    render(<SourcesPanel provenanceRecords={{}} />);
    expect(screen.getByText('No sources for this turn')).toBeInTheDocument();
  });

  it('groups records by source_type with per-group counts', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://example.com/a', title: 'Result A' }),
      rec({ record_id: 'r2', source_type: 'web_search', identifier: 'https://example.com/b', title: 'Result B' }),
      rec({ record_id: 'r3', source_type: 'mcp_tool', identifier: 'data-server:get_prices', title: 'Prices' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Group headers (English from real i18n).
    const webSearch = screen.getByText('Web search');
    const mcp = screen.getByText('Financial data tools');
    expect(webSearch).toBeInTheDocument();
    expect(mcp).toBeInTheDocument();

    // Per-group count badge: web_search → 2, mcp_tool → 1.
    expect(screen.getByTestId('group-count-web_search')).toHaveTextContent('2');
    expect(screen.getByTestId('group-count-mcp_tool')).toHaveTextContent('1');
  });

  it('stacks a ticker into a deck that fans on click, each access opening its own detail', () => {
    // Three market tools hit the same ticker (different content shas).
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'market_data', identifier: 'AAPL', detail: 'company_overview', result_sha256: 'a'.repeat(20), result_size: 512, provider: 'market_data_proxy' }),
      rec({ record_id: 'r2', source_type: 'market_data', identifier: 'AAPL', detail: 'daily_prices', result_sha256: 'b'.repeat(20), result_size: 1024, provider: 'market_data_proxy' }),
      rec({ record_id: 'r3', source_type: 'market_data', identifier: 'AAPL', detail: 'options_chain', result_sha256: 'c'.repeat(20), provider: 'market_data_proxy' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Group count is by ticker (1 row), not by access.
    expect(screen.getByTestId('group-count-market_data')).toHaveTextContent('1');

    // Collapsed deck: one stack, not fanned, front card summarizes the count.
    const stack = screen.getByTestId('source-stack');
    expect(stack).toHaveAttribute('data-fanned', 'false');
    expect(screen.getByText('3 sources')).toBeInTheDocument();
    // Peeked (non-front) access cards are hidden from the a11y tree until fanned.
    expect(screen.queryByRole('button', { name: /Daily prices — View details/ })).not.toBeInTheDocument();

    // Clicking the collapsed deck fans it into a card per access.
    fireEvent.click(screen.getByRole('button', { name: /AAPL — Expand/ }));
    expect(stack).toHaveAttribute('data-fanned', 'true');
    expect(screen.getByRole('button', { name: /Daily prices — View details/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Options chain — View details/ })).toBeInTheDocument();

    // Each access card opens its own detail dialog (its own fingerprint).
    fireEvent.click(screen.getByRole('button', { name: /Daily prices — View details/ }));
    expect(screen.getByText('Checksum')).toBeInTheDocument();
    expect(screen.getByText('Provider')).toBeInTheDocument();
  });

  it('renders a single-access ticker as a flat card (no deck) that opens its detail', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'market_data', identifier: 'AAPL', detail: 'company_overview', result_sha256: 'a'.repeat(20), provider: 'market_data_proxy' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    expect(screen.queryByTestId('source-stack')).not.toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('Company overview')).toBeInTheDocument();
    expect(screen.queryByText(/sources$/)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /AAPL — View details/ }));
    expect(screen.getByText('Checksum')).toBeInTheDocument();
  });

  it('keeps two same-ticker, same-kind market_data snapshots with different content as distinct cards', () => {
    // The native-tool analogue of the mcp_tool fix. Same ticker AND same
    // data-kind, but the result changed between calls — live market data is
    // time-varying, so an identical query seconds apart is a distinct snapshot.
    // Each native call carries its own tool_call_id so both survive storage; the
    // entity deck must then split them by result_sha256 (NOT by the shared kind
    // label) on expand, or the earlier snapshot is silently dropped.
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'market_data', identifier: 'AAPL', detail: 'daily_prices', result_sha256: 'a'.repeat(20), result_size: 512, provider: 'market_data_proxy' }),
      rec({ record_id: 'r2', source_type: 'market_data', identifier: 'AAPL', detail: 'daily_prices', result_sha256: 'b'.repeat(20), result_size: 512, provider: 'market_data_proxy' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Grouped into one ticker row (visual stacking is fine)...
    expect(screen.getByTestId('group-count-market_data')).toHaveTextContent('1');
    // ...but the deck holds both snapshots, not one collapsed card.
    expect(screen.getByText('2 sources')).toBeInTheDocument();

    // On expand, each snapshot is its own card — two cards of the same kind.
    fireEvent.click(screen.getByRole('button', { name: /AAPL — Expand/ }));
    expect(screen.getAllByRole('button', { name: /Daily prices — View details/ })).toHaveLength(2);
  });

  it('collapses two same-ticker, same-kind market_data accesses with identical content to one card', () => {
    // The inverse: a true re-fetch returning byte-identical bytes (same sha) is
    // one access, not a phantom duplicate. The entity deck dedups by content
    // hash, so it renders a single flat card rather than a 2-card deck.
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'market_data', identifier: 'AAPL', detail: 'daily_prices', result_sha256: 'same'.repeat(5), provider: 'market_data_proxy' }),
      rec({ record_id: 'r2', source_type: 'market_data', identifier: 'AAPL', detail: 'daily_prices', result_sha256: 'same'.repeat(5), provider: 'market_data_proxy' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    expect(screen.getByTestId('group-count-market_data')).toHaveTextContent('1');
    expect(screen.queryByTestId('source-stack')).not.toBeInTheDocument();
    expect(screen.queryByText(/sources$/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /AAPL — View details/ })).toBeInTheDocument();
  });

  it('dedups display by (source_type, identifier)', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_fetch', identifier: 'https://example.com/page', title: 'Page' }),
      rec({ record_id: 'r2', source_type: 'web_fetch', identifier: 'https://example.com/page', title: 'Page (again)' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // First record wins; the duplicate identifier renders once.
    expect(screen.getAllByText('Page')).toHaveLength(1);
    expect(screen.queryByText('Page (again)')).not.toBeInTheDocument();
  });

  it('opens a URL in a new tab via the dialog "Open link" action', () => {
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://example.com/a', title: 'Result A' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // The card itself opens the detail dialog; the link opens from inside it.
    fireEvent.click(screen.getByText('Result A'));
    fireEvent.click(screen.getByRole('button', { name: 'Open link' }));
    expect(openSpy).toHaveBeenCalledWith('https://example.com/a', '_blank', 'noopener,noreferrer');
  });

  it('routes file/memo/memory sources through onOpenFile via the dialog and never window.open', () => {
    const openSpy = vi.spyOn(window, 'open').mockImplementation(() => null);
    const onOpenFile = vi.fn();
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'file_read', identifier: 'work/notes.md', title: 'notes.md' }),
      rec({ record_id: 'r2', source_type: 'memo_read', identifier: '.agents/user/memo/brief.md', title: 'brief.md' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} onOpenFile={onOpenFile} />);

    fireEvent.click(screen.getByText('notes.md'));
    fireEvent.click(screen.getByRole('button', { name: 'Open file' }));
    expect(onOpenFile).toHaveBeenCalledWith('work/notes.md');

    // Opening a file closes the dialog; the next card is then reachable.
    fireEvent.click(screen.getByText('brief.md'));
    fireEvent.click(screen.getByRole('button', { name: 'Open file' }));
    expect(onOpenFile).toHaveBeenCalledWith('.agents/user/memo/brief.md');

    expect(openSpy).not.toHaveBeenCalled();
  });

  it('shows a subagent chip when agent starts with "task:"', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://example.com/a', title: 'Result A', agent: 'task:abc123' }),
      rec({ record_id: 'r2', source_type: 'web_search', identifier: 'https://example.com/b', title: 'Result B', agent: 'main' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // One subagent chip for the task: record only.
    expect(screen.getAllByText('Subagent')).toHaveLength(1);
  });

  it('does not crash when provenanceRecords is undefined', () => {
    render(<SourcesPanel />);
    expect(screen.getByText('No sources for this turn')).toBeInTheDocument();
  });

  it('exposes the content fingerprint in a dialog opened from the source card', () => {
    const records = asMap([
      rec({
        record_id: 'r1',
        source_type: 'web_search',
        identifier: 'https://example.com/a',
        title: 'Result A',
        result_sha256: 'abcdef0123456789aaaa',
        result_size: 2048,
        result_snippet: 'A short snippet of the fetched content.',
        provider: 'tavily',
      }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // The card is a real focusable button with an accessible name summarizing
    // the source — clicking it opens the detail dialog.
    const card = screen.getByRole('button', { name: /Result A — View details/ });
    expect(card).toBeInTheDocument();

    fireEvent.click(card);
    expect(screen.getByText('A short snippet of the fetched content.')).toBeInTheDocument();
    expect(screen.getByText('Checksum')).toBeInTheDocument();
    expect(screen.getByText('Provider')).toBeInTheDocument();
  });

  it('falls back to a localized label when title and identifier are missing', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'mcp_tool', identifier: '', title: undefined }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    expect(screen.getByText('Unknown source')).toBeInTheDocument();
  });

  it('hides the scope switch when the thread has no more sources than the turn', () => {
    const turn = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://example.com/a', title: 'Result A' }),
    ]);
    // allRecords identical to the turn set → nothing extra to aggregate.
    render(<SourcesPanel provenanceRecords={turn} allRecords={turn} />);
    expect(screen.queryByRole('button', { name: /All sources/ })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /Current turn/ })).not.toBeInTheDocument();
  });

  it('offers a turn/thread switch and shows aggregated sources on "All sources"', () => {
    const turn = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://example.com/a', title: 'Result A' }),
    ]);
    const thread = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://example.com/a', title: 'Result A' }),
      rec({ record_id: 'r2', source_type: 'web_search', identifier: 'https://example.com/b', title: 'Result B' }),
      rec({ record_id: 'r3', source_type: 'mcp_tool', identifier: 'data-server:get_prices', title: 'Prices' }),
    ]);
    render(<SourcesPanel provenanceRecords={turn} allRecords={thread} />);

    // Switch is present with per-scope counts; defaults to the turn scope.
    expect(screen.getByRole('button', { name: /Current turn \(1\)/ })).toBeInTheDocument();
    const allTab = screen.getByRole('button', { name: /All sources \(3\)/ });
    expect(screen.getByText('Result A')).toBeInTheDocument();
    expect(screen.queryByText('Result B')).not.toBeInTheDocument();

    // Switching to thread scope reveals the other turns' sources.
    fireEvent.click(allTab);
    expect(screen.getByText('Result A')).toBeInTheDocument();
    expect(screen.getByText('Result B')).toBeInTheDocument();
    expect(screen.getByText('Prices')).toBeInTheDocument();
  });

  it('renders an Arguments section in the detail dialog, muting redacted values', () => {
    const records = asMap([
      rec({
        record_id: 'r1',
        source_type: 'web_search',
        identifier: 'https://example.com/a',
        title: 'Result A',
        args: { symbol: 'AAPL', period: '1y', api_key: '[redacted]' },
      }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    fireEvent.click(screen.getByRole('button', { name: /Result A — View details/ }));

    // The Arguments section header plus one row per arg key.
    expect(screen.getByText('Arguments')).toBeInTheDocument();
    expect(screen.getByText('symbol')).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('period')).toBeInTheDocument();

    // The redacted value renders verbatim, in the muted (tertiary) style.
    const redacted = screen.getByText('[redacted]');
    expect(redacted).toBeInTheDocument();
    expect(redacted).toHaveStyle({ color: 'var(--color-text-tertiary)' });
  });

  it('shows the FULL captured args (not a curated subset) as the card subtitle', () => {
    const records = asMap([
      rec({
        record_id: 'r1',
        source_type: 'mcp_tool',
        identifier: 'polygonio:get_stock_data',
        title: 'Stock data',
        args: { symbol: 'AAPL', period: '1y', api_key: '[redacted]' },
      }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // Every arg is rendered, including the redacted one — not just symbol/range —
    // and the redundant `server:tool` identifier is not used as the subtitle.
    expect(
      screen.getByText('symbol: AAPL · period: 1y · api_key: [redacted]'),
    ).toBeInTheDocument();
    expect(screen.queryByText('polygonio:get_stock_data')).not.toBeInTheDocument();
  });

  it('shows full args on non-mcp cards too (e.g. a query-shaped tool call)', () => {
    const records = asMap([
      rec({
        record_id: 'r1',
        source_type: 'mcp_tool',
        identifier: 'hexin_ifind_ds_stock_mcp:get_stock_info',
        title: 'Stock info',
        args: { query: '贵州茅台600519.SH的基本信息' },
      }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // Free-form (non-symbol) args that the old curated label dropped now show.
    expect(
      screen.getByText('query: 贵州茅台600519.SH的基本信息'),
    ).toBeInTheDocument();
  });

  it('shows file rows workspace-relative, stripping the /home/workspace sandbox root', () => {
    const records = asMap([
      rec({
        record_id: 'r1',
        source_type: 'file_read',
        identifier: '/home/workspace/agent.md',
        title: '',
      }),
      rec({
        record_id: 'r2',
        source_type: 'file_read',
        identifier: '/home/workspace',
        title: '',
        args: { path: '/home/workspace', pattern: '**/*.md' },
      }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // The sandbox prefix is stripped for display; the bare root gets a label.
    expect(screen.getByText('agent.md')).toBeInTheDocument();
    expect(screen.getByText('Workspace root')).toBeInTheDocument();
    expect(screen.queryByText('/home/workspace/agent.md')).not.toBeInTheDocument();
  });

  it('humanizes an unmapped source_type group label instead of showing snake_case', () => {
    const records = asMap([
      // A source_type with no i18n group mapping exercises the humanized fallback.
      rec({ record_id: 'r1', source_type: 'custom_future_source' as ProvenanceRecord['source_type'], identifier: 'x', title: 'X' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    expect(screen.getByText('Custom Future Source')).toBeInTheDocument();
    expect(screen.queryByText('custom_future_source')).not.toBeInTheDocument();
  });

  it('groups one web search (shared tool_call_id) into a single deck labeled by the query', () => {
    // A real search emits one record per result, all sharing the tool_call_id.
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://alpha.test/a', title: 'Result A', args: { query: 'spacex funding 2024' }, result_sha256: 'a'.repeat(20) }),
      rec({ record_id: 'r2', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://beta.test/b', title: 'Result B', args: { query: 'spacex funding 2024' }, result_sha256: 'b'.repeat(20) }),
      rec({ record_id: 'r3', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://gamma.test/c', title: 'Result C', args: { query: 'spacex funding 2024' }, result_sha256: 'c'.repeat(20) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // One row for the whole search, not three.
    expect(screen.getByTestId('group-count-web_search')).toHaveTextContent('1');

    // Collapsed deck: labeled by the query, summarizing the result count.
    const stack = screen.getByTestId('source-stack');
    expect(stack).toHaveAttribute('data-fanned', 'false');
    expect(screen.getByText('spacex funding 2024')).toBeInTheDocument();
    expect(screen.getByText('3 results')).toBeInTheDocument();
    // Collapsed, only the query + count show. The peek cards behind the front
    // render blank, so individual result titles are absent from the DOM entirely
    // (not merely aria-hidden) — otherwise they bleed out below the front card as
    // a garbled "tail".
    expect(screen.queryByText('Result A')).not.toBeInTheDocument();
    expect(screen.queryByText('Result B')).not.toBeInTheDocument();
    expect(screen.queryByText('Result C')).not.toBeInTheDocument();
  });

  it('fans a web-search deck into a card per result, each opening its own detail', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://alpha.test/a', title: 'Result A', args: { query: 'q' }, result_sha256: 'a'.repeat(20), result_snippet: 'snippet A' }),
      rec({ record_id: 'r2', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://beta.test/b', title: 'Result B', args: { query: 'q' }, result_sha256: 'b'.repeat(20) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    fireEvent.click(screen.getByRole('button', { name: /q — Expand/ }));
    expect(screen.getByTestId('source-stack')).toHaveAttribute('data-fanned', 'true');

    // Each result is now its own card (per-result title + domain subtitle).
    expect(screen.getByRole('button', { name: /Result A — View details/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Result B — View details/ })).toBeInTheDocument();
    expect(screen.getByText('alpha.test')).toBeInTheDocument();
    expect(screen.getByText('beta.test')).toBeInTheDocument();

    // A result card opens its own fingerprint detail.
    fireEvent.click(screen.getByRole('button', { name: /Result A — View details/ }));
    expect(screen.getByText('snippet A')).toBeInTheDocument();
  });

  it('stacks web_fetch pages from the same domain into a domain deck', () => {
    // Real web_fetch records carry no title — identifier is the URL.
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_fetch', identifier: 'https://sec.gov/a', args: { url: 'https://sec.gov/a', prompt: 'filings detail' }, result_sha256: 'a'.repeat(20) }),
      rec({ record_id: 'r2', source_type: 'web_fetch', identifier: 'https://sec.gov/b', args: { url: 'https://sec.gov/b', prompt: 'debut coverage' }, result_sha256: 'b'.repeat(20) }),
      rec({ record_id: 'r3', source_type: 'web_fetch', identifier: 'https://other.test/x', args: { url: 'https://other.test/x' }, result_sha256: 'c'.repeat(20) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Two rows under "Web pages": one sec.gov deck + one lone other.test page.
    expect(screen.getByTestId('group-count-web_fetch')).toHaveTextContent('2');

    // Only the repeated domain stacks; it's labeled by the domain + page count.
    expect(screen.getAllByTestId('source-stack')).toHaveLength(1);
    expect(screen.getByText('sec.gov')).toBeInTheDocument();
    expect(screen.getByText('2 pages')).toBeInTheDocument();

    // Fanned: each card is a page path on that domain (the domain is the front),
    // and still surfaces its args (e.g. the fetch prompt) as a subtitle.
    fireEvent.click(screen.getByRole('button', { name: /sec\.gov — Expand/ }));
    expect(screen.getByRole('button', { name: /\/a — View details/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /\/b — View details/ })).toBeInTheDocument();
    expect(screen.getByText('prompt: filings detail')).toBeInTheDocument();
    expect(screen.getByText('prompt: debut coverage')).toBeInTheDocument();
    // The redundant url arg is omitted — the path title already conveys it.
    expect(screen.queryByText(/url: https/)).not.toBeInTheDocument();
  });

  it('renders a single-result web search as a flat result card (the page, not the query)', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://alpha.test/a', title: 'Lone Result', args: { query: 'q' }, result_sha256: 'a'.repeat(20) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    expect(screen.queryByTestId('source-stack')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Lone Result — View details/ })).toBeInTheDocument();
  });

  it('keeps two separate searches as two decks', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://a.test/1', title: 'A1', args: { query: 'first' }, result_sha256: '1'.repeat(20) }),
      rec({ record_id: 'r2', source_type: 'web_search', tool_call_id: 'call-1', identifier: 'https://a.test/2', title: 'A2', args: { query: 'first' }, result_sha256: '2'.repeat(20) }),
      rec({ record_id: 'r3', source_type: 'web_search', tool_call_id: 'call-2', identifier: 'https://b.test/1', title: 'B1', args: { query: 'second' }, result_sha256: '3'.repeat(20) }),
      rec({ record_id: 'r4', source_type: 'web_search', tool_call_id: 'call-2', identifier: 'https://b.test/2', title: 'B2', args: { query: 'second' }, result_sha256: '4'.repeat(20) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // One row per query (header badge), two decks.
    expect(screen.getByTestId('group-count-web_search')).toHaveTextContent('2');
    expect(screen.getAllByTestId('source-stack')).toHaveLength(2);
    expect(screen.getByText('first')).toBeInTheDocument();
    expect(screen.getByText('second')).toBeInTheDocument();
  });

  it('caps the collapsed stack to a few peek cards but reports the true count', () => {
    const results = Array.from({ length: 9 }, (_, i) =>
      rec({ record_id: `r${i}`, source_type: 'web_search', tool_call_id: 'c1', identifier: `https://x.test/${i}`, title: `R${i}`, args: { query: 'many' }, result_sha256: String(i).repeat(20) }),
    );
    render(<SourcesPanel provenanceRecords={asMap(results)} />);

    const stack = screen.getByTestId('source-stack');
    // Collapsed: front + a capped number of peek cards (MAX_PEEK_LAYERS = 2),
    // not all nine — so the stack never grows arbitrarily deep.
    expect(stack.querySelectorAll('.source-deck-card')).toHaveLength(3);
    // ...but the front card still reports the true count.
    expect(screen.getByText('9 results')).toBeInTheDocument();

    // Fanned, every result is rendered.
    fireEvent.click(screen.getByRole('button', { name: /many — Expand/ }));
    expect(stack.querySelectorAll('.source-deck-card')).toHaveLength(9);
  });

  it('folds and unfolds a whole category via its header arrow, independently', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', tool_call_id: 'c1', identifier: 'https://a.test/1', title: 'Alpha Result', args: { query: 'q' }, result_sha256: '1'.repeat(20) }),
      rec({ record_id: 'r2', source_type: 'mcp_tool', identifier: 'srv:get_x', title: 'Tool X' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Both categories start expanded with their rows visible.
    const header = screen.getByRole('button', { name: /Web search/ });
    expect(header).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('Alpha Result')).toBeInTheDocument();

    // Folding web search hides only its rows; the count badge stays so a folded
    // category still shows its size, and other categories are untouched.
    fireEvent.click(header);
    expect(header).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByText('Alpha Result')).not.toBeInTheDocument();
    expect(screen.getByTestId('group-count-web_search')).toHaveTextContent('1');
    expect(screen.getByText('Tool X')).toBeInTheDocument();

    // Unfolding restores its rows.
    fireEvent.click(header);
    expect(header).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByText('Alpha Result')).toBeInTheDocument();
  });

  it('clears a fanned deck when its category is folded, so it returns collapsed', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', tool_call_id: 'c1', identifier: 'https://a.test/1', title: 'R1', args: { query: 'q' }, result_sha256: '1'.repeat(20) }),
      rec({ record_id: 'r2', source_type: 'web_search', tool_call_id: 'c1', identifier: 'https://a.test/2', title: 'R2', args: { query: 'q' }, result_sha256: '2'.repeat(20) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Fan the deck open.
    fireEvent.click(screen.getByRole('button', { name: /q — Expand/ }));
    expect(screen.getByTestId('source-stack')).toHaveAttribute('data-fanned', 'true');

    // Folding the category unmounts the deck entirely...
    const header = screen.getByRole('button', { name: /Web search/ });
    fireEvent.click(header);
    expect(screen.queryByTestId('source-stack')).not.toBeInTheDocument();

    // ...and unfolding brings it back COLLAPSED, not still-fanned (the fan state
    // is dropped on fold so it can't linger open behind a collapsed header).
    fireEvent.click(header);
    expect(screen.getByTestId('source-stack')).toHaveAttribute('data-fanned', 'false');
    expect(screen.getByText('2 results')).toBeInTheDocument();
  });

  it('keeps two different URLs with identical content as separate cards in a domain deck', () => {
    // Two distinct pages on one site return byte-identical content (same sha) —
    // e.g. both redirect to the same block/login page. A list deck dedups by URL,
    // so both stay and the page count is not undercounted (entity decks, which
    // share an identifier, still collapse by content hash).
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_fetch', identifier: 'https://site.test/a', args: { url: 'https://site.test/a' }, result_sha256: 'same'.repeat(5) }),
      rec({ record_id: 'r2', source_type: 'web_fetch', identifier: 'https://site.test/b', args: { url: 'https://site.test/b' }, result_sha256: 'same'.repeat(5) }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);

    // Both pages survive: a 2-card deck, not a single collapsed card.
    expect(screen.getByText('2 pages')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /site\.test — Expand/ }));
    expect(screen.getByRole('button', { name: /\/a — View details/ })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /\/b — View details/ })).toBeInTheDocument();
  });

  it('renders a web_fetch with an unparseable URL as a flat leaf row without crashing', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_fetch', identifier: 'not a url', args: { url: 'not a url' } }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // No domain to group under → a single leaf row labeled by the raw identifier.
    expect(screen.queryByTestId('source-stack')).not.toBeInTheDocument();
    expect(screen.getByText('not a url')).toBeInTheDocument();
  });

  it('does not merge web searches that lack both a tool_call_id and a query', () => {
    const records = asMap([
      rec({ record_id: 'r1', source_type: 'web_search', identifier: 'https://x.test/1', title: 'X1' }),
      rec({ record_id: 'r2', source_type: 'web_search', identifier: 'https://y.test/2', title: 'X2' }),
    ]);
    render(<SourcesPanel provenanceRecords={records} />);
    // With no shared grouping key, each falls back to its identifier → two
    // distinct leaf rows, not one deck mislabeled by an arbitrary query.
    expect(screen.getByTestId('group-count-web_search')).toHaveTextContent('2');
    expect(screen.queryByTestId('source-stack')).not.toBeInTheDocument();
    expect(screen.getByText('X1')).toBeInTheDocument();
    expect(screen.getByText('X2')).toBeInTheDocument();
  });
});
