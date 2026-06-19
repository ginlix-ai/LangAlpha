/**
 * SourcesPanel: groups a turn's provenance by source_type with per-group
 * counts, dedups by (source_type, identifier), and tags subagent records.
 * Every source is a card; clicking one opens a detail dialog exposing the
 * content fingerprint. URL/file sources expose an "Open link"/"Open file"
 * action inside that dialog.
 */
import { describe, it, expect, vi, beforeAll, afterEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
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
    const webGroup = webSearch.closest('div')!.parentElement!;
    expect(within(webGroup).getByText('2')).toBeInTheDocument();
    const mcpGroup = mcp.closest('div')!.parentElement!;
    expect(within(mcpGroup).getByText('1')).toBeInTheDocument();
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
    const market = screen.getByText('Market data');
    const marketGroup = market.closest('div')!.parentElement!;
    expect(within(marketGroup).getByText('1')).toBeInTheDocument();

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
});
