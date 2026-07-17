/**
 * Shared provenance dedup helpers: provenanceDisplayKey and
 * countDedupedSources are the single source of truth used by both the Sources
 * pill (MessageList) and the Sources panel so their counts cannot diverge.
 * provenanceRecordKey is the storage key for the live/replay accumulator.
 */
import { describe, it, expect } from 'vitest';
import {
  provenanceMcpKey,
  provenanceDisplayKey,
  countDedupedSources,
  type ProvenanceRecord,
} from '../chat';
import { provenanceRecordKey } from '@/pages/ChatAgent/hooks/utils/streamEventHandlers';

function rec(
  source_type: string,
  identifier: string,
  args_fingerprint?: Record<string, unknown>,
  result_sha256?: string,
): ProvenanceRecord {
  return {
    record_id: `${source_type}:${identifier}:${JSON.stringify(args_fingerprint ?? null)}:${result_sha256 ?? ''}`,
    timestamp: '2026-01-01T00:00:00Z',
    source_type: source_type as ProvenanceRecord['source_type'],
    identifier,
    args_fingerprint,
    result_sha256,
  };
}

describe('provenanceMcpKey', () => {
  it('is empty for non-mcp_tool sources even with a fingerprint and sha', () => {
    expect(provenanceMcpKey(rec('web_search', 'https://x/a', { sha256: 'h' }, 'r'))).toBe('');
  });

  it('is empty for an mcp_tool call with neither args nor a result hash', () => {
    expect(provenanceMcpKey(rec('mcp_tool', 'srv:list_tools'))).toBe('');
  });

  it('includes the args fingerprint', () => {
    expect(provenanceMcpKey(rec('mcp_tool', 'srv:get_prices', { sha256: 'abc' }))).toBe(
      '{"sha256":"abc"}#',
    );
  });

  it('includes the result hash, so same-args calls returning different data differ', () => {
    // Live market data: identical query seconds apart returns a different
    // snapshot. The result hash must keep them distinct.
    const a = provenanceMcpKey(
      rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }, 'r1'),
    );
    const b = provenanceMcpKey(
      rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }, 'r2'),
    );
    expect(a).not.toBe(b);
  });

  it('falls back to the args fingerprint when the result hash is nulled (oversized body)', () => {
    // Bodies over the inline cap have result_sha256 nulled server-side, so the
    // key can only lean on args. Same args still collapse; different args stay
    // distinct — truncation must not merge unrelated oversized calls.
    const same = provenanceMcpKey(rec('mcp_tool', 'srv:dump', { sha256: 'q' }, undefined));
    const sameAgain = provenanceMcpKey(rec('mcp_tool', 'srv:dump', { sha256: 'q' }, undefined));
    const other = provenanceMcpKey(rec('mcp_tool', 'srv:dump', { sha256: 'z' }, undefined));
    expect(same).toBe('{"sha256":"q"}#');
    expect(same).toBe(sameAgain);
    expect(same).not.toBe(other);
  });

  it('keys a no-arg tool purely by its result hash', () => {
    // A tool called with no arguments (e.g. a list/status call) has an empty
    // fingerprint, so the result hash is the only discriminator: same bytes
    // collapse, different bytes stay apart.
    const r1 = provenanceMcpKey(rec('mcp_tool', 'srv:list_holdings', undefined, 'r1'));
    const r2 = provenanceMcpKey(rec('mcp_tool', 'srv:list_holdings', undefined, 'r2'));
    expect(r1).toBe('#r1');
    expect(r1).not.toBe(r2);
  });
});

describe('provenanceDisplayKey', () => {
  it('keys on (source_type, identifier)', () => {
    expect(provenanceDisplayKey(rec('web_search', 'https://example.com/a'))).toBe(
      'web_search https://example.com/a',
    );
  });

  it('tolerates missing fields without throwing', () => {
    expect(provenanceDisplayKey({} as ProvenanceRecord)).toBe(' ');
  });

  it('separates two calls to one mcp_tool by args (the AAPL-vs-NVDA bug)', () => {
    // Both share identifier "price_data:get_stock_data" — only args differ. The
    // key must distinguish them or the second call overwrites the first.
    const aapl = provenanceDisplayKey(
      rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }),
    );
    const nvda = provenanceDisplayKey(
      rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'nvda' }),
    );
    expect(aapl).not.toBe(nvda);
  });
});

describe('countDedupedSources', () => {
  it('returns 0 for null/undefined', () => {
    expect(countDedupedSources(undefined)).toBe(0);
    expect(countDedupedSources(null)).toBe(0);
  });

  it('collapses same web URL to one — ignoring distinct shas (collapse-by-URL)', () => {
    const records = {
      a: { ...rec('web_fetch', 'https://example.com/p'), result_sha256: 'sha-1' },
      b: { ...rec('web_fetch', 'https://example.com/p'), result_sha256: 'sha-2' },
      c: rec('mcp_tool', 'srv:get_prices'),
    };
    // Two distinct display keys: the duplicated URL collapses despite differing
    // shas (web sources intentionally omit sha from the key). mcp_tool keeps sha
    // (see below), but here srv:get_prices has no args/sha so it's its own row.
    expect(countDedupedSources(records)).toBe(2);
  });

  it('matches the panel grouping logic for distinct identifiers', () => {
    const records = {
      a: rec('web_search', 'https://example.com/a'),
      b: rec('web_search', 'https://example.com/b'),
    };
    expect(countDedupedSources(records)).toBe(2);
  });

  it('counts two same-tool mcp calls with different args as two sources', () => {
    // Regression: get_stock_data(AAPL) and get_stock_data(NVDA) in one
    // execute_code/bash block share source_type + identifier; only the args
    // fingerprint differs. Both must be counted.
    const records = {
      a: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }),
      b: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'nvda' }),
    };
    expect(countDedupedSources(records)).toBe(2);
  });

  it('counts same-tool SAME-ticker calls with different args as two sources', () => {
    // Same tool AND same ticker, but different args (e.g. a different date
    // range or interval) → distinct args_fingerprint → distinct sources. The
    // fingerprint is over the WHOLE arg set, not just the symbol.
    const records = {
      jun: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl-jun-1day' }),
      jan: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl-jan-1day' }),
    };
    expect(countDedupedSources(records)).toBe(2);
  });

  it('counts same-args mcp calls that returned different data as two snapshots', () => {
    // Time-varying market data: identical args, different result. The earlier
    // snapshot must not be silently overwritten by the later one.
    const records = {
      morning: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }, 'open-quote'),
      close: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }, 'close-quote'),
    };
    expect(countDedupedSources(records)).toBe(2);
  });

  it('collapses a true duplicate (same args AND same result) to one source', () => {
    // A re-delivered/replayed event for the same access — identical args AND
    // identical result hash — is one source, not two.
    const records = {
      a: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }, 'same-bytes'),
      b: rec('mcp_tool', 'price_data:get_stock_data', { sha256: 'aapl' }, 'same-bytes'),
    };
    expect(countDedupedSources(records)).toBe(1);
  });

  it('shows one card for the same data product fetched in two code blocks', () => {
    // The live storage key includes tool_call_id (so both events are retained),
    // but the display key omits it — identical identifier+args+result collapse to
    // one card. The same statement read in two execute_code blocks shows once.
    const records = {
      a: rec('mcp_tool', 'fundamentals:get_financial_statements', { sha256: 'inc' }, 'bytes'),
      b: rec('mcp_tool', 'fundamentals:get_financial_statements', { sha256: 'inc' }, 'bytes'),
    };
    expect(countDedupedSources(records)).toBe(1);
  });

  it('counts a no-arg mcp tool by its result hash', () => {
    const distinct = {
      a: rec('mcp_tool', 'srv:list_holdings', undefined, 'r1'),
      b: rec('mcp_tool', 'srv:list_holdings', undefined, 'r2'),
    };
    const same = {
      a: rec('mcp_tool', 'srv:list_holdings', undefined, 'r1'),
      b: rec('mcp_tool', 'srv:list_holdings', undefined, 'r1'),
    };
    expect(countDedupedSources(distinct)).toBe(2);
    expect(countDedupedSources(same)).toBe(1);
  });
});

describe('provenanceRecordKey', () => {
  it('is unchanged for non-mcp_tool sources (no discriminator suffix)', () => {
    expect(
      provenanceRecordKey({
        tool_call_id: 'call-1',
        source_type: 'web_search',
        identifier: 'https://example.com/a',
        args_fingerprint: { sha256: 'h' },
        result_sha256: 'r',
      }),
    ).toBe('call-1:web_search:https://example.com/a');
  });

  it('keeps two same-tool mcp calls distinct despite a shared tool_call_id', () => {
    // The root-cause case: both in-sandbox MCP calls inherit the outer
    // execute_code/bash tool_call_id AND the same "server:tool" identifier, so
    // the discriminator is the only thing keeping them from colliding.
    const aapl = provenanceRecordKey({
      tool_call_id: 'bash-1',
      source_type: 'mcp_tool',
      identifier: 'price_data:get_stock_data',
      args_fingerprint: { sha256: 'aapl' },
    });
    const nvda = provenanceRecordKey({
      tool_call_id: 'bash-1',
      source_type: 'mcp_tool',
      identifier: 'price_data:get_stock_data',
      args_fingerprint: { sha256: 'nvda' },
    });
    expect(aapl).not.toBe(nvda);
  });

  it('keeps same-args calls distinct when the result differs (time-varying data)', () => {
    const morning = provenanceRecordKey({
      tool_call_id: 'bash-1',
      source_type: 'mcp_tool',
      identifier: 'price_data:get_stock_data',
      args_fingerprint: { sha256: 'aapl' },
      result_sha256: 'open-quote',
    });
    const close = provenanceRecordKey({
      tool_call_id: 'bash-1',
      source_type: 'mcp_tool',
      identifier: 'price_data:get_stock_data',
      args_fingerprint: { sha256: 'aapl' },
      result_sha256: 'close-quote',
    });
    expect(morning).not.toBe(close);
  });

  it('collapses a byte-identical duplicate within one code block to one storage key', () => {
    // Same tool_call_id, identifier, args AND result (a re-emitted access, or
    // the agent calling an identical historical query twice in one block): the
    // accumulator stores it once rather than as two look-alike cards.
    const call = {
      tool_call_id: 'bash-1',
      source_type: 'mcp_tool',
      identifier: 'price_data:get_stock_data',
      args_fingerprint: { sha256: 'aapl' },
      result_sha256: 'bytes',
    };
    expect(provenanceRecordKey(call)).toBe(provenanceRecordKey({ ...call }));
  });

  it('keeps the same access in two different code blocks under separate keys', () => {
    // Distinct execute_code blocks carry distinct tool_call_ids, which the
    // storage key includes — so both live events survive (the display key later
    // collapses them; see countDedupedSources). This divergence is intentional.
    const common = {
      source_type: 'mcp_tool' as const,
      identifier: 'price_data:get_stock_data',
      args_fingerprint: { sha256: 'aapl' },
      result_sha256: 'bytes',
    };
    expect(provenanceRecordKey({ ...common, tool_call_id: 'bash-1' })).not.toBe(
      provenanceRecordKey({ ...common, tool_call_id: 'bash-2' }),
    );
  });

  it('keeps each market-watch stamp refresh under its own storage key', () => {
    // Stamps carry no tool_call_id, so without the sha discriminator every
    // refresh of a symbol would collide and the accumulator would keep only
    // the latest snapshot.
    const common = {
      source_type: 'market_data',
      identifier: 'NVDA',
      detail: 'market_watch',
    };
    const first = provenanceRecordKey({ ...common, result_sha256: 'stamp-1' });
    const second = provenanceRecordKey({ ...common, result_sha256: 'stamp-2' });
    expect(first).not.toBe(second);
    // A throttled replay of the byte-identical stamp still collapses.
    expect(provenanceRecordKey({ ...common, result_sha256: 'stamp-1' })).toBe(first);
  });

  it('leaves non-market-watch market_data records on the bare key', () => {
    // Direct market tools carry a tool_call_id; their key shape is unchanged.
    const key = provenanceRecordKey({
      tool_call_id: 'call-1',
      source_type: 'market_data',
      identifier: 'NVDA',
      detail: 'quote',
      result_sha256: 'r',
    });
    expect(key).toBe('call-1:market_data:NVDA');
  });
});
