/**
 * Shared provenance dedup helpers: provenanceDisplayKey and
 * countDedupedSources are the single source of truth used by both the Sources
 * pill (MessageList) and the Sources panel so their counts cannot diverge.
 */
import { describe, it, expect } from 'vitest';
import { provenanceDisplayKey, countDedupedSources, type ProvenanceRecord } from '../chat';

function rec(source_type: string, identifier: string): ProvenanceRecord {
  return {
    record_id: `${source_type}:${identifier}`,
    timestamp: '2026-01-01T00:00:00Z',
    source_type: source_type as ProvenanceRecord['source_type'],
    identifier,
  };
}

describe('provenanceDisplayKey', () => {
  it('keys on (source_type, identifier)', () => {
    expect(provenanceDisplayKey(rec('web_search', 'https://example.com/a'))).toBe(
      'web_search https://example.com/a',
    );
  });

  it('tolerates missing fields without throwing', () => {
    expect(provenanceDisplayKey({} as ProvenanceRecord)).toBe(' ');
  });
});

describe('countDedupedSources', () => {
  it('returns 0 for null/undefined', () => {
    expect(countDedupedSources(undefined)).toBe(0);
    expect(countDedupedSources(null)).toBe(0);
  });

  it('collapses same (source_type, identifier) to one — ignoring distinct shas', () => {
    const records = {
      a: { ...rec('web_fetch', 'https://example.com/p'), result_sha256: 'sha-1' },
      b: { ...rec('web_fetch', 'https://example.com/p'), result_sha256: 'sha-2' },
      c: rec('mcp_tool', 'srv:get_prices'),
    };
    // Two distinct display keys: the duplicated URL collapses despite differing
    // shas (live UI omits sha from the key, unlike the DB endpoint).
    expect(countDedupedSources(records)).toBe(2);
  });

  it('matches the panel grouping logic for distinct identifiers', () => {
    const records = {
      a: rec('web_search', 'https://example.com/a'),
      b: rec('web_search', 'https://example.com/b'),
    };
    expect(countDedupedSources(records)).toBe(2);
  });
});
