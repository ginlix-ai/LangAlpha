import { describe, it, expect } from 'vitest';
import { normalizeSubagentText } from '../normalizeSubagentText';

describe('normalizeSubagentText', () => {
  it('collapses soft-wrapped prose into one paragraph', () => {
    expect(normalizeSubagentText('one\ntwo\nthree')).toBe('one two three');
  });

  it('keeps paragraph breaks', () => {
    expect(normalizeSubagentText('a\n\nb')).toBe('a\n\nb');
  });

  it('keeps newlines around structural lines', () => {
    expect(normalizeSubagentText('- one\n- two')).toBe('- one\n- two');
  });

  it('unescapes a literal backslash-n in prose', () => {
    expect(normalizeSubagentText('a\\n\\nb')).toBe('a\n\nb');
  });

  it('returns an empty string for absent content', () => {
    expect(normalizeSubagentText(null)).toBe('');
    expect(normalizeSubagentText(undefined)).toBe('');
  });

  it('preserves the indentation of a fenced JSON answer', () => {
    const doc = '```json\n{\n  "summary": "text",\n  "risks": [\n    "one"\n  ]\n}\n```';
    expect(normalizeSubagentText(doc)).toBe(doc);
  });

  it('leaves an escaped newline inside a JSON string value alone', () => {
    const doc = '```json\n{\n  "note": "first\\nsecond"\n}\n```';
    const out = normalizeSubagentText(doc);
    expect(out).toBe(doc);
    // Still a real escape, so the block round-trips through JSON.parse.
    expect(out).toContain('"first\\nsecond"');
  });

  it('normalizes the prose around a fence without touching the fence', () => {
    const doc = 'lead in\nwrapped\n\n```\ncode\nlines\n```\n\ntail one\ntail two';
    expect(normalizeSubagentText(doc)).toBe(
      'lead in wrapped\n\n```\ncode\nlines\n```\n\ntail one tail two'
    );
  });

  it('does not flatten a fence revealed by unescaping', () => {
    // A wholly escaped payload has no fence lines until the unescape pass runs,
    // so the collapse pass has to re-split rather than reuse earlier spans.
    const out = normalizeSubagentText('intro\\n```\\n{\\n  "a": 1\\n}\\n```');
    expect(out).toBe('intro\n```\n{\n  "a": 1\n}\n```');
  });
});
