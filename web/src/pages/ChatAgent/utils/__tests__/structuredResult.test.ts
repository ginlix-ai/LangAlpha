import { describe, it, expect } from 'vitest';
import { humanizeKey, parseResultPreview, parseStructuredResult } from '../structuredResult';

describe('parseStructuredResult', () => {
  it('parses a bare JSON object and keeps the emitted field order', () => {
    const result = parseStructuredResult('{"summary": "s", "risks": ["one"]}');
    expect(result?.entries.map(([key]) => key)).toEqual(['summary', 'risks']);
  });

  it('parses a fenced JSON object', () => {
    expect(parseStructuredResult('```json\n{"a": 1}\n```')?.entries).toEqual([['a', 1]]);
  });

  it('parses a fence with no info string', () => {
    expect(parseStructuredResult('```\n{"a": 1}\n```')?.entries).toEqual([['a', 1]]);
  });

  it('pretty-prints the raw copy for the disclosure', () => {
    expect(parseStructuredResult('{"a":1}')?.raw).toBe('{\n  "a": 1\n}');
  });

  it.each([
    ['prose wrapping the JSON', 'Here is the result: {"a": 1}'],
    ['a trailing sentence', '{"a": 1}\n\nHope that helps.'],
    ['a JSON array', '[1, 2]'],
    ['a bare string', '"just a string"'],
    ['an empty object', '{}'],
    ['invalid JSON', '{"a": }'],
    ['plain prose', 'no json here'],
    ['empty input', ''],
    ['null input', null],
  ])('rejects %s', (_label, input) => {
    expect(parseStructuredResult(input)).toBeNull();
  });
});

describe('parseResultPreview', () => {
  it('passes an intact payload straight through', () => {
    const result = parseResultPreview('{"a": 1}');
    expect(result?.entries).toEqual([['a', 1]]);
    expect(result?.truncated).toBeUndefined();
  });

  it('recovers the members that arrived whole from a clipped payload', () => {
    const clipped =
      '{"briefs": {"AAPL": {"summary": "done", "eps": 2.01}, "MSFT": {"summary": "partial tex' +
      '\n... [truncated]';
    const result = parseResultPreview(clipped);
    expect(result?.truncated).toBe(true);
    expect(result?.entries).toEqual([
      ['briefs', { AAPL: { summary: 'done', eps: 2.01 } }],
    ]);
  });

  it('shows the payload as received in the raw disclosure', () => {
    const clipped = '{"a": {"b": 1, "c": "cut\n... [truncated]';
    expect(parseResultPreview(clipped)?.raw).toBe(clipped);
  });

  it('does not treat a comma inside a string as a boundary', () => {
    const clipped = '{"a": "one, two", "b": "cut\n... [truncated]';
    expect(parseResultPreview(clipped)?.entries).toEqual([['a', 'one, two']]);
  });

  it('recovers nothing when no member boundary survived', () => {
    expect(parseResultPreview('{"summary": "cut mid str\n... [truncated]')).toBeNull();
  });

  it('rejects a clipped payload that is not an object', () => {
    expect(parseResultPreview('[1, 2, 3\n... [truncated]')).toBeNull();
  });
});

describe('humanizeKey', () => {
  it.each([
    ['summary', 'Summary'],
    ['total_revenue', 'Total revenue'],
    ['risk-factors', 'Risk factors'],
    ['marketCap', 'Market Cap'],
    ['EPS', 'EPS'],
  ])('%s becomes %s', (input, expected) => {
    expect(humanizeKey(input)).toBe(expected);
  });
});

describe('parseStructuredResult number fidelity', () => {
  it('declines an integer that JSON.parse cannot hold', () => {
    // Rendering it would show ...992, and the raw disclosure is regenerated
    // from the parsed object, so both views would agree on the wrong number.
    expect(parseStructuredResult('{"cik": 9007199254740993}')).toBeNull();
  });

  it('declines an exponent that overflows to Infinity', () => {
    expect(parseStructuredResult('{"notional": 1e400}')).toBeNull();
  });

  it('keeps ordinary prices and counts', () => {
    const result = parseStructuredResult('{"price": 231.47, "volume": 91320045}');
    expect(result?.entries).toEqual([
      ['price', 231.47],
      ['volume', 91320045],
    ]);
  });

  it('ignores digits that live inside strings', () => {
    const result = parseStructuredResult('{"note": "id 9007199254740993 stays"}');
    expect(result?.entries).toEqual([['note', 'id 9007199254740993 stays']]);
  });

  it('holds the boundary itself', () => {
    expect(parseStructuredResult('{"n": 9007199254740991}')?.entries).toEqual([
      ['n', 9007199254740991],
    ]);
  });
});
