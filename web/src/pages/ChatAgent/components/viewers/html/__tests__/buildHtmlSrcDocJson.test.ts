import { runInNewContext } from 'node:vm';
import { describe, expect, it } from 'vitest';

import { buildHtmlSrcDoc, type HtmlSrcDocVariant } from '../buildHtmlSrcDoc';

/**
 * Feeds `payload` through the widget's own JSON.parse patch without touching the
 * test runner's JSON.parse: builds the full srcDoc, runs the injected early
 * script and the `data.json` script in a fresh `node:vm` realm with stubbed
 * window/document, then returns the result re-parsed in the test realm.
 * `reviver` is forwarded to the in-realm parse.
 */
function parseWidgetData(
  variant: HtmlSrcDocVariant,
  payload: string,
  reviver?: (key: string, value: unknown) => unknown,
) {
  const html = buildHtmlSrcDoc(variant, {
    html: '<div>probe</div>',
    data: { 'data.json': payload },
  });
  const scripts = Array.from(html.matchAll(/<script>([\s\S]*?)<\/script>/g), (m) => m[1]);
  // Execute the generated early script and data injection in an isolated realm,
  // as the iframe does, without replacing the test runner's JSON.parse.
  const encoded = runInNewContext(
    `${scripts[0]}\n${scripts[1]}\n`
      + 'JSON.stringify(JSON.parse(window.__WIDGET_DATA__["data.json"], reviver));',
    {
      window: { addEventListener() {} },
      document: { addEventListener() {} },
      reviver,
    },
  );
  return JSON.parse(encoded);
}

describe.each(['widget-inline', 'widget-fullscreen'] as const)('%s JSON data', (variant) => {
  it.each([
    {
      name: 'quoted values',
      payload: '{"label":"Infinity Growth","literal":"NaN","negative":"-Infinity"}',
      expected: { label: 'Infinity Growth', literal: 'NaN', negative: '-Infinity' },
    },
    {
      name: 'quoted keys',
      payload: '{"NaN":1,"Infinity":2,"-Infinity":3}',
      expected: { NaN: 1, Infinity: 2, '-Infinity': 3 },
    },
    {
      name: 'escaped quotes and backslashes',
      payload: JSON.stringify({ label: 'say "NaN" and "Infinity"', path: 'C:\\Infinity' }),
      expected: { label: 'say "NaN" and "Infinity"', path: 'C:\\Infinity' },
    },
    {
      name: 'normal nested JSON',
      payload: '{"nested":{"label":"Revenue","values":[1.2300,2e+02]}}',
      expected: { nested: { label: 'Revenue', values: [1.23, 200] } },
    },
    {
      name: 'nonstandard numeric constants',
      payload: '[NaN, Infinity, -Infinity]',
      expected: [null, null, null],
    },
    {
      name: 'mixed nested data',
      payload: '{"value":NaN,"nested":[Infinity,-Infinity,{"label":"NaN"}]}',
      expected: { value: null, nested: [null, null, { label: 'NaN' }] },
    },
  ])('preserves $name', ({ payload, expected }) => {
    expect(parseWidgetData(variant, payload)).toEqual(expected);
  });

  it('preserves the native reviver behavior', () => {
    expect(parseWidgetData(variant, '{"n":2,"label":"Infinity"}',
      (key, value) => key === 'n' ? Number(value) * 3 : value,
    )).toEqual({ n: 6, label: 'Infinity' });
  });

  it('still rejects otherwise invalid JSON as a SyntaxError', () => {
    let error: unknown;
    try {
      parseWidgetData(variant, '{"value":}');
    } catch (caught) {
      error = caught;
    }
    // The error comes from the vm realm, so `instanceof SyntaxError` fails here;
    // the name is what pins "the wrapper still delegates to the native parser".
    expect(error).toBeDefined();
    expect((error as Error).name).toBe('SyntaxError');
  });

  it('stays linear on truncated JSON with escaped quotes', () => {
    // A realistic truncated write: the file is cut off inside a string full of
    // escaped quotes, the worst case for a string-first scan that retries from
    // every quote. The call must finish and the native parser must still
    // reject the payload. The bare NaN makes the text a scan candidate.
    const truncated = '{"v":NaN,"rows":[{"html":"' + '\\"'.repeat(100_000);
    let error: unknown;
    const started = performance.now();
    try {
      parseWidgetData(variant, truncated);
    } catch (caught) {
      error = caught;
    }
    const elapsed = performance.now() - started;

    expect((error as Error).name).toBe('SyntaxError');
    // Generous quadratic-scan detector, not a microbenchmark: a linear scan
    // is four orders of magnitude below this bound.
    expect(elapsed).toBeLessThan(2000);
  });

  it('parses a 12 MB single-string payload without exhausting the regexp stack', () => {
    // A regexp that matches a whole quoted string keeps a backtrack-stack
    // entry per character, so a single very long string can throw RangeError
    // before the native parser ever sees it. The flat token scan has no
    // repetition to backtrack; it must parse and keep the string intact.
    const blob = 'A'.repeat(12_000_000) + ' NaN Infinity -Infinity tail';
    const parsed = parseWidgetData(
      variant,
      `{"blob":"${blob}","v":-Infinity,"scale":1.2300e+02}`,
    ) as { blob: string; v: unknown; scale: unknown };

    expect(parsed.blob).toBe(blob);
    expect(parsed.v).toBeNull();
    expect(parsed.scale).toBe(123);
  }, 60_000);

  it('preserves a large escaped string while a bare NaN beside it becomes null', () => {
    // Every quote and backslash in the value is escaped in the JSON text, so
    // this runs the escape branch at scale; the bare NaN outside the string is
    // the only token that may be rewritten.
    const text = '"\\NaN Infinity -Infinity '.repeat(50_000);
    const parsed = parseWidgetData(
      variant,
      `{"text":${JSON.stringify(text)},"v":NaN}`,
    ) as { text: string; v: unknown };

    expect(parsed.text).toBe(text);
    expect(parsed.v).toBeNull();
  }, 60_000);

  it('rejects a truncated 12 MB string as SyntaxError, not RangeError', () => {
    // The malformed counterpart of the payload above: the scan must finish and
    // hand the text to the native parser, which rejects it. The bare NaN makes
    // the text a scan candidate.
    let error: unknown;
    try {
      parseWidgetData(variant, `{"v":NaN,"blob":"${'A'.repeat(12_000_000)}`);
    } catch (caught) {
      error = caught;
    }

    expect((error as Error).name).toBe('SyntaxError');
  }, 60_000);
});
