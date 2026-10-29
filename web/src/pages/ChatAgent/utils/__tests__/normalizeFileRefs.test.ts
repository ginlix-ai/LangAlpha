// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { normalizeFileRefs } from '../normalizeFileRefs';

describe('normalizeFileRefs', () => {
  // ── Step 1: Backtick unwrapping ──────────────────────────────

  it('unwraps backtick-wrapped markdown link', () => {
    const input = 'See `[report.md](results/report.md)` for details';
    expect(normalizeFileRefs(input)).toBe('See [report.md](results/report.md) for details');
  });

  it('unwraps backtick-wrapped image link', () => {
    const input = '`![chart](charts/fig.png)`';
    expect(normalizeFileRefs(input)).toBe('![chart](charts/fig.png)');
  });

  it('unwraps backtick-wrapped __wsref__ link', () => {
    const input = '`[report.md](__wsref__/abc-123/results/report.md)`';
    expect(normalizeFileRefs(input)).toBe('[report.md](__wsref__/abc-123/results/report.md)');
  });

  it('does not unwrap inline code that is not a markdown link', () => {
    const input = 'Run `results/report.md` to see output';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('does not unwrap fenced code blocks', () => {
    const input = '```\n[report.md](results/report.md)\n```';
    // Fenced code uses triple backticks — single backtick regex does not match
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('unwraps multiple backtick-wrapped links in one message', () => {
    const input = '`[a.md](results/a.md)` and `[b.md](results/b.md)`';
    expect(normalizeFileRefs(input)).toBe('[a.md](results/a.md) and [b.md](results/b.md)');
  });

  // ── Step 2: unwrap `file://`, keep the sandbox root ──────────
  //
  // The scheme has to go, because rehype-sanitize drops it. The root stays:
  // it is how a reference says it starts at the workspace instead of beside
  // the file quoting it, and `agentPaths` canonicalizes it for every reader.

  it('unwraps file:// and leaves the sandbox root in place', () => {
    const input = '[report.md](file:///home/workspace/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](/home/workspace/results/report.md)');
  });

  it('unwraps file:// on the daytona root too', () => {
    const input = '[report.md](file:///home/daytona/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](/home/daytona/results/report.md)');
  });

  it('unwraps file:// on the computer\'s own root when given it', () => {
    // A computer whose user cannot create the stock root keeps its folders
    // under one of its own, and a `file://` link there is dropped the same way.
    const input = '[report.md](file:///home/someone/langalpha/AAPL/report.md)';
    expect(normalizeFileRefs(input, '/home/someone/langalpha'))
      .toBe('[report.md](/home/someone/langalpha/AAPL/report.md)');
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('unwraps file:// on a root the workspace moved off', () => {
    // An older turn wrote its links under the computer it ran on; without the
    // unwrap the sanitizer drops the scheme and the link with it.
    const input = '[reports](file:///srv/old/AAPL/reports/) [r](file:///home/someone/langalpha/x.md)';
    expect(normalizeFileRefs(input, '/home/someone/langalpha', ['/srv/old']))
      .toBe('[reports](/srv/old/AAPL/reports/) [r](/home/someone/langalpha/x.md)');
    expect(normalizeFileRefs(input, '/home/someone/langalpha'))
      .toBe('[reports](file:///srv/old/AAPL/reports/) [r](/home/someone/langalpha/x.md)');
  });

  it('unwraps file:// on an own root written percent-encoded', () => {
    // A destination is read before it is decoded, so a root with a space
    // arrives as `%20` and has to match in that spelling too.
    const input = '[report.md](file:///home/someone/my%20files/AAPL/report.md)';
    expect(normalizeFileRefs(input, '/home/someone/my files'))
      .toBe('[report.md](/home/someone/my%20files/AAPL/report.md)');
  });

  it('unwraps file:// from an image link href', () => {
    const input = '![chart](file:///home/workspace/charts/fig.png)';
    expect(normalizeFileRefs(input)).toBe('![chart](/home/workspace/charts/fig.png)');
  });

  it('does not strip file:// from non-sandbox paths', () => {
    const input = '[etc](file:///etc/passwd)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('reaches inside an angle-bracketed destination and keeps the brackets', () => {
    expect(normalizeFileRefs('[d](<file:///home/workspace/results/Q3 deck.pptx>)'))
      .toBe('[d](</home/workspace/results/Q3 deck.pptx>)');
  });

  // ── A bare sandbox path is left exactly as written ───────────
  //
  // Flattening it here left the reference indistinguishable from a relative
  // one, so a link to `/home/workspace/results/report.md` read from
  // `docs/index.md` opened `docs/results/report.md` whenever both existed.

  it('leaves a bare sandbox path alone, so it stays rooted', () => {
    const input = '[report.md](/home/workspace/results/report.md)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('leaves a bare daytona path alone', () => {
    const input = '[report.md](/home/daytona/results/report.md)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('leaves a bare sandbox path in an image link alone', () => {
    const input = '![chart](/home/workspace/charts/fig.png)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  // ── Step 3: Clean inside __wsref__ paths ─────────────────────

  it('strips file:///home/workspace/ inside __wsref__ path', () => {
    const input = '[report.md](__wsref__/abc-123/file:///home/workspace/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](__wsref__/abc-123/results/report.md)');
  });

  it('strips /home/workspace/ inside __wsref__ path', () => {
    const input = '[report.md](__wsref__/abc-123//home/workspace/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](__wsref__/abc-123/results/report.md)');
  });

  it('strips /home/daytona/ inside __wsref__ path', () => {
    const input = '![chart](__wsref__/abc-123//home/daytona/charts/fig.png)';
    expect(normalizeFileRefs(input)).toBe('![chart](__wsref__/abc-123/charts/fig.png)');
  });

  // ── Combined variants ────────────────────────────────────────

  it('handles backtick + file:// combined', () => {
    const input = '`[report.md](file:///home/workspace/results/report.md)`';
    expect(normalizeFileRefs(input)).toBe('[report.md](/home/workspace/results/report.md)');
  });

  it('handles backtick + __wsref__ + file:// combined', () => {
    const input = '`[report.md](__wsref__/abc-123/file:///home/workspace/results/report.md)`';
    expect(normalizeFileRefs(input)).toBe('[report.md](__wsref__/abc-123/results/report.md)');
  });

  it('handles backtick + absolute path combined', () => {
    const input = '`[report.md](/home/workspace/results/report.md)`';
    expect(normalizeFileRefs(input)).toBe('[report.md](/home/workspace/results/report.md)');
  });

  // ── Passthrough / negative cases ─────────────────────────────

  it('does not modify clean relative paths', () => {
    const input = '[report.md](results/report.md)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('does not modify clean __wsref__ paths', () => {
    const input = '[report.md](__wsref__/abc-123/results/report.md)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('does not modify external URLs', () => {
    const input = '[Google](https://google.com)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('does not modify mailto links', () => {
    const input = '[email](mailto:test@example.com)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('does not modify citation bubbles', () => {
    const input = '([Reuters](https://reuters.com/article))';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  it('handles empty string', () => {
    expect(normalizeFileRefs('')).toBe('');
  });

  it('handles null/undefined gracefully', () => {
    expect(normalizeFileRefs(null as unknown as string)).toBe(null);
    expect(normalizeFileRefs(undefined as unknown as string)).toBe(undefined);
  });

  // ── Step 4: destinations with spaces ─────────────────────────

  it('wraps a spaced file destination so it still parses as a link', () => {
    expect(normalizeFileRefs('[deck](results/Q3 deck.pptx)')).toBe('[deck](<results/Q3 deck.pptx>)');
    expect(normalizeFileRefs('![fig](charts/my fig.png)')).toBe('![fig](<charts/my fig.png>)');
  });

  it('wraps a spaced destination that holds balanced parens', () => {
    expect(normalizeFileRefs('[copy](results/memo (1).docx)')).toBe('[copy](<results/memo (1).docx>)');
  });

  it('wraps a spaced destination that points inside the file', () => {
    expect(normalizeFileRefs('[v](results/Q3 notes.md#valuation)')).toBe('[v](<results/Q3 notes.md#valuation>)');
    expect(normalizeFileRefs('[l](work/my model.py:40-55)')).toBe('[l](<work/my model.py:40-55>)');
  });

  it('wraps a destination whose only space is in the heading it names', () => {
    // `findHeadingIndex` reads an anchor as written before it tries the slug, so
    // a hand-written heading is a reference the panel opens. Requiring the
    // fragment to be space-free stopped CommonMark at that space instead, and
    // the reader got the link's text with nothing to click.
    expect(normalizeFileRefs('[s](results/report.md#Valuation Assumptions)'))
      .toBe('[s](<results/report.md#Valuation Assumptions>)');
    expect(normalizeFileRefs('[s](results/Q3 notes.md#Valuation Assumptions)'))
      .toBe('[s](<results/Q3 notes.md#Valuation Assumptions>)');
  });

  it('leaves a destination written inside code as the agent wrote it', () => {
    const fenced = '```python\nexpected = "[deck](results/Q3 deck.pptx)"\n```';
    expect(normalizeFileRefs(fenced)).toBe(fenced);
    expect(normalizeFileRefs('`x = [m](model.py:42)` then')).toBe('`x = [m](model.py:42)` then');
  });

  // ── Step 5: bare line-suffix destinations ────────────────────

  it('anchors a bare name with a line suffix so the URL filter keeps it', () => {
    expect(normalizeFileRefs('[m](model.py:42)')).toBe('[m](./model.py:42)');
    expect(normalizeFileRefs('[m](model.py:40-55)')).toBe('[m](./model.py:40-55)');
    expect(normalizeFileRefs('[m](my model.py:42)')).toBe('[m](<./my model.py:42>)');
    expect(normalizeFileRefs('[m](work/model.py:42)')).toBe('[m](work/model.py:42)');
    expect(normalizeFileRefs('[s](localhost:8000)')).toBe('[s](localhost:8000)');
    expect(normalizeFileRefs('[s](127.0.0.1:8000)')).toBe('[s](127.0.0.1:8000)');
  });

  it('anchors a bracketed bare name with a line suffix and leaves bracketed URLs alone', () => {
    expect(normalizeFileRefs('[m](<my model.py:42>)')).toBe('[m](<./my model.py:42>)');
    expect(normalizeFileRefs('[m](<work/my model.py:42>)')).toBe('[m](<work/my model.py:42>)');
    expect(normalizeFileRefs('[s](<localhost:8000>)')).toBe('[s](<localhost:8000>)');
  });

  it('anchors a line suffix carrying any of the three title forms', () => {
    // The title is not part of the destination, so requiring `)` right after
    // the line number skipped every titled reference and the URL filter read
    // `model.py:` as a scheme. `turnFiles.ts` already collects these, so the
    // card appeared while the prose link beside it rendered with no href.
    expect(normalizeFileRefs('[m](model.py:42 "source")')).toBe('[m](./model.py:42 "source")');
    expect(normalizeFileRefs("[m](model.py:42 'source')")).toBe("[m](./model.py:42 'source')");
    expect(normalizeFileRefs('[m](model.py:42 (source))')).toBe('[m](./model.py:42 (source))');
    expect(normalizeFileRefs('[m](<model.py:42> "source")')).toBe('[m](<./model.py:42> "source")');
    expect(normalizeFileRefs('[m](<my model.py:42> "source")')).toBe('[m](<./my model.py:42> "source")');
    // A slash ahead of the colon is already a path, title or not.
    expect(normalizeFileRefs('[m](work/model.py:42 "source")')).toBe('[m](work/model.py:42 "source")');
    expect(normalizeFileRefs('[s](localhost:8000 "home")')).toBe('[s](localhost:8000 "home")');
  });

  it('leaves URLs, titles and non-file destinations alone', () => {
    const untouched = [
      '[a](results/a(1).md)',
      '[t](https://example.com/a b.md)',
      '[t](results/a.md "a title")',
      '[x](notes about things)',
      '[expanded](up 2.5)',
      '[foo](bar) baz (qux one.md)',
    ];
    for (const input of untouched) expect(normalizeFileRefs(input)).toBe(input);
  });

  // ── Real-world agent output ──────────────────────────────────

  it('handles PTC agent table with file:// links', () => {
    const input = [
      '| File | Description |',
      '|------|-------------|',
      '| [results/nvda_analysis.md](file:///home/workspace/results/nvda_analysis.md) | Full report |',
      '| ![chart](file:///home/workspace/work/task/charts/fig.png) | Price chart |',
    ].join('\n');
    const expected = [
      '| File | Description |',
      '|------|-------------|',
      '| [results/nvda_analysis.md](/home/workspace/results/nvda_analysis.md) | Full report |',
      '| ![chart](/home/workspace/work/task/charts/fig.png) | Price chart |',
    ].join('\n');
    expect(normalizeFileRefs(input)).toBe(expected);
  });

  it('handles Chief of Staff relayed output with backtick + __wsref__', () => {
    const input = 'Deliverables:\n`[results/nvda_analysis.md](__wsref__/20cc68e8-d057-41f4-9bb1-57aa8d310704/results/nvda_analysis.md)`';
    const expected = 'Deliverables:\n[results/nvda_analysis.md](__wsref__/20cc68e8-d057-41f4-9bb1-57aa8d310704/results/nvda_analysis.md)';
    expect(normalizeFileRefs(input)).toBe(expected);
  });

  /**
   * This runs on every rendered bubble, and each of its five label scans
   * rescanned the line from every unmatched `[`. At 80k brackets that was ~11s
   * of frozen main thread; bounded it is ~0.27s. The ceiling is loose on
   * purpose: the point is the complexity class, not a stopwatch reading.
   */
  it('stays fast on a prose line full of unmatched brackets', () => {
    const input = '['.repeat(80_000);
    const started = performance.now();
    expect(normalizeFileRefs(input)).toBe(input);
    expect(performance.now() - started).toBeLessThan(3_000);
  });
});
