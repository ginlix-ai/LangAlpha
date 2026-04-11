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

  // ── Step 2: file:///home/(workspace|daytona)/ stripping ──────

  it('strips file:///home/workspace/ from link href', () => {
    const input = '[report.md](file:///home/workspace/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](results/report.md)');
  });

  it('strips file:///home/daytona/ from link href', () => {
    const input = '[report.md](file:///home/daytona/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](results/report.md)');
  });

  it('strips file:// from image link href', () => {
    const input = '![chart](file:///home/workspace/charts/fig.png)';
    expect(normalizeFileRefs(input)).toBe('![chart](charts/fig.png)');
  });

  it('does not strip file:// from non-sandbox paths', () => {
    const input = '[etc](file:///etc/passwd)';
    expect(normalizeFileRefs(input)).toBe(input);
  });

  // ── Step 3: /home/(workspace|daytona)/ absolute path stripping

  it('strips /home/workspace/ from link href', () => {
    const input = '[report.md](/home/workspace/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](results/report.md)');
  });

  it('strips /home/daytona/ from link href', () => {
    const input = '[report.md](/home/daytona/results/report.md)';
    expect(normalizeFileRefs(input)).toBe('[report.md](results/report.md)');
  });

  it('strips /home/workspace/ from image link', () => {
    const input = '![chart](/home/workspace/charts/fig.png)';
    expect(normalizeFileRefs(input)).toBe('![chart](charts/fig.png)');
  });

  // ── Step 4: Clean inside __wsref__ paths ─────────────────────

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
    expect(normalizeFileRefs(input)).toBe('[report.md](results/report.md)');
  });

  it('handles backtick + __wsref__ + file:// combined', () => {
    const input = '`[report.md](__wsref__/abc-123/file:///home/workspace/results/report.md)`';
    expect(normalizeFileRefs(input)).toBe('[report.md](__wsref__/abc-123/results/report.md)');
  });

  it('handles backtick + absolute path combined', () => {
    const input = '`[report.md](/home/workspace/results/report.md)`';
    expect(normalizeFileRefs(input)).toBe('[report.md](results/report.md)');
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
      '| [results/nvda_analysis.md](results/nvda_analysis.md) | Full report |',
      '| ![chart](work/task/charts/fig.png) | Price chart |',
    ].join('\n');
    expect(normalizeFileRefs(input)).toBe(expected);
  });

  it('handles flash agent relayed output with backtick + __wsref__', () => {
    const input = 'Deliverables:\n`[results/nvda_analysis.md](__wsref__/20cc68e8-d057-41f4-9bb1-57aa8d310704/results/nvda_analysis.md)`';
    const expected = 'Deliverables:\n[results/nvda_analysis.md](__wsref__/20cc68e8-d057-41f4-9bb1-57aa8d310704/results/nvda_analysis.md)';
    expect(normalizeFileRefs(input)).toBe(expected);
  });
});
