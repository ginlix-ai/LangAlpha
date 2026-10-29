// @vitest-environment node
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import { formatContextBlock } from '../chat-input.contextBlocks';

const t = ((key: string) => key) as unknown as TFunction;

describe('formatContextBlock', () => {
  it('labels pasted text and a quoted agent response', () => {
    expect(formatContextBlock({ path: '', source: 'paste', snippet: 'hello' }, t)).toBe(
      '\n<details>\n<summary>[context.pastedText]</summary>\n\n```\nhello\n```\n</details>',
    );
    expect(formatContextBlock({ path: '', source: 'chat', snippet: 'quoted' }, t)).toContain(
      '<summary>[context.fromAgentResponse]</summary>',
    );
  });

  it('hands a chart over as a hint, not a fenced copy', () => {
    const out = formatContextBlock({ path: '', source: 'chart', label: 'AAPL 1D', snippet: 'candles, 1D' }, t);
    expect(out).toBe('\n<details>\n<summary>[context.chartInPanel: AAPL 1D]</summary>\n\ncandles, 1D\n</details>');
    expect(out).not.toContain('```');
  });

  it('writes a locator as the reopenable link, sized in cells', () => {
    const out = formatContextBlock({ path: 'models/dcf.xlsx', locator: 'Model!B4:D9', snippet: '1\t2' }, t);
    expect(out).toContain('<summary>@models/dcf.xlsx#Model!B4:D9 (18 cells)</summary>');
    expect(out).toContain('```\n1\t2\n```');
  });

  it('keeps a summary to text a summary can hold', () => {
    const out = formatContextBlock({ path: 'models/dcf.xlsx', locator: "'<Model>'!B4", snippet: '1' }, t);
    expect(out).toContain("<summary>@models/dcf.xlsx#'Model'!B4 (1 cell)</summary>");
    const chart = formatContextBlock({ path: '', source: 'chart', label: 'AAPL\n1D', snippet: 'candles' }, t);
    expect(chart).toContain('<summary>[context.chartInPanel: AAPL 1D]</summary>');
  });

  it('fences with more backticks than the snippet holds in a row', () => {
    const out = formatContextBlock({ path: 'notes.md', snippet: 'a\n```\nb\n```' }, t);
    expect(out).toContain('\n````\na\n```\nb\n```\n````\n');
    expect(formatContextBlock({ path: 'notes.md', snippet: 'x' }, t)).toContain('\n```\nx\n```\n');
  });

  it('keeps lines as the unit for everything else', () => {
    const out = formatContextBlock({ path: 'work/model.py', lineStart: 40, lineEnd: 42, lineCount: 3, snippet: 'def npv():' }, t);
    expect(out).toContain('<summary>@work/model.py (lines 40-42, 3 lines)</summary>');
    expect(formatContextBlock({ path: 'notes.md', lineStart: 7, lineEnd: 7, lineCount: 1, snippet: 'x' }, t)).toContain('(lines 7-7, 1 line)');
    expect(formatContextBlock({ path: 'notes.md', snippet: 'x' }, t)).toContain('<summary>@notes.md</summary>');
  });
});
