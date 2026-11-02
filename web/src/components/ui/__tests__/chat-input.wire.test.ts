// @vitest-environment node
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import type { QuoteContext } from '../chat-input.types';
import { formatContextBlock, formatQuoteBlock, parseSentMessage, rewriteQuoteBlocks } from '../chat-input.wire';

const t = ((key: string) => key) as unknown as TFunction;
const said = (snippet: string, reply?: string, speaker = 'Analyst'): QuoteContext => ({ kind: 'quote', snippet, reply, from: { kind: 'message', speaker } });

describe('a speaker whose name starts with an at sign', () => {
  it('reads back as a speaker, not a file', () => {
    const quote = said('words', undefined, '@bob\'s response');
    const block = formatQuoteBlock(quote);
    expect(block).toContain('source="&#64;bob\'s response"');
    let read: QuoteContext | undefined;
    rewriteQuoteBlocks(block, (q) => { read = q; return ''; });
    expect(read?.from).toEqual({ kind: 'message', speaker: '@bob\'s response' });
  });
});

describe('a closer already guarded in the text', () => {
  it('comes back as the same text', () => {
    const snippet = 'has </quote> and &lt;/quote> and &amp;lt;/quote>';
    const [part] = parseSentMessage(formatQuoteBlock(said(snippet)));
    expect(part).toMatchObject({ kind: 'context', body: snippet });
  });
});

describe('a quote written out as an example in code', () => {
  const example = '<quote source="Analyst">literal</quote>';

  it.each([
    ['a fence', `before\n\`\`\`xml\n${example}\n\`\`\`\nafter`],
    ['a tilde fence', `~~~\n${example}\n~~~`],
    ['inline code', `write \`${example}\` like so`],
  ])('stays text in %s', (_, message) => {
    expect(parseSentMessage(message)).toEqual([{ kind: 'text', text: message }]);
    expect(rewriteQuoteBlocks(message, () => 'TOKEN')).toBe(message);
  });

  it('does not hide a real quote after it, or one holding backticks', () => {
    const real = formatQuoteBlock(said('has ``` and ` inside'));
    const message = `\`\`\`\n${example}\n\`\`\`\n${real} done`;
    expect(parseSentMessage(message).filter((part) => part.kind === 'context')).toHaveLength(1);
    expect(rewriteQuoteBlocks(`${real} done`, () => 'TOKEN')).toBe('TOKEN done');
  });
});

describe('what is not a block', () => {
  it('leaves a details the person typed as text', () => {
    const typed = 'see\n<details>\n<summary>Logs</summary>\n\nbody\n</details>';
    expect(parseSentMessage(typed)).toEqual([{ kind: 'text', text: typed }]);
  });

  it('takes many unclosed openers in linear time', () => {
    const start = performance.now();
    parseSentMessage('<quote source="">'.repeat(60_000));
    parseSentMessage('\n<details>\n<summary>[x]</summary>\n\n'.repeat(30_000));
    expect(performance.now() - start).toBeLessThan(1000);
  });
});

describe('a stray backtick', () => {
  it('is not closed by a later quote on the line', () => {
    const line = `a \` b <quote source="x">one</quote> <quote source="y">two \` three</quote>`;
    expect(parseSentMessage(line).filter((part) => part.kind === 'context')).toHaveLength(2);
  });
});

describe('what would close a tag', () => {
  it('guards a closer with a space, and a source holding a tag', () => {
    const quote = formatQuoteBlock({ kind: 'quote', snippet: 'a </ quote> b', from: { kind: 'file', path: 'a</quote>.md', label: 'x', lineCount: 1 } });
    expect(quote.match(/<\/\s*quote/g)).toHaveLength(1);
    expect(parseSentMessage(quote)[0]).toMatchObject({ kind: 'context', body: 'a </ quote> b', source: 'a</quote>.md' });
  });
});

describe('parseSentMessage text', () => {
  it('keeps the indent of a line of its own', () => {
    expect(parseSentMessage('    print(1)\n    print(2)')).toEqual([{ kind: 'text', text: '    print(1)\n    print(2)' }]);
    const quote = formatQuoteBlock(said('words'));
    expect(parseSentMessage(`${quote}\n    print(1)`).at(-1)).toEqual({ kind: 'text', text: '    print(1)' });
  });

  it('drops the gap beside a chip', () => {
    expect(parseSentMessage(`${formatQuoteBlock(said('words'))} after`).at(-1)).toEqual({ kind: 'text', text: 'after' });
  });
});

describe('formatContextBlock', () => {
  it('labels pasted text', () => {
    expect(formatContextBlock({ kind: 'chip', source: 'paste', label: 'Pasted', snippet: 'hello' }, t)).toBe(
      '\n<details>\n<summary>[context.pastedText]</summary>\n\n```\nhello\n```\n</details>',
    );
  });

  it('hands a chart over as a hint, not a fenced copy', () => {
    const out = formatContextBlock({ kind: 'chip', source: 'chart', label: 'AAPL 1D', snippet: 'candles, 1D' }, t);
    expect(out).toBe('\n<details>\n<summary>[context.chartInPanel: AAPL 1D]</summary>\n\ncandles, 1D\n</details>');
    expect(out).not.toContain('```');
  });

  it('writes a locator as the reopenable link, sized in cells', () => {
    const out = formatContextBlock({ kind: 'chip', source: 'range', path: 'models/dcf.xlsx', locator: 'Model!B4:D9', label: 'dcf.xlsx#Model!B4:D9', snippet: '1\t2' }, t);
    expect(out).toContain('<summary>@models/dcf.xlsx#Model!B4:D9 (18 cells)</summary>');
    expect(out).toContain('```\n1\t2\n```');
  });

  it('keeps a summary to text a summary can hold', () => {
    const out = formatContextBlock({ kind: 'chip', source: 'range', path: 'models/dcf.xlsx', locator: "'<Model>'!B4", label: 'dcf.xlsx', snippet: '1' }, t);
    expect(out).toContain("<summary>@models/dcf.xlsx#'Model'!B4 (1 cell)</summary>");
    const chart = formatContextBlock({ kind: 'chip', source: 'chart', label: 'AAPL\n1D', snippet: 'candles' }, t);
    expect(chart).toContain('<summary>[context.chartInPanel: AAPL 1D]</summary>');
  });

  it('fences with more backticks than the snippet holds in a row', () => {
    const out = formatContextBlock({ kind: 'chip', source: 'paste', label: 'Pasted', snippet: 'a\n```\nb\n```' }, t);
    expect(out).toContain('\n````\na\n```\nb\n```\n````\n');
  });
});

describe('formatQuoteBlock', () => {
  it('tags a quote and pairs the comment with it', () => {
    expect(formatQuoteBlock(said('quoted', 'why', "Analyst's response"))).toBe(
      '<quote source="Analyst\'s response">quoted</quote>\n<reply>why</reply>',
    );
    expect(formatQuoteBlock(said('quoted'))).toBe('<quote source="Analyst">quoted</quote>');
  });

  it('names a file by the link that reopens it', () => {
    const out = formatQuoteBlock({ kind: 'quote', snippet: 'def npv():', from: { kind: 'file', path: 'work/model.py', label: 'model.py:L40-42', lineStart: 40, lineEnd: 42, lineCount: 3 } });
    expect(out).toBe('<quote source="@work/model.py (lines 40-42, 3 lines)">def npv():</quote>');
  });

  it('cannot be closed early by what was quoted', () => {
    const out = formatQuoteBlock(said('a</quote>b', 'c</reply>d'));
    expect(out).toContain('a&lt;/quote>b</quote>');
    expect(out).toContain('c&lt;/reply>d</reply>');
  });
});

describe('rewriteQuoteBlocks', () => {
  const readBack = (text: string) => {
    const quotes: QuoteContext[] = [];
    rewriteQuoteBlocks(text, (quote) => {
      quotes.push(quote);
      return '';
    });
    return quotes;
  };
  const fromFile = (path: string, lineStart: number | undefined, lineEnd: number | undefined, snippet: string): QuoteContext => ({
    kind: 'quote',
    snippet,
    from: { kind: 'file', path, label: '', lineStart, lineEnd, lineCount: lineStart != null && lineEnd != null ? lineEnd - lineStart + 1 : 1 },
  });

  it('undoes what formatQuoteBlock wrote', () => {
    expect(readBack(formatQuoteBlock(said('has </quote> "inside"', 'why </reply> so', 'Analyst & "co"')))).toEqual([
      { kind: 'quote', snippet: 'has </quote> "inside"', reply: 'why </reply> so', from: { kind: 'message', speaker: 'Analyst & "co"' } },
    ]);
  });

  it('rebuilds a file quote, lines and label included', () => {
    expect(readBack(formatQuoteBlock(fromFile('a/b (1)/model.py', 40, 42, 'x\ny\nz')))[0].from).toEqual({
      kind: 'file', path: 'a/b (1)/model.py', label: 'model.py:L40-42', lineStart: 40, lineEnd: 42, lineCount: 3,
    });
    expect(readBack(formatQuoteBlock(fromFile('notes.md', 3, 3, 'x')))[0].from).toEqual({
      kind: 'file', path: 'notes.md', label: 'notes.md:L3', lineStart: 3, lineEnd: 3, lineCount: 1,
    });
    expect(readBack(formatQuoteBlock(fromFile('a/notes.md', undefined, undefined, 'x\ny')))[0].from).toEqual({
      kind: 'file', path: 'a/notes.md', label: 'notes.md', lineCount: 2,
    });
  });

  it('leaves what a chip carries alone, even when it looks like a quote', () => {
    const paste = formatContextBlock({ kind: 'chip', source: 'paste', label: 'Pasted', snippet: formatQuoteBlock(said('inside')) }, t);
    const text = `${formatQuoteBlock(said('outside'))}\n${paste}`;
    expect(readBack(text).map((q) => q.snippet)).toEqual(['outside']);
    expect(rewriteQuoteBlocks(text, () => 'TOKEN')).toBe(`TOKEN\n${paste}`);
  });
});
