// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { sameMention } from '../chat-input.helpers';
import { keepTokensWhole, quoteToken, spellOutQuotes, tokenAround } from '../chat-input.quoteTokens';
import type { QuoteMention } from '../chat-input.types';

describe('spellOutQuotes', () => {
  const quote = (snippet: string, token: string, reply?: string): QuoteMention => ({ kind: 'quote', from: { kind: 'message', speaker: 'Analyst' }, snippet, token, reply });

  it('puts each quote where its token sits among the typed words', () => {
    const draft = 'first\n“One?” yes\nthen “Two?”  ';
    expect(spellOutQuotes(draft, [quote('Two?', '“Two?”', 'no'), quote('One?', '“One?”')])).toBe(
      'first\n<quote source="Analyst">One?</quote> yes\nthen <quote source="Analyst">Two?</quote>\n<reply>no</reply>',
    );
  });

  it('takes neither a token nor a dollar pattern from inside a quote', () => {
    const out = spellOutQuotes('“A” “B”', [quote('cites “B” for $& and $1', '“A”'), quote('b', '“B”')]);
    expect(out).toBe('<quote source="Analyst">cites “B” for $& and $1</quote> <quote source="Analyst">b</quote>');
  });

  it('leaves a draft with no quotes exactly as typed', () => {
    expect(spellOutQuotes(' plain ', [])).toBe(' plain ');
  });
});

describe('quoteToken', () => {
  const words = (token: string) => token.replace(/[\u2002\u2003\u2060]/g, '').replace(/\u00A0/g, ' ');

  it('reads as where the quote starts, on one line', () => {
    expect(words(quoteToken('\nWhich ticker\nnext', []))).toBe('Which ticker');
    expect(words(quoteToken('Which ticker should we use for this whole model of ours?', []))).toBe('Which ticker should we use for this wh…');
    expect(words(quoteToken('一二三四五六七八九十一二三四五六七八九十一二三', []))).toBe('一二三四五六七八九十一二三四五六七八九…');
  });

  it('stays in one piece on a line', () => {
    expect(quoteToken('two words', [])).not.toMatch(/ /);
  });

  it('keeps two quotes that start alike apart, and one from being the start of another', () => {
    const first = quoteToken('Same', []);
    const second = quoteToken('Same', [first]);
    expect(words(second)).toBe('Same 2');
    expect(words(quoteToken('Same', [first, second]))).toBe('Same 3');
    expect(quoteToken('Same words', []).includes(first)).toBe(false);
  });
});

describe('tokenAround', () => {
  it('finds a token only with the caret strictly inside it', () => {
    const text = 'a “x” “x”';
    expect(tokenAround(text, 2, ['“x”'])).toBeNull();
    expect(tokenAround(text, 3, ['“x”'])).toEqual({ start: 2, end: 5 });
    expect(tokenAround(text, 5, ['“x”'])).toBeNull();
    expect(tokenAround(text, 7, ['“x”'])).toEqual({ start: 6, end: 9 });
  });
});

describe('spellOutQuotes, with a token cut short', () => {
  it('sends none of the marks a broken token left behind', () => {
    const token = quoteToken('Revenue grew', []);
    const broken = `before ${token.slice(0, 7)} after`;
    expect(spellOutQuotes(broken, [])).not.toMatch(/[\u2003\u2060\u2002]/);
  });

  it('leaves what is typed alone when there is no quote', () => {
    expect(spellOutQuotes('  indented\n', [])).toBe('  indented\n');
  });
});

describe('keepTokensWhole', () => {
  const token = quoteToken('Revenue grew', []);
  const other = quoteToken('Margins fell', []);
  const before = `a ${token} b`;

  it('takes the whole token when an edit cuts into it', () => {
    const at = before.indexOf(token) + 4;
    expect(keepTokensWhole(before, before.slice(0, at) + before.slice(at + 2), [token], at)).toEqual({ text: 'a  b', caret: 2 });
  });

  it('puts what lands inside a token after it', () => {
    const at = before.indexOf(token) + 4;
    expect(keepTokensWhole(before, `${before.slice(0, at)}x${before.slice(at)}`, [token], at + 1)).toEqual({ text: `a ${token}x b`, caret: 2 + token.length + 1 });
  });

  it('has nothing to say about an edit beside it, or one that removes it whole', () => {
    expect(keepTokensWhole(before, `${before}!`, [token], before.length + 1)).toBeNull();
    expect(keepTokensWhole(before, 'a  b', [token], 2)).toBeNull();
  });

  it('leaves the next token alone when one that starts alike goes', () => {
    expect(keepTokensWhole(`${token} ${other}`, other, [token, other], 0)).toBeNull();
  });

  it('leaves the next token alone when another is put before it', () => {
    expect(keepTokensWhole(`${other}foo`, `${token}${other}foo`, [other], token.length)).toBeNull();
  });

  it('takes a token pasted over another as itself', () => {
    const after = `bar${other}`;
    expect(keepTokensWhole(`bar${token}`, after, [token, other], after.length)).toEqual({ text: after, caret: after.length });
  });
});

describe('sameMention on a file quote', () => {
  const from = { kind: 'file', path: 'a.md', label: 'a.md', lineCount: 1 } as const;

  it('takes a range that was never known, however it reads back, as itself', () => {
    expect(sameMention({ kind: 'quote', snippet: 'x', from: { ...from, lineStart: null, lineEnd: null } }, { kind: 'quote', snippet: 'x', from })).toBe(true);
  });

  it('tells the same words on another line apart', () => {
    expect(sameMention({ kind: 'quote', snippet: 'x', from: { ...from, lineStart: 1, lineEnd: 1 } }, { kind: 'quote', snippet: 'x', from: { ...from, lineStart: 9, lineEnd: 9 } })).toBe(false);
  });
});

describe('sameMention on a quote from a message', () => {
  const quote = (message?: string) => ({ kind: 'quote', snippet: 'x', from: { kind: 'message', speaker: 'Analyst', message } }) as const;

  it('tells the same words in two replies apart', () => {
    expect(sameMention(quote('m1'), quote('m2'))).toBe(false);
    expect(sameMention(quote('m1'), quote('m1'))).toBe(true);
  });

  it('takes one read back from what was sent as the one it was', () => {
    expect(sameMention(quote('m1'), quote())).toBe(true);
  });
});
