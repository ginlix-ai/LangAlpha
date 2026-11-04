// @vitest-environment node
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import type { ChipContext } from '@/components/ui/chat-input.types';
import { contextSource, formatContextBlock, formatQuoteBlock, parseSentMessage } from '@/components/ui/chat-input.wire';
import { markDraftTokens, sentMessageForCopy, sentMessageWords } from '../sentMessage';

const t = ((key: string) => key) as unknown as TFunction;
const said = (snippet: string, reply?: string, speaker = 'Analyst') => formatQuoteBlock({ kind: 'quote', snippet, reply, from: { kind: 'message', speaker } });
const chartChip: ChipContext = { kind: 'chip', source: 'chart', label: 'AAPL 1D', snippet: 'candles' };

describe('markDraftTokens in code', () => {
  it.each(['~~~sh\ncat @notes.md\n~~~', '````\ncat @notes.md\n````', 'run ``cat @notes.md`` now'])('leaves %j alone', (text) => {
    expect(markDraftTokens(text)).toBe(text);
  });
});

describe('sentMessageForCopy', () => {
  it('leaves a message with nothing quoted exactly as it was', () => {
    expect(sentMessageForCopy('  plain <b>words</b>\n')).toBe('  plain <b>words</b>\n');
  });

  it('copies a quote as a markdown quote, with no tags and its reply after it', () => {
    const quote = said('one\ntwo', 'why?');
    expect(sentMessageForCopy(`first ${quote} last`)).toBe('first\n\n> one\n> two\n\nwhy?\n\nlast');
  });
});

describe('parseSentMessage', () => {
  it('reads back every block the composer writes, in composed order', () => {
    const quote = said('has </quote> inside', 'why </reply> so', 'Analyst\'s "reply"');
    const file = formatQuoteBlock({ kind: 'quote', snippet: 'line', from: { kind: 'file', path: 'a/notes.md', label: 'notes.md:L3', lineStart: 3, lineEnd: 3, lineCount: 1 } });
    const paste = formatContextBlock({ kind: 'chip', source: 'paste', label: 'Pasted', snippet: 'has ``` inside' }, t);
    const chart = formatContextBlock(chartChip, t);
    expect(parseSentMessage(`${quote}\n${file}\nmy words\n${paste}\n${chart}`)).toEqual([
      { kind: 'context', body: 'has </quote> inside', reply: 'why </reply> so' },
      { kind: 'break' },
      { kind: 'context', source: 'a/notes.md', body: 'line' },
      { kind: 'break' },
      { kind: 'text', text: 'my words' },
      { kind: 'break' },
      { kind: 'context', body: 'has ``` inside' },
      { kind: 'break' },
      { kind: 'context', source: 'context.chartInPanel: AAPL 1D', body: 'candles', chart: true },
    ]);
  });

  it('keeps each reply with its own quote, and typed words where they were typed', () => {
    expect(parseSentMessage(`three calls:\n${said('First?', 'yes')} ${said('Second?')} and\nkeep it short\n${said('Third?', 'no')} rerun it`)).toEqual([
      { kind: 'text', text: 'three calls:' },
      { kind: 'break' },
      { kind: 'context', body: 'First?', reply: 'yes' },
      { kind: 'context', body: 'Second?' },
      { kind: 'text', text: 'and\nkeep it short' },
      { kind: 'break' },
      { kind: 'context', body: 'Third?', reply: 'no' },
      { kind: 'text', text: 'rerun it' },
    ]);
  });

  it('reads a quote sent before quotes had tags', () => {
    expect(parseSentMessage('look\n\n<details>\n<summary>[From agent response]</summary>\n\n```\nold\n```\n</details>')).toEqual([
      { kind: 'text', text: 'look' },
      { kind: 'break' },
      { kind: 'context', body: 'old' },
    ]);
  });

  it('leaves a message with no block as the words it is', () => {
    expect(parseSentMessage('use <details> here')).toEqual([{ kind: 'text', text: 'use <details> here' }]);
  });
});

describe('sentMessageWords', () => {
  it('reads what the user said, and no tags', () => {
    expect(sentMessageWords(`first ${said('One?', 'yes')} then\n${said('Two?')}`)).toBe('first yes then');
  });

  it('falls back to what was quoted, then to a chart by name', () => {
    expect(sentMessageWords(said('One?'))).toBe('One?');
    expect(sentMessageWords(formatContextBlock({ ...chartChip, snippet: 'hint' }, t))).toBe('context.chartInPanel: AAPL 1D');
    expect(sentMessageWords('plain')).toBe('plain');
  });
});

describe('contextSource', () => {
  it('names where a block came from', () => {
    expect(contextSource('[Pasted text]')).toBe('Pasted text');
    expect(contextSource('@a/notes.md (lines 3-3, 1 line)')).toBe('a/notes.md');
  });
});

describe('markDraftTokens', () => {
  it('marks a command only at the start, and a file anywhere outside code', () => {
    expect(markDraftTokens('/dcf see @a/b.md and /tmp')).toBe(
      '<span class="draft-token">/dcf</span> see <span class="draft-token">@a/b.md</span> and /tmp',
    );
    expect(markDraftTokens('run `@x` and a@b.com')).toBe('run `@x` and a@b.com');
  });
});

describe('markDraftTokens in an indented code block', () => {
  it('leaves a path there as written, and marks the one beside it', () => {
    const marked = markDraftTokens('see @a.md\n\n    @b.md\n');
    expect(marked).toContain('    @b.md');
    expect(marked).not.toMatch(/^see @a\.md/);
  });
});
