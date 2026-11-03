import type { TFunction } from 'i18next';
import { describeLocatorSize } from '@/pages/ChatAgent/utils/a1';
import { mapOutsideCode } from '@/pages/ChatAgent/utils/markdownSegments';
import type { ChipContext, QuoteContext, QuoteOrigin } from './chat-input.types';

/*
 * The text a message carries its context in, written on send and read back
 * wherever that message comes back, so the two directions live side by side.
 */

/** One more backtick than the longest run inside, so the body cannot close the fence. */
function fence(body: string): string {
  const longest = Math.max(2, ...Array.from(body.matchAll(/`+/g), (m) => m[0].length));
  const ticks = '`'.repeat(longest + 1);
  return `${ticks}\n${body}\n${ticks}`;
}

/** A sheet name or a path may carry `<` or `>` and a chart label a newline;
 *  neither survives inside `<summary>` as the text it was. */
const summaryText = (text: string) => text.replace(/[<>]/g, '').replace(/[\r\n]+/g, ' ');

/** Only the tag's own closer needs guarding, so the rest reaches the agent as it was selected. */
const guardCloser = (text: string, tag: string) => text
  // What already reads as a guarded closer gets an `&amp;`, so that reading it back can tell the two apart.
  .replace(new RegExp(`&((?:amp;)*lt;/\\s*${tag})`, 'gi'), '&amp;$1')
  .replace(new RegExp(`</(\\s*${tag})`, 'gi'), '&lt;/$1');
const unguard = (text: string, tag: string) => text
  .replace(new RegExp(`&lt;/(\\s*${tag})`, 'gi'), '</$1')
  .replace(new RegExp(`&amp;((?:amp;)*lt;/\\s*${tag})`, 'gi'), '&$1');
const attribute = (text: string) => text.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/[\r\n]+/g, ' ');
const fromAttribute = (text: string) => text.replace(/&quot;/g, '"').replace(/&#64;/g, '@').replace(/&lt;/g, '<').replace(/&amp;/g, '&');

/** A leading `@` is how a file reads, so a speaker's name is kept from starting with one. */
function quoteSource(from: QuoteOrigin): string {
  if (from.kind === 'message') return attribute(from.speaker).replace(/^@/, '&#64;');
  const lines = from.lineStart != null
    ? ` (lines ${from.lineStart}-${from.lineEnd}, ${from.lineCount} line${from.lineCount !== 1 ? 's' : ''})`
    : '';
  return attribute(`@${from.path}${lines}`);
}

const isFileSource = (rawSource: string) => rawSource.startsWith('@');

/** `quoteSource` read back. Only a file's name is left to label it by. */
function quoteOrigin(rawSource: string, snippet: string): QuoteOrigin {
  const source = fromAttribute(rawSource);
  if (!isFileSource(rawSource)) return { kind: 'message', speaker: source };
  const lines = /^(.*) \(lines (\d+)-(\d+), (\d+) lines?\)$/s.exec(source.slice(1));
  const path = lines ? lines[1] : source.slice(1);
  const name = path.slice(path.lastIndexOf('/') + 1);
  if (!lines) return { kind: 'file', path, label: name, lineCount: snippet.split('\n').length };
  const [lineStart, lineEnd, lineCount] = lines.slice(2).map(Number);
  const label = lineEnd !== lineStart ? `${name}:L${lineStart}-${lineEnd}` : `${name}:L${lineStart}`;
  return { kind: 'file', path, label, lineStart, lineEnd, lineCount };
}

/**
 * A quote as the agent reads it, and as the sent message reads it back
 * (`parseSentMessage`). The tags separate what was quoted and what the user said
 * about it from what they typed directly around it, which goes untagged, and a
 * reply always follows its quote, which is how the two are paired.
 */
export function formatQuoteBlock({ snippet, reply, from }: QuoteContext): string {
  const quote = `<quote source="${quoteSource(from)}">${guardCloser(snippet, 'quote')}</quote>`;
  return reply ? `${quote}\n<reply>${guardCloser(reply, 'reply')}</reply>` : quote;
}

/**
 * The `<details>` block a chip appends to the message on send. The summary is
 * what the agent reads first, so each source writes the one that tells it how to
 * treat the body: a label for pasted text, a hint for a live chart, and a
 * reopenable `@path#locator` for a sheet range.
 */
export function formatContextBlock(chip: ChipContext, t: TFunction): string {
  switch (chip.source) {
    case 'paste':
      return `\n<details>\n<summary>[${t('context.pastedText')}]</summary>\n\n${fence(chip.snippet)}\n</details>`;
    // A chart is a live view: the hint says what is on screen and nothing
    // more, so the agent fetches the bars itself rather than reading a copy.
    case 'chart':
      return `\n<details>\n<summary>[${t('context.chartInPanel')}: ${summaryText(chip.label)}]</summary>\n\n${chip.snippet}\n</details>`;
    // A locator names the spot in the file's own vocabulary (`Model!B4:D9`),
    // so the summary is the link that reopens it. The block below it may be
    // capped, and says so in its own first line.
    case 'range': {
      const described = describeLocatorSize(chip.locator);
      const size = described ? ` (${described})` : '';
      return `\n<details>\n<summary>@${summaryText(`${chip.path}#${chip.locator}`)}${size}</summary>\n\n${fence(chip.snippet)}\n</details>`;
    }
  }
}

/*
 * What the composer writes: a quote with its reply, or the `<details>` of a chip,
 * which is also how a quote went out before quotes had tags. A summary is a
 * `[label]` or an `@path`; any other `<details>` is what the person typed. A fence
 * is matched by its own length, since the body may hold shorter runs of backticks;
 * a chart's hint alone has none.
 */
const BLOCK_OPEN = '<quote source="([^"]*)">|\\n?<details>\\n<summary>(\\[[^\\n]*\\]|@[^\\n]*)</summary>\\n\\n';
const QUOTE_CLOSE = '</quote>';
const REPLY_OPEN = /\s*<reply>/y;
const REPLY_CLOSE = '</reply>';
const CHIP_CLOSE = '\n</details>';
const CHIP_FENCE = /(`{3,})\n/y;
/** Well past any message a person sends; what is past it just skips the check. */
const MAX_CODE_CHECKS = 100;
const MAX_CODE_LOOKAHEAD = 4096;
const MARK = '';

type Block = { index: number; end: number } & (
  | { kind: 'quote'; rawSource: string; snippet: string; reply?: string }
  | { kind: 'chip'; summary: string; body: string; fenced: boolean }
);

/**
 * The blocks of `content`, found by searching for each closer: a pattern spanning
 * opener to closer rescans the rest of the message for every opener left unclosed.
 * Once a closer is not there it is not there for any later opener either.
 */
function* allBlocks(content: string): Generator<Block> {
  const open = new RegExp(BLOCK_OPEN, 'g');
  let quotesClose = true;
  let repliesClose = true;
  let chipsClose = true;
  for (let match = open.exec(content); match; match = open.exec(content)) {
    const start = open.lastIndex;
    let block: Block | null = null;
    if (match[1] != null) {
      const close: number = quotesClose ? content.indexOf(QUOTE_CLOSE, start) : -1;
      quotesClose = close !== -1;
      if (quotesClose) {
        let end = close + QUOTE_CLOSE.length;
        let reply: string | undefined;
        REPLY_OPEN.lastIndex = end;
        if (repliesClose && REPLY_OPEN.test(content)) {
          const replyStart = REPLY_OPEN.lastIndex;
          const replyClose = content.indexOf(REPLY_CLOSE, replyStart);
          repliesClose = replyClose !== -1;
          if (repliesClose) {
            reply = unguard(content.slice(replyStart, replyClose), 'reply') || undefined;
            end = replyClose + REPLY_CLOSE.length;
          }
        }
        block = { kind: 'quote', index: match.index, end, rawSource: match[1], snippet: unguard(content.slice(start, close), 'quote'), reply };
      }
    } else if (chipsClose) {
      const close = content.indexOf(CHIP_CLOSE, start);
      chipsClose = close !== -1;
      if (chipsClose) {
        CHIP_FENCE.lastIndex = start;
        const fence = CHIP_FENCE.exec(content);
        const fenceClose = fence ? content.indexOf(`\n${fence[1]}${CHIP_CLOSE}`, CHIP_FENCE.lastIndex) : -1;
        const summary = match[2];
        block = fence && fenceClose !== -1
          ? { kind: 'chip', index: match.index, end: fenceClose + fence[1].length + 1 + CHIP_CLOSE.length, summary, body: content.slice(CHIP_FENCE.lastIndex, fenceClose), fenced: true }
          : { kind: 'chip', index: match.index, end: close + CHIP_CLOSE.length, summary, body: content.slice(start, close), fenced: false };
      }
    }
    if (block) {
      yield block;
      open.lastIndex = block.end;
    }
  }
}

/** On the same line, a later quote's body may hold the backtick that would seem to close typed code. */
const LATER_QUOTE = /<quote source="[^"]*">.*?<\/quote>(?:\s*<reply>.*?<\/reply>)?/g;

/**
 * The blocks of `content`, leaving what a person typed as an example inside code.
 * Only the typed text can open code: a quote's own body may hold any backticks.
 */
function* blocksOutsideCode(content: string): Generator<Block> {
  let typed = '';
  let at = 0;
  let checks = 0;
  for (const block of allBlocks(content)) {
    const before = typed + content.slice(at, block.index);
    if (checks < MAX_CODE_CHECKS && /[`~]/.test(before)) {
      checks += 1;
      const restOfLine = content.slice(block.end, block.end + MAX_CODE_LOOKAHEAD).split('\n', 1)[0].replace(LATER_QUOTE, '');
      if (mapOutsideCode(before + MARK + restOfLine, (prose) => prose.replace(MARK, '')).includes(MARK)) continue;
    }
    typed = before;
    at = block.end;
    yield block;
  }
}

/** `text` with each quote in it rewritten. Another block's body may hold what
 *  looks like a quote, which stays the text it is. */
export function rewriteQuoteBlocks(text: string, rewrite: (quote: QuoteContext) => string): string {
  let rewritten = '';
  let at = 0;
  for (const block of blocksOutsideCode(text)) {
    if (block.kind !== 'quote') continue;
    rewritten += text.slice(at, block.index) + rewrite({ kind: 'quote', snippet: block.snippet, reply: block.reply, from: quoteOrigin(block.rawSource, block.snippet) });
    at = block.end;
  }
  return rewritten + text.slice(at);
}

export type SentPart =
  | { kind: 'text'; text: string }
  | { kind: 'context'; source?: string; body: string; reply?: string; chart?: boolean }
  | { kind: 'break' };

/** `[From agent response]` and `@notes.md (lines 3-3, 1 line)` as a reader wants them. */
export function contextSource(summary: string): string {
  return summary.replace(/^\[|\]$/g, '').replace(/^@/, '').replace(/\s\(lines? .*\)$/, '');
}

/** Only a file is worth naming beside its text; the rest came from this chat. */
const fileSource = (summary: string) => (summary.startsWith('@') ? contextSource(summary) : undefined);

/** A sent message read back, in the order it was composed. */
export function parseSentMessage(content: string): SentPart[] {
  const parts: SentPart[] = [];
  const pushBreak = () => {
    if (parts.length > 0 && parts.at(-1)!.kind !== 'break') parts.push({ kind: 'break' });
  };
  // A line break the user typed beside a chip is where the next row starts.
  const pushText = (raw: string) => {
    if (!raw.trim()) {
      if (raw.includes('\n')) pushBreak();
      return;
    }
    const startsLine = /^\s*\n/.test(raw);
    if (startsLine) pushBreak();
    // Beside a chip its gap goes. On a line of its own its indent is its meaning.
    const beside = !startsLine && parts.length > 0;
    const text = (beside ? raw.trimStart() : raw.replace(/^(?:[ \t]*\r?\n)+/, '')).trimEnd();
    parts.push({ kind: 'text', text });
    if (/\n\s*$/.test(raw)) pushBreak();
  };
  let at = 0;
  for (const block of blocksOutsideCode(content)) {
    pushText(content.slice(at, block.index));
    if (block.kind === 'quote') {
      parts.push({ kind: 'context', source: isFileSource(block.rawSource) ? contextSource(fromAttribute(block.rawSource)) : undefined, body: block.snippet, reply: block.reply });
    } else if (block.fenced) {
      parts.push({ kind: 'context', source: fileSource(block.summary), body: block.body });
    } else {
      parts.push({ kind: 'context', source: contextSource(block.summary), body: block.body, chart: true });
    }
    at = block.end;
  }
  pushText(content.slice(at));
  if (parts.at(-1)?.kind === 'break') parts.pop();
  return parts;
}
