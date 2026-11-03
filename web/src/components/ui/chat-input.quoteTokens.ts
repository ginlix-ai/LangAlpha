import { sameMention } from './chat-input.helpers';
import type { MentionedFile, QuoteContext, QuoteMention } from './chat-input.types';
import { formatQuoteBlock, rewriteQuoteBlocks } from './chat-input.wire';

/** What a quote's chip reads: where the quote starts, on one line. */
export function quotePreview(snippet: string): string {
  const firstLine = snippet.split('\n').find((line) => line.trim()) ?? '';
  return firstLine.trim().replace(/\s+/g, ' ');
}

/** About what a sent chip shows before it clips. A wide character counts for two. */
const QUOTE_TOKEN_WIDTH = 38;
const WIDE_CHAR = /[\u2E80-\uA4CF\uAC00-\uD7A3\uF900-\uFAFF\uFE30-\uFE4F\uFF00-\uFF60\uFFE0-\uFFE6]/;

/** Holds the line against a break, and takes no room of its own. */
const JOIN = '\u2060';
/** Blank room in the draft for the icon the layer draws there, and the gap after it. */
const QUOTE_TOKEN_LEAD = `\u2003${JOIN}\u2002${JOIN}`;

/**
 * What stands for a quote in the draft, which reads as its chip will once sent:
 * room for the icon, then where the quote starts, kept to one piece as a chip
 * is. The closing joiner is what stops one token being the start of another, and
 * two quotes that start alike are numbered, since a token is how the send finds
 * its quote.
 */
export function quoteToken(snippet: string, taken: string[]): string {
  let width = 0;
  let words = '';
  for (const char of quotePreview(snippet).replaceAll(JOIN, '')) {
    width += WIDE_CHAR.test(char) ? 2 : 1;
    if (width > QUOTE_TOKEN_WIDTH) {
      words += '…';
      break;
    }
    words += char;
  }
  words = words.replace(/ /g, '\u00A0');
  let token = `${QUOTE_TOKEN_LEAD}${words}${JOIN}`;
  for (let n = 2; taken.includes(token); n++) token = `${QUOTE_TOKEN_LEAD}${words}\u00A0${n}${JOIN}`;
  return token;
}

export const tokensOf = (mentions: MentionedFile[]) => mentions.flatMap((f) => (f.kind === 'quote' ? [f.token] : []));

/** A quote lasts as long as its token: a host can replace the draft wholesale. */
export const heldInDraft = <M extends MentionedFile>(mentions: M[], draft: string): M[] =>
  mentions.filter((f) => f.kind !== 'quote' || draft.includes(f.token));

/** The token the caret is strictly inside, whose text a keystroke there would break. */
export function tokenAround(text: string, caret: number, tokens: string[]): { start: number; end: number } | null {
  for (const token of tokens) {
    for (let start = text.indexOf(token); start >= 0 && start < caret; start = text.indexOf(token, start + 1)) {
      if (caret < start + token.length) return { start, end: start + token.length };
    }
  }
  return null;
}

export const escapeToken = (token: string) => token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * The draft as it goes out, each quote where its token sits. One pass, because a
 * quote's text may hold the words of another's token. A token's marks that stand
 * for no quote, pasted in from elsewhere, are nothing for the agent to read.
 */
export function spellOutQuotes(draft: string, quotes: QuoteMention[]): string {
  const pattern = new RegExp([...quotes.map((f) => escapeToken(f.token)), QUOTE_TOKEN_LEAD, JOIN].join('|'), 'g');
  const spelled = draft.replace(pattern, (piece) => {
    const quote = quotes.find((f) => f.token === piece);
    return quote ? formatQuoteBlock(quote) : '';
  });
  return quotes.length > 0 ? spelled.trim() : spelled;
}

/**
 * An edit no key handler saw (Android's Backspace, a spellcheck, a drop) that cut
 * into a token takes all of it, or lands beside it if it only added text. Null
 * when no token was touched in part.
 */
export function keepTokensWhole(before: string, after: string, tokens: string[], caret: number): { text: string; caret: number } | null {
  // Read from where the edit ended, since every token starts with the same lead:
  // told only the two texts, a diff runs on from one token into the next.
  let suffix = after.length - caret;
  if (!before.endsWith(after.slice(caret))) {
    suffix = 0;
    const most = Math.min(before.length, after.length);
    while (suffix < most && before[before.length - 1 - suffix] === after[after.length - 1 - suffix]) suffix += 1;
  }
  const shortest = Math.min(before.length, after.length) - suffix;
  let prefix = 0;
  while (prefix < shortest && before[prefix] === after[prefix]) prefix += 1;
  let start = prefix;
  let end = before.length - suffix;
  const added = after.slice(prefix, after.length - suffix);
  let touched = false;
  for (const token of tokens) {
    for (let at = before.indexOf(token); at !== -1; at = before.indexOf(token, at + 1)) {
      const tokenEnd = at + token.length;
      const whole = prefix <= at && tokenEnd <= before.length - suffix;
      const cuts = prefix < tokenEnd && before.length - suffix > at;
      const inside = prefix === before.length - suffix && at < prefix && prefix < tokenEnd;
      if (inside) {
        start = tokenEnd;
        end = tokenEnd;
        touched = true;
      } else if (cuts && !whole) {
        start = Math.min(start, at);
        end = Math.max(end, tokenEnd);
        touched = true;
      }
    }
  }
  if (!touched) return null;
  // Having the same lead, a token pasted over another reads as the end of it.
  const pasted = after.slice(start, after.length - suffix);
  const put = start < prefix && pasted.length > QUOTE_TOKEN_LEAD.length && pasted.startsWith(QUOTE_TOKEN_LEAD) && pasted.endsWith(JOIN) ? pasted : added;
  return { text: before.slice(0, start) + put + before.slice(end), caret: start + put.length };
}

/**
 * What comes back to a draft that holds `held`. A token is how the send finds its
 * quote, so a returning quote stands on the token the draft has for the same words
 * and reply, and takes new words when its own now stand for something else. A
 * message returns spelled out, so its quote blocks read back into tokens too.
 */
export function readGivenBack(
  text: string, mentions: MentionedFile[], held: MentionedFile[],
): { text: string; mentions: MentionedFile[] } {
  const known = held.filter((f) => f.kind === 'quote');
  const taken = [...tokensOf(held), ...tokensOf(mentions)];
  const renamed = new Map<string, string>();
  const fresh: MentionedFile[] = [];
  const standsFor = (quote: QuoteContext) => known.find((f) => sameMention(f, quote) && f.reply === quote.reply);

  for (const m of mentions) {
    if (m.kind !== 'quote') {
      if (!held.some((p) => sameMention(p, m))) fresh.push(m);
      continue;
    }
    const same = standsFor(m);
    if (same) {
      renamed.set(m.token, same.token);
      continue;
    }
    const mention = known.some((f) => f.token === m.token) ? { ...m, token: quoteToken(m.snippet, taken) } : m;
    renamed.set(m.token, mention.token);
    taken.push(mention.token);
    known.push(mention);
    fresh.push(mention);
  }

  // One pass, because one token's new words may be another's old ones.
  const swapped = renamed.size === 0 ? text : text.replace(
    new RegExp([...renamed.keys()].map(escapeToken).join('|'), 'g'),
    (token) => renamed.get(token) ?? token,
  );
  const tokened = rewriteQuoteBlocks(swapped, (quote) => {
    const same = standsFor(quote);
    if (same) return same.token;
    const mention = { ...quote, token: quoteToken(quote.snippet, taken) };
    taken.push(mention.token);
    known.push(mention);
    fresh.push(mention);
    return mention.token;
  });
  return { text: tokened, mentions: fresh };
}
