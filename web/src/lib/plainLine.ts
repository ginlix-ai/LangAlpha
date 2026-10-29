/**
 * Markdown as one line of plain text, for a preview that quotes it: the
 * minimap's hover card, a subagent row's title and result, a hand-off's brief.
 *
 * Only a matched pair is formatting. Research prose is full of literal `*`,
 * `_` and `~` (BRK_B, A * B, ~5%), and stripping them by character rewrites
 * the sentence the preview exists to quote.
 */

/** Quote, heading and list markers at the head of a line, nested or not. */
const BLOCK_MARK = /^[ \t]*(?:>[ \t]*)*(?:#{1,6}[ \t]+|[-*+][ \t]+|\d+[.)][ \t]+)?/gm;

/** Links to their text, images dropped, paired emphasis and code unwrapped. */
export function flattenInline(text: string): string {
  return text
    .replace(/!\[[^\]]*\]\([^)]*\)/g, '')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/\*\*(\S(?:[^*]*\S)?)\*\*/g, '$1')
    .replace(/__(\S(?:[^_]*\S)?)__/g, '$1')
    .replace(/~~([^~]+)~~/g, '$1')
    .replace(/`([^`]+)`/g, '$1')
    .replace(/(^|[\s(])([*_])(?=\S)([^*_]*?[^\s*_])\2(?=[\s).,;:!?]|$)/g, '$1$3');
}

/** The text on one line: block markers gone, a table's cells side by side. */
export function flattenMarkdown(markdown: string): string {
  return flattenInline(markdown.replace(BLOCK_MARK, ''))
    .replace(/\|/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function clip(text: string, max: number): string {
  return text.length > max ? text.slice(0, max).trimEnd() + '…' : text;
}
