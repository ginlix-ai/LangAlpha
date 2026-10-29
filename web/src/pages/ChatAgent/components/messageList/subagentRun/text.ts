/**
 * One-line text for a subagent row, cut from text written for something else:
 * a task description written for the subagent, closing prose written for the
 * main agent, a refusal written for the model.
 */
import { clip, flattenMarkdown } from '@/lib/plainLine';

/** A plain line cut at a word, without the colon that led into what followed. */
function cutLine(line: string, maxLen: number): string {
  const cleaned = line.replace(/:$/, '');
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.slice(0, maxLen).replace(/\s+\S*$/, '') + '…';
}

/** The first line of a task description, as plain text trimmed to a title. */
export function summarize(text: string | undefined, maxLen = 100): string {
  if (!text || typeof text !== 'string') return '';
  return cutLine(flattenMarkdown(text.split(/\n/)[0]), maxLen);
}

// Neither a stop reason nor a launch reply is bounded: the credit denial is
// the platform's copy and the rest are exception text.
const REASON_MAX_CHARS = 180;

export function clampReason(text: string): string {
  return clip(text, REASON_MAX_CHARS);
}

/** Markdown that frames a line rather than saying anything: headings, rules,
 *  a fence, and table rows, which cut to one line read as a header with no
 *  table under it. */
const FRAME_ONLY = /^(#{1,6}\s|[-*_]{3,}$|\||```)/;

/**
 * The first line of a subagent's closing prose that says something, as plain
 * text. A heading is skipped rather than shown, since "Summary" tells the
 * reader nothing the row's state has not already.
 */
export function resultLine(text: string | undefined): string {
  if (!text) return '';
  for (const raw of text.split(/\n/)) {
    const line = raw.trim();
    if (!line || FRAME_ONLY.test(line)) continue;
    const plain = flattenMarkdown(line);
    if (plain) return cutLine(plain, 140);
  }
  return '';
}

/** "could not start Task-3 - <why>", which the launch tools put before the
 *  reason; the dash is an em dash, a double hyphen or a hyphen. */
const COULD_NOT_START = /^could not (start|resume) \S+\s+(\u2014|--|-)\s+/i;

/**
 * Why a launch was refused, from a reply written for the model. Its envelope
 * ("Refused: ", "Error: ", "could not start Task-3: ") repeats what the row
 * already says, and every sentence after the first tells the model what to do
 * next, so only the first sentence is the reason.
 */
export function refusalReason(reply: string | undefined): string {
  if (!reply) return '';
  const line = reply
    .trim()
    .replace(/^(refused|error)\s*:\s*/i, '')
    .split(/\n/)[0]
    .trim()
    .replace(COULD_NOT_START, '');
  return clampReason(line.match(/^.*?[.!?](?=\s+[A-Z])/)?.[0] ?? line);
}
