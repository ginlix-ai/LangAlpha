import { parseSentMessage } from '@/components/ui/chat-input.wire';
import { mapOutsideCode, splitLines } from '../../utils/markdownSegments';

/** A sent message as a line of words: what the user said, else what they quoted. */
export function sentMessageWords(content: string): string {
  const parts = parseSentMessage(content);
  const said = parts.flatMap((part) => {
    if (part.kind === 'text') return [part.text];
    return part.kind === 'context' && part.reply ? [part.reply] : [];
  });
  if (said.length > 0) return said.join(' ');
  const first = parts.find((part) => part.kind === 'context');
  if (first?.kind !== 'context') return content;
  return first.chart ? first.source ?? '' : first.body;
}

/** A sent message for the clipboard: its words as typed, what it quoted as markdown quotes. */
export function sentMessageForCopy(content: string): string {
  const parts = parseSentMessage(content);
  if (!parts.some((part) => part.kind === 'context')) return content;
  return parts.flatMap((part) => {
    if (part.kind === 'break') return [];
    if (part.kind === 'text') return [part.text];
    if (part.chart) return [part.source ?? ''];
    const quoted = part.body.split('\n').map((line) => `> ${line}`).join('\n');
    return [part.reply ? `${quoted}\n\n${part.reply}` : quoted];
  }).join('\n\n');
}

const SKILL_TOKENS = /^(?:\/[\w.-]+(?=\s|$)\s*)+/;
const FILE_TOKEN = /(^|\s)(@[\w./#:!-]*[\w/-])/g;

/** Four spaces or a tab begins a code block, in which no markup is markup. */
const INDENTED = /^(?: {4}|\t)/;

const mark = (token: string) => `<span class="draft-token">${token}</span>`;

/**
 * Tints `/skill` and `@file` as the draft did. A slash counts only where a
 * command goes, at the very start: anywhere else it is as likely to be a path.
 * Nothing in a code span or fence is touched.
 */
export function markDraftTokens(text: string): string {
  const skills = SKILL_TOKENS.exec(text)?.[0] ?? '';
  const rest = mapOutsideCode(text.slice(skills.length), (prose) => splitLines(prose)
    .map((line) => (INDENTED.test(line) ? line : line.replace(FILE_TOKEN, (_, lead: string, token: string) => `${lead}${mark(token)}`)))
    .join(''));
  return skills.replace(/\/[\w.-]+/g, mark) + rest;
}
