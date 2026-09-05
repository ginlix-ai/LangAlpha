import type { Attachment } from '@/types/sse';
import type { MessageRecord } from './messageList/types';
import { assistantText } from './messageList/messageText';

export interface TurnEntry {
  id: string;
  /** Empty when the send carried no text and no attachment names. */
  prompt: string;
  reply: string;
  /** The newest turn is in flight and has produced no reply text yet. */
  pending: boolean;
}

export const PROMPT_MAX = 140;
export const REPLY_MAX = 280;
// Raw markdown read per message before flattening. The card only ever shows
// the head, so the tail never earns its regex passes; the slack covers markup
// (fences, link targets) that is longer than the text it wraps.
const RAW_SLACK = 4;

// Flattened text per settled message. Settled messages keep their object
// identity across streamed chunks (the store maps untouched entries through),
// so a thread's history is flattened once, not once per token.
const flatCache = new WeakMap<object, string>();

function flatten(markdown: string): string {
  return markdown
    .replace(/!\[[^\]]*\]\([^)]*\)/g, '')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/^\s{0,3}(#{1,6}\s+|>\s?|[-*+]\s+|\d+\.\s+)/gm, '')
    // Only a matched pair is formatting. Research prose is full of literal
    // `*`, `_` and `~` (BRK_B, A * B, ~5%), and stripping them by character
    // rewrites the sentence the preview exists to quote.
    .replace(/\*\*([^*]+)\*\*/g, '$1')
    .replace(/__([^_]+)__/g, '$1')
    .replace(/~~([^~]+)~~/g, '$1')
    .replace(/`([^`]+)`/g, '$1')
    .replace(/(^|[\s(])([*_])(?=\S)([^*_]*?[^\s*_])\2(?=[\s).,;:!?]|$)/g, '$1$3')
    .replace(/\|/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/** One line of prose for the hover card. Fenced code is dropped in favour of the prose around it, unless the message is nothing but code, in which case the code text is the preview. */
export function plainText(markdown: unknown): string {
  if (typeof markdown !== 'string') return '';
  const prose = flatten(markdown.replace(/```[\s\S]*?(```|$)/g, ' '));
  return prose || flatten(markdown.replace(/```[^\n]*/g, ' '));
}

export function clip(text: string, max: number): string {
  return text.length > max ? text.slice(0, max).trimEnd() + '…' : text;
}

// `raw` is a producer, not a string: a settled message must cost nothing on a
// cache hit, and building the raw text at the call site would sort and join
// every prior message's segments once per streamed chunk.
function flatOf(message: MessageRecord, raw: () => string, max: number): string {
  const settled = !message.isStreaming;
  if (settled) {
    const hit = flatCache.get(message);
    if (hit !== undefined) return hit;
  }
  const flat = plainText(raw().slice(0, max * RAW_SLACK));
  if (settled) flatCache.set(message, flat);
  return flat;
}

function promptOf(message: MessageRecord): string {
  const text = flatOf(message, () => (typeof message.content === 'string' ? message.content : ''), PROMPT_MAX);
  if (text) return clip(text, PROMPT_MAX);
  // Two shapes reach here: the history `Attachment` (name) and the upload-time
  // `AttachmentMeta` a live send stores as-is (the name is on the File).
  const attachments = message.attachments as (Attachment & { file?: { name?: string } })[] | undefined;
  const names = (attachments ?? []).map((a) => a.name || a.file?.name || '').filter(Boolean);
  return clip(names.join(', '), PROMPT_MAX);
}

export function entriesEqual(prev: TurnEntry[], next: TurnEntry[]): boolean {
  return (
    next.length === prev.length &&
    next.every((e, i) => {
      const p = prev[i];
      return e.id === p.id && e.prompt === p.prompt && e.reply === p.reply && e.pending === p.pending;
    })
  );
}

/**
 * One entry per prompt-and-reply pair. Consecutive user bubbles with no
 * assistant between them are one entry: steering drains as a batch, so
 * everything queued during a single tool call is delivered together and
 * answered by one continuation. Splitting them would leave every prompt but
 * the last showing a card with nothing under it, permanently.
 *
 * A send that failed is the exception at every step. It gets its own tick in
 * transcript order, joins no batch and takes no follower, and never claims the
 * continuation that lands under it: the agent was never given it, so a reply
 * below it answers the last message that was actually delivered. It is never
 * pending either, since the turn still running is the one above it.
 */
const failedSend = (m: MessageRecord): boolean => !!(m as { queueError?: unknown }).queueError;

type Draft = { id: string; prompts: string[]; reply: string; answered: boolean; failed: boolean };

export function buildEntries(messages: MessageRecord[], turnInFlight: boolean): TurnEntry[] {
  const drafts: Draft[] = [];
  // The delivered entry replies attach to, which a failed send in between does
  // not displace.
  let open: Draft | null = null;

  for (const m of messages) {
    if (m.role === 'user') {
      if (failedSend(m)) {
        drafts.push({ id: String(m.id), prompts: [promptOf(m)], reply: '', answered: false, failed: true });
        continue;
      }
      // The batch keeps the first bubble's id, so its tick lands the reader at
      // the top of what they said rather than in the middle of it.
      if (open && !open.answered) {
        open.prompts.push(promptOf(m));
        continue;
      }
      open = { id: String(m.id), prompts: [promptOf(m)], reply: '', answered: false, failed: false };
      drafts.push(open);
    } else if (m.role === 'assistant' && open) {
      open.answered = true;
      open.reply += flatOf(m, () => assistantText(m), REPLY_MAX) + ' ';
    }
  }

  const entries = drafts.map((d) => ({
    id: d.id,
    prompt: clip(d.prompts.filter(Boolean).join(' · '), PROMPT_MAX),
    reply: clip(d.reply.trim(), REPLY_MAX),
    pending: false,
  }));
  for (let i = drafts.length - 1; turnInFlight && i >= 0; i--) {
    if (drafts[i].failed) continue;
    if (entries[i].reply === '') entries[i].pending = true;
    break;
  }
  return entries;
}
