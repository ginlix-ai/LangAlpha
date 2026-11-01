import type { ArtifactCommentsShown } from '@/types/api';
import type { ContentSegmentRecord } from './types';
import { inChunkOrder } from '../../session/stream/textChunks';

/** The reply's prose in transcript order. Segments can land out of order, so they are sorted by `order` the way the bubble renders them; a message with no text segments falls back to its flat `content`. Shared by copy, the minimap preview and the deliverables deck so all three read the same text. `max` stops reading once that much text is in hand, for a caller that shows only the head of a reply still streaming. */
export function assistantText(message: { contentSegments?: unknown; content?: unknown }, max = Infinity): string {
  const segments = message.contentSegments as ContentSegmentRecord[] | undefined;
  let text = '';
  if (segments) {
    for (const s of inChunkOrder(segments)) {
      if (text.length >= max) break;
      if (s.type === 'text') text += (s.content ?? '').slice(0, max - text.length);
    }
  }
  return text || (typeof message.content === 'string' ? message.content.slice(0, max) : '');
}

/** A user message's text as its bubble shows it: its words, then a line for each file comment it sent. For Copy and the minimap preview. */
export function userMessageText(message: { content?: unknown; artifactComments?: unknown }): string {
  const words = typeof message.content === 'string' ? message.content : '';
  const shown = message.artifactComments as ArtifactCommentsShown | undefined;
  if (!shown) return words;
  const lines = shown.comments.map((c) => {
    const on = [c.label, c.quote ? `"${c.quote}"` : null].filter(Boolean).join(' ');
    return `${c.number}. ${on ? `${on}: ` : ''}${c.text}`;
  });
  return [words.trim(), ...lines].filter(Boolean).join('\n');
}
