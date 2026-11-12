import type { MessageRecord } from '../../hooks/utils/types';

/**
 * The bubbles of the turn `bubbleId` writes to, holding where each image
 * path they streamed was captured. A turn a reconnect split across bubbles
 * shares one run, so every assistant bubble under its count takes the map.
 */
export function pinCapturedImages(messages: MessageRecord[], bubbleId: string, images: Readonly<Record<string, string>>): MessageRecord[] {
  const bubble = messages.find((m) => m.id === bubbleId);
  if (!bubble) return messages;
  const turn = bubble.turnIndex;
  let changed = false;
  const next = messages.map((m) => {
    if (m.role !== 'assistant') return m;
    if (m.id !== bubbleId && (turn === undefined || m.turnIndex !== turn)) return m;
    changed = true;
    return { ...m, capturedImages: { ...(m.capturedImages as Record<string, string> | undefined), ...images } };
  });
  return changed ? next : messages;
}
