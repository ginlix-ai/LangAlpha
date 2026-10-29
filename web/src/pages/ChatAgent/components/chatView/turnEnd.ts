import { useEffect, useEffectEvent, useRef } from 'react';
import { findMessageElement } from '../../utils/scrollDom';
import type { ChatMessage } from '@/types/chat';

function lastAssistant(messages: ReadonlyArray<ChatMessage>): ChatMessage | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === 'assistant') return messages[i];
  }
  return null;
}

/**
 * Calls `onEnd` when a turn completes: `isStreaming` true -> false, once per
 * turn (an interrupt keeps it true, the caller decides). Both chat surfaces
 * land the "When a reply finishes" preference through this, each with its own
 * scroll engine.
 */
export function useTurnEnd(messages: ReadonlyArray<ChatMessage>, isStreaming: boolean, onEnd: () => void): void {
  const wasStreamingRef = useRef(isStreaming);
  // The last assistant bubble of the last idle render. A turn that produced
  // nothing (a regenerate or edit whose checkpoint fetch failed and put the
  // old transcript back) ends with that same bubble, and nothing has finished.
  const idleTailRef = useRef<ChatMessage | null | undefined>(undefined);
  const end = useEffectEvent(onEnd);
  useEffect(() => {
    const wasStreaming = wasStreamingRef.current;
    wasStreamingRef.current = isStreaming;
    const tail = lastAssistant(messages);
    const idleTail = idleTailRef.current;
    if (!isStreaming) idleTailRef.current = tail;
    if (!wasStreaming || isStreaming) return;
    if (idleTail !== undefined && tail === idleTail) return;
    end();
  }, [isStreaming, messages]);
}

/** The bubble of the finished turn's reply in scroller `c`, or null when the turn rendered none. */
export function findTurnReply(c: HTMLElement, messages: ReadonlyArray<ChatMessage>): string | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role === 'user') return null; // the finishing turn has no rendered reply
    if (m.role !== 'assistant') continue;
    // Orphan (empty, settled) assistant bubbles stay in state but never render.
    if (findMessageElement(c, m.id)) return m.id;
  }
  return null;
}
