import { useMemo, type RefObject } from 'react';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { useStableHandler } from '@/hooks/useStableHandler';
import { draftWidgetPrompt, widgetSendParts, type WidgetPrompts, type WidgetSendParts, type WidgetSent } from '../../utils/widgetPrompt';
import type { NewThreadCarry } from './newThreadCarry';

interface WidgetPromptHost {
  chatInputRef: RefObject<ChatInputHandle | null>;
  /** Sends `text` in this thread as its composer would, with the host's own
   *  contexts and settings beside the widget's `parts`. */
  send: (text: string, parts: WidgetSendParts) => void;
  /** Opens a new thread whose first message is `text` with `parts`, keeping
   *  `carry`; absent where the host has no second thread to open. */
  openThread?: (text: string, parts: WidgetSendParts, carry: NewThreadCarry) => void;
  /** What a new thread opened from here keeps (`newThreadCarry`), read at the click. */
  readCarry: () => NewThreadCarry;
}

/**
 * A chat host's answers to a widget's prompt calls, as one object whose
 * identity holds while the host re-renders on every streamed chunk: it rides
 * the transcript's action context, and a new one would re-render every
 * settled bubble. The widget's data rides as a widget-context card on every
 * path; the draft lands in the composer, deck card and text both.
 */
export function useWidgetPromptActions({ chatInputRef, send, openThread, readCarry }: WidgetPromptHost): WidgetPrompts {
  const sendPrompt = useStableHandler((text: string, sent?: WidgetSent) => send(text, widgetSendParts(sent)));
  const draftPrompt = useStableHandler((text: string, sent?: WidgetSent) =>
    draftWidgetPrompt(chatInputRef.current, text, sent));
  const startThread = useStableHandler((text: string, sent?: WidgetSent) =>
    openThread?.(text, widgetSendParts(sent), readCarry()));
  const opens = !!openThread;
  return useMemo(
    () => ({ sendPrompt, draftPrompt, ...(opens ? { startThread } : {}) }),
    [sendPrompt, draftPrompt, startThread, opens],
  );
}
