import type { RefObject } from 'react';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { useStableHandler } from '@/hooks/useStableHandler';

/**
 * What the queued comments' deck asks of the composer it sits in. "Add to
 * turn" is Enter's send; where files hold the send back, the comments wait
 * with them and the composer takes the focus, where the hint beside Send says
 * why. A keep that empties the queue leaves the focus in the text box.
 */
export function useDeckComposer(chatInputRef: RefObject<ChatInputHandle | null>): {
  onAddToTurn: () => void;
  focusComposer: () => void;
} {
  const onAddToTurn = useStableHandler(() => {
    const input = chatInputRef.current;
    if (input && !input.send() && input.holdsSend()) input.focus();
  });
  const focusComposer = useStableHandler(() => { chatInputRef.current?.focus(); });
  return { onAddToTurn, focusComposer };
}
