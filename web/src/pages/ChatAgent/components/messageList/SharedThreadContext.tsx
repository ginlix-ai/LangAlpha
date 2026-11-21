import { createContext, useContext } from 'react';
import { sharedBubble, type SharedBubble, type SharedThreadView } from '../../session/sharedThread';
import type { ChatMessage } from '@/types/chat';

export const SharedThreadContext = createContext<SharedThreadView | undefined>(undefined);

/** Any other thread names no sender and offers every rewrite and review. */
const UNSHARED: SharedBubble = { sender: undefined, canRewrite: true, humanReview: true, answersPause: true };

export function useSharedBubble(message: ChatMessage, turnIndex: number | null): SharedBubble {
  const view = useContext(SharedThreadContext);
  return view ? sharedBubble(view, message, turnIndex) : UNSHARED;
}
