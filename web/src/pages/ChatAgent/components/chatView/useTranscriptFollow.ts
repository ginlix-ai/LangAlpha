import type { TurnEndScroll } from '@/lib/turnEndScroll';
import type { useChatMessages } from '../../hooks/useChatMessages';
import type { StreamFollowControls, TurnStart } from './streamFollow';
import { findTurnReply, useTurnEnd } from './turnEnd';

type ChatHandle = ReturnType<typeof useChatMessages>;
type TurnStarters = Pick<ChatHandle, 'handleSendMessage' | 'handleEditMessage' | 'handleRegenerate' | 'handleRetry'>;
type TurnState = Pick<ChatHandle, 'messages' | 'isLoading' | 'pendingInterrupt'>;

/** A pending interrupt clears isLoading, but the turn is still open: the reply
 *  resumes once the reader answers, so the follow keeps its claim and the
 *  turn-end landing waits. */
export function isTurnOpen({ isLoading, pendingInterrupt }: TurnState): boolean {
  return isLoading || !!pendingInterrupt;
}

/**
 * The follow policy of both chat surfaces, each over its own scroll engine. A
 * turn the reader starts (a send or steer, an edit, a regenerate or retry)
 * takes them to it wherever they had scrolled, since their message and the
 * reply land there; a message sent into a turn still running is a steer. A
 * finished turn lands where the "When a reply finishes" preference says.
 * Returns the chat's turn starters with the rejoin built in, so a new send
 * site cannot leave it out. A turn that resumes on its own (an answered
 * approval, a reconnect) is not the reader's to follow.
 */
export function useTranscriptFollow(
  chat: TurnStarters & TurnState,
  follow: StreamFollowControls,
  getScroller: () => HTMLElement | null,
  turnEndScroll: TurnEndScroll,
): TurnStarters {
  const { messages } = chat;
  useTurnEnd(messages, isTurnOpen(chat), () => {
    if (turnEndScroll !== 'reply_start') return;
    const c = getScroller();
    const id = c && findTurnReply(c, messages);
    if (id) follow.landOnReply(id);
  });
  const rejoining = <A extends unknown[], R>(kind: Exclude<TurnStart, 'steer'>, start: (...args: A) => R) => (...args: A): R => {
    // A steer by the send's own test: with only an interrupt pending, what the
    // reader sends opens a turn of its own.
    const steer = chat.isLoading;
    const started = start(...args);
    follow.rejoin(steer ? 'steer' : kind);
    return started;
  };
  return {
    handleSendMessage: rejoining('send', chat.handleSendMessage),
    handleEditMessage: rejoining('restart', chat.handleEditMessage),
    handleRegenerate: rejoining('restart', chat.handleRegenerate),
    handleRetry: rejoining('restart', chat.handleRetry),
  };
}
