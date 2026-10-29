/**
 * Which of the transcript's prose is still typing out, by message and block.
 *
 * A turn that stops while its reply is still typing is drawn running until the
 * typing ends (see `useTurnPresentation`), and the reply's streaming indicator
 * steps aside while it types. Both read this store, which every text block
 * writes from a layout effect as it reports. The list decides a settle in its
 * own layout effect, which runs after those of every block drawn in the same
 * commit, so the answer it reads is the one for what that commit drew. Relayed
 * through state instead, the answer came effects after the decision, and a
 * reply whose last words arrived with the end of its stream settled under the
 * words still typing.
 */
import { createContext, useCallback, useContext, useLayoutEffect, useSyncExternalStore } from 'react';

export class ProseTyping {
  /** The blocks still typing, by message. A message with none has no entry. */
  private readonly typing = new Map<string, Set<string>>();
  private readonly listeners = new Set<() => void>();

  /** Records whether one block is typing. Saying it again changes nothing. */
  report(message: string, block: string, typing: boolean): void {
    const blocks = this.typing.get(message);
    if (typing === !!blocks?.has(block)) return;
    if (typing) {
      if (blocks) blocks.add(block);
      else this.typing.set(message, new Set([block]));
    } else if (blocks) {
      blocks.delete(block);
      if (blocks.size === 0) this.typing.delete(message);
    }
    for (const listener of [...this.listeners]) listener();
  }

  isTyping(message: string): boolean {
    return this.typing.has(message);
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
}

/** The list's store; a surface with no list above it has no turn to settle. */
export const ProseTypingContext = createContext<ProseTyping | null>(null);

/**
 * What a text block hands `TextMessageContent` to report through, which it
 * calls from a layout effect. The block's entry goes when the block does, so
 * prose that unmounts mid-type cannot hold its turn running.
 */
export function useProseReport(message: string | undefined, block: string): (revealed: boolean) => void {
  const store = useContext(ProseTypingContext);
  useLayoutEffect(() => (store && message ? () => store.report(message, block, false) : undefined), [store, message, block]);
  return useCallback((revealed: boolean) => {
    if (store && message) store.report(message, block, !revealed);
  }, [store, message, block]);
}

const NO_SUBSCRIPTION = () => () => {};

/** Whether `message` has prose typing out. Only asked while `watching`, so a
 *  settled bubble is not told of every other bubble's typing. */
export function useMessageTyping(message: string, watching: boolean): boolean {
  const store = useContext(ProseTypingContext);
  const live = watching ? store : null;
  return useSyncExternalStore(live ? live.subscribe : NO_SUBSCRIPTION, () => !!live?.isTyping(message));
}
