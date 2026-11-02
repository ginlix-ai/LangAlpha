import { useCallback, useEffect, useState } from 'react';
import { usePreferences } from '@/hooks/usePreferences';
import { modelPrefs } from '@/lib/modelPreferences';
import type { NextSummary } from '../utils/api';
import type { useChatMessages } from './useChatMessages';

type ChatHandle = ReturnType<typeof useChatMessages>;
type TurnStarters = Pick<ChatHandle, 'handleSendMessage' | 'handleEditMessage' | 'handleRegenerate' | 'handleRetry'>;

export interface NextSummaryHint {
  /** What the next send summarizes first; null for nothing, or while a turn runs. */
  reason: NextSummary['reason'] | null;
  /** The user skipped the cold summary for the next send. */
  skipped: boolean;
  /** What the next send asks for (`keep_full_context`): the skip, or a cold
   *  reading `afterBreakOff` hides, whose setting may not be stored yet. */
  keepFullContext: boolean;
  toggleSkip: () => void;
}

/**
 * What the composer says the next send summarizes first: the server's
 * `next_summary`, one with an `at` (`cold`, most `rebuild`s) only from that time,
 * so the hint comes up while the user types if that time passes. The time is
 * the server's, so it is counted from the reading's arrival by the server's
 * own `now` beside it, whatever this machine's clock says. None while a
 * turn runs, since a send then steers it rather than starting a turn. Only a
 * `cold` summary can be skipped. A skip belongs to the reading it was made on,
 * so a new reading, another thread's included, starts without one, and lasts
 * until a run of the send's own starts, which clears the reading: a send
 * refused or lost before that keeps it for the next. The server reads the
 * account's setting when the thread answers, so a reading taken before the
 * user turned it off can still name a cold one: `afterBreakOff` hides those,
 * and the send skips them, since the server may still hold the setting on
 * while the write is on its way or failing.
 */
export function useNextSummaryHint(next: NextSummary | null, running: boolean, afterBreakOff = false): NextSummaryHint {
  // The `at` whose time has come; a timer sets it, so the render reads no clock.
  const [reached, setReached] = useState<number | null>(null);
  const [skippedAt, setSkippedAt] = useState<number | null>(null);
  const timed = next && 'at' in next ? next : null;
  const at = timed?.at ?? null;
  const wait = timed ? timed.at - timed.now : null;
  useEffect(() => {
    if (at === null || wait === null) return;
    const timer = setTimeout(() => setReached(at), Math.max(0, wait));
    return () => clearTimeout(timer);
  }, [at, wait]);
  if (!next && skippedAt !== null) setSkippedAt(null);
  const turnedOff = afterBreakOff && next?.reason === 'cold';
  const hidden = turnedOff || (at !== null && reached !== at);
  const reason = running || !next || hidden ? null : next.reason;
  const skipped = reason === 'cold' && skippedAt === at;
  const toggleSkip = useCallback(() => setSkippedAt(skipped ? null : at), [skipped, at]);
  return { reason, skipped, keepFullContext: skipped || turnedOff, toggleSkip };
}

/**
 * The hint for a chat surface's composer, with the chat's turn starters
 * carrying the skip while it asks for one, so a surface that shows the line
 * starts every turn with it: a send, a widget's prompt, an edit, a regenerate
 * or a Retry. Otherwise they are the chat's own, called as given.
 */
export function useNextSummary(
  chat: TurnStarters & Pick<ChatHandle, 'nextSummary' | 'isLoading'>,
): { summary: NextSummaryHint; starters: TurnStarters } {
  const { preferences } = usePreferences();
  const summary = useNextSummaryHint(chat.nextSummary, chat.isLoading, modelPrefs(preferences).cold_compaction === false);
  if (!summary.keepFullContext) return { summary, starters: chat };
  return {
    summary,
    starters: {
      handleSendMessage: (message, additionalContext, attachmentMeta, options, ...rest) =>
        chat.handleSendMessage(message, additionalContext, attachmentMeta, { ...options, keepFullContext: true }, ...rest),
      handleEditMessage: (messageId, newContent, modelOptions) => chat.handleEditMessage(messageId, newContent, modelOptions, true),
      handleRegenerate: (messageId, modelOptions) => chat.handleRegenerate(messageId, modelOptions, true),
      handleRetry: (modelOptions) => chat.handleRetry(modelOptions, true),
    },
  };
}
