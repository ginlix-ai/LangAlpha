/**
 * The turn-level retry the server asks for. When a run fails before it reaches
 * the agent, on an error the server classes as transient (a timeout, a dropped
 * connection), its stream ends with `retry` instead of `error`. Either frame
 * names its recovery: `retry` when the failed run is on the thread, so the next
 * attempt goes through POST /retry naming it, and `resend` when the server never
 * started one, so the request goes again as it was. This module decides what
 * the next attempt sends, whether the client sends it itself, and leaves the
 * reply failed, with Retry on it, once it stops.
 */

import { finalizeAssistantMessage } from './finalizeMessage';
import { updateMessage } from '../../hooks/utils/messageHelpers';
import type { ChatMessage } from '@/types/chat';
import type { SSEEvent, SetMessages } from '../types';

/** What a `retry` frame, or a failure `error` frame, says about the attempt. */
export interface RetryNotice {
  autoRetry: boolean;
  /** The failed attempt's number on its turn, counting from 1. */
  attempt: number | null;
  maxRetries: number | null;
  recovery: 'retry' | 'resend';
}

export function readRetryNotice(event: SSEEvent): RetryNotice {
  return {
    autoRetry: event.auto_retry === true,
    attempt: typeof event.retry_count === 'number' ? event.retry_count : null,
    maxRetries: typeof event.max_retries === 'number' ? event.max_retries : null,
    // A server that names no recovery only ever meant POST /retry.
    recovery: event.recovery === 'resend' ? 'resend' : 'retry',
  };
}

/** What a turn sent, kept so a failure can send it again. */
export interface TurnRequest {
  message: string | null;
  additionalContext: Record<string, unknown>[] | null;
  /** An edit or regenerate: the checkpoint it forks from, and its turn. */
  checkpointId: string | null;
  forkFromTurn: number | null;
  /** Set for POST /retry. The server refuses (409) a run that is no longer
   * the thread's latest attempt, rather than retry a different one. */
  retryOf: { runId: string | null } | null;
  /** The send's subagents pick. A resend may be what creates the thread, so
   * it carries the pick too. */
  subagentsAllowed?: boolean;
}

export const sendRequest = (
  message: string,
  additionalContext: Record<string, unknown>[] | null,
  subagentsAllowed?: boolean,
): TurnRequest => ({ message, additionalContext, checkpointId: null, forkFromTurn: null, retryOf: null, subagentsAllowed });

export const forkRequest = (
  message: string | null,
  checkpointId: string,
  forkFromTurn: number,
): TurnRequest => ({ message, additionalContext: null, checkpointId, forkFromTurn, retryOf: null });

const retryRequest = (
  runId: string | null,
  additionalContext: Record<string, unknown>[] | null,
): TurnRequest => ({ message: null, additionalContext, checkpointId: null, forkFromTurn: null, retryOf: { runId } });

/** The Retry a reply gets when nothing better is known: the thread's latest
 * failed run, from where its checkpoint left off. */
export const LATEST_RUN_RETRY: TurnRequest = retryRequest(null, null);

/**
 * The request that recovers from `request` failing as `notice` says. A started
 * run is retried by id; the server reruns its message from what it stored when
 * the message never reached the agent, but attachments and widget context only
 * this client still holds, so they ride along. Null when there is nothing the
 * client can send again (a resume, whose answers are not kept here).
 */
export function recoveryRequest(
  request: TurnRequest | null,
  notice: RetryNotice,
  failedRunId: string | null,
): TurnRequest | null {
  if (notice.recovery === 'resend') return request;
  return retryRequest(failedRunId, request?.additionalContext ?? null);
}

/** The Retry a failed reply holds, kept for the bubble it would replace. */
export interface HeldRetry {
  bubbleId: string;
  request: TurnRequest;
}

export function retryRequestFor(held: HeldRetry | null, bubbleId: string | undefined): TurnRequest {
  return held && held.bubbleId === bubbleId ? held.request : LATEST_RUN_RETRY;
}

/**
 * Resends one turn may make on its own. The server numbers attempts only while
 * the request still owns its run; a failure outside that window reports
 * attempt 1 every time, so its `max_retries` alone cannot end the chain.
 */
const MAX_AUTO_RETRIES = 3;

function shouldAutoRetry(notice: RetryNotice, spent: number): boolean {
  if (!notice.autoRetry) return false;
  const budget = notice.maxRetries === null
    ? MAX_AUTO_RETRIES
    : Math.min(MAX_AUTO_RETRIES, notice.maxRetries);
  if (spent >= budget) return false;
  return notice.attempt === null || notice.maxRetries === null || notice.attempt <= notice.maxRetries;
}

/** 1s, 2s, 4s: room for a dropped connection to come back, and short beside
 * the minute a computer that is still starting can hold an attempt. */
const autoRetryDelayMs = (spent: number): number => 1000 * 2 ** spent;

export interface RetrySettlerDeps {
  setMessages: SetMessages;
  getMessages: () => readonly ChatMessage[];
  /** What the failed stream sent; null for one the client cannot send again. */
  request: TurnRequest | null;
  /** The run whose stream just ended on the notice. */
  failedRunId: string | null;
  /** Another attempt at the turn, in place of `truncateIndex`; `spent` counts
   * the resends before it. */
  resend: (request: TurnRequest, truncateIndex: number, snapshot: readonly ChatMessage[], spent: number) => void;
  /** Hands the reply's Retry what to send, once the client stops on its own. */
  holdRetry: (held: HeldRetry | null) => void;
  wasStoppedRef: { readonly current: boolean };
  sessionEpochRef: { readonly current: number };
  threadIdRef: { readonly current: string | null };
  /** Shown on a reply that failed before it said anything. */
  failedText: string;
  /** The same, once the client's own resends of it are spent. */
  exhaustedText: string;
}

const failReply = (deps: RetrySettlerDeps, bubbleId: string, text: string): void => {
  deps.setMessages((prev) =>
    updateMessage(prev, bubbleId, (msg) =>
      msg.role === 'assistant' && msg.isStreaming
        ? { ...finalizeAssistantMessage(msg, 'failed'), content: msg.content || text }
        : msg,
    ),
  );
};

/**
 * Settles a stream that ended on a retry notice. True when a resend is
 * scheduled: the bubble stays streaming and the turn keeps its loading state
 * and stream ownership until the resend takes them over, so the caller skips
 * its own finalize and cleanup. False once the bubble is marked failed; the
 * caller then ends the turn as usual.
 */
export function settleRetryNotice(
  deps: RetrySettlerDeps,
  notice: RetryNotice,
  bubbleId: string,
  spent: number,
): boolean {
  const next = recoveryRequest(deps.request, notice, deps.failedRunId);
  if (!next || !shouldAutoRetry(notice, spent)) {
    failReply(deps, bubbleId, spent > 0 ? deps.exhaustedText : deps.failedText);
    deps.holdRetry(next && { bubbleId, request: next });
    return false;
  }
  const epoch = deps.sessionEpochRef.current;
  const tid = deps.threadIdRef.current;
  setTimeout(() => {
    // A stop during the wait already settled the bubble and the session.
    if (deps.wasStoppedRef.current) return;
    // A newer turn, another thread or leaving the chat took the session, and
    // that owner manages the loading state; only this bubble is left to settle.
    if (deps.sessionEpochRef.current !== epoch || deps.threadIdRef.current !== tid) {
      failReply(deps, bubbleId, deps.failedText);
      return;
    }
    // The resend replaces the bubble in the same write that adds its own, so
    // the reply never shows as settled between attempts.
    const transcript = deps.getMessages();
    const at = transcript.findIndex((m) => m.id === bubbleId);
    deps.resend(next, at === -1 ? transcript.length : at, transcript, spent + 1);
  }, autoRetryDelayMs(spent));
  return true;
}
