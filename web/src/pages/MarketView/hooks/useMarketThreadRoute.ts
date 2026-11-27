import { useCallback, useEffect, useRef, useState } from 'react';
import { useSearchParams } from 'react-router';
import { useEditSearchParams } from '@/hooks/useEditSearchParams';
import type { SendOptions } from '../../ChatAgent/session/types';
import type { WidgetSendParts } from '../../ChatAgent/utils/widgetPrompt';
import { getMarketThreadId, setMarketThreadId, clearMarketThreadId } from '../utils/threadPersistence';
import { readMarketViewRoute } from '../utils/marketRoute';

/** A widget's startThread, carried into the new chat it opens. */
export interface WidgetFirstSend {
  text: string;
  parts: WidgetSendParts;
  /** The model and settings of the chat it came from, which the new thread keeps. */
  options: SendOptions;
}

export interface MarketThreadRoute {
  /** Keys the chat: a new one remounts it, so its engine starts on `threadId`. */
  key: string;
  /** The thread the chat opens on; `__default__` for a new chat. */
  threadId: string;
  /** What the chat was opened to send first; only `startThread` opens one with it. */
  firstSend?: WidgetFirstSend;
  selectThread: (threadId: string) => void;
  startNewChat: () => void;
  /** A new chat that sends `first` once mounted. */
  startThread: (first: WidgetFirstSend) => void;
}

/** A thread to open, with what the chat opened on it sends first. Replaced
 *  whole on every move, so no other move can inherit a pending first send. */
interface Opened {
  /** A thread id, or `__default__#<n>`: the suffix forces a remount even
   *  when already on a new chat, and is stripped before the engine sees it. */
  init: string;
  firstSend?: WidgetFirstSend;
}

const freshChat = (): Opened => ({ init: `__default__#${Date.now()}` });

/**
 * Which thread the market chat shows. URL `?thread=` wins, then localStorage
 * keyed by (workspace, symbol), then a new chat. A new symbol restores the
 * last thread seen for it in this workspace; a new workspace or scope starts
 * a clean slate. A new chat also drops `?thread` from the URL, which is
 * rewritten for a real thread once the chat names one, so a reload never
 * reopens the previous symbol's or chat's conversation.
 */
export function useMarketThreadRoute(workspaceId: string | null, symbol: string): MarketThreadRoute {
  const [searchParams] = useSearchParams();
  const editSearchParams = useEditSearchParams();
  const [opened, setOpened] = useState<Opened>(() => ({
    init: readMarketViewRoute(searchParams).threadId || (getMarketThreadId(workspaceId, symbol) ?? '__default__'),
  }));

  const dropThreadParam = useCallback(
    () => editSearchParams((params) => params.delete('thread'), { replace: true }),
    [editSearchParams],
  );

  const lastSymbolRef = useRef(symbol);
  useEffect(() => {
    if (lastSymbolRef.current === symbol) return;
    lastSymbolRef.current = symbol;
    const restored = getMarketThreadId(workspaceId, symbol);
    setOpened({ init: restored ?? '__default__' });
    if (!restored) dropThreadParam();
  }, [symbol, workspaceId, dropThreadParam]);

  // localStorage is keyed by (workspace, symbol), so the previous scope's
  // pointer stays intact and is reachable via the history dropdown.
  const prevWorkspaceRef = useRef<string | null>(workspaceId);
  useEffect(() => {
    const prev = prevWorkspaceRef.current;
    if (prev === workspaceId) return;
    prevWorkspaceRef.current = workspaceId;
    if (prev === null || workspaceId === null) return;
    setOpened(freshChat());
    dropThreadParam();
  }, [workspaceId, dropThreadParam]);

  const selectThread = useCallback((threadId: string) => {
    setMarketThreadId(workspaceId, symbol, threadId);
    setOpened({ init: threadId });
  }, [workspaceId, symbol]);

  const openFresh = useCallback((firstSend?: WidgetFirstSend) => {
    clearMarketThreadId(workspaceId, symbol);
    setOpened({ ...freshChat(), ...(firstSend ? { firstSend } : {}) });
    dropThreadParam();
  }, [workspaceId, symbol, dropThreadParam]);

  const startNewChat = useCallback(() => openFresh(), [openFresh]);

  return {
    key: opened.init,
    threadId: opened.init.split('#')[0],
    firstSend: opened.firstSend,
    selectThread,
    startNewChat,
    startThread: openFresh,
  };
}
