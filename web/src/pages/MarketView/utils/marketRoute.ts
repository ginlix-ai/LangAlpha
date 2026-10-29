/**
 * The spelling of a MarketView URL. `buildMarketViewUrl` writes it for the
 * chat's chart tab and the dashboard header;
 * `readMarketViewRoute` reads it back for `MarketView.tsx` and
 * `MarketChatPanel.tsx`, so the two sides share one set of param names.
 * Older Dashboard call sites still spell `/market?symbol=` by hand.
 */
import type { ComposerScope } from '@/components/ui/chat-input.types';
import { inAppPath } from '@/lib/inAppPath';

export interface MarketViewRoute {
  symbol: string;
  timeframe?: string;
  /** The workspace whose chat should continue beside the chart. Naming one
   *  also selects the workspace scope. */
  workspaceId?: string | null;
  threadId?: string | null;
  /** Where "Return to chat" goes: a path inside the app, or the reader drops it. */
  returnTo?: string | null;
}

/** What a MarketView URL carries, each field null when the param is absent. */
export interface MarketViewRouteParams {
  symbol: string | null;
  timeframe: string | null;
  workspaceId: string | null;
  scope: ComposerScope | null;
  threadId: string | null;
  returnTo: string | null;
}

/** The thread id that means "no particular thread"; the URL never carries it. */
export const DEFAULT_MARKET_THREAD = '__default__';

const PARAM = {
  symbol: 'symbol',
  timeframe: 'tf',
  // Read only, for links from before `ws` alone named the scope.
  mode: 'mode',
  workspaceId: 'ws',
  threadId: 'thread',
  returnTo: 'returnTo',
} as const;

/** Every param the route owns, for a reader that clears them once consumed. */
export const MARKET_VIEW_ROUTE_PARAMS: readonly string[] = Object.values(PARAM);

export function buildMarketViewUrl({ symbol, timeframe, workspaceId, threadId, returnTo }: MarketViewRoute): string {
  const sp = new URLSearchParams();
  sp.set(PARAM.symbol, symbol);
  if (timeframe) sp.set(PARAM.timeframe, timeframe);
  if (workspaceId) sp.set(PARAM.workspaceId, workspaceId);
  if (threadId && threadId !== DEFAULT_MARKET_THREAD) sp.set(PARAM.threadId, threadId);
  if (returnTo) sp.set(PARAM.returnTo, returnTo);
  return `/market?${sp.toString()}`;
}

export function readMarketViewRoute(searchParams: URLSearchParams): MarketViewRouteParams {
  const workspaceId = searchParams.get(PARAM.workspaceId);
  // An old link's `mode` wins, in its old names: 'fast' is the Chief of Staff.
  const mode = searchParams.get(PARAM.mode);
  const legacyScope: ComposerScope | null = mode === 'fast' ? 'all' : mode === 'ptc' ? 'workspace' : null;
  return {
    symbol: searchParams.get(PARAM.symbol),
    timeframe: searchParams.get(PARAM.timeframe),
    workspaceId,
    scope: legacyScope ?? (workspaceId ? 'workspace' : null),
    threadId: searchParams.get(PARAM.threadId),
    returnTo: inAppPath(searchParams.get(PARAM.returnTo)),
  };
}
