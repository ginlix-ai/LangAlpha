import React, { Suspense, useEffect } from 'react';
import { useParams, useSearchParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { Loader } from '@/components/ui/loader';
import { GRANT_RENEW_MARGIN_MS, MIN_RENEW_INTERVAL_MS, msUntilRenewal } from '@/lib/expiry';
import { queryKeys } from '@/lib/queryKeys';
import { useAuth } from '../../contexts/AuthContext';
import { apiErrorStatus, retryUnlessClientError } from '@/api/errors';
import { getSharedMetadata, type ShareMetadata } from './api';
import ShareUnavailable from './ShareUnavailable';
import SharedAppView from './SharedAppView';

// A file or app page never loads the chat replay, and an app page never loads
// the file viewers.
const SharedChatView = React.lazy(() => import('./SharedChatView'));
const SharedFileView = React.lazy(() => import('./SharedFileView'));

/** An app's signed URL lasts an hour; swapping it reloads the running app, so only near the end. */
const APP_URL_MARGIN_MS = 5 * 60_000;

/** How long until the page's credential is due for renewal, or null for a link that has none. */
function renewalDelay({ data, dataUpdatedAt }: { data?: ShareMetadata; dataUpdatedAt: number }): number | null {
  if (data?.kind === 'app') return msUntilRenewal(data.expires_in, dataUpdatedAt, APP_URL_MARGIN_MS);
  if (data?.kind === 'file' && data.access === 'owner') {
    return msUntilRenewal(data.expires_in, dataUpdatedAt, GRANT_RENEW_MARGIN_MS);
  }
  return null;
}

function titleOf(data: ShareMetadata): string {
  switch (data.kind) {
    case 'thread':
      return data.title || data.workspace_name;
    case 'file':
      return data.name;
    case 'app':
      return data.title;
    default:
      data satisfies never;
      return '';
  }
}

function Loading() {
  return (
    <div className="flex items-center justify-center min-h-screen" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <Loader size={24} className="text-(--color-text-tertiary)" />
    </div>
  );
}

/**
 * `/s/` (chats) and `/a/` (files and apps). One metadata read decides what
 * the code is and the page hands off to the view for it, so either prefix
 * opens any code. The read waits for the session, because the owner of a
 * private link is only told apart from a visitor by the bearer.
 */
export default function SharePage(): React.ReactElement {
  const { shareToken = '' } = useParams<{ shareToken: string }>();
  const [search] = useSearchParams();
  const asVisitor = search.get('as') === 'visitor';
  // An app opened at a page other than its entry: an old preview URL's suffix.
  const path = search.get('path');
  const { isInitialized, userId } = useAuth();

  const metadata = useQuery({
    // The answer depends on who asks. A sign-in that reaches this tab from the
    // OAuth popup or another tab keeps the page mounted, so without the viewer
    // in the key a signed-out 404 would stay on screen with its Sign in gone.
    queryKey: queryKeys.share.metadata(shareToken, userId, asVisitor, path),
    queryFn: () => getSharedMetadata(shareToken, { asVisitor, path }),
    enabled: !!shareToken && isInitialized,
    staleTime: 60_000,
    refetchInterval: (query) => {
      const delay = renewalDelay(query.state);
      return delay === null ? false : Math.max(delay, MIN_RENEW_INTERVAL_MS);
    },
    // The interval sleeps with a hidden tab, so one that comes back past its
    // renewal asks at once rather than on the next tick. Any earlier read
    // would mint a new grant and reload the page the owner is reading.
    refetchOnWindowFocus: (query) => renewalDelay(query.state) === 0,
    refetchOnReconnect: (query) => renewalDelay(query.state) === 0,
    retry: retryUnlessClientError,
  });

  const title = metadata.data ? titleOf(metadata.data) : '';
  useEffect(() => {
    if (!title) return;
    const previous = document.title;
    document.title = title;
    return () => { document.title = previous; };
  }, [title]);

  // A failed refetch keeps the last answer, and the page it drew keeps working
  // until that answer's own credential lapses.
  const data = metadata.data;
  if (!data) {
    if (!metadata.isError) return <Loading />;
    if (apiErrorStatus(metadata.error) === 404) return <ShareUnavailable />;
    return <ShareUnavailable variant="failed" onRetry={() => void metadata.refetch()} />;
  }

  switch (data.kind) {
    case 'thread':
      return (
        <Suspense fallback={<Loading />}>
          <SharedChatView key={shareToken} shareToken={shareToken} metadata={data} />
        </Suspense>
      );
    case 'file':
      return (
        <Suspense fallback={<Loading />}>
          <SharedFileView key={shareToken} code={shareToken} metadata={data} />
        </Suspense>
      );
    case 'app':
      return <SharedAppView key={shareToken} metadata={data} />;
    default:
      // A kind from a newer server than this tab: say so rather than crash.
      data satisfies never;
      return <ShareUnavailable variant="failed" onRetry={() => window.location.reload()} />;
  }
}
