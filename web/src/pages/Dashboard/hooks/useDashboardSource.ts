import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { apiErrorDetail, apiErrorReason, apiErrorStatus, retryUnlessClientError } from '@/api/errors';
import { toast } from '@/components/ui/use-toast';
import { useAuth } from '@/contexts/AuthContext';
import { useArtifactGrant } from '@/hooks/useArtifact';
import { latestPreferences } from '@/hooks/usePreferenceKey';
import { appGrantKey } from '@/lib/appBridge/useAppScopes';
import type { AppDashboard, PageDashboard, SavedDashboard } from '@/lib/dashboards';
import { isPageListed, readCachedDashboards } from '@/lib/dashboards/saved';
import { setKept } from '@/lib/dashboards/keptCopy';
import { MIN_RENEW_INTERVAL_MS, msUntilRenewal } from '@/lib/expiry';
import { queryKeys } from '@/lib/queryKeys';
import type { Artifact } from '@/types/artifacts';
import {
  APP_PREVIEW_SANDBOX,
  ISOLATED_PAGE_SANDBOX,
  SERVED_HTML_SANDBOX,
} from '../../ChatAgent/components/viewers/html/sandbox';
import { buildServeUrl, versionPrefix } from '../../ChatAgent/components/viewers/html/wsfilesUrl';
import { getArtifact } from '../../ChatAgent/utils/api/artifacts';
import { getSharedMetadata, type ShareMetadata } from '../../SharedChat/api';

/** Swapping the signed URL reloads the app, so it is renewed only near its end. */
const URL_MARGIN_MS = 5 * 60_000;
/** How often a kept page's record is read for a new version, besides the events that say so. */
const PAGE_POLL_MS = 60_000;

/** Why a saved dashboard has nothing to show. */
export type Away =
  /** Its app, page or workspace was removed. */
  | { kind: 'gone' }
  /** A kept page with no version cut yet, which keeping it again cuts. */
  | { kind: 'noVersion' }
  | { kind: 'failed'; detail: string | null };

/** Where a saved dashboard's frame loads from, or why it cannot. */
export interface DashboardSource {
  url: string | null;
  sandbox: string;
  /** The server's name for it, when it has one. */
  title: string | null;
  /** What the user's grants to it are kept under; null keeps none (yet). */
  grantKey: string | null;
  /** Set once the read failed with no address to show; never while one stands. */
  away: Away | null;
  /** Ask again. For a page with no copy, that keeps one first. */
  refresh: () => void;
  /** A `refresh` the user asked for is under way. */
  refreshing: boolean;
}

/** `refresh` with the flag that says the user is waiting on it. */
function useRefresh(run: () => Promise<unknown>): Pick<DashboardSource, 'refresh' | 'refreshing'> {
  const [refreshing, setRefreshing] = useState(false);
  const refresh = () => {
    setRefreshing(true);
    void run().then(() => setRefreshing(false), () => setRefreshing(false));
  };
  return { refresh, refreshing };
}

/**
 * Says a read the user asked for failed behind a frame that stands. With
 * nothing on screen the notice in its place says it; here nothing else would.
 */
function useStaleNotice(dashboard: SavedDashboard | null): (detail: string | null) => void {
  const { t } = useTranslation();
  return (detail) => {
    if (!dashboard) return;
    toast({
      variant: 'destructive',
      title: t('customDashboard.reloadFailed', { name: dashboard.title || t('customDashboard.untitled') }),
      description: detail ?? t(dashboard.kind === 'page' ? 'customDashboard.away.failedDetailPage' : 'customDashboard.away.failedDetail'),
    });
  };
}

function renewalDelay({ data, dataUpdatedAt }: { data?: ShareMetadata; dataUpdatedAt: number }): number | null {
  if (data?.kind !== 'app') return null;
  return msUntilRenewal(data.expires_in, dataUpdatedAt, URL_MARGIN_MS);
}

/** The server's sentence for an app it could not open, without its log prefix. */
function failureDetail(err: unknown): string | null {
  const detail = apiErrorDetail(err);
  if (typeof detail !== 'string' || !detail) return null;
  const text = detail.replace(/^Sandbox is not reachable: /, '');
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/**
 * A running app. The metadata read is the one the `/a/` page makes: it starts
 * a stopped computer and reruns the app's command, so opening the dashboard
 * is what brings the app back. Its URL is signed for an hour.
 */
function useAppSource(app: AppDashboard | null): DashboardSource {
  const { isInitialized, userId } = useAuth();
  const code = app?.code ?? '';
  const path = app?.path ?? null;
  const metadata = useQuery({
    queryKey: queryKeys.share.metadata(code, userId, false, path),
    queryFn: () => getSharedMetadata(code, { path }),
    enabled: !!app && isInitialized,
    staleTime: 60_000,
    refetchInterval: (query) => {
      const delay = renewalDelay(query.state);
      return delay === null ? false : Math.max(delay, MIN_RENEW_INTERVAL_MS);
    },
    // Every read past the server's short cache mints a new URL, so a focus
    // asks only once renewal is due. An app that went down meanwhile comes
    // back through the reload button.
    refetchOnWindowFocus: (query) => renewalDelay(query.state) === 0,
    refetchOnReconnect: (query) => renewalDelay(query.state) === 0,
    retry: retryUnlessClientError,
  });
  const data = metadata.data?.kind === 'app' ? metadata.data : null;
  const stale = useStaleNotice(app);
  const refresh = useRefresh(async () => {
    const read = await metadata.refetch();
    if (read.isError && read.data) stale(failureDetail(read.error));
  });

  let away: Away | null = null;
  if (metadata.isError && !data) {
    away = apiErrorStatus(metadata.error) === 404 ? { kind: 'gone' } : { kind: 'failed', detail: failureDetail(metadata.error) };
  } else if (metadata.data && !data) {
    // The code names something else now, which no wait turns into an app.
    away = { kind: 'gone' };
  }
  return {
    url: data?.url ?? null,
    sandbox: APP_PREVIEW_SANDBOX,
    title: data?.title || null,
    // Grants follow the command the app is started with, named in the answer
    // that carries its URL, so none is read under a command the frame is not running.
    grantKey: data ? appGrantKey({ code, command_key: data.command_key }) : null,
    away,
    ...refresh,
  };
}

/**
 * A page kept on the server: the latest version, framed from LangAlpha with
 * the computer off. A turn that changes its files cuts a new version, and the
 * grant lasts hours and is renewed, so nothing on screen waits on the computer.
 */
function usePageSource(page: PageDashboard | null): DashboardSource {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const artifactId = page?.artifactId ?? '';
  const record = useQuery<Artifact, Error>({
    queryKey: queryKeys.artifacts.detail(artifactId),
    queryFn: () => getArtifact(artifactId),
    enabled: !!page,
    staleTime: 30_000,
    // Events read it again when a version is cut; this is the backstop. Not
    // for a record that failed, which the user asks for again.
    refetchInterval: (query) => (query.state.status === 'error' ? false : PAGE_POLL_MS),
    retry: retryUnlessClientError,
  });
  const grant = useArtifactGrant(page?.artifactId);
  const keep = useMutation({
    mutationFn: () => setKept(qc, artifactId, true),
    onError: (err) =>
      toast({ variant: 'destructive', title: t('customDashboard.keepFailed'), description: apiErrorReason(err) ?? undefined }),
  });

  const seq = record.data?.latest_seq ?? 0;
  const noVersion = !!record.data && seq === 0;
  const stale = useStaleNotice(page);
  const refresh = useRefresh(async () => {
    // A failed keep has said so itself; the read after it is not the user's wait.
    if (noVersion) return keep.mutateAsync().catch(() => {});
    const [read, granted] = await Promise.all([record.refetch(), grant.refetch()]);
    if ((read.isError && read.data) || (granted.isError && granted.data)) stale(null);
  });

  // A listed page is a kept one. A removal's release that crossed a pin
  // (another tab, an undo after its window) can leave it listed and let go,
  // where it would show its last version and quietly stop following the file.
  // Let go is also what a removal on another device looks like from here, so
  // the row is read first: off the list there, it leaves this screen instead.
  // Asked once: a keep that fails here is tried again the next time it opens.
  const letGo = record.data?.kept === false && seq > 0;
  useEffect(() => {
    if (!letGo) return;
    void latestPreferences(qc)
      .then(() => (isPageListed(readCachedDashboards(qc), artifactId) ? setKept(qc, artifactId, true) : null))
      .catch(() => {});
  }, [letGo, artifactId, qc]);
  const url =
    record.data && seq > 0 && grant.data ? buildServeUrl(versionPrefix(grant.data.base, seq), record.data.path) : null;
  let away: Away | null = null;
  if (record.isError && !record.data) {
    away = apiErrorStatus(record.error) === 404 ? { kind: 'gone' } : { kind: 'failed', detail: null };
  } else if (noVersion) {
    away = { kind: 'noVersion' };
  } else if (grant.isError && !grant.data) {
    away = { kind: 'failed', detail: null };
  }
  return {
    url,
    sandbox: grant.data?.isolated ? ISOLATED_PAGE_SANDBOX : SERVED_HTML_SANDBOX,
    title: null,
    grantKey: page?.code ?? null,
    away,
    ...refresh,
  };
}

/** Where the chosen dashboard loads from. Both kinds are read by hooks, so each is asked with what it is not as null. */
export function useDashboardSource(dashboard: SavedDashboard): DashboardSource {
  const page = usePageSource(dashboard.kind === 'page' ? dashboard : null);
  const app = useAppSource(dashboard.kind === 'app' ? dashboard : null);
  return dashboard.kind === 'page' ? page : app;
}
