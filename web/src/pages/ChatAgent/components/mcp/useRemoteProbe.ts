import { useEffect, useMemo } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import { headersFingerprint } from './mcpHeadersFingerprint';
import type { ProbePending } from './McpProbePanel';
import { unreachableResult, verdictIs } from './mcpProbeVerdict';
import { probeTarget, type FormAction, type FormState } from './mcpServerDraft';
import { formatApiErrorDetail, type McpProbeInput, type McpProbeResult } from '../../utils/api';

/** How long the form rests before a remote address is checked. */
export const PROBE_DEBOUNCE_MS = 700;

/** Whether two addresses name the same host. An address that will not parse is
 *  never the one already saved. */
function sameOrigin(a: string, b: string): boolean {
  try {
    return new URL(a).origin === new URL(b).origin;
  } catch {
    return false;
  }
}

interface RemoteProbeOptions {
  form: FormState;
  dispatch: (action: FormAction) => void;
  onProbe?: (body: McpProbeInput, signal?: AbortSignal) => Promise<McpProbeResult>;
  /** The edited server's saved address ('' for none), or null for an add. */
  savedUrl: string | null;
  /** Whether a header still holds a placeholder from a pasted config, which
   *  is not a key: asking the server about it would only report it rejected. */
  standInHeaders: boolean;
}

/**
 * The host-side check of the form's remote address, asked as the user types.
 *
 * React Query owns it: the key IS the question (this address, which
 * credential), so an answer can never be shown against a different one, and
 * typing back to an address already checked shows its verdict at once. The
 * credential rides in the key as a digest, never as itself: the key outlives
 * the form in the cache, and naming the credential is all the key needs.
 */
export function useRemoteProbe({ form, dispatch, onProbe, savedUrl, standInHeaders }: RemoteProbeOptions) {
  const queryClient = useQueryClient();
  const { draft, trusted, headersSettled } = form;
  const target = useMemo(() => probeTarget(draft), [draft]);
  const headersKey = target ? headersFingerprint(target.headers) : '';
  const probeKey = target ? `${target.url}\n${headersKey}` : '';
  const restedKey = useDebouncedValue(probeKey, PROBE_DEBOUNCE_MS);
  // When editing, the headers hold the server's own credential (or the refs the
  // host resolves into it), so re-pointing an existing server would hand that
  // credential to every hostname typed on the way to the intended one. A new
  // host waits for Save, which is the point the user says it is the right one;
  // a new path on the same host is the address the credential already went to.
  const held = savedUrl !== null && !!target && !sameOrigin(target.url, savedUrl);
  // A draft that carries a header carries a credential, and `https://acme.co`
  // is a complete, reachable host on the way to `https://acme.corp.example`.
  // Resting is not the user saying the address is finished, so with a header
  // in hand the automatic check waits for an address the key may go to, and
  // for the key itself to be finished. A draft with no header keeps the rest.
  const hasHeaders = !!target && Object.keys(target.headers).length > 0;
  const keyTrusted = !!target && trusted.includes(target.url);
  const canCheck = !!onProbe && !!target && !held && !standInHeaders;
  const query = useQuery({
    queryKey: queryKeys.mcp.probe(target?.url ?? '', headersKey),
    queryFn: async ({ signal }) => {
      const url = target!.url;
      const result = await onProbe!({ url, headers: target!.headers }, signal);
      // An address that answered about a key is the server the user meant, so
      // a key typed after that goes to it without the address being committed
      // again.
      if (verdictIs(result.verdict, 'aboutTheKey')) dispatch({ type: 'trust', url });
      return result;
    },
    enabled: canCheck && (hasHeaders ? keyTrusted && headersSettled : restedKey === probeKey),
    staleTime: Infinity,
    retry: false,
  });
  // A verdict is only of use to the form that asked for it, so it leaves with
  // the form rather than resting in the cache for its gcTime.
  useEffect(
    () => () => {
      queryClient.removeQueries({ queryKey: queryKeys.mcp.probes() });
    },
    [queryClient],
  );

  const result =
    query.data ?? (query.error ? unreachableResult(formatApiErrorDetail(query.error)) : null);
  const pending: ProbePending | null =
    hasHeaders && !result ? (keyTrusted ? 'keyAdded' : 'keyHeld') : null;
  return {
    held,
    canCheck,
    probing: query.isFetching,
    result,
    pending,
    /** Asked by hand: the user saying this is the address and the key is
     *  ready, the same as leaving both fields. */
    check: () => {
      if (!target) return;
      dispatch({ type: 'trust', url: target.url });
      dispatch({ type: 'settle' });
      void query.refetch();
    },
    /** The save runs this same check and refuses this answer, so while it
     *  stands for exactly this form, Add is not the step to lead with. */
    refuses: !!result && verdictIs(result.verdict, 'refusesSave'),
    /** What the check listed, for the confirmation of exactly this server. */
    toolCount: result && verdictIs(result.verdict, 'ok') ? (result.tools ?? []).length : null,
  };
}
