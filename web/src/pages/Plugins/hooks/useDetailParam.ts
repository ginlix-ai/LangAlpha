import { useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { parseDetail, withDetail, type DetailKind, type DetailRef } from '../utils/detailParam';

/**
 * The `?detail=` overlay for one tab: read the param, resolve it against that
 * tab's rows, and open/close it without touching the rest of the query string.
 *
 * All three tabs had their own copy, and the interesting half — "a deep link
 * to a row that no longer exists must not park a dead param in the URL" —
 * depends on knowing when the list behind it has *finished answering*. Three
 * copies meant three chances to get that wait wrong; here it is one decision,
 * with `settled` as the single thing each tab still has to supply.
 */

export interface DetailParam<T> {
  /** The resolved row, or null when nothing of this kind is open. */
  target: T | null;
  open: (name: string, workspaceId?: string | null) => void;
  close: () => void;
}

export function useDetailParam<T>(
  kind: DetailKind,
  /** Resolve a ref of this tab's kind to a row, or null if there is no match. */
  resolve: (ref: DetailRef) => T | null,
  /** The lists this tab resolves against have loaded, so a miss is a real miss. */
  settled: boolean,
): DetailParam<T> {
  const [searchParams, setSearchParams] = useSearchParams();
  const ref = parseDetail(searchParams);
  const target = ref?.kind === kind ? resolve(ref) : null;

  function open(name: string, workspaceId: string | null = null) {
    setSearchParams(withDetail(searchParams, { kind, name, workspaceId }), {
      replace: true,
    });
  }
  function close() {
    setSearchParams(withDetail(searchParams, null), { replace: true });
  }

  const stale = ref?.kind === kind && !target && settled;
  useEffect(() => {
    if (stale) close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [stale]);

  return { target, open, close };
}
