/**
 * Keeps the cached list of vendor terms waiting on the user in step with the
 * user feed, which tells of each change and lists the waiting vendors in
 * every snapshot.
 */
import type { QueryClient } from '@tanstack/react-query';
import type { PendingVendorTerms } from '@/api/vendorTerms';
import { queryKeys } from '@/lib/queryKeys';

/** A vendor's terms started or stopped waiting, here or anywhere else. */
export function refreshPendingVendorTerms(qc: QueryClient): Promise<void> {
  return qc.invalidateQueries({ queryKey: queryKeys.vendorTerms.pending() });
}

/**
 * Read the waiting terms again when a snapshot's vendors differ from the
 * cached ones. A connection's first snapshot (`resync`) always reads them: a
 * snapshot names only vendors, so a request that ended and a new one for the
 * same vendor while detached look unchanged, though its `requested_at` moved.
 */
export function reconcilePendingVendorTerms(qc: QueryClient, vendors: string[] | undefined, resync: boolean): void {
  if (resync) {
    void refreshPendingVendorTerms(qc);
    return;
  }
  if (!vendors) return;
  const cached = qc.getQueryData<PendingVendorTerms[]>(queryKeys.vendorTerms.pending());
  if (cached) {
    const known = new Set(cached.map((p) => p.vendor));
    if (known.size === vendors.length && vendors.every((v) => known.has(v))) return;
  }
  void refreshPendingVendorTerms(qc);
}

/** The key a pending request is known by: a later request for the same vendor is a new one. */
export const vendorTermsRequestKey = (p: { vendor: string; requested_at: string }): string =>
  `${p.vendor}:${p.requested_at}`;
